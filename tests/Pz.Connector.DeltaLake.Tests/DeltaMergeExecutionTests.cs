using Apache.Arrow;
using DeltaLake.Table;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>Runs the statement <see cref="DeltaMergeSql.Build"/> actually produces through real
/// delta-rs, against a real Delta table on local disk.
///
/// The rest of the merge-SQL suite asserts on the GUARD's verdict — whether a predicate was accepted
/// or refused — which only ever tests this connector's model of the SQL dialect against itself. The
/// defect these tests exist to catch is precisely a disagreement between that model and the dialect,
/// so it is invisible to any assertion phrased in terms of the model. Here the assertion is about
/// consequences: a merge whose ON clause still requires key equality adds the one source row and
/// leaves every existing row exactly as it was. Any predicate that widened the ON clause instead
/// rewrites existing rows and never inserts, which these assertions see.
///
/// Local disk, no docker, no network. Every delta-rs call goes through <see cref="DeltaBigStack"/>,
/// because delta-kernel-rs can exhaust a default .NET thread stack and take the test host down with
/// it — a risk a test shares with production code.</summary>
public class DeltaMergeExecutionTests
{
    /// <summary>A source row whose key matches nothing in the fixture table (ids 0-19). Its values are
    /// distinctive so a row that was rewritten with them is unmistakable.</summary>
    private const long AbsentKey = 1000;

    private static readonly (long Id, string Dt, double Amt) SourceRow = (AbsentKey, "2026-01-01", 500000d);

    private static DeltaWriteOptions Opts(
        string[] keys, string[]? partitionBy = null, string? mergePredicate = null) =>
        new("merge", keys, partitionBy ?? [], mergePredicate, 1024);

    [Fact]
    public Task A_generated_merge_inserts_the_absent_key_and_leaves_every_other_row_alone() =>
        AssertNarrowAsync(DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"]), null));

    [Fact]
    public Task A_generated_merge_with_a_composite_key_stays_narrow() =>
        AssertNarrowAsync(DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id", "dt"]), null));

    [Theory]
    [InlineData("target.dt >= '2026-01-01'")]
    [InlineData("target.id IN (1, 2)")]
    [InlineData("NOT (target.amt IS NULL) AND target.amt <> -1")]
    [InlineData("target.dt LIKE '2026%'")]
    [InlineData("target.dt = 'a b' OR target.amt >= -1.5")]
    public Task An_accepted_merge_predicate_cannot_widen_the_on_clause(string predicate) =>
        AssertNarrowAsync(DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: predicate), null));

    [Fact]
    public Task A_derived_partition_filter_cannot_widen_the_on_clause() =>
        AssertNarrowAsync(DeltaMergeSql.Build(
            DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]),
            [new PartitionFilter("dt", ["'2026-01-01'", "'2026-01-02'"])]));

    [Fact]
    public async Task The_assertion_has_teeth_because_a_widened_on_clause_fails_it()
    {
        // The same statement shape Build produces, with a payload the guard refuses spliced into the
        // ON clause by hand. Without this, "no row changed" could be passing because the harness
        // cannot see a change at all, and every assertion above would be worthless.
        const string Widened =
            "MERGE INTO target USING source ON target.\"id\" = source.\"id\" AND " +
            "(target.dt = $$($$ OR 1=1) OR (1=1 OR target.dt = $$)$$)\n" +
            "WHEN MATCHED THEN UPDATE SET target.\"dt\" = source.\"dt\", target.\"amt\" = source.\"amt\"\n" +
            "WHEN NOT MATCHED THEN INSERT (\"id\", \"dt\", \"amt\") VALUES " +
            "(source.\"id\", source.\"dt\", source.\"amt\")";

        var outcome = await MergeAsync(Widened);

        Assert.Equal(0, outcome.Added);
        Assert.Equal(20, outcome.Changed);
    }

    [Fact]
    public async Task The_assertion_also_sees_an_injection_that_no_row_counter_notices()
    {
        // A predicate that leaves a parenthesis OPEN reaches past the ON clause: the generated
        // statement's own trailing ')' closes a VALUES '(' the predicate opened, and the injected
        // WHEN NOT MATCHED clause then writes the added row instead of the generated one. Added,
        // Changed and Total all come out exactly as a correct merge would produce them — only the
        // VALUE of the added row differs, which is why AssertNarrowAsync compares it to its source.
        const string Injected =
            "MERGE INTO target USING source ON target.\"id\" = source.\"id\" AND " +
            "($$($$ IS NOT NULL AND 1=1) WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT " +
            "(\"id\", \"dt\", \"amt\") VALUES (source.\"id\", $$)$$ || source.\"dt\", source.\"amt\")\n" +
            "WHEN MATCHED THEN UPDATE SET target.\"dt\" = source.\"dt\", target.\"amt\" = source.\"amt\"\n" +
            "WHEN NOT MATCHED THEN INSERT (\"id\", \"dt\", \"amt\") VALUES " +
            "(source.\"id\", source.\"dt\", source.\"amt\")";

        var outcome = await MergeAsync(Injected);

        Assert.Equal(1, outcome.Added);
        Assert.Equal(0, outcome.Changed);
        Assert.Equal(21, outcome.Total);

        var added = Assert.Single(outcome.Rows, r => r.Id == AbsentKey);
        Assert.Equal(")" + SourceRow.Dt, added.Dt);
        Assert.NotEqual(SourceRow.Dt, added.Dt);
    }

    private static async Task AssertNarrowAsync(string sql)
    {
        var outcome = await MergeAsync(sql);

        Assert.Equal(1, outcome.Added);
        Assert.Equal(0, outcome.Changed);
        Assert.Equal(21, outcome.Total);

        // The added row's VALUES, not just its existence. A predicate that leaves a parenthesis open
        // lets the generated statement's own trailing ')' close a WHEN clause the predicate itself
        // opened, and the injected clause then writes the row instead of the generated one — a merge
        // that adds exactly one row, changes nothing else, and still corrupts what it wrote. Counting
        // rows cannot see that; comparing the row to its source can.
        var added = Assert.Single(outcome.Rows, r => r.Id == AbsentKey);
        Assert.Equal(SourceRow.Dt, added.Dt);
        Assert.Equal(SourceRow.Amt, added.Amt);
    }

    private static async Task<(int Added, int Changed, int Total, IReadOnlyList<(long Id, string Dt, double Amt)> Rows)> MergeAsync(string sql)
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-merge-exec").FullName;
        try
        {
            var location = await DeltaTestTable.CreateLocalAsync(dir, 20);
            var before = await DeltaReader.RowsAsync(location);

            await DeltaBigStack.RunAsync(async () =>
            {
                // No ConfigureAwait(false) on this delegate's own awaits: DeltaBigStack pumps plain
                // awaits back onto the big-stack thread, and opting out would run the merge itself on
                // a default-stack pool thread.
                using var engine = new DeltaEngine(EngineOptions.Default);
                var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
                try
                {
                    var source = DeltaTestTable.RowsWithAmounts([SourceRow]);
                    await table.MergeAsync(sql, [source], DeltaTestTable.Schema, default);
                }
                finally
                {
                    if (table is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }
                }

                return 0;
            });

            var after = await DeltaReader.RowsAsync(location);
            var byId = before.ToDictionary(r => r.Id);
            var changed = after.Count(r => byId.TryGetValue(r.Id, out var b) && (b.Dt != r.Dt || b.Amt != r.Amt));
            var added = after.Count(r => !byId.ContainsKey(r.Id));

            return (added, changed, after.Count, after);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }
}
