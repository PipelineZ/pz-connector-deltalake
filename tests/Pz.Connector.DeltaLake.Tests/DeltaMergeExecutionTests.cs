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
        AssertNarrowAsync(DeltaMergeSql.Build(DeltaTestTable.Schema, DeltaTestTable.Columns, Opts(["id"]), null));

    [Fact]
    public Task A_generated_merge_with_a_composite_key_stays_narrow() =>
        AssertNarrowAsync(DeltaMergeSql.Build(DeltaTestTable.Schema, DeltaTestTable.Columns,
            Opts(["id", "dt"]), null));

    [Theory]
    [InlineData("target.dt >= '2026-01-01'")]
    [InlineData("target.id IN (1, 2)")]
    [InlineData("NOT (target.amt IS NULL) AND target.amt <> -1")]
    [InlineData("target.dt LIKE '2026%'")]
    [InlineData("target.dt = 'a b' OR target.amt >= -1.5")]
    public Task An_accepted_merge_predicate_cannot_widen_the_on_clause(string predicate) =>
        AssertNarrowAsync(DeltaMergeSql.Build(DeltaTestTable.Schema, DeltaTestTable.Columns,
            Opts(["id"], mergePredicate: predicate), null));

    [Fact]
    public Task A_derived_partition_filter_cannot_widen_the_on_clause() =>
        AssertNarrowAsync(DeltaMergeSql.Build(
            DeltaTestTable.Schema, DeltaTestTable.Columns, Opts(["id", "dt"], ["dt"]),
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

    [Theory]
    [InlineData("a'b", "'a''b'", 1)]
    [InlineData("O'Brien", "'O''Brien'", 1)]
    [InlineData("a''b", "'a''''b'", 2)]
    [InlineData("a'''b", "'a''''''b'", 2)]
    public async Task A_doubled_quote_in_a_predicate_is_faithful_once_and_not_twice(
        string value, string literal, int expectedRows)
    {
        // The measurement the merge_predicate refusal was adopted on, pinned where a dialect change can
        // fail it. ONE doubled quote is faithful: the predicate names the row it was written to name
        // and the merge updates it in place. TWO in a row are not: the literal compares as a value with
        // a level of doubling removed, the row that should have matched is invisible, and a second copy
        // of the same key is inserted with no error.
        //
        // The statement is hand-built because Build now refuses every literal here, faithful or not --
        // which is exactly the trade this test records. If delta-rs ever stops dropping the level, the
        // 2s below become 1s, this test fails, and whoever reads it learns the refusal has outlived
        // its reason rather than finding an assertion about a version nobody pinned.
        var dir = Directory.CreateTempSubdirectory("pz-delta-merge-exec").FullName;
        try
        {
            var location = await DeltaTestTable.CreateLocalFromAsync(
                dir, [(0L, value, 0d)], ["dt"]);

            await MergeSourceAsync(
                location,
                $"MERGE INTO target USING source ON target.\"id\" = source.\"id\" AND (target.dt = {literal})\n" +
                "WHEN MATCHED THEN UPDATE SET target.\"dt\" = source.\"dt\", target.\"amt\" = source.\"amt\"\n" +
                "WHEN NOT MATCHED THEN INSERT (\"id\", \"dt\", \"amt\") VALUES " +
                "(source.\"id\", source.\"dt\", source.\"amt\")",
                DeltaTestTable.RowsWithAmounts([(0L, value, 42d)]));

            var after = await DeltaReader.RowsAsync(location);
            Assert.Equal(expectedRows, after.Count);
            if (expectedRows == 1)
            {
                Assert.Equal(42d, Assert.Single(after).Amt);
            }
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public async Task An_escaped_column_name_would_constrain_a_different_column_and_duplicate_the_row()
    {
        // Why a column name carrying a quote is refused rather than doubled, stated as a contrast. The
        // table holds two columns whose names differ by one level of doubling, and the merge is keyed
        // and partitioned on the second. The source carries that key column UNCHANGED, so a statement
        // that names the column the merge actually resolves it by matches the existing row and updates
        // it in place; the statement escaping produces names a column that resolves to the FIRST one,
        // so the ON clause compares the wrong values, the target row is invisible to the scan, and a
        // second row with the same key is inserted.
        //
        // The inserted row's own values are the second half of the proof: the column that should have
        // received the second source column's value holds the FIRST one's instead, because the INSERT
        // clause mis-resolved the same way.
        //
        // The name doubled TWICE is not a supported spelling and is not offered as a workaround — it is
        // here only because a contrast needs a control, and it is what the merge path's extra level of
        // unescaping happens to accept today. If that ever changes, this control stops matching and
        // this test fails, which is the right place for a reader to find out.
        const string Resolves = "\"a\"\"\"\"\"\"\"\"b\"";
        const string Escaped = "\"a\"\"\"\"b\"";

        var dir = Directory.CreateTempSubdirectory("pz-delta-merge-exec").FullName;
        try
        {
            Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
                () => DeltaMergeSql.Build(QuotedNameSchema, DeltaTestTable.ColumnsOf(QuotedNameSchema),
                    Opts(["id", "a\"\"b"], ["a\"\"b"]), null));

            var control = await CreateTwoQuotedColumnsAsync(dir, ["a\"\"b"], "control");
            await MergeQuotedAsync(control, QuotedMerge(Resolves), QuotedNameRow("ONE-NEW", "TWO"));

            var updated = Assert.Single(await ReadTwoQuotedColumnsAsync(control));
            Assert.Equal("ONE-NEW", updated.First);
            Assert.Equal("TWO", updated.Second);

            var mangled = await CreateTwoQuotedColumnsAsync(dir, ["a\"\"b"], "mangled");
            await MergeQuotedAsync(mangled, QuotedMerge(Escaped), QuotedNameRow("ONE-NEW", "TWO"));

            var rows = await ReadTwoQuotedColumnsAsync(mangled);
            Assert.Equal(2, rows.Count);
            var inserted = Assert.Single(rows, r => r.Second == "ONE-NEW");
            Assert.Equal("ONE-NEW", inserted.First);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>The statement shape a merge keyed and partitioned on the second column produces, with
    /// that column's name written as <paramref name="name"/> everywhere it appears.</summary>
    private static string QuotedMerge(string name) =>
        $"MERGE INTO target USING source ON target.\"id\" = source.\"id\" " +
        $"AND target.{name} = source.{name} AND target.{name} IN ('TWO')\n" +
        "WHEN MATCHED THEN UPDATE SET target.\"a\"\"b\" = source.\"a\"\"b\"\n" +
        $"WHEN NOT MATCHED THEN INSERT (\"id\", \"a\"\"b\", {name}) VALUES " +
        $"(source.\"id\", source.\"a\"\"b\", source.{name})";

    [Fact]
    public async Task An_escaped_column_name_would_leave_an_ordinary_column_stale_with_no_key_involved()
    {
        // The same mis-resolution where no key and no partition column is involved at all, which is why
        // the refusal covers every column the statement names rather than only the merge keys. The
        // UPDATE SET assigns the first column twice and never touches the second, so the row is updated,
        // the merge succeeds, and one column silently keeps its pre-merge value.
        var dir = Directory.CreateTempSubdirectory("pz-delta-merge-exec").FullName;
        try
        {
            var location = await CreateTwoQuotedColumnsAsync(dir, [], "stale");

            Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
                () => DeltaMergeSql.Build(QuotedNameSchema, DeltaTestTable.ColumnsOf(QuotedNameSchema),
                    Opts(["id"]), null));

            await MergeQuotedAsync(
                location,
                "MERGE INTO target USING source ON target.\"id\" = source.\"id\"\n" +
                "WHEN MATCHED THEN UPDATE SET target.\"a\"\"b\" = source.\"a\"\"b\", " +
                "target.\"a\"\"\"\"b\" = source.\"a\"\"\"\"b\"\n" +
                "WHEN NOT MATCHED THEN INSERT (\"id\", \"a\"\"b\", \"a\"\"\"\"b\") VALUES " +
                "(source.\"id\", source.\"a\"\"b\", source.\"a\"\"\"\"b\")",
                QuotedNameRow("ONE-NEW", "TWO-NEW"));

            var row = Assert.Single(await ReadTwoQuotedColumnsAsync(location));
            Assert.Equal("ONE-NEW", row.First);
            Assert.Equal("TWO", row.Second);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>Two columns whose names differ only by one level of quote doubling — the shape that
    /// makes a mis-resolved identifier land on a real column instead of failing loudly.</summary>
    private static readonly Schema QuotedNameSchema = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int64Type.Default).Nullable(false))
        .Field(f => f.Name("a\"b").DataType(Apache.Arrow.Types.StringType.Default).Nullable(false))
        .Field(f => f.Name("a\"\"b").DataType(Apache.Arrow.Types.StringType.Default).Nullable(false))
        .Build();

    private static RecordBatch QuotedNameRow(string first, string second)
    {
        var id = new Int64Array.Builder();
        var a = new StringArray.Builder();
        var b = new StringArray.Builder();
        id.Append(0);
        a.Append(first);
        b.Append(second);
        return new RecordBatch(QuotedNameSchema, [id.Build(), a.Build(), b.Build()], 1);
    }

    private static Task<string> CreateTwoQuotedColumnsAsync(
        string dir, string[] partitionBy, string name = "quoted") =>
        DeltaBigStack.RunAsync(async () =>
        {
            var location = Path.Combine(dir, name);
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.CreateTableAsync(
                new TableCreateOptions(location, QuotedNameSchema)
                { PartitionBy = partitionBy, SaveMode = SaveMode.ErrorIfExists },
                default);
            await table.InsertAsync(
                [QuotedNameRow("ONE", "TWO")], QuotedNameSchema,
                new InsertOptions { SaveMode = SaveMode.Append }, default);
            return location;
        });

    private static Task MergeQuotedAsync(string location, string sql, RecordBatch source) =>
        DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
            await table.MergeAsync(sql, [source], QuotedNameSchema, default);
            return 0;
        });

    private static Task<IReadOnlyList<(long Id, string First, string Second)>> ReadTwoQuotedColumnsAsync(
        string location) =>
        DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
            var rows = new List<(long, string, string)>();
            // Positional, not by name: naming the columns in this query would need the very quoting
            // this test exists to show is unsafe.
            var query = new SelectQuery("select * from tbl") { TableAlias = "tbl" };
            await foreach (var batch in table.QueryAsync(query, default))
            {
                using (batch)
                {
                    var byName = batch.Schema.FieldsList.Select((f, i) => (f.Name, i))
                        .ToDictionary(x => x.Name, x => x.i, StringComparer.Ordinal);
                    var id = (Int64Array)batch.Column(byName["id"]);
                    for (var i = 0; i < batch.Length; i++)
                    {
                        rows.Add((
                            id.GetValue(i) ?? 0,
                            DeltaReader.Text(batch.Column(byName["a\"b"]), i),
                            DeltaReader.Text(batch.Column(byName["a\"\"b"]), i)));
                    }
                }
            }

            return (IReadOnlyList<(long, string, string)>)rows;
        });

    private static Task MergeSourceAsync(string location, string sql, RecordBatch source) =>
        DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
            await table.MergeAsync(sql, [source], DeltaTestTable.Schema, default);
            return 0;
        });

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
