using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Table;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>Strategy 'merge' end to end, through the sink rather than through
/// <see cref="DeltaMergeSql"/> directly. Everything the two suites next door prove about the generated
/// statement and the derived predicate is only worth anything if a real write session actually runs it,
/// and this is the first task in which one does.
///
/// The assertions are about CONSEQUENCES, because that is the only way a merge defect is visible: a
/// duplicated row and a stale column both look exactly like a successful merge.
///
/// Reads go through <see cref="DeltaReader"/>, not <see cref="DuckDbReader"/>, so they run offline —
/// the one cross-engine assertion below is a SkippableFact for that reason and for that reason only.
/// "fail_on_change" is pz's own default schema_policy and the spelling an ordinary output carries;
/// "evolve" is the only other spelling this connector acts on.</summary>
public class MergeErrorTests
{
    private static ConnectorConfig Cfg(string root) => new(new Dictionary<string, object?> { ["root"] = root });

    private static OutputSpec Merge(string[] keys, params (string Key, object? Value)[] options) =>
        new("lake", "orders", "merge", "fail_on_change", options.ToDictionary(p => p.Key, p => p.Value))
        { Keys = keys };

    private static async Task<ISink> OpenSink(string root) =>
        await ((ISinkConnector)new DeltaLakeConnector()).OpenAsync(Cfg(root), default);

    private static string TempDir(string name) => Directory.CreateTempSubdirectory(name).FullName;

    [Fact]
    public async Task Merge_upserts_by_keys()
    {
        var dir = TempDir("pz-delta-merge");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);   // ids 0..3, amt = id
        await using var sink = await OpenSink(dir);

        await using (var s = await sink.BeginWriteAsync(Merge(["id"]), DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts(
                [(2, DeltaTestTable.Partition(2), 99.0), (7, DeltaTestTable.Partition(7), 7.0)]), default);
            await s.CommitAsync(default);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(5, rows.Count);
        Assert.Equal(99.0, rows.Single(r => r.Id == 2).Amt);
        Assert.Contains(rows, r => r.Id == 7);
    }

    [SkippableFact]
    public async Task A_merged_table_reads_the_same_through_duckdb_the_other_engine()
    {
        // A merge does not only add files: it marks the files it superseded as removed. An engine that
        // read the data files and ignored the removes would see BOTH copies of the updated row and call
        // it four rows plus one. Only a second engine can show that the log this connector wrote says
        // what it means, which is why this one assertion is worth a network gate.
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(), "DuckDB delta extension unavailable");

        var dir = TempDir("pz-delta-merge-duckdb");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);
        await using var sink = await OpenSink(dir);
        await using (var s = await sink.BeginWriteAsync(Merge(["id"]), DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts(
                [(2, DeltaTestTable.Partition(2), 99.0), (7, DeltaTestTable.Partition(7), 7.0)]), default);
            await s.CommitAsync(default);
        }

        var rows = await DuckDbReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(5, rows.Count);
        Assert.Equal(99.0, rows.Single(r => r.Id == 2).Amt);
    }

    [Fact]
    public async Task Merge_is_idempotent()
    {
        var dir = TempDir("pz-delta-idem");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);
        await using var sink = await OpenSink(dir);

        for (var i = 0; i < 2; i++)
        {
            // A fresh batch per pass: the session CLONES what it is given and disposes the clone on
            // commit, but reusing one instance across two sessions would still be the shape that hides
            // an ownership bug rather than exposing it.
            await using var s = await sink.BeginWriteAsync(Merge(["id"]), DeltaTestTable.Schema, default);
            await s.WriteBatchAsync(
                DeltaTestTable.RowsWithAmounts([(2, DeltaTestTable.Partition(2), 99.0)]), default);
            await s.CommitAsync(default);
        }

        Assert.Equal(4, await DeltaReader.CountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task Merge_into_an_empty_table_inserts_everything()
    {
        var dir = TempDir("pz-delta-mergeempty");
        await using var sink = await OpenSink(dir);
        await using (var s = await sink.BeginWriteAsync(Merge(["id"]), DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(DeltaTestTable.Rows(0, 10), default);
            await s.CommitAsync(default);
        }

        Assert.Equal(10, await DeltaReader.CountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task Merge_matching_zero_rows_succeeds()
    {
        var dir = TempDir("pz-delta-nomatch");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);
        await using var sink = await OpenSink(dir);
        await using var s = await sink.BeginWriteAsync(Merge(["id"]), DeltaTestTable.Schema, default);
        await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(9999, "2026-01-01", 1.0)]), default);
        var result = await s.CommitAsync(default);

        // RowsWritten counts what the pipeline handed over, not what the merge decided to do with it —
        // the sink cannot tell an update from an insert without asking the table again.
        Assert.Equal(1, result.RowsWritten);
        Assert.Equal(5, await DeltaReader.CountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task Merge_with_no_rows_at_all_commits_nothing()
    {
        // No batch was ever written, so there is no statement to build and no commit to make. The
        // transaction log must not grow: a commit that changed nothing is still a version every reader
        // has to walk past.
        var dir = TempDir("pz-delta-mergenone");
        var location = await DeltaTestTable.CreateLocalAsync(dir, rows: 4);
        var before = DeltaReader.CommitCount(location);
        await using var sink = await OpenSink(dir);
        await using (var s = await sink.BeginWriteAsync(Merge(["id"]), DeltaTestTable.Schema, default))
        {
            Assert.Equal(0, (await s.CommitAsync(default)).RowsWritten);
        }

        Assert.Equal(before, DeltaReader.CommitCount(location));
        Assert.Equal(4, await DeltaReader.CountAsync(location));
    }

    [Fact]
    public async Task Duplicate_source_keys_resolve_last_writer_wins_instead_of_failing_the_write()
    {
        // This used to be a refusal (PZDL0402, "deduplicate upstream"), and the refusal was only half
        // real: delta-rs raises that error when the repeated key ALREADY EXISTS in the target, and
        // says nothing at all when it does not -- the same input then committed two rows for one key,
        // silently. Resolving the repeat before the statement runs closes the silent half and makes
        // this connector agree with the merge contract the rest of the ecosystem implements: the LAST
        // row for a key is the one that lands. MergeDuplicateKeyTests carries the full shape.
        var dir = TempDir("pz-delta-dupkeys");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 10);
        await using var sink = await OpenSink(dir);

        await using (var s = await sink.BeginWriteAsync(Merge(["id"]), DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts(
                [(5, "2026-01-06", 1.0), (5, "2026-01-06", 2.0), (5, "2026-01-06", 3.0)]), default);
            await s.CommitAsync(default);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(10, rows.Count);
        Assert.Equal(3.0, rows.Single(r => r.Id == 5).Amt);
    }

    [Fact]
    public async Task A_merge_predicate_may_name_a_column_only_the_table_has()
    {
        // The seam that makes this work: a `target.`-qualified name resolves against the TABLE's schema,
        // a `source.`-qualified or bare one against the batch. Under schema_policy: evolve a table
        // legitimately carries nullable columns a given write does not produce, and narrowing the merge
        // by one of them is the ordinary reason to write a predicate at all. Resolving both sides
        // against the batch refused exactly that, with advice the author could not follow.
        //
        // Every other fixture in this suite has identical columns on both sides, so no other test in
        // this repository can see this.
        var dir = TempDir("pz-delta-targetonly");
        await using var sink = await OpenSink(dir);

        await using (var seed = await sink.BeginWriteAsync(
            new OutputSpec("lake", "orders", "append", "fail_on_change", new Dictionary<string, object?>()),
            WithArchived, default))
        {
            await seed.WriteBatchAsync(
                ArchivedRows([(1L, "2026-01-01", 1.0, null), (2L, "2026-01-02", 2.0, "yes")]), default);
            await seed.CommitAsync(default);
        }

        var spec = Merge(["id"], ("merge_predicate", "target.archived IS NULL")) with { SchemaPolicy = "evolve" };
        await using (var s = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(
                DeltaTestTable.RowsWithAmounts([(1, "2026-01-01", 99.0), (2, "2026-01-02", 99.0)]), default);
            await s.CommitAsync(default);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));

        // id=1 has archived IS NULL, so the predicate lets it match and it is updated. id=2 does not,
        // so it does not match and the merge INSERTS the incoming row beside it — three rows, and the
        // original id=2 keeps its old amount. That asymmetry is what proves the predicate ran rather
        // than being ignored.
        Assert.Equal(3, rows.Count);
        Assert.Equal(99.0, rows.Single(r => r.Id == 1).Amt);
        Assert.Contains(rows, r => r.Id == 2 && r.Amt == 2.0);
    }

    [Fact]
    public async Task A_merge_predicate_naming_a_table_column_on_the_source_side_is_still_refused()
    {
        // The discriminating half of the seam above: 'archived' is a column of the TABLE, not of this
        // write, so `source.archived` names nothing and must stay refused. A fix that resolved both
        // sides against the table would let this through and the merge would fail inside delta-rs.
        var dir = TempDir("pz-delta-sourceonly");
        await using var sink = await OpenSink(dir);
        await using (var seed = await sink.BeginWriteAsync(
            new OutputSpec("lake", "orders", "append", "fail_on_change", new Dictionary<string, object?>()),
            WithArchived, default))
        {
            await seed.WriteBatchAsync(ArchivedRows([(1L, "2026-01-01", 1.0, null)]), default);
            await seed.CommitAsync(default);
        }

        var spec = Merge(["id"], ("merge_predicate", "source.archived IS NULL")) with { SchemaPolicy = "evolve" };
        await using var s = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default);
        await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(1, "2026-01-01", 99.0)]), default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await s.CommitAsync(default));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Fact]
    public async Task A_predicate_the_lexical_guard_admits_but_the_parser_rejects_points_at_the_predicate()
    {
        // The allowlist in DeltaMergeSql is a lexical scan, not a parser, so a predicate built entirely
        // from permitted tokens can still be nonsense. When DataFusion says so, the next step has to
        // name the one fragment of the statement this connector did not write — the generic write
        // fallback tells the user to check the protocol version and the schema, neither of which has
        // anything to do with a statement that did not parse.
        var dir = TempDir("pz-delta-parsefail");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 2);
        await using var sink = await OpenSink(dir);

        await using var s = await sink.BeginWriteAsync(
            Merge(["id"], ("merge_predicate", "target.id = 1 IN ()")), DeltaTestTable.Schema, default);
        await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(1L, "2026-01-01", 9.0)]), default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await s.CommitAsync(default));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
        Assert.Contains("merge_predicate", ex.Message);
        Assert.DoesNotContain("protocol version", ex.Message);

        // The generated statement is not a diagnostic a user can act on and carries partition literals.
        Assert.DoesNotContain("WHEN MATCHED", ex.Message);
    }

    [Fact]
    public async Task A_null_merge_key_is_refused_rather_than_silently_duplicating_a_row()
    {
        // Measured against real delta-rs: the ON clause is target.k = source.k, equality against a null
        // is null and never true, so the row matches nothing, WHEN NOT MATCHED fires, and a key already
        // in the table gains a second copy — with no error and no row count out of place.
        var dir = TempDir("pz-delta-nullkey");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);
        await using var sink = await OpenSink(dir);

        await using var s = await sink.BeginWriteAsync(
            Merge(["dt"]), NullableDt, default);
        await s.WriteBatchAsync(NullableDtRows([(1L, null, 5.0)]), default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await s.CommitAsync(default));
        Assert.Contains(DeltaErrors.UnmatchableMergeKey, ex.Message);
        Assert.Contains("'dt'", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Theory]
    [InlineData("append")]
    [InlineData("replace")]
    public async Task An_empty_partition_value_is_refused_on_every_strategy(string strategy)
    {
        // The silent one, and the reason this guard is not merge-only. Measured against real delta-rs:
        // a NULLABLE partition column holding an empty value writes SUCCESSFULLY, lands in a directory
        // named 'dt=', and reads back as NULL — the user's value changed, on the plainest append there
        // is, with no error anywhere. Refused per batch, before anything is buffered, because an append
        // flushes bounded generations and a flushed generation is a commit that cannot be unwound.
        var dir = TempDir("pz-delta-emptypart-" + strategy);
        await using var sink = await OpenSink(dir);
        var spec = new OutputSpec("lake", "orders", strategy, "fail_on_change",
            new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "dt" } });

        await using var s = await sink.BeginWriteAsync(spec, NullableDt, default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await s.WriteBatchAsync(NullableDtRows([(1L, string.Empty, 1.0)]), default));
        Assert.Contains(DeltaErrors.UnusablePartitionValue, ex.Message);
        Assert.Contains("'dt'", ex.Message);
        Assert.Contains("null", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.False(ex.IsTransient);

        // Refused before the value could reach storage: nothing was written.
        Assert.Equal(0, await DeltaReader.CountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task An_empty_partition_value_is_refused_on_a_merge_whose_partition_column_is_not_a_key()
    {
        // The gap the merge-key guard cannot see: 'dt' is a partition column but NOT a key, so
        // PZDL0405 never looks at it. Without this refusal the write succeeds and the row's dt becomes
        // null.
        var dir = TempDir("pz-delta-emptypart-merge");
        await using var sink = await OpenSink(dir);
        var spec = Merge(["id"], ("partition_by", new List<object?> { "dt" }));

        await using var s = await sink.BeginWriteAsync(spec, NullableDt, default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await s.WriteBatchAsync(NullableDtRows([(1L, string.Empty, 5.0)]), default));
        Assert.Contains(DeltaErrors.UnusablePartitionValue, ex.Message);
        Assert.Contains("'dt'", ex.Message);
    }

    [Fact]
    public async Task An_empty_partition_value_is_refused_on_a_table_the_write_does_not_declare_partitions_for()
    {
        // partition_by is honoured only by table CREATION, so a run against a table an earlier run
        // partitioned need not declare it — and routinely does not. The guard's authority therefore has
        // to be the table's own partition columns; reading options.PartitionBy would leave every such
        // run unprotected.
        var dir = TempDir("pz-delta-emptypart-undeclared");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 2, partitionBy: ["dt"]);
        await using var sink = await OpenSink(dir);
        var spec = new OutputSpec("lake", "orders", "append", "fail_on_change", new Dictionary<string, object?>());

        await using var s = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(9L, string.Empty, 1.0)]), default));
        Assert.Contains(DeltaErrors.UnusablePartitionValue, ex.Message);
        Assert.Contains("'dt'", ex.Message);
    }

    [Fact]
    public async Task Every_offending_partition_column_is_named_in_one_refusal()
    {
        // Aggregate, never fail-one-at-a-time: a user fixing one column per run is a user running the
        // pipeline once per column.
        var dir = TempDir("pz-delta-emptypart-many");
        await using var sink = await OpenSink(dir);
        var spec = new OutputSpec("lake", "orders", "append", "fail_on_change",
            new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "dt", "region" } });

        await using var s = await sink.BeginWriteAsync(spec, TwoPartitions, default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await s.WriteBatchAsync(TwoPartitionRows(string.Empty, string.Empty), default));
        Assert.Contains(DeltaErrors.UnusablePartitionValue, ex.Message);
        Assert.Contains("'dt'", ex.Message);
        Assert.Contains("'region'", ex.Message);
    }

    [Fact]
    public async Task A_null_partition_value_is_not_mistaken_for_an_empty_one()
    {
        // Measured: GetValueLength reports 0 for a NULL entry in every one of these encodings, so an
        // unguarded length test would refuse a legitimately null partition value. A null written is a
        // null read back — nothing changes on the way through — so it is allowed, and this is what
        // keeps the guard from being a length test that happens to work.
        var dir = TempDir("pz-delta-nullpart");
        await using var sink = await OpenSink(dir);
        var spec = new OutputSpec("lake", "orders", "append", "fail_on_change",
            new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "dt" } });

        await using (var s = await sink.BeginWriteAsync(spec, NullableDt, default))
        {
            await s.WriteBatchAsync(NullableDtRows([(1L, null, 1.0), (2L, "x", 2.0)]), default);
            Assert.Equal(2, (await s.CommitAsync(default)).RowsWritten);
        }

        // RowCountAsync, not CountAsync: the shaped read cannot represent the null it just wrote.
        Assert.Equal(2, await DeltaReader.RowCountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task An_empty_binary_partition_value_is_refused_too()
    {
        // Not defensive breadth: measured. A nullable BINARY partition column holding zero bytes lands
        // in the same 'dt=' directory and reads back null exactly as an empty string does, so the guard
        // covers the binary encodings as well as the string ones.
        var dir = TempDir("pz-delta-emptybin");
        await using var sink = await OpenSink(dir);
        var spec = new OutputSpec("lake", "orders", "append", "fail_on_change",
            new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "dt" } });

        await using var s = await sink.BeginWriteAsync(spec, BinaryDt, default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await s.WriteBatchAsync(BinaryDtRows(), default));
        Assert.Contains(DeltaErrors.UnusablePartitionValue, ex.Message);
        Assert.Contains("'dt'", ex.Message);
    }

    [Theory]
    [InlineData("view")]
    [InlineData("large")]
    public async Task An_empty_partition_value_is_refused_in_every_string_encoding(string encoding)
    {
        // The guard claims to cover all six string and binary encodings, and the two above only reach
        // two of them. The route to the others is the one the guard's own doc comment describes: which
        // encoding a batch arrives in depends on the plan that produced it, NOT on the column's Delta
        // type — pz's hub is DuckDB, whose Arrow export produces string-view columns. So the SCHEMA
        // still says utf8, which is what Reconcile compares and what keeps the write legal, while the
        // ARRAY is a StringViewArray or a LargeStringArray. Measured: Apache.Arrow builds that batch.
        var dir = TempDir("pz-delta-emptyenc-" + encoding);
        await using var sink = await OpenSink(dir);
        var spec = new OutputSpec("lake", "orders", "append", "fail_on_change",
            new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "dt" } });

        await using var s = await sink.BeginWriteAsync(spec, NullableDt, default);

        IArrowArray dt = encoding == "view"
            ? new StringViewArray.Builder().Append(string.Empty).Build()
            : new LargeStringArray.Builder().Append(string.Empty).Build();
        var batch = new RecordBatch(NullableDt,
            [new Int64Array.Builder().Append(1L).Build(), dt, new DoubleArray.Builder().Append(1.0).Build()], 1);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await s.WriteBatchAsync(batch, default));
        Assert.Contains(DeltaErrors.UnusablePartitionValue, ex.Message);
        Assert.Contains("'dt'", ex.Message);
    }

    [Fact]
    public async Task An_empty_value_in_an_ordinary_column_is_left_alone()
    {
        // The discriminating control. Only a PARTITION value becomes a directory name, so only a
        // partition value can turn into a null on the way through. Refusing an empty string in an
        // ordinary column would refuse legitimate data.
        var dir = TempDir("pz-delta-emptyordinary");
        await using var sink = await OpenSink(dir);
        var spec = new OutputSpec("lake", "orders", "append", "fail_on_change", new Dictionary<string, object?>());

        await using (var s = await sink.BeginWriteAsync(spec, NullableDt, default))
        {
            await s.WriteBatchAsync(NullableDtRows([(1L, string.Empty, 1.0)]), default);
            Assert.Equal(1, (await s.CommitAsync(default)).RowsWritten);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(string.Empty, Assert.Single(rows).Dt);
    }

    [Fact]
    public async Task A_partition_value_too_long_for_the_filesystem_is_reported_without_the_value()
    {
        // A partition value becomes a path segment, and every common local filesystem caps one at 255
        // bytes. delta-rs reports it as a failure to open a file and puts the WHOLE PATH in the
        // message — which contains the value, and a partition value is user data.
        var value = new string('a', 300);
        var dir = TempDir("pz-delta-longpart");
        await using var sink = await OpenSink(dir);
        var spec = new OutputSpec("lake", "orders", "append", "fail_on_change",
            new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "dt" } });

        await using var s = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default);
        await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(1L, value, 1.0)]), default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await s.CommitAsync(default));
        Assert.Contains(DeltaErrors.UnusablePartitionValue, ex.Message);
        Assert.DoesNotContain(value, ex.Message);
        Assert.DoesNotContain(dir, ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task The_skip_reason_reaches_a_finished_session_and_names_no_value()
    {
        // The seam audit, run rather than trusted: LastSkipReason is the one of the two that is safe to
        // surface, because the deriver builds it from column names and character classes. Here it is on
        // a real session after a real merge, with a partition value chosen so that a deriver leaking one
        // would leak this.
        const string Secret = "zzsecretzz";
        var dir = TempDir("pz-delta-skipreason");
        await using var sink = await OpenSink(dir);
        var spec = Merge(["id"], ("partition_by", new List<object?> { "dt" }));

        var session = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default);
        await using (session)
        {
            await session.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(1L, Secret, 1.0)]), default);
            await session.CommitAsync(default);
        }

        var typed = Assert.IsType<DeltaWriteSession>(session);
        Assert.NotNull(typed.LastSkipReason);
        Assert.Contains("'dt'", typed.LastSkipReason);
        Assert.DoesNotContain(Secret, typed.LastSkipReason);

        // The other half of the seam: the statement DOES carry literals, which is exactly why nothing
        // in src/ may read it. Asserted here so the reason the two are treated differently is written
        // down where a reader of either will see it.
        Assert.NotNull(typed.LastMergeSql);
        Assert.Contains("MERGE INTO target", typed.LastMergeSql);
    }

    [Fact]
    public async Task Merge_under_evolve_widens_the_table_rather_than_dropping_the_column()
    {
        // The silent one this fix round exists for. Measured against the shipped library: delta-rs
        // ACCEPTS a merge statement naming a column the table does not have, commits it, updates and
        // inserts every row correctly — and DROPS the column. Green run, right row counts, missing data.
        // The same write on 'append' widens the table, so the two strategies disagreed about what
        // schema_policy: evolve means.
        var dir = TempDir("pz-delta-mergeevolve");
        await using var sink = await OpenSink(dir);

        await using (var seed = await sink.BeginWriteAsync(
            new OutputSpec("lake", "orders", "append", "fail_on_change", new Dictionary<string, object?>()),
            DeltaTestTable.Schema, default))
        {
            await seed.WriteBatchAsync(
                DeltaTestTable.RowsWithAmounts([(1L, "2026-01-01", 1.0), (2L, "2026-01-02", 2.0)]), default);
            await seed.CommitAsync(default);
        }

        var spec = Merge(["id"]) with { SchemaPolicy = "evolve" };
        await using (var s = await sink.BeginWriteAsync(spec, WithArchived, default))
        {
            await s.WriteBatchAsync(
                ArchivedRows([(1L, "2026-01-01", 9.0, "kept"), (3L, "2026-01-03", 3.0, "added")]), default);
            await s.CommitAsync(default);
        }

        var location = Path.Combine(dir, "orders");

        // The column exists — a row count alone would read 3 whether or not it survived.
        Assert.Contains("archived", await DeltaReader.ColumnsAsync(location));

        var rows = await ArchivedReadAsync(location);
        Assert.Equal(3, rows.Count);
        Assert.Equal("kept", rows.Single(r => r.Id == 1).Archived);
        Assert.Equal("added", rows.Single(r => r.Id == 3).Archived);

        // The row that predates the column reads null in it, which is the only honest value for it.
        Assert.Null(rows.Single(r => r.Id == 2).Archived);
    }

    [Fact]
    public async Task A_merge_predicate_may_name_a_column_this_write_is_about_to_add()
    {
        // The identifier seam after widening. `targetColumns` is snapshotted at BeginWriteAsync, before
        // the table has the column, so a `target.`-qualified name for a column this write ADDS resolves
        // only because the statement is built against the table's columns UNIONED with the write's.
        // With a stale list this is refused PZDL0107 before anything is written, so the seam cannot
        // regress silently — which it could until this test existed.
        var dir = TempDir("pz-delta-widenseam");
        await using var sink = await OpenSink(dir);
        await SeedAsync(sink);

        var spec = Merge(["id"], ("merge_predicate", "target.archived IS NULL")) with
        { SchemaPolicy = "evolve" };
        await using (var s = await sink.BeginWriteAsync(spec, WithArchived, default))
        {
            await s.WriteBatchAsync(ArchivedRows([(1L, "2026-01-01", 9.0, "kept")]), default);
            await s.CommitAsync(default);
        }

        var location = Path.Combine(dir, "orders");
        Assert.Contains("archived", await DeltaReader.ColumnsAsync(location));
        Assert.Contains(await ArchivedReadAsync(location), r => r.Id == 1 && r.Archived == "kept");
    }

    [Fact]
    public async Task A_refused_merge_predicate_leaves_the_table_exactly_as_it_found_it()
    {
        // A configuration error must not mutate shared state. Widening before the statement was
        // validated meant a run that never wrote a row still added a column and a commit to the table's
        // log — and the NEXT run of the same pipeline, under the default schema_policy, was then
        // refused for a column the failed run had left behind.
        var dir = TempDir("pz-delta-refusenowiden");
        await using var sink = await OpenSink(dir);
        await SeedAsync(sink);

        var location = Path.Combine(dir, "orders");
        var commitsBefore = DeltaReader.CommitCount(location);

        var spec = Merge(["id"], ("merge_predicate", "target.dt = 'a;b'")) with { SchemaPolicy = "evolve" };
        await using (var s = await sink.BeginWriteAsync(spec, WithArchived, default))
        {
            await s.WriteBatchAsync(ArchivedRows([(1L, "2026-01-01", 9.0, "kept")]), default);
            var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await s.CommitAsync(default));
            Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
        }

        Assert.DoesNotContain("archived", await DeltaReader.ColumnsAsync(location));
        Assert.Equal(commitsBefore, DeltaReader.CommitCount(location));
    }

    [Fact]
    public async Task Merge_without_evolve_still_refuses_a_column_the_table_lacks()
    {
        // The discriminating control: widening happens only because the user asked for it. Under the
        // default policy the refusal — and the advice to set schema_policy: evolve — must stand, and
        // that advice is now true for merge as well as for append.
        var dir = TempDir("pz-delta-mergenoevolve");
        await using var sink = await OpenSink(dir);
        await using (var seed = await sink.BeginWriteAsync(
            new OutputSpec("lake", "orders", "append", "fail_on_change", new Dictionary<string, object?>()),
            DeltaTestTable.Schema, default))
        {
            await seed.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(1L, "2026-01-01", 1.0)]), default);
            await seed.CommitAsync(default);
        }

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Merge(["id"]), WithArchived, default));
        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);
        Assert.Contains("archived", ex.Message);
    }

    [Theory]
    [InlineData("append")]
    [InlineData("merge")]
    public async Task Adding_a_not_null_column_under_evolve_is_refused_where_older_rows_survive(
        string strategy)
    {
        // The worst failure shape this connector has produced, and the reason the rule is pre-flight
        // rather than mapped from a write failure. Measured against the shipped library: on 'append',
        // adding a NOT NULL column to a table that already has rows COMMITS successfully and says
        // nothing — and every later read of that table fails with "Non-nullable column 'note' is
        // missing from the physical schema". Silent at write time, loud afterwards, in another process
        // belonging to another person, where no error of ours can reach them. Delta has no value to give
        // the column in rows committed before it existed, and none can be invented.
        var dir = TempDir("pz-delta-notnulladd-" + strategy);
        await using var sink = await OpenSink(dir);
        await SeedAsync(sink);

        var spec = Spec(strategy) with { SchemaPolicy = "evolve" };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, WithRequiredNote, default));

        // The interpolated phrase, not the bare strategy name: the shared next step names both "append"
        // and "merge" as boilerplate, so Assert.Contains(strategy, …) passes even when the message
        // never mentions which strategy was refused. Naming it is the whole point of scoping the rule
        // by strategy, and this is what pins it.
        Assert.Contains($"strategy '{strategy}'", ex.Message);

        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);
        Assert.Contains("'note'", ex.Message);
        Assert.Contains("NOT NULL", ex.Message);

        // Both real workarounds are named, and the refusal says plainly that it stands even on a table
        // that holds no rows — a user who reads "cannot be added" and then empties the table must not
        // be left thinking that will help.
        Assert.Contains("nullable", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("new table", ex.Message);
        Assert.Contains("no rows right now", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task Replace_may_add_a_not_null_column_because_no_older_row_survives_it()
    {
        // The control that keeps the rule honest, and it replaces an [InlineData("replace")] that had
        // pinned a false refusal. 'replace' is one SaveMode.Overwrite commit and that overwrite is
        // TOTAL, not per-partition — measured: on a table partitioned by 'dt' with rows in two
        // partitions, a replace writing only the first leaves the second's row GONE. So no row survives
        // that predates the added column, the reason the rule gives ("rows this write leaves in place")
        // does not apply, and the write commits and reads back with the value present.
        //
        // The table here is PARTITIONED and the write touches one partition on purpose: that is the
        // shape in which a per-partition overwrite would have left an older row behind and made this
        // the same defect append has.
        var dir = TempDir("pz-delta-replace-notnull");
        await using var sink = await OpenSink(dir);
        var partitioned = new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "dt" } };

        await using (var seed = await sink.BeginWriteAsync(
            new OutputSpec("lake", "orders", "append", "fail_on_change", partitioned),
            DeltaTestTable.Schema, default))
        {
            await seed.WriteBatchAsync(
                DeltaTestTable.RowsWithAmounts([(1L, "pA", 1.0), (2L, "pB", 2.0)]), default);
            await seed.CommitAsync(default);
        }

        await using (var s = await sink.BeginWriteAsync(
            new OutputSpec("lake", "orders", "replace", "evolve", partitioned), WithRequiredNote, default))
        {
            await s.WriteBatchAsync(RequiredNoteRows(), default);
            Assert.Equal(1, (await s.CommitAsync(default)).RowsWritten);
        }

        // Read the added column back with a real projection. A count(*) never touches the physical
        // schema, which is exactly why an unreadable table can look fine to one.
        var rows = await NoteReadAsync(Path.Combine(dir, "orders"));
        Assert.Equal(("n", 1L), (Assert.Single(rows).Note, Assert.Single(rows).Id));
    }

    [Fact]
    public async Task Replace_with_no_rows_leaves_the_schema_alone()
    {
        // The other edge of the exemption: a replace that writes nothing takes the DeleteAsync path,
        // which empties the table and changes no schema at all — measured. So exempting 'replace' does
        // not depend on a widening that never happens.
        var dir = TempDir("pz-delta-replace-empty");
        await using var sink = await OpenSink(dir);
        await SeedAsync(sink);

        await using (var s = await sink.BeginWriteAsync(
            new OutputSpec("lake", "orders", "replace", "evolve", new Dictionary<string, object?>()),
            WithRequiredNote, default))
        {
            Assert.Equal(0, (await s.CommitAsync(default)).RowsWritten);
        }

        var location = Path.Combine(dir, "orders");
        Assert.DoesNotContain("note", await DeltaReader.ColumnsAsync(location));
        Assert.Equal(0, await DeltaReader.RowCountAsync(location));
    }

    [Theory]
    [InlineData("append")]
    [InlineData("replace")]
    [InlineData("merge")]
    public async Task Adding_a_nullable_column_under_evolve_still_evolves_on_every_strategy(string strategy)
    {
        // The discriminating control. The rule refuses one thing only — a NOT NULL addition — and must
        // leave ordinary schema evolution working on every strategy, which is what a user writing
        // 'evolve' asked for.
        var dir = TempDir("pz-delta-nullableadd-" + strategy);
        await using var sink = await OpenSink(dir);
        await SeedAsync(sink);

        var spec = Spec(strategy) with { SchemaPolicy = "evolve" };
        await using (var s = await sink.BeginWriteAsync(spec, WithArchived, default))
        {
            await s.WriteBatchAsync(ArchivedRows([(1L, "2026-01-01", 9.0, "kept")]), default);
            await s.CommitAsync(default);
        }

        var location = Path.Combine(dir, "orders");
        Assert.Contains("archived", await DeltaReader.ColumnsAsync(location));

        // Contains, not Single: 'append' leaves the seed row beside the new one while 'replace' and
        // 'merge' leave one. What every strategy must agree on is that the added column carries its
        // value — a row count alone would pass even if the column had been dropped.
        Assert.Contains(await ArchivedReadAsync(location), r => r.Id == 1 && r.Archived == "kept");
    }

    [Theory]
    [InlineData("evolve")]
    [InlineData("fail_on_change")]
    public async Task A_column_differing_from_an_existing_one_only_in_case_is_refused_before_the_open(
        string policy)
    {
        // Delta cannot hold two columns whose names differ only in case. Under 'evolve' the Ordinal
        // lookup reads 'AMT' as a brand-new column and let it through, so the write was refused only at
        // commit — by delta-rs, as an uncoded failure whose next step named the protocol version, which
        // has nothing to do with two spellings of one name, and only after the table had been opened
        // and the whole pipeline buffered.
        var dir = TempDir("pz-delta-casecollide-" + policy);
        await using var sink = await OpenSink(dir);
        await SeedAsync(sink);

        var spec = Spec("append") with { SchemaPolicy = policy };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, ShoutingAmt, default));

        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);

        // The whole sentence, not its words. Both spellings appear in the message anyway once the
        // second loop reports 'amt' as a table column the write does not produce, and the shared next
        // step contains the word "case" as boilerplate — so asserting on those pieces would pass with
        // the refusal removed. Only the sentence that names one column AS the other's case variant
        // varies with this rule.
        Assert.Contains("column 'AMT' differs from the table's 'amt' only in case", ex.Message);
        Assert.DoesNotContain("protocol version", ex.Message);
    }

    [Fact]
    public async Task Every_not_null_column_the_write_adds_is_named_in_one_refusal()
    {
        // Aggregate, never fail-one-at-a-time: Reconcile already collects its problems, and the rule
        // joins that collection rather than short-circuiting it.
        var dir = TempDir("pz-delta-notnulladd-many");
        await using var sink = await OpenSink(dir);
        await SeedAsync(sink);

        var spec = Spec("append") with { SchemaPolicy = "evolve" };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, WithTwoRequired, default));
        Assert.Contains("'note'", ex.Message);
        Assert.Contains("'region'", ex.Message);
    }

    private static OutputSpec Spec(string strategy) =>
        strategy == "merge"
            ? Merge(["id"])
            : new OutputSpec("lake", "orders", strategy, "fail_on_change", new Dictionary<string, object?>());

    private static async Task SeedAsync(ISink sink)
    {
        await using var seed = await sink.BeginWriteAsync(
            new OutputSpec("lake", "orders", "append", "fail_on_change", new Dictionary<string, object?>()),
            DeltaTestTable.Schema, default);
        await seed.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(1L, "2026-01-01", 1.0)]), default);
        await seed.CommitAsync(default);
    }

    [Fact]
    public async Task A_nan_merge_key_is_refused_rather_than_silently_duplicating_a_row()
    {
        // NaN never equals itself, so target.k = source.k is false for it and WHEN NOT MATCHED fires.
        // Measured through the raw library: three identical merge passes of two rows, one keyed NaN,
        // leave FOUR rows — one new duplicate per run, forever, on a merge that reports success.
        var dir = TempDir("pz-delta-nankey");
        await using var sink = await OpenSink(dir);

        await using var s = await sink.BeginWriteAsync(Merge(["rate"]), RateKeyed, default);
        await s.WriteBatchAsync(RateRows(double.NaN, 1.5), default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await s.CommitAsync(default));
        Assert.Contains(DeltaErrors.UnmatchableMergeKey, ex.Message);
        Assert.Contains("'rate'", ex.Message);
        Assert.Contains("NaN", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task A_negative_zero_merge_key_is_allowed_because_it_matches()
    {
        // The discriminating control for the NaN check. -0.0 = 0.0 is TRUE, so such a row matches and
        // updates in place — measured, three identical passes leave two rows. Refusing it would refuse
        // a key that works.
        var dir = TempDir("pz-delta-negzero");
        await using var sink = await OpenSink(dir);
        for (var pass = 0; pass < 2; pass++)
        {
            await using var s = await sink.BeginWriteAsync(Merge(["rate"]), RateKeyed, default);
            await s.WriteBatchAsync(RateRows(-0.0, 1.5), default);
            await s.CommitAsync(default);
        }

        Assert.Equal(2, await DeltaReader.RowCountAsync(Path.Combine(dir, "orders")));
    }

    /// <summary>A table one nullable column wider than <see cref="DeltaTestTable.Schema"/>: the only
    /// shape in which the two sides of a merge have different columns, and therefore the only one that
    /// can tell a target-side name from a source-side one.</summary>
    private static readonly Schema WithArchived = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
        .Field(f => f.Name("archived").DataType(StringType.Default).Nullable(true))
        .Build();

    private static RecordBatch ArchivedRows(IReadOnlyList<(long Id, string Dt, double Amt, string? Archived)> rows)
    {
        var id = new Int64Array.Builder();
        var dt = new StringArray.Builder();
        var amt = new DoubleArray.Builder();
        var archived = new StringArray.Builder();
        foreach (var r in rows)
        {
            id.Append(r.Id);
            dt.Append(r.Dt);
            amt.Append(r.Amt);
            if (r.Archived is null) { archived.AppendNull(); } else { archived.Append(r.Archived); }
        }

        return new RecordBatch(WithArchived, [id.Build(), dt.Build(), amt.Build(), archived.Build()], rows.Count);
    }

    /// <summary>Reads the wide table back including the column a merge under 'evolve' added. The shaped
    /// reader next door carries three columns and cannot see a fourth, which is exactly the blind spot
    /// that let a dropped column look like a successful merge.</summary>
    private static Task<IReadOnlyList<(long Id, string? Archived)>> ArchivedReadAsync(string location) =>
        DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(
                new TableOptions { TableLocation = location }, default);
            try
            {
                var rows = new List<(long, string?)>();
                var query = new SelectQuery("select id, archived from tbl order by id")
                { TableAlias = "tbl" };
                await foreach (var batch in table.QueryAsync(query, default))
                {
                    using (batch)
                    {
                        var id = (Int64Array)batch.Column(0);
                        for (var i = 0; i < batch.Length; i++)
                        {
                            rows.Add((id.GetValue(i) ?? 0,
                                batch.Column(1).IsNull(i) ? null : DeltaReader.Text(batch.Column(1), i)));
                        }
                    }
                }

                return (IReadOnlyList<(long, string?)>)rows;
            }
            finally
            {
                if (table is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
        });

    /// <summary>A column the write adds that is NOT nullable — impossible to add to a table that
    /// already has rows, because those rows have no value for it.</summary>
    private static readonly Schema WithRequiredNote = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
        .Field(f => f.Name("note").DataType(StringType.Default).Nullable(false))
        .Build();

    private static RecordBatch RequiredNoteRows() =>
        new(WithRequiredNote,
            [
                new Int64Array.Builder().Append(1L).Build(),
                new StringArray.Builder().Append("pA").Build(),
                new DoubleArray.Builder().Append(9.0).Build(),
                new StringArray.Builder().Append("n").Build(),
            ],
            1);

    /// <summary>Projects the NOT NULL column a replace added. A row count would pass over a table whose
    /// physical schema cannot satisfy it, because count(*) never reads a column.</summary>
    private static Task<IReadOnlyList<(long Id, string Note)>> NoteReadAsync(string location) =>
        DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
            try
            {
                var rows = new List<(long, string)>();
                var query = new SelectQuery("select id, note from tbl order by id") { TableAlias = "tbl" };
                await foreach (var batch in table.QueryAsync(query, default))
                {
                    using (batch)
                    {
                        var id = (Int64Array)batch.Column(0);
                        for (var i = 0; i < batch.Length; i++)
                        {
                            rows.Add((id.GetValue(i) ?? 0, DeltaReader.Text(batch.Column(1), i)));
                        }
                    }
                }

                return (IReadOnlyList<(long, string)>)rows;
            }
            finally
            {
                if (table is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
        });

    /// <summary>'amt' spelled 'AMT': a column Delta cannot hold beside the one the table already
    /// has.</summary>
    private static readonly Schema ShoutingAmt = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("AMT").DataType(DoubleType.Default).Nullable(true))
        .Build();

    /// <summary>Two NOT NULL additions at once, so an aggregate refusal has more than one to name.</summary>
    private static readonly Schema WithTwoRequired = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
        .Field(f => f.Name("note").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("region").DataType(StringType.Default).Nullable(false))
        .Build();

    /// <summary>A double-keyed table, so a key can carry NaN.</summary>
    private static readonly Schema RateKeyed = new Schema.Builder()
        .Field(f => f.Name("rate").DataType(DoubleType.Default).Nullable(false))
        .Field(f => f.Name("label").DataType(StringType.Default).Nullable(true))
        .Build();

    private static RecordBatch RateRows(params double[] rates)
    {
        var rate = new DoubleArray.Builder();
        var label = new StringArray.Builder();
        foreach (var r in rates)
        {
            rate.Append(r);
            label.Append("x");
        }

        return new RecordBatch(RateKeyed, [rate.Build(), label.Build()], rates.Length);
    }

    /// <summary>Two partition columns, so an aggregate refusal has more than one thing to name.</summary>
    private static readonly Schema TwoPartitions = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(true))
        .Field(f => f.Name("region").DataType(StringType.Default).Nullable(true))
        .Build();

    private static RecordBatch TwoPartitionRows(string dt, string region) =>
        new(TwoPartitions,
            [
                new Int64Array.Builder().Append(1L).Build(),
                new StringArray.Builder().Append(dt).Build(),
                new StringArray.Builder().Append(region).Build(),
            ],
            1);

    /// <summary>A BINARY partition column: the same empty-value defect as a string one, measured.</summary>
    private static readonly Schema BinaryDt = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("dt").DataType(BinaryType.Default).Nullable(true))
        .Build();

    private static RecordBatch BinaryDtRows()
    {
        var dt = new BinaryArray.Builder();
        dt.Append(ReadOnlySpan<byte>.Empty);
        return new RecordBatch(BinaryDt, [new Int64Array.Builder().Append(1L).Build(), dt.Build()], 1);
    }

    /// <summary><see cref="DeltaTestTable.Schema"/> with 'dt' nullable, so a null can reach a key.</summary>
    private static readonly Schema NullableDt = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(true))
        .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
        .Build();

    private static RecordBatch NullableDtRows(IReadOnlyList<(long Id, string? Dt, double Amt)> rows)
    {
        var id = new Int64Array.Builder();
        var dt = new StringArray.Builder();
        var amt = new DoubleArray.Builder();
        foreach (var r in rows)
        {
            id.Append(r.Id);
            if (r.Dt is null) { dt.AppendNull(); } else { dt.Append(r.Dt); }
            amt.Append(r.Amt);
        }

        return new RecordBatch(NullableDt, [id.Build(), dt.Build(), amt.Build()], rows.Count);
    }
}
