using Apache.Arrow;
using Apache.Arrow.Types;
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
    public async Task Duplicate_source_keys_produce_the_mapped_pz_error_not_a_raw_datafusion_string()
    {
        var dir = TempDir("pz-delta-dupkeys");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 10);
        await using var sink = await OpenSink(dir);

        await using var s = await sink.BeginWriteAsync(Merge(["id"]), DeltaTestTable.Schema, default);
        await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts(
            [(5, "2026-01-06", 1.0), (5, "2026-01-06", 2.0), (5, "2026-01-06", 3.0)]), default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await s.CommitAsync(default));
        Assert.Contains(DeltaErrors.DuplicateMergeKeys, ex.Message);
        Assert.Contains("id", ex.Message);
        Assert.Contains("deduplicate", ex.Message, StringComparison.OrdinalIgnoreCase);

        // The generated statement is not a diagnostic the user can act on, and it carries partition
        // literals, which are their data.
        Assert.DoesNotContain("WHEN MATCHED", ex.Message);
        Assert.False(ex.IsTransient);
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
