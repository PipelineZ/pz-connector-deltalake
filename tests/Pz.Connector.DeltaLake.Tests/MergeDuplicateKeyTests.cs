using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>A merge whose own input names the same key twice. Nothing upstream promises it will not:
/// a pipeline's SELECT is not deduplicated, and the engine hands a session whatever batches the
/// pipeline produced, so two rows for one key inside a single session is an ordinary input shape
/// rather than an abuse.
///
/// A raw Delta MERGE cannot express what should happen to it. The ON clause matches the TARGET against
/// the source, so two source rows for a key the target does not hold both fall to WHEN NOT MATCHED and
/// both are INSERTED — one commit, no error, and a table that now holds a duplicate of a key the
/// output declared unique. That is the same silent duplication the null and NaN key refusals exist to
/// prevent, arriving by a different route.
///
/// The resolution is last-writer-wins — the LATER row for a key is the one that lands — which is the
/// merge contract the rest of the ecosystem already implements: pz's reference InMemorySink absorbs a
/// repeated key, its postgres sink takes DISTINCT ON per key ordered by arrival, and its sql server
/// sink carries an identity ordinal for the same tiebreak. This connector resolves it in Arrow instead
/// of in SQL, because Delta's MERGE source is a batch collection with no arrival-order column to sort
/// on.
///
/// Assertions are about consequences, not about the resolver's own bookkeeping: a duplicated row and a
/// correct merge look identical to a row count taken on the wrong side of the commit.</summary>
public class MergeDuplicateKeyTests
{
    private static ConnectorConfig Cfg(string root) => new(new Dictionary<string, object?> { ["root"] = root });

    private static OutputSpec Merge(params string[] keys) =>
        new("lake", "orders", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = keys };

    private static async Task<ISink> OpenSink(string root) =>
        await ((ISinkConnector)new DeltaLakeConnector()).OpenAsync(Cfg(root), default);

    [Fact]
    public async Task A_key_repeated_across_two_batches_lands_once_with_the_later_value()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-dup-across").FullName;
        await DeltaTestTable.CreateLocalAsync(dir, rows: 0);
        await using var sink = await OpenSink(dir);

        await using (var s = await sink.BeginWriteAsync(Merge("id"), DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(1, "2026-01-01", 1d)]), default);
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(1, "2026-01-01", 2d)]), default);
            await s.CommitAsync(default);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(2d, Assert.Single(rows).Amt);
    }

    [Fact]
    public async Task A_key_repeated_inside_one_batch_lands_once_with_the_later_value()
    {
        // The across-batches case alone would be satisfied by merging each batch separately; this one
        // would not, which is why both are here.
        var dir = Directory.CreateTempSubdirectory("pz-delta-dup-within").FullName;
        await DeltaTestTable.CreateLocalAsync(dir, rows: 0);
        await using var sink = await OpenSink(dir);

        await using (var s = await sink.BeginWriteAsync(Merge("id"), DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts(
                [(1, "2026-01-01", 1d), (2, "2026-01-01", 20d), (1, "2026-01-01", 2d)]), default);
            await s.CommitAsync(default);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(2, rows.Count);
        Assert.Equal(2d, rows.Single(r => r.Id == 1).Amt);
        Assert.Equal(20d, rows.Single(r => r.Id == 2).Amt);
    }

    [Fact]
    public async Task A_repeated_key_the_target_already_holds_updates_it_once_to_the_later_value()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-dup-existing").FullName;
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);   // ids 0..3, amt = id
        await using var sink = await OpenSink(dir);

        await using (var s = await sink.BeginWriteAsync(Merge("id"), DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts(
                [(2, DeltaTestTable.Partition(2), 98d)]), default);
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts(
                [(2, DeltaTestTable.Partition(2), 99d)]), default);
            await s.CommitAsync(default);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(4, rows.Count);
        Assert.Equal(99d, rows.Single(r => r.Id == 2).Amt);
    }

    [Fact]
    public async Task A_composite_key_repeats_only_when_every_part_repeats()
    {
        // Same 'id', different 'dt': under keys [id, dt] these are two DIFFERENT keys and both must
        // survive. A resolver keyed on the first column alone would drop one of them, which is data
        // loss rather than deduplication — the failure mode in the opposite direction.
        var dir = Directory.CreateTempSubdirectory("pz-delta-dup-composite").FullName;
        await DeltaTestTable.CreateLocalAsync(dir, rows: 0);
        await using var sink = await OpenSink(dir);

        await using (var s = await sink.BeginWriteAsync(Merge("id", "dt"), DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts(
                [(1, "2026-01-01", 1d), (1, "2026-01-02", 10d), (1, "2026-01-01", 2d)]), default);
            await s.CommitAsync(default);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(2, rows.Count);
        Assert.Equal(2d, rows.Single(r => r.Dt == "2026-01-01").Amt);
        Assert.Equal(10d, rows.Single(r => r.Dt == "2026-01-02").Amt);
    }

    /// <summary>The documented limit, pinned so it cannot go stale silently: a duplicate ALREADY in
    /// the table is neither detected nor repaired. The resolver reads the write's own buffer, not the
    /// target, so two rows the table already holds for one key are outside its reach — the merge
    /// updates every copy and leaves them all there, commits, and reports success.
    ///
    /// Not a defect this connector can fix at a price worth paying: detecting it means reading the
    /// whole target on every merge, which costs more than the operation it would protect. It is
    /// recorded in docs/reference/write.md as a limit, and asserted here so a delta-rs release that
    /// starts refusing it makes this test fail rather than leaving the documentation wrong.</summary>
    [Fact]
    public async Task A_duplicate_already_in_the_table_survives_the_merge_and_is_not_reported()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-dup-intable").FullName;
        await DeltaTestTable.CreateLocalAsync(dir, rows: 0);
        await using var sink = await OpenSink(dir);

        // An append does not resolve keys -- it is how a table comes to hold two rows for one key in
        // the first place.
        var append = new OutputSpec("lake", "orders", "append", "fail_on_change", new Dictionary<string, object?>());
        await using (var s = await sink.BeginWriteAsync(append, DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts(
                [(5, "2026-01-06", 1d), (5, "2026-01-06", 2d)]), default);
            await s.CommitAsync(default);
        }

        await using (var s = await sink.BeginWriteAsync(Merge("id"), DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(5, "2026-01-06", 9d)]), default);
            await s.CommitAsync(default);
        }

        // Both copies updated, both still there, and no error was raised on the way.
        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(5, r.Id));
        Assert.All(rows, r => Assert.Equal(9d, r.Amt));
    }

    [Fact]
    public async Task A_merge_key_the_resolver_cannot_compare_is_refused_before_the_table_is_touched()
    {
        // A list column is WRITABLE (DeltaTypeSupport accepts it) but not comparable row to row, so the
        // resolver could not tell a repeat from two distinct keys. Skipping the resolution for it would
        // reinstate the silent duplicate; refusing says so instead.
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(new ListType(Int64Type.Default)).Nullable(false))
            .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
            .Build();

        var dir = Directory.CreateTempSubdirectory("pz-delta-dup-badkey").FullName;
        await using var sink = await OpenSink(dir);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Merge("id"), schema, default));

        Assert.Contains(DeltaErrors.UnresolvableMergeKeyType, ex.Message);
        Assert.Contains("'id'", ex.Message);
        Assert.False(ex.IsTransient);

        // Refused before anything opened or created a table: a configuration error must not leave a
        // table behind that the next run then has to reconcile against.
        Assert.False(Directory.Exists(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task An_input_with_no_repeated_key_reaches_the_merge_whole()
    {
        // The common case has to stay a pass-through: every row of a duplicate-free input must land,
        // in one commit, exactly as it did before a resolver stood between the buffer and delta-rs.
        var dir = Directory.CreateTempSubdirectory("pz-delta-dup-none").FullName;
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);
        await using var sink = await OpenSink(dir);

        await using (var s = await sink.BeginWriteAsync(Merge("id"), DeltaTestTable.Schema, default))
        {
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts(
                [(2, DeltaTestTable.Partition(2), 99d), (7, DeltaTestTable.Partition(7), 7d)]), default);
            await s.WriteBatchAsync(DeltaTestTable.RowsWithAmounts(
                [(8, DeltaTestTable.Partition(8), 8d)]), default);
            await s.CommitAsync(default);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(6, rows.Count);
        Assert.Equal(99d, rows.Single(r => r.Id == 2).Amt);
        Assert.Equal(7d, rows.Single(r => r.Id == 7).Amt);
        Assert.Equal(8d, rows.Single(r => r.Id == 8).Amt);
    }
}
