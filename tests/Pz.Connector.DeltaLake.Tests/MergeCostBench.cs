using System.Diagnostics;
using Apache.Arrow;
using DeltaLake.Interfaces;
using DeltaLake.Table;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>What a merge costs, and — measured rather than assumed — what actually makes it cheap.
///
/// Three timings against one table, differing only in the statement:
/// <list type="bullet">
/// <item><description>UNPRUNED: the partition column is not in the ON clause and no IN list narrows the
/// target. This is the cost of scanning the whole table.</description></item>
/// <item><description>PARTITION-JOINED: the partition column IS in the ON clause, no IN list. This is
/// the shape strategy 'merge' generates whenever partition_by is a subset of keys.</description></item>
/// <item><description>DERIVED: partition-joined plus the IN list <see cref="DeltaPartitionPredicate"/>
/// produces.</description></item>
/// </list>
///
/// The result, on delta-rs 0.33.0 (see docs/reference/write.md for the table): unpruned costs 19–38x
/// partition-joined, and DERIVED IS INDISTINGUISHABLE FROM PARTITION-JOINED. delta-rs builds its own
/// early filter from the source's partition values when the partition column is joined, which is
/// exactly and only the case in which deriving one is sound — so the connector's IN list is a hedge
/// against that optimization going away, not the thing that delivers the speed. A long list costs a
/// little: 200 literals ran about 17% slower than none, because it is parsed and planned and prunes
/// nothing new.
///
/// The fixture's shape is the measurement. Three rules, each learned by getting it wrong first:
///
/// MANY PARTITIONS, FEW TOUCHED. Pruning can only pay when the write touches a small share of them.
/// <see cref="DeltaTestTable.Partition"/> cycles through eight values, so any realistic source batch
/// touches all eight and the predicate names the whole table — a fixture in which nothing can appear.
/// This one uses 200 partitions and confines the source to five.
///
/// KEYS SCATTERED ACROSS FILES. Contiguous keys let min/max file statistics prune almost perfectly on
/// their own, which is a best case nobody has. Here a partition holds every 200th id, so no file's id
/// range is narrow.
///
/// THE FIRST TIMING IS DISCARDED. The first delta-rs call in a process pays one-time initialization
/// that swamps everything after it.
///
/// The whole run is ONE <see cref="DeltaBigStack"/> delegate, and that is load-bearing rather than
/// tidy: a table does NOT outlive the engine that made it. Creating the table under one
/// <c>using var engine</c> and merging against it under another kills the process outright — a native
/// crash with no managed exception, which the test host reports as an aborted run, not a failure.</summary>
internal static class MergeCostBench
{
    private const int Partitions = 200;

    /// <summary>How many partitions the source batch is confined to.</summary>
    private const int TouchedPartitions = 5;

    internal readonly record struct Result(
        long Unpruned, long PartitionJoined, long Derived, int Literals, long DeriveMs);

    /// <summary>Partition of an id: 200 of them, and consecutive ids land in different ones.</summary>
    public static string Partition(long id) => $"p{id % Partitions:D3}";

    public static Task<Result> RunAsync(int tableRows, int sourceRows) =>
        DeltaBigStack.RunAsync(async () =>
        {
            // No ConfigureAwait(false) anywhere inside this delegate: DeltaBigStack pumps plain awaits
            // back onto the big-stack thread, and opting out would run everything after the first await
            // on a default-stack pool thread.
            var location = Path.Combine(Directory.CreateTempSubdirectory("pz-delta-bench").FullName, "bench");
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.CreateTableAsync(
                new TableCreateOptions(location, DeltaTestTable.Schema)
                { PartitionBy = ["dt"], SaveMode = SaveMode.ErrorIfExists }, default);

            try
            {
                for (var offset = 0; offset < tableRows; offset += 100_000)
                {
                    var count = Math.Min(100_000, tableRows - offset);
                    using var chunk = Rows(Enumerable.Range(offset, count).Select(i => (long)i).ToArray());
                    await table.InsertAsync(
                        [chunk], DeltaTestTable.Schema, new InsertOptions { SaveMode = SaveMode.Append }, default);
                }

                // Fixed seed: two runs of a benchmark that cannot be compared to each other are not a
                // regression test, and Random() without one is not reproducible.
                var rnd = new Random(42);
                var perPartition = Math.Max(1, tableRows / Partitions);
                var ids = Enumerable.Range(0, sourceRows)
                    .Select(_ => (long)((rnd.Next(perPartition) * Partitions) + rnd.Next(TouchedPartitions)))
                    .Distinct()
                    .Order()
                    .ToArray();
                using var batch = Rows(ids);

                // partition_by a subset of keys is the only shape in which deriving is sound, so it is
                // the only shape worth measuring the derivation in.
                var joined = new DeltaWriteOptions("merge", ["id", "dt"], ["dt"], null, 1024);

                var sw = Stopwatch.StartNew();
                var outcome = DeltaPartitionPredicate.Derive([batch], joined);
                var deriveMs = sw.ElapsedMilliseconds;
                if (outcome.Filters is null)
                {
                    throw new InvalidOperationException(
                        "the benchmark's own fixture blocked the derivation: " + outcome.SkipReason);
                }

                var columns = DeltaTestTable.Columns;
                var unprunedSql = DeltaMergeSql.Build(
                    DeltaTestTable.Schema, columns, new DeltaWriteOptions("merge", ["id"], ["dt"], null, 1024), null);
                var joinedSql = DeltaMergeSql.Build(DeltaTestTable.Schema, columns, joined, null);
                var derivedSql = DeltaMergeSql.Build(DeltaTestTable.Schema, columns, joined, outcome.Filters);

                await Time(table, joinedSql, batch);   // discarded: one-time initialization
                var joinedMs = await Time(table, joinedSql, batch);
                var derivedMs = await Time(table, derivedSql, batch);
                var unprunedMs = await Time(table, unprunedSql, batch);
                return new Result(unprunedMs, joinedMs, derivedMs, outcome.Filters[0].Literals.Count, deriveMs);
            }
            finally
            {
                if (table is IDisposable d)
                {
                    d.Dispose();
                }
            }
        });

    private static RecordBatch Rows(IReadOnlyList<long> ids)
    {
        var id = new Int64Array.Builder();
        var dt = new StringArray.Builder();
        var amt = new DoubleArray.Builder();
        foreach (var value in ids)
        {
            id.Append(value);
            dt.Append(Partition(value));
            amt.Append(value);
        }

        return new RecordBatch(DeltaTestTable.Schema, [id.Build(), dt.Build(), amt.Build()], ids.Count);
    }

    private static async Task<long> Time(ITable table, string sql, RecordBatch batch)
    {
        var sw = Stopwatch.StartNew();
        await table.MergeAsync(sql, [batch], DeltaTestTable.Schema, default);
        return sw.ElapsedMilliseconds;
    }
}
