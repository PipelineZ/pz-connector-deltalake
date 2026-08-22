using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Table;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>What delta-rs actually STORES for an Arrow type it accepts, and the first write's right to
/// succeed against the table it has just created.
///
/// A Delta table does not necessarily come back shaped like the Arrow schema that made it: an unsigned
/// integer comes back signed, every string encoding comes back utf8, a dictionary comes back as its
/// value type. A reconcile comparing the table against the schema REQUESTED therefore refused the very
/// first write to the table it had itself created, for ten Arrow types, and left an empty table behind
/// for every later run to fail against.
///
/// The pinning theory below is the guard against that returning: it creates a real table from every
/// writable candidate DeltaTypeSupportTests carries, reads the schema back, and holds
/// DeltaArrowTypes.Stored to it. A delta-rs that starts normalising something else fails the build
/// rather than reaching a user as a refusal they cannot act on.</summary>
public class DeltaStoredTypeTests
{
    /// <summary>The writable half of DeltaTypeSupportTests' candidate set. Filtered rather than skipped
    /// inside the test, so every case that runs asserts something — a refused type never reaches a
    /// create and has nothing to observe.</summary>
    public static TheoryData<string, IArrowType> WritableCandidates()
    {
        var data = new TheoryData<string, IArrowType>();
        foreach (var row in DeltaTypeSupportTests.Candidates())
        {
            var label = (string)row[0]!;
            var type = (IArrowType)row[1]!;
            if (DeltaTypeSupport.IsWritable(type))
            {
                data.Add(label, type);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(WritableCandidates))]
    public async Task Stored_says_what_delta_rs_puts_in_the_table(string label, IArrowType type)
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("k").DataType(Int64Type.Default).Nullable(true))
            .Field(f => f.Name("v").DataType(type).Nullable(true))
            .Build();

        var dir = Directory.CreateTempSubdirectory($"pz-delta-stored-{label}").FullName;
        try
        {
            var location = Path.Combine(dir, "orders");
            await DeltaBigStack.RunAsync(async () =>
            {
                using var engine = new DeltaEngine(EngineOptions.Default);
                await engine.CreateTableAsync(
                    new TableCreateOptions(location, schema) { SaveMode = SaveMode.ErrorIfExists }, default);
            });

            var actual = (await DeltaReader.ArrowSchemaAsync(location)).FieldsList[1].DataType;
            Assert.Equal(DeltaArrowTypes.Describe(actual), DeltaArrowTypes.Describe(DeltaArrowTypes.Stored(type)));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>The ten types delta-rs normalises on the way in, driven through the REAL sink twice.
    ///
    /// Once to prove the create's own first write is not refused — the defect left an empty table and a
    /// PZDL0301 describing a table shape no earlier run had chosen. Once more, in a second session, to
    /// prove the load path agrees: a fix that only skipped the reconcile on a create would leave every
    /// later run failing exactly as before.</summary>
    public static TheoryData<string, IArrowType> NormalisedByDeltaRs() => new()
    {
        { "uint8", UInt8Type.Default },
        { "uint16", UInt16Type.Default },
        { "uint32", UInt32Type.Default },
        { "uint64", UInt64Type.Default },
        { "date64", Date64Type.Default },
        { "stringview", StringViewType.Default },
        { "largestring", LargeStringType.Default },
        { "binaryview", BinaryViewType.Default },
        { "largebinary", LargeBinaryType.Default },
        { "fixedsizebinary_4", new FixedSizeBinaryType(4) },
        { "dictionary_int32_string", new DictionaryType(Int32Type.Default, StringType.Default, false) },
        // Not one of the ten, and here because the same comparison decides it: Delta stores one
        // timestamp precision, so a nanosecond column is stored as microseconds.
        { "timestamp_ns_utc", new TimestampType(TimeUnit.Nanosecond, "UTC") },
    };

    [Theory]
    [MemberData(nameof(NormalisedByDeltaRs))]
    public async Task A_type_delta_rs_normalises_writes_on_the_first_run_and_on_the_second(
        string label, IArrowType type)
    {
        var dir = Directory.CreateTempSubdirectory($"pz-delta-norm-{label}").FullName;
        var schema = new Schema.Builder()
            .Field(f => f.Name("k").DataType(Int64Type.Default).Nullable(true))
            .Field(f => f.Name("v").DataType(type).Nullable(true))
            .Build();

        await using var sink = await ((ISinkConnector)new DeltaLakeConnector())
            .OpenAsync(new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dir }), default);
        var spec = new OutputSpec("lake", "orders", "append", "fail_on_change", new Dictionary<string, object?>());

        for (var run = 0; run < 2; run++)
        {
            await using var session = await sink.BeginWriteAsync(spec, schema, default);
            var k = new Int64Array.Builder();
            k.Append(run);
            await session.WriteBatchAsync(
                new RecordBatch(schema, [k.Build(), DeltaTypeSupportTests.SingleValueArrayFor(type)], 1), default);
            await session.CommitAsync(default);
        }

        Assert.Equal(2, await DeltaReader.RowCountAsync(Path.Combine(dir, "orders")));
    }

    /// <summary>A MERGE keyed on a type delta-rs normalises, through the real sink, twice.
    ///
    /// The append path above proves the reconcile; this proves what the reconcile now lets through
    /// actually works — the generated statement, the duplicate-key resolver and delta-rs's own ON
    /// clause all meet a source column whose Arrow type is not the one the table stores. While the
    /// reconcile compared the offered type, no merge on any of these key types could run at all, and
    /// the resolver arms written for them answered for nothing.</summary>
    [Theory]
    [InlineData("uint32")]
    [InlineData("largestring")]
    public async Task A_merge_keyed_on_a_normalised_type_upserts_rather_than_duplicating(string label)
    {
        var dir = Directory.CreateTempSubdirectory($"pz-delta-normmerge-{label}").FullName;
        IArrowType type = label == "uint32" ? UInt32Type.Default : LargeStringType.Default;
        var schema = new Schema.Builder()
            .Field(f => f.Name("k").DataType(type).Nullable(false))
            .Field(f => f.Name("v").DataType(Int64Type.Default).Nullable(true))
            .Build();

        await using var sink = await ((ISinkConnector)new DeltaLakeConnector())
            .OpenAsync(new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dir }), default);
        var spec = new OutputSpec(
            "lake", "orders", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["k"] };

        for (var run = 0; run < 2; run++)
        {
            await using var session = await sink.BeginWriteAsync(spec, schema, default);
            IArrowArray key = label == "uint32"
                ? new UInt32Array.Builder().Append(7u).Build()
                : new LargeStringArray.Builder().Append("seven").Build();
            var value = new Int64Array.Builder();
            value.Append(run);
            await session.WriteBatchAsync(new RecordBatch(schema, [key, value.Build()], 1), default);
            await session.CommitAsync(default);
        }

        // One row, not two: the second run matched the first run's row on a key column whose Arrow
        // type the table does not store.
        Assert.Equal(1, await DeltaReader.RowCountAsync(Path.Combine(dir, "orders")));
    }

    /// <summary>The one shape the fix above makes reachable, and therefore has to refuse itself.
    ///
    /// A dictionary-encoded column is stored as its value type, so a reconcile comparing against the
    /// stored type lets it straight through — correctly, for an ordinary column. As a PARTITION column
    /// delta-rs then fails the insert with "Error partitioning record batch: Missing partition column",
    /// which carries neither the column nor the output and lands as PZDL0404's catch-all. Refused
    /// pre-flight instead, with the table not created.</summary>
    [Fact]
    public async Task A_dictionary_encoded_partition_column_is_refused_before_the_table_is_created()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-dictpart").FullName;
        var dictionary = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var schema = new Schema.Builder()
            .Field(f => f.Name("k").DataType(Int64Type.Default).Nullable(true))
            .Field(f => f.Name("v").DataType(dictionary).Nullable(true))
            .Build();

        await using var sink = await ((ISinkConnector)new DeltaLakeConnector())
            .OpenAsync(new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dir }), default);
        var spec = new OutputSpec(
            "lake", "orders", "append", "fail_on_change",
            new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "v" } });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, schema, default));

        Assert.Contains(DeltaErrors.UnwritableArrowType, ex.Message, StringComparison.Ordinal);
        Assert.Contains("'v'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Next step:", ex.Message, StringComparison.Ordinal);
        Assert.False(Directory.Exists(Path.Combine(dir, "orders")));
    }

    /// <summary>The same refusal for a run that declares no partition_by at all and inherits the
    /// table's own partition columns — the case the pre-flight check cannot see. An earlier run
    /// partitioned by a plain string column; this one sends the same column dictionary-encoded.</summary>
    [Fact]
    public async Task A_dictionary_encoded_column_is_refused_when_the_table_is_already_partitioned_by_it()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-dictpart2").FullName;
        var plain = new Schema.Builder()
            .Field(f => f.Name("k").DataType(Int64Type.Default).Nullable(true))
            .Field(f => f.Name("v").DataType(StringType.Default).Nullable(true))
            .Build();
        var location = Path.Combine(dir, "orders");
        await DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            await engine.CreateTableAsync(
                new TableCreateOptions(location, plain) { PartitionBy = ["v"], SaveMode = SaveMode.ErrorIfExists },
                default);
        });

        var dictionary = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var incoming = new Schema.Builder()
            .Field(f => f.Name("k").DataType(Int64Type.Default).Nullable(true))
            .Field(f => f.Name("v").DataType(dictionary).Nullable(true))
            .Build();

        await using var sink = await ((ISinkConnector)new DeltaLakeConnector())
            .OpenAsync(new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dir }), default);
        var spec = new OutputSpec("lake", "orders", "append", "fail_on_change", new Dictionary<string, object?>());

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, incoming, default));

        Assert.Contains(DeltaErrors.UnwritableArrowType, ex.Message, StringComparison.Ordinal);
        Assert.Contains("'v'", ex.Message, StringComparison.Ordinal);
    }
}
