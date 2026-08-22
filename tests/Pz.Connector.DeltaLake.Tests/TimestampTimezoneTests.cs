using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Table;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>The timezone SPELLING pz actually produces, end to end through the real sink.
///
/// Every Arrow schema pz hands a sink spells a UTC timestamp's timezone <c>"+00:00"</c> — its own
/// convention, applied to every TIMESTAMP column, and applied whatever the pipeline SQL says. delta-rs
/// accepts only <c>"UTC"</c>. Nothing in this repository used the <c>"+00:00"</c> spelling until this
/// file: every timestamp fixture constructed <c>"UTC"</c> or no timezone, which are the two spellings
/// pz never sends, so the suite modelled a producer that does not exist and proved nothing about the
/// one that does.
///
/// Both halves of the failure are pinned here, because they are one cause with two faces: the CREATE
/// of a new table, and the reconcile against a table an earlier writer already created as UTC.</summary>
public class TimestampTimezoneTests
{
    /// <summary>The exact type pz's own ArrowInterop produces for a DuckDB TIMESTAMP column.</summary>
    private static readonly TimestampType PzUtc = new(TimeUnit.Microsecond, "+00:00");

    private static Schema SchemaWith(TimestampType timestamp) => new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(true))
        .Field(f => f.Name("ts").DataType(timestamp).Nullable(true))
        .Build();

    private static RecordBatch Rows(TimestampType timestamp, long from, int count)
    {
        var id = new Int64Array.Builder();
        var ts = new TimestampArray.Builder(timestamp);
        for (var i = 0; i < count; i++)
        {
            id.Append(from + i);
            ts.Append(DateTimeOffset.UnixEpoch.AddDays(from + i));
        }

        return new RecordBatch(SchemaWith(timestamp), [id.Build(), ts.Build()], count);
    }

    private static ConnectorConfig Cfg(string root) => new(new Dictionary<string, object?> { ["root"] = root });

    private static OutputSpec Out(string mode = "append") =>
        new("lake", "orders", mode, "fail_on_change", new Dictionary<string, object?>());

    private static async Task<ISink> OpenSink(string root) =>
        await ((ISinkConnector)new DeltaLakeConnector()).OpenAsync(Cfg(root), default);

    private static async Task WriteAsync(ISink sink, RecordBatch batch)
    {
        await using var session = await sink.BeginWriteAsync(Out(), batch.Schema, default);
        await session.WriteBatchAsync(batch, default);
        await session.CommitAsync(default);
    }

    [Fact]
    public async Task A_pz_spelled_timestamp_creates_the_table_and_writes_into_it()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-ts-create").FullName;
        await using var sink = await OpenSink(dir);

        await WriteAsync(sink, Rows(PzUtc, 0, 4));

        var location = Path.Combine(dir, "orders");
        Assert.Equal(4, await DeltaReader.RowCountAsync(location));
        // Stored as the one spelling delta-rs has: the connector rewrote the timezone, it did not
        // invent a second column type.
        Assert.Equal(
            "timestamp[Microsecond, tz=UTC]",
            DeltaArrowTypes.Describe((await DeltaReader.ArrowSchemaAsync(location)).FieldsList[1].DataType));
    }

    [Fact]
    public async Task A_second_run_writes_into_the_table_the_first_run_created()
    {
        // The reconcile path, against a table this connector itself made. A create that succeeds and a
        // reconcile that then refuses the same schema is the shape that leaves an empty table behind.
        var dir = Directory.CreateTempSubdirectory("pz-delta-ts-again").FullName;
        await using var sink = await OpenSink(dir);

        await WriteAsync(sink, Rows(PzUtc, 0, 3));
        await WriteAsync(sink, Rows(PzUtc, 3, 3));

        Assert.Equal(6, await DeltaReader.RowCountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task A_pz_spelled_timestamp_writes_into_a_table_another_writer_created_as_UTC()
    {
        // Spark, or delta-rs itself, spells the timezone "UTC". pz spells the same zone "+00:00" and
        // cannot be made to say otherwise, so a reconcile that reported the difference would name a
        // remedy — cast the column in the pipeline SQL — that nobody can carry out.
        var dir = Directory.CreateTempSubdirectory("pz-delta-ts-existing").FullName;
        var location = Path.Combine(dir, "orders");
        var utc = new TimestampType(TimeUnit.Microsecond, "UTC");
        await DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.CreateTableAsync(
                new TableCreateOptions(location, SchemaWith(utc)) { SaveMode = SaveMode.ErrorIfExists }, default);
            await table.InsertAsync(
                [Rows(utc, 0, 2)], SchemaWith(utc), new InsertOptions { SaveMode = SaveMode.Append }, default);
        });

        await using var sink = await OpenSink(dir);
        await WriteAsync(sink, Rows(PzUtc, 2, 2));

        Assert.Equal(4, await DeltaReader.RowCountAsync(location));
    }

    [Fact]
    public async Task A_timestamp_in_a_zone_Delta_cannot_store_is_refused_by_name_before_anything_is_created()
    {
        // The other direction, and the reason the rewrite is confined to spellings of ONE zone: a
        // timestamp in a real zone is a genuine difference, so it is refused with a coded message that
        // names the column and the zone — and the table is not created.
        var dir = Directory.CreateTempSubdirectory("pz-delta-ts-zone").FullName;
        await using var sink = await OpenSink(dir);
        var schema = SchemaWith(new TimestampType(TimeUnit.Microsecond, "America/New_York"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out(), schema, default));

        Assert.Contains(DeltaErrors.UnwritableArrowType, ex.Message, StringComparison.Ordinal);
        Assert.Contains("'ts'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("America/New_York", ex.Message, StringComparison.Ordinal);
        Assert.Contains("UTC", ex.Message, StringComparison.Ordinal);
        Assert.False(Directory.Exists(Path.Combine(dir, "orders")));
    }

    [Theory]
    [InlineData("+00:00")]
    [InlineData("-00:00")]
    [InlineData("+0000")]
    [InlineData("+00")]
    [InlineData("Z")]
    [InlineData("utc")]
    public void Every_spelling_of_the_zero_offset_becomes_the_one_delta_rs_accepts(string timezone)
    {
        var canonical = (TimestampType)DeltaArrowTypes.Canonical(new TimestampType(TimeUnit.Microsecond, timezone));
        Assert.Equal("UTC", canonical.Timezone);
        Assert.Equal(TimeUnit.Microsecond, canonical.Unit);
    }

    [Theory]
    [InlineData("+05:30")]
    [InlineData("-08:00")]
    [InlineData("America/New_York")]
    [InlineData("Etc/UTC")]
    [InlineData("GMT")]
    public void A_timezone_that_is_not_a_spelling_of_the_zero_offset_is_left_alone(string timezone)
    {
        var type = new TimestampType(TimeUnit.Microsecond, timezone);
        Assert.Same(type, DeltaArrowTypes.Canonical(type));
        Assert.False(DeltaTypeSupport.IsWritable(type));
    }

    [Fact]
    public void A_schema_needing_no_rewrite_comes_back_as_itself()
    {
        // Reference identity is the contract Canonical's callers rely on to leave an untouched schema
        // untouched — a rewrite that always allocated would be indistinguishable from one that changed
        // something.
        var schema = DeltaTestTable.Schema;
        Assert.Same(schema, DeltaArrowTypes.Canonical(schema));
    }

    [Fact]
    public void The_rewrite_reaches_a_timestamp_nested_in_a_list_a_struct_and_a_map()
    {
        // delta-rs refuses the offset spelling just as hard inside a container — measured, for both a
        // list and a struct — so a rewrite that stopped at the top level would leave the same
        // unactionable failure one level down.
        Assert.Equal(
            "list<timestamp[Microsecond, tz=UTC]>",
            DeltaArrowTypes.Describe(DeltaArrowTypes.Canonical(new ListType(new Field("item", PzUtc, true)))));
        Assert.Equal(
            "struct<t: timestamp[Microsecond, tz=UTC]>",
            DeltaArrowTypes.Describe(DeltaArrowTypes.Canonical(new StructType([new Field("t", PzUtc, true)]))));
        Assert.Equal(
            "map<utf8, timestamp[Microsecond, tz=UTC]>",
            DeltaArrowTypes.Describe(DeltaArrowTypes.Canonical(new MapType(StringType.Default, PzUtc))));
    }

    [Fact]
    public void The_rewrite_preserves_a_field_name_and_its_nullability()
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("created_at").DataType(PzUtc).Nullable(false))
            .Build();

        var canonical = DeltaArrowTypes.Canonical(schema);
        Assert.Equal("created_at", canonical.FieldsList[0].Name);
        Assert.False(canonical.FieldsList[0].IsNullable);
    }
}
