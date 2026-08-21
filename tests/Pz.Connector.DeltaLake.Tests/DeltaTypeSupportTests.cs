using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Table;
using Pz.Connectors.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Pz.Connector.DeltaLake.Tests;

public class DeltaTypeSupportTests(ITestOutputHelper output)
{
    // Nested candidates cover the Struct/List/Map recursion in DeltaTypeSupport.IsWritable: one whose
    // inner type is on the refusal list (must come back unwritable through the recursion, not just at
    // the top level) and one whose inner type is ordinary (must stay writable). Time32 has its own
    // entry because the refusal list names it explicitly -- an untested entry there would be exactly
    // the "guessed, not observed" mistake this task exists to avoid.
    //
    // DurationType has no public constructor in Apache.Arrow 23.0.0 -- only static singletons
    // (DurationType.Second/.Millisecond/.Microsecond/.Nanosecond). The brief's `new
    // DurationType(TimeUnit.Microsecond)` does not compile against the version this project
    // references; every duration candidate below uses the real API instead.
    public static TheoryData<string, IArrowType> Candidates() => new()
    {
        { "bool", BooleanType.Default },
        { "int8", Int8Type.Default },
        { "int16", Int16Type.Default },
        { "int32", Int32Type.Default },
        { "int64", Int64Type.Default },
        { "uint8", UInt8Type.Default },
        { "uint16", UInt16Type.Default },
        { "uint32", UInt32Type.Default },
        { "uint64", UInt64Type.Default },
        { "float", FloatType.Default },
        { "double", DoubleType.Default },
        { "string", StringType.Default },
        { "binary", BinaryType.Default },
        { "date32", Date32Type.Default },
        { "date64", Date64Type.Default },
        { "timestamp_us_utc", new TimestampType(TimeUnit.Microsecond, "UTC") },
        { "timestamp_ns_utc", new TimestampType(TimeUnit.Nanosecond, "UTC") },
        { "decimal128_38_9", new Decimal128Type(38, 9) },
        { "time32_ms", new Time32Type(TimeUnit.Millisecond) },
        { "time64_us", new Time64Type(TimeUnit.Microsecond) },
        { "duration_us", DurationType.Microsecond },
        { "interval_month", new IntervalType(IntervalUnit.YearMonth) },
        {
            "list_of_interval",
            new ListType(new Field("item", new IntervalType(IntervalUnit.YearMonth), true))
        },
        {
            "list_of_string",
            new ListType(new Field("item", StringType.Default, true))
        },
        {
            "struct_containing_duration",
            new StructType([new Field("d", DurationType.Microsecond, true)])
        },
        {
            "struct_of_ordinary",
            new StructType([new Field("n", Int64Type.Default, true)])
        },
        {
            "map_string_to_duration",
            new MapType(StringType.Default, DurationType.Microsecond)
        },
        {
            "map_string_to_string",
            new MapType(StringType.Default, StringType.Default)
        },
    };

    [Theory]
    [MemberData(nameof(Candidates))]
    public async Task IsWritable_agrees_with_what_delta_rs_actually_accepts(string label, IArrowType type)
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("k").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("v").DataType(type).Nullable(true))
            .Build();

        var dir = Directory.CreateTempSubdirectory($"pz-delta-type-{label}").FullName;
        try
        {
            var createAccepted = await ProbeCreateAsync(dir, schema, label);
            var insertAccepted = await ProbeInsertAsync(dir, schema, label);

            // Create-time schema validation can be more permissive than write-time conversion, and the
            // guard this task builds runs at BeginWriteAsync -- protecting a write, not a create. When
            // the two disagree, the refusal list must follow the STRICTER (insert) observation: a type
            // that creates fine but fails on insert must still be refused, or the guard would let a
            // write through that fails Rust-side with no pz context, which is exactly what this task
            // exists to prevent.
            if (createAccepted != insertAccepted)
            {
                output.WriteLine(
                    $"{label}: create/insert DISAGREE (create={createAccepted}, insert={insertAccepted}) " +
                    "-- following the stricter (insert) observation");
            }

            var accepted = createAccepted && insertAccepted;
            Assert.Equal(accepted, DeltaTypeSupport.IsWritable(type));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private async Task<bool> ProbeCreateAsync(string dir, Schema schema, string label)
    {
        try
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            await DeltaBigStack.RunAsync(() => engine.CreateTableAsync(
                new TableCreateOptions(Path.Combine(dir, "create-only"), schema) { SaveMode = SaveMode.ErrorIfExists },
                default));
            return true;
        }
        catch (Exception ex)
        {
            output.WriteLine($"{label}: create rejected by delta-rs — {ex.GetType().Name}: {ex.Message}");
            return false;
        }
    }

    private async Task<bool> ProbeInsertAsync(string dir, Schema schema, string label)
    {
        var location = Path.Combine(dir, "create-and-insert");
        try
        {
            await DeltaBigStack.RunAsync(async () =>
            {
                using var engine = new DeltaEngine(EngineOptions.Default);
                var table = await engine.CreateTableAsync(
                    new TableCreateOptions(location, schema) { SaveMode = SaveMode.ErrorIfExists }, default);

                var k = new Int64Array.Builder();
                k.Append(1);
                var v = NullArrayFor(schema.FieldsList[1].DataType);
                var batch = new RecordBatch(schema, [k.Build(), v], 1);

                await table.InsertAsync([batch], schema, new InsertOptions { SaveMode = SaveMode.Append }, default);
            });
            return true;
        }
        catch (Exception ex)
        {
            output.WriteLine($"{label}: insert rejected by delta-rs — {ex.GetType().Name}: {ex.Message}");
            return false;
        }
    }

    // The probe only needs a well-formed, single all-null value of the candidate type -- it is
    // exercising schema/type acceptance during the write path, not value conversion. Each arm uses the
    // real Apache.Arrow builder for that leaf type; List/Map append a null entry directly (their
    // builders own constructing an empty inner values array of the declared value type), and Struct --
    // which Apache.Arrow gives no builder for -- is assembled from its children directly, recursing so
    // nested candidates are covered the same way as top-level ones.
    private static IArrowArray NullArrayFor(IArrowType type) => type switch
    {
        BooleanType => new BooleanArray.Builder().AppendNull().Build(),
        Int8Type => new Int8Array.Builder().AppendNull().Build(),
        Int16Type => new Int16Array.Builder().AppendNull().Build(),
        Int32Type => new Int32Array.Builder().AppendNull().Build(),
        Int64Type => new Int64Array.Builder().AppendNull().Build(),
        UInt8Type => new UInt8Array.Builder().AppendNull().Build(),
        UInt16Type => new UInt16Array.Builder().AppendNull().Build(),
        UInt32Type => new UInt32Array.Builder().AppendNull().Build(),
        UInt64Type => new UInt64Array.Builder().AppendNull().Build(),
        FloatType => new FloatArray.Builder().AppendNull().Build(),
        DoubleType => new DoubleArray.Builder().AppendNull().Build(),
        StringType => new StringArray.Builder().AppendNull().Build(),
        BinaryType => new BinaryArray.Builder().AppendNull().Build(),
        Date32Type => new Date32Array.Builder().AppendNull().Build(),
        Date64Type => new Date64Array.Builder().AppendNull().Build(),
        TimestampType ts => new TimestampArray.Builder(ts).AppendNull().Build(),
        Decimal128Type d => new Decimal128Array.Builder(d).AppendNull().Build(),
        Time32Type t32 => new Time32Array.Builder(t32).AppendNull().Build(),
        Time64Type t64 => new Time64Array.Builder(t64).AppendNull().Build(),
        DurationType dur => new DurationArray.Builder(dur).AppendNull().Build(),
        IntervalType { Unit: IntervalUnit.YearMonth } => new YearMonthIntervalArray.Builder().AppendNull().Build(),
        ListType lt => new ListArray.Builder(lt.ValueDataType).AppendNull().Build(),
        MapType mt => new MapArray.Builder(mt).AppendNull().Build(),
        StructType st => BuildNullStruct(st),
        _ => throw new NotSupportedException(
            $"DeltaTypeSupportTests.NullArrayFor has no case for {type.GetType().Name} ({type.Name}) -- " +
            "add one alongside the corresponding Candidates() entry."),
    };

    private static IArrowArray BuildNullStruct(StructType type)
    {
        var children = type.Fields.Select(f => NullArrayFor(f.DataType)).ToArray();
        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(false);
        return new StructArray(type, length: 1, children, validity.Build(), nullCount: 1);
    }

    [Fact]
    public void Assert_names_the_offending_column_and_its_type()
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("span").DataType(new IntervalType(IntervalUnit.YearMonth)).Nullable(true))
            .Build();

        var ex = Assert.Throws<PzConnectorException>(() => DeltaTypeSupport.Assert(schema));
        Assert.Contains(DeltaErrors.UnwritableArrowType, ex.Message);
        Assert.Contains("span", ex.Message);
        Assert.Contains("Next step:", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Assert_reports_every_unwritable_column_not_just_the_first()
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("a").DataType(new IntervalType(IntervalUnit.YearMonth)).Nullable(true))
            .Field(f => f.Name("b").DataType(DurationType.Microsecond).Nullable(true))
            .Build();

        var ex = Assert.Throws<PzConnectorException>(() => DeltaTypeSupport.Assert(schema));
        Assert.Contains("a", ex.Message);
        Assert.Contains("b", ex.Message);
    }

    [Fact]
    public void Assert_passes_a_schema_of_ordinary_types() =>
        DeltaTypeSupport.Assert(DeltaTestTable.Schema);
}
