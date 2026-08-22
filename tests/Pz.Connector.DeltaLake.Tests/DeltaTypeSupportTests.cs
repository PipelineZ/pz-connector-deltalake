using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using DeltaLake.Errors;
using DeltaLake.Table;
using Pz.Connectors.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Pz.Connector.DeltaLake.Tests;

public class DeltaTypeSupportTests(ITestOutputHelper output)
{
    // Candidates cover every constructible Apache.Arrow.Types.ArrowTypeId value -- not just the ones
    // the brief happened to guess -- because "the list is observed, not guessed" means observing the
    // whole enum, not a convenient subset. Two ArrowTypeId values are absent on purpose:
    //   - RecordBatch has no concrete IArrowType implementation in Apache.Arrow 23.0.0 (confirmed via
    //     reflection over the shipped assembly: no "RecordBatchType" class exists, and
    //     Apache.Arrow.RecordBatch itself implements IArrowArray, not IArrowType). It identifies a
    //     container format, not a column type -- there is no Field whose DataType could ever be
    //     ArrowTypeId.RecordBatch, so there is nothing to place in a Schema and nothing to probe.
    //   - Extension is represented by Bool8Type below (a concrete, shipped ExtensionType subclass)
    //     rather than by a bare "ArrowTypeId.Extension" instance, because Apache.Arrow.ExtensionType
    //     itself is abstract -- there is no default/bare instance of it to construct. Bool8Type.Default
    //     genuinely reports TypeId == ArrowTypeId.Extension (confirmed via reflection), so this is a
    //     real observation of that TypeId, not a stand-in for one.
    //
    // Nested candidates cover the Struct/List/Map recursion in DeltaTypeSupport.IsWritable: for each of
    // List, Struct, and Map, one candidate whose inner type is on the refusal list (must come back
    // unwritable through the recursion, not just at the top level) and one whose inner type is
    // ordinary (must stay writable). Map gets a THIRD: an unwritable type on the KEY side specifically
    // (map_duration_to_string) -- IsWritable's Map arm is a conjunction of the key and value sides, and
    // a conjunction with only its second operand ever driven false is an unobserved first operand in
    // every way that matters. List gets a second nesting level (list_of_list_of_string /
    // list_of_list_of_interval) so the recursion is proven to actually recurse, not just handle one
    // level and coincidentally look right for the rest.
    //
    // DurationType has no public constructor in Apache.Arrow 23.0.0 -- only static singletons
    // (DurationType.Second/.Millisecond/.Microsecond/.Nanosecond). The brief's `new
    // DurationType(TimeUnit.Microsecond)` does not compile against the version this project
    // references; every duration candidate below uses the real singleton API.
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
        // The spelling pz ACTUALLY produces for every TIMESTAMP column, and the two other timezone
        // shapes a producer can offer. delta-rs takes "UTC", an empty timezone and none at all, and
        // refuses every other string with the same schema error an unsupported type gets -- so the
        // offset spelling belongs in this observed set even though the sink rewrites it to "UTC"
        // before this predicate sees it (DeltaArrowTypes.Canonical, pinned by TimestampTimezoneTests).
        // Without a candidate here, this theory kept agreeing with delta-rs about spellings pz never
        // sends while saying nothing about the one it always does.
        { "timestamp_us_zero_offset", new TimestampType(TimeUnit.Microsecond, "+00:00") },
        { "timestamp_us_named_zone", new TimestampType(TimeUnit.Microsecond, "America/New_York") },
        { "timestamp_us_empty_tz", new TimestampType(TimeUnit.Microsecond, string.Empty) },
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
            "list_of_list_of_string",
            new ListType(new Field("item", new ListType(new Field("item", StringType.Default, true)), true))
        },
        {
            "list_of_list_of_interval",
            new ListType(new Field("item",
                new ListType(new Field("item", new IntervalType(IntervalUnit.YearMonth), true)), true))
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
            "map_duration_to_string",
            new MapType(DurationType.Microsecond, StringType.Default)
        },
        {
            "map_string_to_string",
            new MapType(StringType.Default, StringType.Default)
        },
        { "null_type", NullType.Default },
        { "halffloat", HalfFloatType.Default },
        { "fixedsizebinary_4", new FixedSizeBinaryType(4) },
        { "decimal256_50_9", new Decimal256Type(50, 9) },
        { "decimal32_9_2", new Decimal32Type(9, 2) },
        { "decimal64_18_4", new Decimal64Type(18, 4) },
        { "binaryview", BinaryViewType.Default },
        { "stringview", StringViewType.Default },
        { "listview_of_string", new ListViewType(new Field("item", StringType.Default, true)) },
        { "largelist_of_string", new LargeListType(new Field("item", StringType.Default, true)) },
        { "largebinary", LargeBinaryType.Default },
        { "largestring", LargeStringType.Default },
        { "largelistview_of_string", new LargeListViewType(new Field("item", StringType.Default, true)) },
        { "fixedsizelist_of_int64_2", new FixedSizeListType(Int64Type.Default, 2) },
        { "extension_bool8", Bool8Type.Default },
        { "dictionary_int32_string", new DictionaryType(Int32Type.Default, StringType.Default, false) },
        { "runendencoded_int32_int64", new RunEndEncodedType(Int32Type.Default, Int64Type.Default) },
        {
            "union_sparse_int64_string",
            new UnionType(
                [new Field("a", Int64Type.Default, true), new Field("b", StringType.Default, true)],
                [0, 1], UnionMode.Sparse)
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
            //
            // For every REFUSED candidate EXCEPT FixedSizeList, ProbeInsertAsync's own internal
            // CreateTableAsync call fails first, with the identical message CREATE alone produces --
            // InsertAsync and the SingleValueArrayFor-built value are never reached for those. So for
            // that part of the negative set, "create and insert agree" is the same create-time schema
            // check observed twice, not two independent signals; it does NOT prove delta-rs's
            // write-time conversion itself refuses these types, only that the type never gets that far.
            // FixedSizeList is the one named exception: its CREATE succeeds, so ProbeInsertAsync's
            // InsertAsync call genuinely runs and is the ONLY evidence that refuses it -- the opposite
            // situation from every other refused candidate, and the reason this paragraph is spelled
            // out rather than replaced by a blanket "create and insert agree". For every ACCEPTED
            // candidate, InsertAsync likewise genuinely runs to completion and DOES exercise the
            // separate write path -- that half of the agreement is real, independent evidence too.
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
        catch (DeltaLakeException ex)
        {
            // Narrowed to delta-rs's own exception type deliberately: a candidate with no matching
            // SingleValueArrayFor case throws NotSupportedException, which must fail the test loudly as
            // a harness gap, not get silently logged and counted as "rejected by delta-rs."
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
                var v = SingleValueArrayFor(schema.FieldsList[1].DataType);
                var batch = new RecordBatch(schema, [k.Build(), v], 1);

                await table.InsertAsync([batch], schema, new InsertOptions { SaveMode = SaveMode.Append }, default);
            });
            return true;
        }
        catch (DeltaLakeException ex)
        {
            output.WriteLine($"{label}: insert rejected by delta-rs — {ex.GetType().Name}: {ex.Message}");
            return false;
        }
    }

    // Builds one well-formed row of the candidate type -- exercising schema/type acceptance during the
    // write path, not value conversion. Most arms build an all-null value via the real Apache.Arrow
    // builder for that leaf type. A few types carry no meaningful "null" shortcut and get the smallest
    // legal NON-null value instead (Dictionary: one dictionary entry plus one index pointing at it;
    // RunEndEncoded: one run of length one; Union: one child slot selected) -- noted per-arm below.
    // List/Map append a null entry directly (their builders own constructing an empty inner values
    // array of the declared value/key/value type); Struct -- which Apache.Arrow gives no builder for --
    // is assembled from its children directly, recursing so nested candidates are covered the same way
    // as top-level ones. The Dictionary/RunEndEncoded/Union arms hardcode child types matching this
    // file's own single candidate of each kind (int32-indexed dictionary of strings; int32 run-ends
    // over int64 values; a two-field int64/string sparse union) -- they are not general-purpose
    // builders for arbitrary Dictionary/RunEndEncoded/Union shapes.
    internal static IArrowArray SingleValueArrayFor(IArrowType type) => type switch
    {
        NullType => new NullArray(1),
        BooleanType => new BooleanArray.Builder().AppendNull().Build(),
        Int8Type => new Int8Array.Builder().AppendNull().Build(),
        Int16Type => new Int16Array.Builder().AppendNull().Build(),
        Int32Type => new Int32Array.Builder().AppendNull().Build(),
        Int64Type => new Int64Array.Builder().AppendNull().Build(),
        UInt8Type => new UInt8Array.Builder().AppendNull().Build(),
        UInt16Type => new UInt16Array.Builder().AppendNull().Build(),
        UInt32Type => new UInt32Array.Builder().AppendNull().Build(),
        UInt64Type => new UInt64Array.Builder().AppendNull().Build(),
        HalfFloatType => new HalfFloatArray.Builder().AppendNull().Build(),
        FloatType => new FloatArray.Builder().AppendNull().Build(),
        DoubleType => new DoubleArray.Builder().AppendNull().Build(),
        StringType => new StringArray.Builder().AppendNull().Build(),
        BinaryType => new BinaryArray.Builder().AppendNull().Build(),
        Date32Type => new Date32Array.Builder().AppendNull().Build(),
        Date64Type => new Date64Array.Builder().AppendNull().Build(),
        TimestampType ts => new TimestampArray.Builder(ts).AppendNull().Build(),
        // Decimal128/256/32/64Type all derive from FixedSizeBinaryType (confirmed via reflection:
        // Decimal128Type.BaseType == FixedSizeBinaryType) -- these four arms must come BEFORE the
        // bare FixedSizeBinaryType arm below, or that arm's pattern silently swallows all of them and
        // the compiler flags the decimal arms as unreachable.
        Decimal128Type d128 => new Decimal128Array.Builder(d128).AppendNull().Build(),
        Decimal256Type d256 => new Decimal256Array.Builder(d256).AppendNull().Build(),
        Decimal32Type d32 => new Decimal32Array.Builder(d32).AppendNull().Build(),
        Decimal64Type d64 => new Decimal64Array.Builder(d64).AppendNull().Build(),
        FixedSizeBinaryType fsb => BuildNullFixedSizeBinary(fsb),
        Time32Type t32 => new Time32Array.Builder(t32).AppendNull().Build(),
        Time64Type t64 => new Time64Array.Builder(t64).AppendNull().Build(),
        DurationType dur => new DurationArray.Builder(dur).AppendNull().Build(),
        IntervalType { Unit: IntervalUnit.YearMonth } => new YearMonthIntervalArray.Builder().AppendNull().Build(),
        BinaryViewType => new BinaryViewArray.Builder().AppendNull().Build(),
        StringViewType => new StringViewArray.Builder().AppendNull().Build(),
        LargeBinaryType => new LargeBinaryArray.Builder().AppendNull().Build(),
        LargeStringType => new LargeStringArray.Builder().AppendNull().Build(),
        ListType lt => new ListArray.Builder(lt.ValueDataType).AppendNull().Build(),
        ListViewType lvt => new ListViewArray.Builder(lvt.ValueDataType).AppendNull().Build(),
        LargeListType llt => new LargeListArray.Builder(llt.ValueDataType).AppendNull().Build(),
        LargeListViewType llvt => new LargeListViewArray.Builder(llvt.ValueDataType).AppendNull().Build(),
        FixedSizeListType fsl => new FixedSizeListArray.Builder(fsl.ValueDataType, fsl.ListSize).AppendNull().Build(),
        MapType mt => new MapArray.Builder(mt).AppendNull().Build(),
        StructType st => BuildNullStruct(st),
        Bool8Type => new Bool8Array(Bool8Type.Default, new Int8Array.Builder().AppendNull().Build()),
        DictionaryType dict => BuildDictionary(dict),
        RunEndEncodedType ree => BuildRunEndEncoded(ree),
        UnionType u => BuildSparseUnion(u),
        _ => throw new NotSupportedException(
            $"DeltaTypeSupportTests.SingleValueArrayFor has no case for {type.GetType().Name} ({type.Name}) -- " +
            "add one alongside the corresponding Candidates() entry."),
    };

    private static IArrowArray BuildNullStruct(StructType type)
    {
        var children = type.Fields.Select(f => SingleValueArrayFor(f.DataType)).ToArray();
        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(false);
        return new StructArray(type, length: 1, children, validity.Build(), nullCount: 1);
    }

    private static IArrowArray BuildNullFixedSizeBinary(FixedSizeBinaryType type)
    {
        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(false);
        var data = new ArrayData(type, length: 1, nullCount: 1, offset: 0,
            buffers: [validity.Build(), new ArrowBuffer(new byte[type.ByteWidth])]);
        return new FixedSizeBinaryArray(data);
    }

    // Dictionary encoding always carries a real index into a real dictionary -- there is no "all null"
    // shortcut the way a plain nullable column has. This builds the smallest legal non-null
    // dictionary-encoded value: one dictionary entry ("x"), one index (0) pointing at it. Hardcodes
    // Int32/String because that is this file's one Dictionary candidate's index/value type.
    private static IArrowArray BuildDictionary(DictionaryType type)
    {
        var indices = new Int32Array.Builder().Append(0).Build();
        var dictionaryValues = new StringArray.Builder().Append("x").Build();
        return new DictionaryArray(type, indices, dictionaryValues);
    }

    // One run of length one over one (null) value -- the smallest legal run-end-encoded array.
    // Hardcodes Int32/Int64 because that is this file's one RunEndEncoded candidate's run-end/value type.
    private static IArrowArray BuildRunEndEncoded(RunEndEncodedType type)
    {
        var runEnds = new Int32Array.Builder().Append(1).Build();
        var values = new Int64Array.Builder().AppendNull().Build();
        return new RunEndEncodedArray(runEnds, values);
    }

    // A one-row sparse union selecting child 0. Hardcodes an Int64/String two-field shape because that
    // is this file's one Union candidate's field layout.
    private static IArrowArray BuildSparseUnion(UnionType type)
    {
        var children = new IArrowArray[]
        {
            new Int64Array.Builder().AppendNull().Build(),
            new StringArray.Builder().AppendNull().Build(),
        };
        var typeIds = new ArrowBuffer.Builder<byte>().Append(0).Build();
        return new SparseUnionArray(type, 1, children, typeIds, nullCount: 0);
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

        // Quoted, and it has to be. Assert's fixed wording contains a bare "a" unconditionally, in
        // "have", "Lake", "cast" and "a supported type" -- so Assert.Contains("a", ...) passed even
        // when the message named NO columns at all, which is exactly the failure this test's name
        // promises to catch. The quoted form appears only where a column is named.
        var ex = Assert.Throws<PzConnectorException>(() => DeltaTypeSupport.Assert(schema));
        Assert.Contains("'a'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("'b'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Assert_passes_a_schema_of_ordinary_types() =>
        DeltaTypeSupport.Assert(DeltaTestTable.Schema);
}
