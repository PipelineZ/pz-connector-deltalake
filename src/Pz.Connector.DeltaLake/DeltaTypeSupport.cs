using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.DeltaLake;

/// <summary>A pre-flight refusal list, not a type mapping — delta-rs owns Arrow→Delta conversion. This
/// exists so a type Delta cannot represent fails at BeginWriteAsync with a named column, instead of
/// surfacing as a Rust-side error partway through a write with no pz context attached.
///
/// The set below is OBSERVED, over every constructible <see cref="ArrowTypeId"/> value, not a
/// convenient subset of them: DeltaTypeSupportTests writes each candidate through a real delta-rs
/// create AND a real delta-rs insert and asserts this predicate agrees with the stricter of the two.
/// When delta-rs gains a type, the test fails and this list changes — never the other way round.
///
/// <see cref="ArrowTypeId.List"/>/<see cref="ArrowTypeId.Struct"/>/<see cref="ArrowTypeId.Map"/> are
/// the only containers whose element type is recursed into — that recursion is itself observed
/// (DeltaTypeSupportTests carries a nested-unwritable and a nested-ordinary candidate, two levels deep
/// for List, and separately for the Map key and value sides). LargeList/ListView/LargeListView were
/// each observed writable with an ordinary inner type but were NOT probed with an unwritable inner type,
/// so their element type is deliberately NOT recursed into below — DuckDB's Arrow export does not
/// produce any of the three (LIST becomes <see cref="ArrowTypeId.List"/>, ARRAY becomes <see
/// cref="ArrowTypeId.FixedSizeList"/>), so this is an accepted, documented gap rather than a silent
/// one; closing it needs its own observed candidates first.</summary>
internal static class DeltaTypeSupport
{
    public static bool IsWritable(IArrowType type) => type.TypeId switch
    {
        // No Delta physical type exists for these at all: Null, HalfFloat, Interval, Duration,
        // Time32/64, Decimal32/64/256, RunEndEncoded, Union all fail at CREATE with delta-rs's own
        // "Invalid data type for Delta Lake: <Type>" schema error.
        ArrowTypeId.Null or ArrowTypeId.HalfFloat or ArrowTypeId.Interval or ArrowTypeId.Duration or
            ArrowTypeId.Time32 or ArrowTypeId.Time64 or ArrowTypeId.Decimal32 or ArrowTypeId.Decimal64 or
            ArrowTypeId.Decimal256 or ArrowTypeId.RunEndEncoded or ArrowTypeId.Union => false,
        // FixedSizeList is the one candidate where CREATE and INSERT disagreed: CREATE accepts a
        // fixed_size_list column, but a real INSERT through DeltaLake.Net 0.33.0 fails --
        // "column types must match schema types, expected List(Int64, field: 'element') but found
        // List(Int64)" at first read as an FFI marshaling defect. It is narrower than that: Delta's
        // canonical Arrow round-trip expects the list's inner field named "element"; Apache.Arrow's
        // own `new FixedSizeListType(valueType, listSize)` constructor -- the shape this connector's
        // own type-construction path uses -- names it "item" instead. Verified directly: rebuilding
        // the same probe with the inner field explicitly named "element" makes BOTH create and insert
        // succeed. So this is not a permanent block on ARRAY-shaped columns, only on the default Arrow
        // field name this connector currently produces for one -- refusing is still correct today
        // (nothing in this connector names that field "element" yet), but a future task that wants to
        // support ARRAY columns should rename the inner field before assuming FixedSizeList is a dead
        // end. Since this guard protects the write path (BeginWriteAsync), not the create path, the
        // stricter (insert) observation governs regardless: refused as the connector stands today.
        ArrowTypeId.FixedSizeList => false,
        ArrowTypeId.Struct => ((StructType)type).Fields.All(f => IsWritable(f.DataType)),
        ArrowTypeId.List => IsWritable(((ListType)type).ValueDataType),
        ArrowTypeId.Map => IsWritable(((MapType)type).KeyField.DataType) &&
                           IsWritable(((MapType)type).ValueField.DataType),
        _ => true,
    };

    public static void Assert(Schema schema)
    {
        var bad = schema.FieldsList.Where(f => !IsWritable(f.DataType)).ToList();
        if (bad.Count == 0)
        {
            return;
        }

        var named = string.Join(", ", bad.Select(f => $"'{f.Name}' ({f.DataType.Name})"));
        throw DeltaErrors.Fail(DeltaErrors.UnwritableArrowType,
            $"these columns have Arrow types Delta Lake cannot store: {named}",
            "cast them in the pipeline SQL to a supported type — a duration or interval as a numeric " +
            "count of units, a time-of-day as a string or a timestamp");
    }
}
