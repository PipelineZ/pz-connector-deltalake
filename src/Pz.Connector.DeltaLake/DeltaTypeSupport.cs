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
/// one; closing it needs its own observed candidates first.
///
/// This predicate answers for the type EXACTLY as spelled, which is why it is applied to a schema
/// <see cref="DeltaArrowTypes.Canonical(Schema)"/> has already normalised. A timestamp whose timezone
/// denotes UTC in some other spelling — pz writes <c>"+00:00"</c> on every TIMESTAMP column it produces
/// — is a spelling delta-rs refuses and this predicate refuses too; the rewrite happens upstream so
/// that a difference the user can act on is the only thing that reaches an error message.
///
/// A DICTIONARY-encoded column returns TRUE here, and that is correct rather than an oversight, but the
/// reason is worth writing down because it is not obvious. Called against raw delta-rs, a dictionary
/// column writes fine as an ordinary column and fails the INSERT when delta-rs has to partition by it
/// ("Error partitioning record batch: Missing partition column") — an uncoded failure that would seem
/// to need a guard here. It does not: no write reaches it. Delta stores the column's value type, so the
/// table's schema comes back utf8 where the batch said dictionary, and DeltaLakeSink's Reconcile refuses
/// the write with PZDL0301 before a row is sent — measured through the sink, partitioned and not.
/// Adding a partition-specific refusal on top would be dead code. The cost of Reconcile getting there
/// first is that an ordinary dictionary column delta-rs WOULD accept is refused too; the remedy it
/// names (cast the column in the pipeline SQL) is the right one either way.</summary>
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
        // Delta stores every timestamp as an instant in UTC, so delta-rs accepts a timezone of "UTC",
        // an empty one or none at all and refuses every other string outright — measured, with the
        // identical "Invalid data type for Delta Lake" schema error a type it has no room for at all
        // produces. A zone-qualified timestamp is therefore a real refusal and not a spelling: the
        // spellings that DENOTE UTC are rewritten to "UTC" by DeltaArrowTypes.Canonical before this
        // predicate ever sees them, so anything left here names a zone Delta cannot carry.
        ArrowTypeId.Timestamp => ((TimestampType)type).Timezone is not { Length: > 0 } tz || tz == "UTC",
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

        // The FULLY PARAMETERIZED type, not Arrow's bare Name: "timestamp" alone does not say which
        // timezone was refused, and the timezone is the whole reason a timestamp is ever refused here.
        var named = string.Join(", ", bad.Select(f => $"'{f.Name}' ({DeltaArrowTypes.Describe(f.DataType)})"));
        throw DeltaErrors.Fail(DeltaErrors.UnwritableArrowType,
            $"these columns have Arrow types Delta Lake cannot store: {named}",
            "cast them in the pipeline SQL to a supported type — a duration or interval as a numeric " +
            "count of units, a time-of-day as a string or a timestamp, a timestamp carrying a named " +
            "or offset time zone as one in UTC, which is the only zone Delta stores an instant in");
    }
}
