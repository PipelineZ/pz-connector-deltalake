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
/// A DICTIONARY-encoded column returns TRUE here, and as an ORDINARY column that is the whole story —
/// measured, a dictionary column creates and inserts fine, and Delta stores it as its value type. As a
/// PARTITION column it is not: delta-rs fails the insert with "Error partitioning record batch: Missing
/// partition column", which is why <see cref="AssertPartitionable"/> exists beside this predicate
/// rather than inside it. The distinction cannot be made here, because this predicate sees a type and
/// not which columns the table partitions by.</summary>
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

    /// <summary>Refuses a dictionary-encoded PARTITION column, which is the one shape
    /// <see cref="IsWritable"/> cannot answer for on its own.
    ///
    /// Measured against DeltaLake.Net 0.33.0: a dictionary-encoded column partitions nothing. The
    /// create succeeds, and the insert fails with "Error partitioning record batch: Missing partition
    /// column" — a message that names neither the column nor the output, and lands as PZDL0404's
    /// catch-all whose next step talks about protocol versions. As an ordinary column the same
    /// encoding writes perfectly well, so this cannot be a refusal of the type.
    ///
    /// Called TWICE by DeltaLakeSink, and both are needed. Once pre-flight against the declared
    /// partition_by, so the create never happens — a configuration error must not leave a table
    /// behind. Once after the table is open against the table's OWN partition columns, which a run
    /// that declares no partition_by inherits: an earlier run can have partitioned by a column this
    /// run sends dictionary-encoded, and nothing before this point would notice.</summary>
    public static void AssertPartitionable(
        Schema schema, IReadOnlyList<string> partitionColumns, string output)
    {
        if (partitionColumns.Count == 0)
        {
            return;
        }

        // Ordinal, and it has to stay Ordinal: Delta column names are case-sensitive. A partition
        // column the write does not carry is Reconcile's problem, not this one.
        var names = partitionColumns.ToHashSet(StringComparer.Ordinal);
        var bad = schema.FieldsList
            .Where(f => names.Contains(f.Name) && f.DataType is DictionaryType)
            .Select(f => $"'{f.Name}'")
            .ToList();

        if (bad.Count == 0)
        {
            return;
        }

        // Every offending column at once: a user fixing one column per run is a user running the
        // pipeline once per column.
        throw DeltaErrors.Fail(DeltaErrors.UnwritableArrowType,
            $"output '{output}': partition column(s) {string.Join(", ", bad)} arrive dictionary-encoded, " +
            "and Delta cannot build a partition directory from a dictionary-encoded column — the same " +
            "column writes correctly when it is not a partition column",
            "cast those columns in the pipeline SQL so they arrive as plain values, or partition by a " +
            "column that does");
    }

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
