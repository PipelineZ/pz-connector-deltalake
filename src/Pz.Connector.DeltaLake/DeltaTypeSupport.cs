using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.DeltaLake;

/// <summary>A pre-flight refusal list, not a type mapping — delta-rs owns Arrow→Delta conversion. This
/// exists so a type Delta cannot represent fails at BeginWriteAsync with a named column, instead of
/// surfacing as a Rust-side error partway through a write with no pz context attached.
///
/// The set below is OBSERVED: DeltaTypeSupportTests writes each candidate through a real delta-rs
/// create AND a real delta-rs insert and asserts this predicate agrees with the stricter of the two.
/// When delta-rs gains a type, the test fails and this list changes — never the other way round.</summary>
internal static class DeltaTypeSupport
{
    public static bool IsWritable(IArrowType type) => type.TypeId switch
    {
        // Delta's physical types have no interval or duration; a time-of-day column has no Delta
        // counterpart either. Everything else in the candidate set round-trips.
        ArrowTypeId.Interval or ArrowTypeId.Duration or ArrowTypeId.Time32 or ArrowTypeId.Time64 => false,
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
