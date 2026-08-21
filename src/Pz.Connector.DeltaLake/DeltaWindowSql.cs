using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>Wraps a delta_scan fragment in the incremental window's predicate so DuckDB can skip data
/// files using the transaction log's per-file min/max statistics. Pushing the bound is what turns a
/// full-table scan into a few-file scan; not pushing it is still correct, just slow.
///
/// A lower bound alone is pushed too — for a file-listing source that buys nothing, but here it is the
/// whole point.</summary>
internal static class DeltaWindowSql
{
    public static string Wrap(string fragment, DatasetSpec spec)
    {
        if (spec.WatermarkCursor is null)
        {
            return fragment;
        }

        if (spec.WatermarkValue is null)
        {
            // A cursor name with no value is what the engine stamps on a first run / --full-refresh,
            // which is equivalent to no watermark at all -- unless an upper bound came with it, which
            // would mean the engine's own pairing invariant broke.
            if (spec.WatermarkUpperBound is not null)
            {
                throw new InvalidOperationException(
                    "DeltaWindowSql.Wrap: WatermarkUpperBound is set but WatermarkValue is null — the " +
                    "engine always pairs a windowed dataset's lower and upper bounds together");
            }

            return fragment;
        }

        var cursor = QuoteIdentifier(spec.WatermarkCursor);
        var lowerOp = spec.WatermarkLowerInclusive ? ">=" : ">";
        var predicate = $"{cursor} {lowerOp} '{EscapeLiteral(spec.WatermarkValue)}'";

        if (spec.WatermarkUpperBound is not null)
        {
            predicate += $" and {cursor} <= '{EscapeLiteral(spec.WatermarkUpperBound)}'";
        }

        return $"(select * from {fragment} where {predicate})";
    }

    private static string QuoteIdentifier(string identifier) => $"\"{identifier.Replace("\"", "\"\"")}\"";

    private static string EscapeLiteral(string value) => value.Replace("'", "''");
}
