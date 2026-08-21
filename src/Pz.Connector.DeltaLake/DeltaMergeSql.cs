using System.Text.RegularExpressions;
using Apache.Arrow;

namespace Pz.Connector.DeltaLake;

/// <summary>One partition column and the literal values this write touches, used to narrow the merge's
/// target scan. Literals arrive already quoted and escaped from the deriver.</summary>
internal sealed record PartitionFilter(string Column, IReadOnlyList<string> Literals);

/// <summary>Generates the DataFusion MERGE statement delta-rs executes. The target is always aliased
/// `target` and the incoming batches are always `source` — delta-rs fixes both names, and a bare
/// predicate is rejected by its SQL parser, so the whole statement must be produced here.
///
/// The ON clause is the only thing that decides what a merge costs: it is what prunes the target scan.
/// An unconstrained merge scans the whole table, so its cost grows with the TABLE, not the input.</summary>
internal static class DeltaMergeSql
{
    /// <summary>A merge_predicate must be a predicate, not an arbitrary SQL tail. Statement
    /// terminators and comment introducers would let it append a second statement to the generated
    /// MERGE, so they are refused outright rather than escaped.</summary>
    private static readonly Regex Forbidden = new(@"(;|--|/\*|\*/)", RegexOptions.Compiled);

    public static string Build(
        Schema schema, DeltaWriteOptions options, IReadOnlyList<PartitionFilter>? partitionFilters)
    {
        var columns = schema.FieldsList.Select(f => f.Name).ToList();
        var keys = options.Keys;
        var nonKeys = columns.Where(c => !keys.Contains(c, StringComparer.Ordinal)).ToList();

        var on = string.Join(" AND ", keys.Select(k => $"target.{Q(k)} = source.{Q(k)}"));

        if (options.MergePredicate is { Length: > 0 } predicate)
        {
            // The predicate is interpolated as "AND (<predicate>)". A predicate that closes that
            // paren early and opens a fresh one — e.g. "1=1) OR (1=1" — turns the whole ON clause
            // into an expression that is true for every row, independent of the key match: the
            // terminator/comment blocklist below cannot see this because the result is well-formed
            // SQL, not a second statement. Rejecting on unbalanced parens closes that escape.
            if (Forbidden.IsMatch(predicate) || !HasBalancedParens(predicate))
            {
                throw DeltaErrors.Fail(DeltaErrors.InvalidMergePredicate,
                    "'merge_predicate' must be a single, self-contained boolean expression; it may not " +
                    "contain a statement separator, a comment, or an unbalanced parenthesis",
                    "write it as a plain predicate, e.g. merge_predicate: \"target.dt >= '2026-01-01'\"");
            }

            on += $" AND ({predicate})";
        }

        // One IN list per partition column. Across several columns this describes a SUPERSET of the
        // (p1, p2, ...) combinations actually present, which is exactly the right direction of error:
        // a wider predicate scans more files, and can never hide a row from the match.
        foreach (var filter in partitionFilters ?? [])
        {
            on += $" AND target.{Q(filter.Column)} IN ({string.Join(", ", filter.Literals)})";
        }

        // Key columns are excluded from the UPDATE SET: they are what matched, so assigning them is a
        // no-op that only makes the statement longer.
        var insertColumns = string.Join(", ", columns.Select(Q));
        var insertValues = string.Join(", ", columns.Select(c => $"source.{Q(c)}"));

        var lines = new List<string> { $"MERGE INTO target USING source ON {on}" };

        // When every column is a key, a matched row is identical in every column to the row it
        // matched — there is nothing left for an UPDATE to change. Omitting WHEN MATCHED rather than
        // emitting one with an empty SET keeps the statement valid SQL; the merge still inserts rows
        // whose key is absent, which is the correct "insert if not already present" behavior for a
        // table with no non-key data.
        if (nonKeys.Count > 0)
        {
            var set = string.Join(", ", nonKeys.Select(c => $"target.{Q(c)} = source.{Q(c)}"));
            lines.Add($"WHEN MATCHED THEN UPDATE SET {set}");
        }

        lines.Add($"WHEN NOT MATCHED THEN INSERT ({insertColumns}) VALUES ({insertValues})");

        return string.Join("\n", lines);
    }

    /// <summary>True when every '(' outside a quoted region has a matching ')' and depth never goes
    /// negative. Quoted regions (single-quoted string literals and double-quoted identifiers, both
    /// using SQL's doubled-character escape) are skipped entirely: a naive counter that does not do
    /// this can be fooled by a predicate like <c>col = '(' OR 1=1) OR (1=1 OR col = ')'</c>, whose raw
    /// character count balances (one quoted '(', one real ')', one real '(', one quoted ')') even
    /// though the two REAL parentheses close the wrapping group early and open a fresh, unconstrained
    /// one — the exact escape this check exists to block, just padded so a quote-blind scan misses
    /// it. This is a lexical scan, not a SQL parser: it tracks quote state and paren depth only.</summary>
    private static bool HasBalancedParens(string predicate)
    {
        var depth = 0;
        char? quote = null;

        for (var i = 0; i < predicate.Length; i++)
        {
            var c = predicate[i];

            if (quote is { } q)
            {
                if (c == q)
                {
                    if (i + 1 < predicate.Length && predicate[i + 1] == q)
                    {
                        i++; // A doubled quote character escapes itself; the region continues.
                    }
                    else
                    {
                        quote = null;
                    }
                }

                continue;
            }

            switch (c)
            {
                case '\'' or '"':
                    quote = c;
                    break;
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    if (depth < 0)
                    {
                        return false;
                    }

                    break;
            }
        }

        return depth == 0 && quote is null;
    }

    private static string Q(string identifier) => $"\"{identifier.Replace("\"", "\"\"")}\"";
}
