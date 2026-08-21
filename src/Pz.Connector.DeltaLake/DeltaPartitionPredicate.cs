using System.Globalization;
using Apache.Arrow;

namespace Pz.Connector.DeltaLake;

/// <summary>Either the filters to narrow a merge's target scan, or the reason none could be derived —
/// the reason is surfaced to the user so a slow merge is explained rather than mysterious. Both null
/// means there was nothing to derive at all (no partition columns, or no rows), which needs no
/// explaining.
///
/// A reason names COLUMNS but never VALUES: it reaches run artifacts and logs, and a partition value is
/// user data.</summary>
internal sealed record DerivationOutcome(IReadOnlyList<PartitionFilter>? Filters, string? SkipReason);

/// <summary>Derives a partition predicate from the data being merged, and refuses to when doing so
/// would be unsound.
///
/// THE RULE: derivation is sound only when every partition column is also a merge key. Otherwise a
/// row's partition value can change while its key does not; the derived predicate then hides that
/// row's CURRENT partition from the target scan, the merge sees NOT MATCHED, and it inserts a second
/// copy. A silent duplicate is worse than a slow merge, so an unsound derivation is skipped, never
/// approximated. When the rule does hold, the ON clause already requires target.p = source.p for every
/// partition column p, so the IN list adds no selectivity the merge did not already have — it only
/// tells the scan where to look, which is exactly why it cannot change the result.
///
/// This is also the enforcement point for what a partition literal may be. Everything it emits is
/// concatenated into SQL by <see cref="DeltaMergeSql"/>, which is the last place that could catch a
/// malformed one and has no way to know a value's type or provenance; the check it applies there is
/// defence in depth, not the guarantee. The guarantee is here: every literal is one complete,
/// self-contained SQL literal for the column's own Arrow type — a bare decimal token or a
/// '\''-quoted string with every embedded quote doubled — built from the incoming batch's own values
/// and never from configuration or free text.</summary>
internal static class DeltaPartitionPredicate
{
    /// <summary>Beyond this many distinct values an IN list costs more to parse and plan than the
    /// pruning saves.</summary>
    internal const int MaxDistinctValues = 256;

    public static DerivationOutcome Derive(IReadOnlyList<RecordBatch> batches, DeltaWriteOptions options)
    {
        if (options.PartitionBy.Count == 0)
        {
            return new DerivationOutcome(null, null);
        }

        // Ordinal, and it has to stay Ordinal: Delta column names are case-sensitive, so a key spelled
        // "DT" does not name the partition column "dt" and must not be read as covering it.
        var notKeys = options.PartitionBy.Where(p => !options.Keys.Contains(p, StringComparer.Ordinal)).ToList();
        if (notKeys.Count > 0)
        {
            return new DerivationOutcome(null,
                $"partition column(s) {string.Join(", ", notKeys.Select(p => $"'{p}'"))} are not listed in " +
                "'keys', so a derived partition predicate could duplicate rows whose partition value " +
                "changed. Set 'merge_predicate' to narrow the merge explicitly, or add the partition " +
                "column(s) to 'keys'");
        }

        var filters = new List<PartitionFilter>(options.PartitionBy.Count);

        foreach (var column in options.PartitionBy)
        {
            // SortedSet orders the QUOTED literals, which is what makes the generated SQL byte-stable
            // across runs. Numeric columns therefore sort lexically — irrelevant to an IN list, and it
            // keeps determinism a property of one comparer rather than of each type's ordering.
            var literals = new SortedSet<string>(StringComparer.Ordinal);

            foreach (var batch in batches)
            {
                // The ordinal lookup is what makes the emitted Column a name from the Arrow schema
                // rather than the user's text: a partition_by entry that does not match a column
                // exactly skips the derivation instead of naming a column the statement does not have.
                // The comparer is passed explicitly so this does not rest on a library default.
                var index = batch.Schema.GetFieldIndex(column, StringComparer.Ordinal);
                if (index < 0)
                {
                    return new DerivationOutcome(null,
                        $"partition column '{column}' is not present in the data being written, so this " +
                        "connector cannot tell which partitions the merge touches");
                }

                var array = batch.Column(index);
                for (var row = 0; row < array.Length; row++)
                {
                    if (array.IsNull(row))
                    {
                        return new DerivationOutcome(null,
                            $"partition column '{column}' contains a null value, which an IN list cannot " +
                            "express — IN (NULL) matches nothing, so the predicate would hide exactly the " +
                            "rows it has to find. Set 'merge_predicate' to narrow the merge explicitly");
                    }

                    if (Literal(array, row) is not { } literal)
                    {
                        return new DerivationOutcome(null,
                            $"partition column '{column}' has type {array.Data.DataType.Name}, which this " +
                            "connector does not render as a SQL literal. Set 'merge_predicate' to narrow the " +
                            "merge explicitly");
                    }

                    literals.Add(literal);
                    if (literals.Count > MaxDistinctValues)
                    {
                        return new DerivationOutcome(null,
                            $"this write touches more than {MaxDistinctValues} distinct values of partition " +
                            $"column '{column}'. Set 'merge_predicate' to narrow the merge explicitly");
                    }
                }
            }

            if (literals.Count == 0)
            {
                // No rows anywhere for this column, so there are no partitions to name. A
                // PartitionFilter with an empty list renders "IN ()", which is a parser error inside
                // the merge rather than anything a user could act on — and a write with no rows has
                // nothing to prune in the first place, so there is nothing to explain either.
                return new DerivationOutcome(null, null);
            }

            filters.Add(new PartitionFilter(column, [.. literals]));
        }

        return new DerivationOutcome(filters, null);
    }

    /// <summary>One complete SQL literal for the value at <paramref name="row"/>, or null for any type
    /// this connector will not render — guessing a literal form for an unfamiliar type is how a
    /// predicate silently stops matching, and skipping the derivation only costs speed.
    ///
    /// Every form below was measured end to end against the shipped delta-rs: a table partitioned on a
    /// column of that type, merged with the literal this method produces, matches its existing row
    /// rather than inserting a second copy. Three deliberate absences:
    /// <list type="bullet">
    /// <item><description>BOOLEAN. Measured: DataFusion accepts IN (true) and refuses IN ('true') with
    /// "Cannot infer common argument type for comparison operation Boolean = Utf8". The one form that
    /// works is neither a bare number nor a quoted string, so it is outside the alphabet the statement
    /// generator accepts from this method, and emitting it would turn a merge into a hard failure. A
    /// boolean column has at most two partitions, so the pruning forgone is at most half a scan.</description></item>
    /// <item><description>FLOAT/DOUBLE. Equality against a value that round-trips through a decimal
    /// rendering and a directory name is exactly the silent non-match this method exists to avoid.</description></item>
    /// <item><description>TIMESTAMP. Its rendering depends on unit and time zone, so a single format
    /// string here would be a guess about the value, not about the type.</description></item>
    /// <item><description>Both of the last two were observed to match for one convenient value, which
    /// is not the same as being right for every value — the difference is the whole reason they are
    /// absent, so an observation of one value is not grounds to add them.</description></item>
    /// </list></summary>
    private static string? Literal(IArrowArray array, int row) => array switch
    {
        // The same logical type arrives in any of three encodings depending on the plan that produced
        // the batch, not on the column's Delta type; the literal is identical for all three.
        StringArray a => Quote(a.GetString(row)),
        StringViewArray a => Quote(a.GetString(row)),
        LargeStringArray a => Quote(a.GetString(row)),
        Int8Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        Int16Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        Int32Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        Int64Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        // Delta has no unsigned types: delta-rs casts an unsigned column to Int64 on the way in and
        // fails the write outright on a value that does not fit ("Can't cast value ... to type Int64"),
        // so a bare decimal token is always in range for any value that can reach a partition.
        UInt8Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        UInt16Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        UInt32Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        UInt64Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        Date32Array a => Quote(a.GetDateTime(row)!.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
        Date64Array a => Quote(a.GetDateTime(row)!.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
        _ => null,
    };

    /// <summary>Doubling the quote is the whole escape: this SQL dialect applies no backslash escaping
    /// inside an unprefixed '...' literal, measured against the shipped parser rather than assumed —
    /// a value ending in a backslash is merged and matched by MergeSafetyExecutionTests. If that ever
    /// changed, "a\" would escape its own closing quote and the literal would swallow the SQL after it,
    /// which is why the value that reaches a merge from DATA, where nobody has to be malicious for it
    /// to be hostile, is pinned by an execution test rather than by this comment.</summary>
    private static string Quote(string value) => $"'{value.Replace("'", "''")}'";
}
