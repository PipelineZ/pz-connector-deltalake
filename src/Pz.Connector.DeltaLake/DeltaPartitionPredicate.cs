using System.Globalization;
using Apache.Arrow;

namespace Pz.Connector.DeltaLake;

/// <summary>Either the filters to narrow a merge's target scan, or the reason none could be derived.
/// Both null means there was nothing to derive at all (no partition columns, or no rows).
///
/// <see cref="SkipReason"/> is NOT surfaced to the user, and nothing in this assembly reads it. There
/// is nowhere to put it: connector ABI 0.2.2 gives a sink no note, warning or logger channel, and
/// WriteResult carries only a row and a batch count — so a skipped derivation is invisible, and
/// DeltaWriteSession's LastSkipReason is a test seam and only that. It is built to be SAFE to surface
/// the day the ABI grows a channel additively: it names COLUMNS and character classes, never a value,
/// because a partition value is user data. That is a property held in reserve, not a behaviour that
/// exists.</summary>
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
/// self-contained SQL literal for the column's own Arrow type — a bare decimal token, or a quoted
/// string that is exactly two quote characters with the value between them and NO quote or backslash
/// inside — built from the incoming batch's own values and never from configuration or free text.
/// A value that cannot meet that shape is refused rather than escaped; <see cref="Render"/> records
/// what escaping was measured to do instead.</summary>
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

                    var rendered = Render(array, row);
                    if (rendered.Literal is not { } literal)
                    {
                        return new DerivationOutcome(null,
                            $"partition column '{column}' {rendered.Because}. Set 'merge_predicate' to " +
                            "narrow the merge explicitly");
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

    /// <summary>One literal, or the reason there is none — the reason completes the sentence
    /// "partition column 'x' …". The two refusals are kept apart because they send a reader to
    /// different places: an unrenderable TYPE is a property of the column and refuses every row of it,
    /// while an unrenderable VALUE says nothing about the column's type and may refuse one row out of
    /// millions. Neither ever carries the value itself.</summary>
    private readonly record struct Rendered(string? Literal, string? Because);

    /// <summary>Renders one value, or refuses it. Refusing costs speed and nothing else, which is why
    /// every branch below refuses rather than approximates.
    ///
    /// Every form this produces was measured end to end against the shipped delta-rs: a table
    /// partitioned on a column of that type, merged with the literal produced here, matches its
    /// existing row rather than inserting a second copy. Four deliberate absences:
    /// <list type="bullet">
    /// <item><description>BOOLEAN. Measured: DataFusion accepts IN (true) and refuses IN ('true') with
    /// "Cannot infer common argument type for comparison operation Boolean = Utf8". The one form that
    /// works is neither a bare number nor a quoted string, so it is outside the alphabet the statement
    /// generator accepts from this method, and emitting it would turn a merge into a hard failure. A
    /// boolean column has at most two partitions, so the pruning forgone is at most half a scan.</description></item>
    /// <item><description>FLOAT/DOUBLE. Equality against a value that round-trips through a decimal
    /// rendering and a directory name is exactly the silent non-match this method exists to avoid, and
    /// it is not hypothetical: -0.0 renders as "-0", names a partition the table does not have, and
    /// duplicates the row.</description></item>
    /// <item><description>TIMESTAMP. Its rendering depends on unit and time zone, and the obvious
    /// ISO-8601 form silently truncates sub-second precision and duplicates. One format string here
    /// would be a guess about the value, not about the type.</description></item>
    /// <item><description>DICTIONARY-encoded columns. Not a rendering question at all: measured, a
    /// dictionary-encoded PARTITION column cannot be written to a Delta table by this library in the
    /// first place ("Error partitioning record batch: Missing partition column"), so a literal derived
    /// from one would narrow a merge that is already doomed. A dictionary-encoded ordinary column
    /// writes fine, and is never read here.</description></item>
    /// </list></summary>
    private static Rendered Render(IArrowArray array, int row)
    {
        if (Text(array, row) is { } text)
        {
            // A quote or a backslash in a partition value is refused, never escaped. Measured against
            // the shipped library: a value carrying '' merges as though it carried one quote, so the
            // IN list names a partition the table does not have, the target row stays invisible to the
            // scan, and the merge inserts a second copy of a key it should have matched — silently.
            // A value carrying \' fails the whole write with an unterminated-literal parser error.
            // The narrower rule that admits it's and back\ is a list of the shapes one build of one
            // parser mishandles, and it readmits the duplicate the moment that parser's unescaping
            // shifts; refusing both characters outright cannot fail that way. The cost is no pruning
            // on a value carrying either character, which costs speed and never a row.
            //
            // This is one of three refusals in this connector resting on that one measured premise --
            // see DeltaMergeSql's type doc comment for the other two. All three have execution tests
            // against real delta-rs, so a version bump that changes the unescaping fails them together;
            // they are to be re-decided as a set, never relaxed one at a time.
            return text.AsSpan().IndexOfAny('\'', '\\') >= 0
                ? new Rendered(null,
                    "holds a value this connector does not render as a SQL literal: a partition value " +
                    "carrying a quote or a backslash is refused rather than escaped, because how a SQL " +
                    "engine unescapes one is not something this connector can depend on")
                : new Rendered(Quote(text), null);
        }

        if (array is Date32Array or Date64Array)
        {
            return Day(array, row) is { } day
                ? new Rendered(Quote(day), null)
                : new Rendered(null,
                    "holds a date outside the range this connector renders as a SQL literal");
        }

        return Number(array, row) is { } number
            ? new Rendered(number, null)
            : new Rendered(null,
                $"has type {array.Data.DataType.Name}, which this connector does not render as a SQL " +
                "literal");
    }

    /// <summary>The value as text when the column is one of the string encodings, null for every other
    /// type. One extractor rather than a branch per encoding at each use, so nothing can disagree about
    /// which arrays are strings. The same logical type arrives in any of these encodings depending on
    /// the plan that produced the batch, not on the column's Delta type.</summary>
    private static string? Text(IArrowArray array, int row) => array switch
    {
        StringArray a => a.GetString(row),
        StringViewArray a => a.GetString(row),
        LargeStringArray a => a.GetString(row),
        _ => null,
    };

    /// <summary>A bare decimal token, or null for anything that is not an integer column. Delta has no
    /// unsigned types: delta-rs casts an unsigned column to Int64 on the way in and fails the write
    /// outright on a value that does not fit ("Can't cast value ... to type Int64"), so a bare decimal
    /// token is always in range for any value that can reach a partition.</summary>
    private static string? Number(IArrowArray array, int row) => array switch
    {
        Int8Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        Int16Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        Int32Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        Int64Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        UInt8Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        UInt16Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        UInt32Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        UInt64Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        _ => null,
    };

    /// <summary>The calendar date, or null when the day count is outside what .NET can express. Arrow
    /// DATE is an int32 day count that reaches far past DateTime's year 9999 — measured: 3_000_000,
    /// int.MaxValue and int.MinValue all raise ArgumentOutOfRangeException out of GetDateTime — and pz's
    /// own hub supports dates past 9999, so the conversion is guarded rather than trusted. Derive runs
    /// on the buffered batch BEFORE the merge, so an exception escaping here would abort a write that
    /// would otherwise have succeeded: this file's contract is that a value it cannot handle costs
    /// speed, never the write.</summary>
    private static string? Day(IArrowArray array, int row)
    {
        try
        {
            var value = array switch
            {
                Date32Array a => a.GetDateTime(row),
                Date64Array a => a.GetDateTime(row),
                _ => (DateTime?)null,
            };

            return value?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }

    /// <summary>Wraps a value that has already been refused if it contained a quote or a backslash, so
    /// there is nothing left to escape: the literal is exactly two quotes with the value between them.
    /// Escaping is not the alternative that was passed over here — it was measured to be the thing that
    /// goes wrong. See <see cref="Render"/>.</summary>
    private static string Quote(string value) => $"'{value}'";
}
