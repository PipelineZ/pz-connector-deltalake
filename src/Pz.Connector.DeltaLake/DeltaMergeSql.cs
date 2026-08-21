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
/// An unconstrained merge scans the whole table, so its cost grows with the TABLE, not the input.
///
/// A user-supplied merge_predicate may only compare the output's own columns against literals. Function
/// calls, casts, CASE and subqueries are refused — a deliberate limit, not an oversight: the predicate
/// is interpolated into a statement handed to a SQL engine this connector does not own, and the
/// vocabulary that engine accepts is not a vocabulary this connector can enumerate. A subquery is worse
/// than merely unsupported there, because it aborts inside native code before any error can be raised
/// and the merge call then never returns. Compute a derived value in the pipeline's own SQL and compare
/// against a column here.</summary>
internal static class DeltaMergeSql
{
    /// <summary>A merge_predicate must be a predicate, not an arbitrary SQL tail. Statement
    /// terminators and comment introducers would let it append a second statement to the generated
    /// MERGE, so they are refused outright rather than escaped.
    ///
    /// This match is deliberately quote-BLIND, so <c>target.dt = 'a;b'</c> is refused even though the
    /// separator is inside a string literal. That costs a legal predicate, and it is worth it: the
    /// value of this check is that it holds no matter what the quote model below gets wrong. Teaching
    /// it about quoted regions would couple the two controls, and one bug in the quote model would
    /// then disable both at once.</summary>
    private static readonly Regex Forbidden = new(@"(;|--|/\*|\*/)", RegexOptions.Compiled);

    /// <summary>Characters refused anywhere in a merge_predicate, inside quotes as well as outside.
    /// Each one opens a lexical region this connector does not model: <c>$</c> begins a dollar-quoted
    /// string (<c>$$…$$</c>, <c>$tag$…$tag$</c>) whose contents DataFusion treats as string bytes; a
    /// backtick begins a backtick-delimited identifier; a backslash is the escape character inside the
    /// prefixed string literals refused below; and NUL terminates the string for anything that reads
    /// it as C text. Any of them lets a predicate hide a parenthesis from the scan below while
    /// DataFusion still sees it — or the reverse — which is exactly how a predicate escapes the
    /// parenthesised group it is interpolated into.</summary>
    private static readonly char[] Unmodelled = ['$', '`', '\\', '\0'];

    /// <summary>The complete operator vocabulary a merge predicate may use, longest first so a run of
    /// operator characters decomposes greedily (<c>&lt;=</c> before <c>&lt;</c>).</summary>
    private static readonly string[] Operators = ["<=", ">=", "<>", "!=", "=", "<", ">", "+", "-", "*", "/"];

    private const string OperatorChars = "=<>!+-*/";

    /// <summary>The complete word vocabulary. Every other bare word in a predicate has to be a column of
    /// the output's own schema, which is what keeps SELECT, FROM, EXISTS and every function name out
    /// without this connector having to know their names. Compared case-insensitively because SQL
    /// keywords are; column names are not, and are compared ordinally.</summary>
    private static readonly string[] Keywords =
        ["AND", "OR", "NOT", "IS", "NULL", "IN", "BETWEEN", "LIKE", "TRUE", "FALSE"];

    /// <summary>The two table aliases delta-rs fixes for a merge. A qualified reference may only use
    /// these; anything else names a table this statement does not have.</summary>
    private static readonly string[] Qualifiers = ["target", "source"];

    /// <summary>A partition literal that is a bare number: an optional sign, digits, an optional
    /// fraction, an optional exponent, and nothing else. Anything with more structure than this has to
    /// arrive as a quoted string literal instead.</summary>
    private static readonly Regex NumericLiteral = new(
        @"\A[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?\z", RegexOptions.Compiled);

    public static string Build(
        Schema schema, DeltaWriteOptions options, IReadOnlyList<PartitionFilter>? partitionFilters)
    {
        var columns = schema.FieldsList.Select(f => f.Name).ToList();
        var keys = options.Keys;

        // Ordinal, and it has to stay Ordinal: Delta column names are case-sensitive, so a key spelled
        // "ID" does not name the column "id" and must not silently exclude it from the UPDATE SET.
        var nonKeys = columns.Where(c => !keys.Contains(c, StringComparer.Ordinal)).ToList();

        var on = string.Join(" AND ", keys.Select(k => $"target.{Q(k)} = source.{Q(k)}"));

        if (options.MergePredicate is { } predicate)
        {
            // The predicate is interpolated as "AND (<predicate>)". A predicate that closes that
            // paren early and opens a fresh one — e.g. "1=1) OR (1=1" — turns the whole ON clause
            // into an expression that is true for every row, independent of the key match; one that
            // leaves a paren open lets the generator's own trailing ')' close a clause the predicate
            // opened, which is how a predicate reaches past the ON clause and injects its own WHEN
            // clause. Both results are well-formed SQL, so the terminator blocklist cannot see them.
            if (Forbidden.IsMatch(predicate)
                || predicate.AsSpan().IndexOfAny(Unmodelled) >= 0
                || IsBlank(predicate)
                || !IsWellFormedPredicate(predicate, columns))
            {
                throw DeltaErrors.Fail(DeltaErrors.InvalidMergePredicate,
                    "'merge_predicate' must be a single, self-contained boolean expression written only " +
                    "from this output's own columns (optionally qualified 'target.' or 'source.'), " +
                    "'\''-quoted string literals, numbers, balanced parentheses, the operators " +
                    "= <> != < > <= >= + - * / and the words AND OR NOT IS NULL IN BETWEEN LIKE TRUE " +
                    "FALSE; it may not be blank, and it may not contain a statement separator, a " +
                    "comment, an unbalanced parenthesis, a dollar sign, a backtick, a backslash, or a " +
                    "prefixed string literal",
                    "write it as a plain comparison over this output's columns, e.g. merge_predicate: " +
                    "\"target.dt >= '2026-01-01'\". Function calls, casts, CASE and subqueries are not " +
                    "accepted here: derive the value in the pipeline's SQL and compare a column to it");
            }

            on += $" AND ({predicate})";
        }

        // One IN list per partition column. Across several columns this describes a SUPERSET of the
        // (p1, p2, ...) combinations actually present, which is exactly the right direction of error:
        // a wider predicate scans more files, and can never hide a row from the match.
        foreach (var filter in partitionFilters ?? [])
        {
            // The producer of a PartitionFilter is the enforcement point for what a literal may be —
            // it is the only party that knows the partition column's type and where the value came
            // from. This is defence in depth: the one place that concatenates literals into SQL should
            // not depend entirely on its caller. An empty list is refused for a different reason: it
            // renders "IN ()", which is a parser error at merge time rather than a coded config error.
            if (filter.Literals.Count == 0 || !filter.Literals.All(IsSelfContainedLiteral))
            {
                throw DeltaErrors.Fail(DeltaErrors.InvalidMergePredicate,
                    $"the partition filter derived for column '{filter.Column}' is not usable: a filter " +
                    "must carry at least one value, and each value must be a bare number or a single " +
                    "quoted string literal that ends where the value ends",
                    "narrow the merge with 'merge_predicate' instead, or report this as a connector bug " +
                    "with the partition column's type");
            }

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

    private static bool IsBlank(string predicate)
    {
        foreach (var c in predicate)
        {
            if (!char.IsWhiteSpace(c))
            {
                return false;
            }
        }

        // A blank predicate renders "AND (   )", which fails at merge time with a raw parser error. It
        // is a configuration mistake and is reported as one, rather than normalized to "absent": an
        // option the user wrote and this connector silently ignored is its own kind of wrong answer.
        return true;
    }

    /// <summary>True when the predicate is built entirely from the vocabulary a merge predicate needs,
    /// every word in it is a permitted keyword or a column of <paramref name="columns"/>, and its
    /// parentheses balance outside quoted regions.
    ///
    /// This is an ALLOWLIST, and that is the whole point. The predicate is handed to a lexer this
    /// connector does not own, cannot see and does not version-pin, so any check shaped as "block the
    /// forms I know about" has to agree with that lexer byte for byte forever and silently opens a
    /// hole the first time the foreign lexer learns a new one. Refusing everything outside a fixed,
    /// small vocabulary fails the other way: an unknown construct is refused, not passed through. That
    /// applies to WORDS exactly as it applies to characters, which is why an unrecognized bare word is
    /// refused rather than a list of dangerous ones being blocked: SELECT, FROM and EXISTS are only the
    /// three that happen to be known to abort delta-rs inside native code, where the merge call then
    /// never returns and there is no error for anything to report.
    ///
    /// So a predicate may contain: a column of this output's schema, optionally qualified 'target.' or
    /// 'source.' and optionally '"'-quoted; a keyword from the list above; a '\''-quoted string literal
    /// (doubled-character escape only, which is the sole escape this SQL dialect applies to it); a
    /// number; whitespace; ','; balanced parentheses; and the operators listed above.
    ///
    /// Balance is checked here rather than in a second pass because both checks need the same model of
    /// where quoted regions start and end. Two scanners carrying two copies of that model is the same
    /// must-agree-forever problem this allowlist exists to escape, one scope smaller.
    ///
    /// This is a lexical scan, not SQL parsing: it recognizes token boundaries, resolves each word
    /// against a name list, and tracks paren depth. It makes no attempt to understand the
    /// expression.</summary>
    private static bool IsWellFormedPredicate(string predicate, IReadOnlyList<string> columns)
    {
        var depth = 0;
        var i = 0;

        while (i < predicate.Length)
        {
            var c = predicate[i];

            if (char.IsWhiteSpace(c) || c == ',')
            {
                i++;
            }
            else if (c == '(')
            {
                depth++;
                i++;
            }
            else if (c == ')')
            {
                depth--;
                if (depth < 0)
                {
                    return false;
                }

                i++;
            }
            else if (c == '\'')
            {
                // A quote directly after an identifier character is a string-literal PREFIX: E'…',
                // e'…', U&'…', R'…', N'…', X'…', B'…'. Refusing the whole shape kills the class in one
                // rule rather than one prefix letter at a time, which matters because the set of
                // recognized prefixes belongs to the foreign lexer. It matters most for E'…', whose
                // contents are backslash-escaped: there, a quote can be escaped rather than doubled,
                // so the region this scanner measures and the region DataFusion measures end in
                // different places, and a parenthesis falls on opposite sides of the two.
                if (i > 0 && IsIdentifierChar(predicate[i - 1]))
                {
                    return false;
                }

                var end = SkipQuoted(predicate, i, c);
                if (end < 0)
                {
                    return false;
                }

                i = end;
            }
            else if (c == '"' || char.IsAsciiLetter(c) || c == '_')
            {
                if (!TryReadWord(predicate, ref i, out var word))
                {
                    return false;
                }

                if (i < predicate.Length && predicate[i] == '.')
                {
                    // Only the two aliases delta-rs fixes for a merge may qualify a reference, and only
                    // a column may follow the dot. A third part ("target.dt.x") needs no check of its
                    // own: '.' is not a token anywhere else, so the second dot falls through to the
                    // refusal at the end of this loop.
                    i++;
                    if (!Qualifiers.Contains(word, StringComparer.Ordinal)
                        || !TryReadWord(predicate, ref i, out var column)
                        || !columns.Contains(column, StringComparer.Ordinal))
                    {
                        return false;
                    }
                }
                else if (!Keywords.Contains(word, StringComparer.OrdinalIgnoreCase)
                         && !columns.Contains(word, StringComparer.Ordinal))
                {
                    return false;
                }
            }
            else if (char.IsAsciiDigit(c))
            {
                SkipNumber(predicate, ref i);
            }
            else if (OperatorChars.Contains(c))
            {
                var start = i;
                while (i < predicate.Length && OperatorChars.Contains(predicate[i]))
                {
                    i++;
                }

                if (!IsOperatorRun(predicate.AsSpan(start, i - start)))
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        return depth == 0;
    }

    /// <summary>Reads one word — a bare identifier or a '"'-quoted one — advancing
    /// <paramref name="i"/> past it and yielding the name with the quoting removed. False if there is
    /// no word there, if a quoted one never closes, or if a '"' sits directly after an identifier
    /// character, which is the string-prefix shape refused above.</summary>
    private static bool TryReadWord(string s, ref int i, out string word)
    {
        word = string.Empty;

        if (i >= s.Length)
        {
            return false;
        }

        if (s[i] == '"')
        {
            if (i > 0 && IsIdentifierChar(s[i - 1]))
            {
                return false;
            }

            var end = SkipQuoted(s, i, '"');
            if (end < 0)
            {
                return false;
            }

            word = s[(i + 1)..(end - 1)].Replace("\"\"", "\"");
            i = end;
            return true;
        }

        if (!char.IsAsciiLetter(s[i]) && s[i] != '_')
        {
            return false;
        }

        var start = i;
        i++;
        while (i < s.Length && IsIdentifierChar(s[i]))
        {
            i++;
        }

        word = s[start..i];
        return true;
    }

    /// <summary>Advances past a numeric literal: digits, an optional fraction, an optional exponent.
    /// A trailing 'e' with no digits after it is left where it is, so the word rules above see it and
    /// refuse it rather than this method swallowing a name.</summary>
    private static void SkipNumber(string s, ref int i)
    {
        while (i < s.Length && char.IsAsciiDigit(s[i]))
        {
            i++;
        }

        if (i < s.Length && s[i] == '.')
        {
            i++;
            while (i < s.Length && char.IsAsciiDigit(s[i]))
            {
                i++;
            }
        }

        if (i >= s.Length || (s[i] != 'e' && s[i] != 'E'))
        {
            return;
        }

        var mark = i;
        i++;
        if (i < s.Length && (s[i] == '+' || s[i] == '-'))
        {
            i++;
        }

        if (i < s.Length && char.IsAsciiDigit(s[i]))
        {
            while (i < s.Length && char.IsAsciiDigit(s[i]))
            {
                i++;
            }
        }
        else
        {
            i = mark;
        }
    }

    /// <summary>True when a run of operator characters decomposes into a sequence of known operators.
    /// A run rather than a character at a time, so a lone '!' — which is not an operator on its own —
    /// is refused while "!=" is not.</summary>
    private static bool IsOperatorRun(ReadOnlySpan<char> run)
    {
        while (!run.IsEmpty)
        {
            var matched = false;
            foreach (var op in Operators)
            {
                if (run.StartsWith(op))
                {
                    run = run[op.Length..];
                    matched = true;
                    break;
                }
            }

            if (!matched)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>The index one past the closing quote of the region starting at <paramref name="start"/>,
    /// or -1 if the region never closes. A doubled quote character escapes itself and the region
    /// continues — the only escape this SQL dialect applies to an unprefixed literal, confirmed against
    /// the shipped parser: a backslash inside <c>'…'</c> is an ordinary byte, not an escape.</summary>
    private static int SkipQuoted(string s, int start, char quote)
    {
        var i = start + 1;
        while (i < s.Length)
        {
            if (s[i] == quote)
            {
                if (i + 1 < s.Length && s[i + 1] == quote)
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }

            i++;
        }

        return -1;
    }

    /// <summary>True when a partition-filter value is one complete literal and nothing more: a bare
    /// number, or a single-quoted string whose closing quote is its last character. The closing-quote
    /// rule is what separates a value containing a comma or a semicolon — legal content, admitted —
    /// from a second value smuggled into one slot, such as <c>'a', 'b'</c>.</summary>
    private static bool IsSelfContainedLiteral(string literal) =>
        literal.Length > 0
        && (NumericLiteral.IsMatch(literal)
            || (literal[0] == '\'' && SkipQuoted(literal, 0, '\'') == literal.Length));

    private static bool IsIdentifierChar(char c) => char.IsAsciiLetterOrDigit(c) || c == '_';

    private static string Q(string identifier) => $"\"{identifier.Replace("\"", "\"\"")}\"";
}
