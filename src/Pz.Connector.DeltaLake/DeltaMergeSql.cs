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
/// A user-supplied merge_predicate may only compare named columns against literals — the output's own
/// columns on the `source.` side, the existing table's on the `target.` side. Function calls, casts,
/// CASE and subqueries are refused — a deliberate limit, not an oversight: the predicate is interpolated
/// into a statement handed to a SQL engine this connector does not own, and the vocabulary that engine
/// accepts is not a vocabulary this connector can enumerate. A subquery is worse than merely unsupported
/// there, because it aborts inside native code before any error can be raised and the merge call then
/// never returns. Compute a derived value in the pipeline's own SQL and compare against a column here.
///
/// THREE REFUSALS IN THIS CONNECTOR SHARE ONE PREMISE, AND MUST BE RE-DECIDED TOGETHER: the doubled
/// quote inside a merge_predicate literal (<see cref="Inspect"/>), the quote or backslash inside a
/// derived partition value (<see cref="DeltaPartitionPredicate"/>'s Render), and the '"' inside a column
/// name (<see cref="RefuseUnquotableNames"/>). All three rest on one measured behaviour: this library's
/// merge path collapses a run of doubled quotes one level further than its own SELECT path does, so a
/// value or name written with an escape compares as a different value or resolves to a different column
/// — silently, as a duplicated row or a stale column. Each has an execution test against real delta-rs,
/// so a version bump that fixes the underlying behaviour fails all three TOGETHER. That is deliberate.
/// Do not relax one because its test started failing; they are a set.</summary>
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

    /// <summary>The complete word vocabulary. Every other word in a predicate has to name a column of
    /// the output's own schema, which is what keeps SELECT, FROM, EXISTS and every function name out
    /// without this connector having to know their names — for every schema whose column names are not
    /// themselves SQL words. An output column literally named "select" would re-admit "IN (select 1)",
    /// and column names come from the pipeline's own SQL, so the author of a merge_predicate can mint
    /// one. That is a limit of this check, not a hole in it: the same author already controls the
    /// predicate, so nothing crosses a privilege boundary, and every OTHER schema is covered.
    ///
    /// Matched case-insensitively, and only against BARE words: a keyword is case-insensitive in this
    /// dialect, but a '"'-quoted token is an identifier and is never a keyword.</summary>
    private static readonly string[] Keywords =
        ["AND", "OR", "NOT", "IS", "NULL", "IN", "BETWEEN", "LIKE", "TRUE", "FALSE"];

    /// <summary>The two table aliases delta-rs fixes for a merge. A qualified reference may only use
    /// these; anything else names a table this statement does not have. Matched ordinally for the same
    /// reason column names are — see <see cref="Inspect"/>; "TARGET.dt" is refused here
    /// and refused by delta-rs.</summary>
    private static readonly string[] Qualifiers = ["target", "source"];

    /// <summary>How a merge_predicate failed inspection. Two outcomes rather than one because the two
    /// need different next steps: an unqualified column reference is a predicate the author almost got
    /// right and can fix by adding one word, and telling them "it must be a boolean expression" would
    /// leave them staring at an expression that already is one.</summary>
    private enum PredicateVerdict
    {
        Ok,
        Unqualified,
        Malformed,
    }

    /// <summary>A partition literal that is a bare number: an optional sign, digits, an optional
    /// fraction, an optional exponent, and nothing else. Anything with more structure than this has to
    /// arrive as a quoted string literal instead.</summary>
    private static readonly Regex NumericLiteral = new(
        @"\A[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?\z", RegexOptions.Compiled);

    /// <summary>Builds the statement. <paramref name="schema"/> is the schema of the batch being
    /// written — the `source` side, and the only side whose columns this generator renders into the ON,
    /// SET and INSERT clauses. <paramref name="targetColumns"/> is the EXISTING TABLE's column list,
    /// used for one thing only: resolving a <c>target.</c>-qualified name in a user's merge_predicate.
    /// The two lists are not interchangeable and neither may stand in for the other — see
    /// <see cref="Inspect"/> for what each of them being wrong would cost.</summary>
    public static string Build(
        Schema schema, IReadOnlyList<string> targetColumns, DeltaWriteOptions options,
        IReadOnlyList<PartitionFilter>? partitionFilters)
    {
        var columns = schema.FieldsList.Select(f => f.Name).ToList();
        var keys = options.Keys;

        RefuseUnquotableNames(columns, keys, partitionFilters);

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
            var verdict = Forbidden.IsMatch(predicate)
                || predicate.AsSpan().IndexOfAny(Unmodelled) >= 0
                || IsBlank(predicate)
                ? PredicateVerdict.Malformed
                : Inspect(predicate, columns, targetColumns);

            if (verdict is PredicateVerdict.Unqualified)
            {
                // A bare name on BOTH sides can never resolve -- measured, not assumed: delta-rs
                // answers one with "Ambiguous reference to unqualified field". A bare name that only
                // the TARGET has does resolve there, measured too, and is refused anyway: whether it
                // resolves depends on the pipeline's SELECT list, so the day that pipeline starts
                // producing a column of the same name the predicate becomes ambiguous and the write
                // fails on a run that changed nothing about the predicate. One word ('target.') costs
                // the author nothing and makes the predicate say what it means. The message therefore
                // says a side is REQUIRED rather than claiming a bare name matches nothing.
                throw DeltaErrors.Fail(DeltaErrors.InvalidMergePredicate,
                    "'merge_predicate' must say which side of the merge each column belongs to: a merge " +
                    "has both an existing row and an incoming one, and this connector does not guess " +
                    "which of them a bare column name meant",
                    "qualify every column with 'target.' for the existing row or 'source.' for the " +
                    "incoming one, e.g. merge_predicate: \"target.dt >= '2026-01-01'\"");
            }

            if (verdict is PredicateVerdict.Malformed)
            {
                throw DeltaErrors.Fail(DeltaErrors.InvalidMergePredicate,
                    "'merge_predicate' must be a single, self-contained boolean expression written only " +
                    "from column names qualified 'target.' (a column of the table being written into) " +
                    "or 'source.' (a column of this output), " +
                    "'\''-quoted string literals carrying no quote of their own, numbers, balanced " +
                    "parentheses, the operators " +
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
                    "quoted string literal that ends where the value ends and carries no quote or " +
                    "backslash of its own",
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

    /// <summary>Inspects the predicate: whether it is built entirely from the vocabulary a merge
    /// predicate needs, whether every word in it is a permitted keyword or a column of
    /// <paramref name="columns"/> qualified with the side it belongs to, and whether its parentheses
    /// balance outside quoted regions.
    ///
    /// This is an ALLOWLIST, and that is the whole point. The predicate is handed to a lexer this
    /// connector does not own, cannot see and does not version-pin, so any check shaped as "block the
    /// forms I know about" has to agree with that lexer byte for byte forever and silently opens a
    /// hole the first time the foreign lexer learns a new one. Refusing everything outside a fixed,
    /// small vocabulary fails the other way: an unknown construct is refused, not passed through. That
    /// applies to WORDS exactly as it applies to characters, which is why an unrecognized word is
    /// refused rather than a list of dangerous ones being blocked: SELECT, FROM and EXISTS are only the
    /// three that happen to be known to abort delta-rs inside native code, where the merge call then
    /// never returns and there is no error for anything to report. The one thing this does not cover is
    /// a schema whose own column names are SQL words — see the keyword table above.
    ///
    /// So a predicate may contain: a column of this output's schema, qualified 'target.' or 'source.'
    /// and optionally '"'-quoted; a keyword from the list above; a '\''-quoted string literal
    /// (doubled-character escape only, which is the sole escape this SQL dialect applies to it); a
    /// number; whitespace; ','; balanced parentheses; and the operators listed above.
    ///
    /// The two sides resolve against two DIFFERENT name lists, and that is the whole reason this method
    /// takes both. <c>source.</c> and bare names resolve against the batch being written;
    /// <c>target.</c> names resolve against the existing table. Under <c>schema_policy: evolve</c> a
    /// table legitimately carries nullable columns a given write does not produce, and narrowing a merge
    /// by one of them (<c>target.archived IS NULL</c>) is the ordinary reason to write a predicate at
    /// all — resolving <c>target.</c> against the batch refuses exactly that, with advice ("name this
    /// output's own columns") the author cannot follow. Measured against the shipped library: a merge
    /// whose source lacks 'archived' runs that predicate fine.
    ///
    /// The qualifier is REQUIRED on a column, and the reason is narrower than "unqualified names are
    /// bad": delta-rs resolves an unqualified name against both sides at once and errors with
    /// "Ambiguous reference to unqualified field" only when the name is on both. Measured: with a
    /// target wider than the incoming batch, an unqualified reference to a target-ONLY column resolves
    /// fine — so refusing a bare name is NOT total, and the refusal message must not claim it is. It is
    /// refused all the same because whether it resolves is a property of the pipeline's SELECT list,
    /// not of the predicate: the same predicate becomes ambiguous, and the write fails, the day the
    /// pipeline starts selecting a column of that name.
    ///
    /// Column names are matched ORDINALLY, quoted or not, because that is what the merge path does with
    /// them — measured against the shipped library, not assumed. Inside a MERGE predicate delta-rs
    /// resolves a bare identifier verbatim and answers a mismatch with "Column names are case
    /// sensitive": against a column 'dt' it accepts 'target.dt' and refuses 'target.DT', and against a
    /// column 'Amt' it accepts 'target.Amt' and refuses 'target.AMT'. Note this is NOT the behavior of
    /// the same engine's ordinary SELECT planner, which lowercases an unquoted identifier first
    /// ('select DT' resolves to column 'dt' there, and bare 'Amt' resolves to nothing at all). Folding
    /// case here to follow that rule would accept predicates the merge then refuses.
    ///
    /// Balance is checked here rather than in a second pass because both checks need the same model of
    /// where quoted regions start and end. Two scanners carrying two copies of that model is the same
    /// must-agree-forever problem this allowlist exists to escape, one scope smaller.
    ///
    /// This is a lexical scan, not SQL parsing: it recognizes token boundaries, resolves each word
    /// against a name list, and tracks paren depth. It makes no attempt to understand the
    /// expression.</summary>
    private static PredicateVerdict Inspect(
        string predicate, IReadOnlyList<string> columns, IReadOnlyList<string> targetColumns)
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
                    return PredicateVerdict.Malformed;
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
                    return PredicateVerdict.Malformed;
                }

                // A doubled quote INSIDE the literal is refused, even though it is this dialect's own
                // escape and the region it delimits is measured correctly here. The reason is what the
                // engine does with the region afterwards: measured against the shipped library, a
                // predicate literal 'a''b' compares as a'b — faithful — while 'a''''b' compares as
                // a'b too, not the a''b its author wrote, and the row that should have matched is
                // inserted a second time instead. Telling the two apart needs a model of a foreign
                // parser's unescaping that is already known to differ between its own paths, so the
                // escape is refused outright. The cost is that a value containing a quote cannot be
                // named in a merge_predicate at all; the alternative is a predicate that silently
                // means something its author did not write. This is one of the three refusals that
                // share that one measured premise -- see the type doc comment. If its execution test
                // starts failing after a version bump, all three are to be re-decided together, not
                // this one on its own.
                var end = SkipQuoted(predicate, i, c);
                if (end < 0 || predicate.AsSpan(i + 1, end - i - 2).Contains('\''))
                {
                    return PredicateVerdict.Malformed;
                }

                i = end;
            }
            else if (c == '"' || char.IsAsciiLetter(c) || c == '_')
            {
                if (!TryReadWord(predicate, ref i, out var word, out var quoted))
                {
                    return PredicateVerdict.Malformed;
                }

                if (i < predicate.Length && predicate[i] == '.')
                {
                    // Only the two aliases delta-rs fixes for a merge may qualify a reference, and only
                    // a column may follow the dot. A third part ("target.dt.x") needs no check of its
                    // own: '.' is not a token anywhere else, so the second dot falls through to the
                    // refusal at the end of this loop.
                    i++;
                    if (!Qualifiers.Contains(word, StringComparer.Ordinal)
                        || !TryReadWord(predicate, ref i, out var column, out _)
                        || !Side(word).Contains(column, StringComparer.Ordinal))
                    {
                        return PredicateVerdict.Malformed;
                    }
                }
                else if (!quoted && Keywords.Contains(word, StringComparer.OrdinalIgnoreCase))
                {
                    // A keyword stands alone; only a column needs a side.
                }
                else if (columns.Contains(word, StringComparer.Ordinal)
                         || targetColumns.Contains(word, StringComparer.Ordinal))
                {
                    return PredicateVerdict.Unqualified;
                }
                else
                {
                    return PredicateVerdict.Malformed;
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
                    return PredicateVerdict.Malformed;
                }
            }
            else
            {
                return PredicateVerdict.Malformed;
            }
        }

        return depth == 0 ? PredicateVerdict.Ok : PredicateVerdict.Malformed;

        // Which name list a qualifier resolves against. 'target' is the table as it stands; 'source' is
        // the batch on its way in. Nothing else reaches here — the caller has already checked the word
        // against Qualifiers.
        IReadOnlyList<string> Side(string qualifier) =>
            string.Equals(qualifier, "target", StringComparison.Ordinal) ? targetColumns : columns;
    }

    /// <summary>Reads one word — a bare identifier or a '"'-quoted one — advancing
    /// <paramref name="i"/> past it and yielding the name with the delimiters removed, plus whether it
    /// was quoted, which decides whether it may be a keyword. False if there is no word there, if a
    /// quoted one never closes, or if a '"' sits directly after an identifier character, which is the
    /// string-prefix shape refused above.
    ///
    /// A doubled '"' inside the delimiters is NOT read back as one character. It cannot name anything:
    /// <see cref="RefuseUnquotableNames"/> has already refused every schema whose column names contain
    /// a quote, so a word carrying one matches no column either way and falls through to the refusal at
    /// the end of the scan.</summary>
    private static bool TryReadWord(string s, ref int i, out string word, out bool quoted)
    {
        word = string.Empty;
        quoted = false;

        if (i >= s.Length)
        {
            return false;
        }

        if (s[i] == '"')
        {
            quoted = true;
            if (i > 0 && IsIdentifierChar(s[i - 1]))
            {
                return false;
            }

            var end = SkipQuoted(s, i, '"');
            if (end < 0)
            {
                return false;
            }

            word = s[(i + 1)..(end - 1)];
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
    /// number, or a quoted string that is exactly two quote characters with the value between them.
    /// Requiring the closing quote to be the last character is what separates a value containing a
    /// comma or a semicolon — legal content, admitted — from a second value smuggled into one slot,
    /// such as <c>'a', 'b'</c>.
    ///
    /// An interior quote or backslash is refused rather than accepted as an escape, which is stricter
    /// than the dialect and deliberately so. Measured against the shipped library: a literal carrying
    /// a doubled quote compares as a value with one fewer level of doubling than it was written with,
    /// so a filter built from a value containing <c>''</c> names a partition the table does not have,
    /// the target row stays invisible to the merge's scan, and the row is inserted a second time with
    /// no error; a literal carrying <c>\'</c> fails the whole write with an unterminated-literal
    /// parser error. Those are the exact two shapes a producer that escaped instead of refusing would
    /// hand over, so this is the check that has to see them — a guard that accepts every escape its
    /// producer might invent cannot catch the producer being wrong about escaping.</summary>
    private static bool IsSelfContainedLiteral(string literal) =>
        NumericLiteral.IsMatch(literal)
        || (literal.Length >= 2
            && literal[0] == '\''
            && literal[^1] == '\''
            && !literal.AsSpan(1, literal.Length - 2).ContainsAny('\'', '\\'));

    private static bool IsIdentifierChar(char c) => char.IsAsciiLetterOrDigit(c) || c == '_';

    /// <summary>Refuses every name this statement would have to quote and cannot. Aggregated and
    /// reported once: a user fixing one column name per run is a user running the pipeline once per
    /// column.
    ///
    /// The set covers every name that reaches <see cref="Q"/> — the output's own columns, the merge
    /// keys, and the partition columns — because the damage is not confined to the ON clause. Measured
    /// against the shipped library on a table holding both a column named <c>a"b</c> and one named
    /// <c>a""b</c>: as a key and partition column, the second name's rendered form resolves to the
    /// FIRST column, the target row is invisible to the scan and the merge inserts a duplicate of a key
    /// it should have matched; as an ordinary column, the same rendered form makes
    /// <c>WHEN MATCHED THEN UPDATE SET</c> assign the first column twice and leave the second holding
    /// its pre-merge value — silent stale data, no error, no key involved. On a table where no sibling
    /// column absorbs the mangled name, the merge fails outright with "No field named".
    ///
    /// A name carrying ONE quote renders faithfully today — measured, matched, updated in place — so
    /// this refusal costs it. That is the same trade taken for a partition VALUE carrying a quote and
    /// for the same reason: telling the faithful case from the unfaithful one means modelling how a
    /// foreign parser unescapes, which is the thing already known to differ between that parser's own
    /// paths. A backslash is NOT refused here, and that is measured too, not an oversight: a column
    /// named <c>back\</c> and one named <c>a\b</c> both render, match and update correctly, and the
    /// one shape that would worry — a backslash immediately before the closing delimiter — needs a '"'
    /// in the name, which is refused above.
    ///
    /// The third of the three refusals sharing one premise — see the type doc comment. A version bump
    /// that fixes the unescaping fails all three execution tests at once, and that is the signal to
    /// re-decide all three, not to relax this one.</summary>
    private static void RefuseUnquotableNames(
        IReadOnlyList<string> columns, IReadOnlyList<string> keys,
        IReadOnlyList<PartitionFilter>? partitionFilters)
    {
        var offenders = columns
            .Concat(keys)
            .Concat((partitionFilters ?? []).Select(f => f.Column))
            .Where(n => !IsQuotableName(n))
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToList();

        if (offenders.Count == 0)
        {
            return;
        }

        throw DeltaErrors.Fail(DeltaErrors.UnquotableColumnName,
            "strategy 'merge' cannot name these columns in a SQL statement because their names contain " +
            $"a '\"': {string.Join(", ", offenders.Select(n => $"'{n}'"))}",
            "rename them in the pipeline's SQL — a quote in a column name has to be doubled to be " +
            "quoted, and the merge resolves a doubled quote to a different column than the one meant. " +
            "Strategy append and replace are unaffected");
    }

    /// <summary>The precondition <see cref="Q"/> depends on, in one place so the aggregate report above
    /// and the backstop below cannot drift apart.</summary>
    private static bool IsQuotableName(string name) => !name.Contains('"');

    /// <summary>Wraps a name that has already been refused if it contained a quote, so there is nothing
    /// left to escape. The throw is a backstop, not the report: it exists so a future caller that
    /// reaches this without going through <see cref="RefuseUnquotableNames"/> fails loudly rather than
    /// emitting an identifier that silently names a different column.</summary>
    private static string Q(string identifier) =>
        IsQuotableName(identifier)
            ? $"\"{identifier}\""
            : throw DeltaErrors.Fail(DeltaErrors.UnquotableColumnName,
                "a column name containing a '\"' reached the merge statement generator",
                "report this as a connector bug: the name should have been refused before this point");
}
