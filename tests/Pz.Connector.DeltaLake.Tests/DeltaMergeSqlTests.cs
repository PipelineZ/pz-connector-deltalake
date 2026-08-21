using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class DeltaMergeSqlTests
{
    private static DeltaWriteOptions Opts(
        string[] keys, string[]? partitionBy = null, string? mergePredicate = null) =>
        new("merge", keys, partitionBy ?? [], mergePredicate, 1024);

    private static void AssertGolden(string name, string actual)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "golden", name);
        var expected = File.ReadAllText(path).Replace("\r\n", "\n");
        Assert.Equal(expected.TrimEnd('\n'), actual.Replace("\r\n", "\n").TrimEnd('\n'));
    }

    [Fact]
    public void A_single_key_merge_matches_its_golden() =>
        AssertGolden("merge-single-key.sql", DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"]), null));

    [Fact]
    public void A_composite_key_merge_ands_every_key_in_the_on_clause() =>
        AssertGolden("merge-composite-key.sql", DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id", "dt"]), null));

    [Fact]
    public void An_explicit_merge_predicate_is_appended_to_the_on_clause() =>
        AssertGolden("merge-explicit-predicate.sql",
            DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: "target.dt >= '2026-01-01'"), null));

    [Fact]
    public void A_derived_partition_predicate_is_appended_to_the_on_clause() =>
        AssertGolden("merge-derived-partition.sql",
            DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]),
                [new PartitionFilter("dt", ["'2026-01-01'", "'2026-01-02'"])]));

    [Fact]
    public void Key_columns_are_not_updated_because_they_are_what_matched()
    {
        var sql = DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"]), null);
        Assert.DoesNotContain("SET target.\"id\"", sql);
        Assert.Contains("target.\"amt\" = source.\"amt\"", sql);
    }

    [Fact]
    public void Identifiers_are_quoted_so_a_reserved_word_column_still_works()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("order").DataType(Apache.Arrow.Types.Int64Type.Default).Nullable(false))
            .Field(f => f.Name("select").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
            .Build();
        var sql = DeltaMergeSql.Build(schema, Opts(["order"]), null);
        Assert.Contains("\"order\"", sql);
        Assert.Contains("\"select\"", sql);
    }

    [Fact]
    public void Generation_is_deterministic_across_calls()
    {
        // Asserted against a written-out expectation rather than against a second call in the same
        // process: two calls agreeing proves nothing about a generator that has become unordered, and
        // a hash set enumerated twice in one process is very likely to agree with itself.
        var filters = new[] { new PartitionFilter("dt", ["'x'"]) };
        var sql = DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]), filters);

        Assert.Equal(
            "MERGE INTO target USING source ON target.\"id\" = source.\"id\" AND target.\"dt\" = source.\"dt\"" +
            " AND target.\"dt\" IN ('x')\n" +
            "WHEN MATCHED THEN UPDATE SET target.\"amt\" = source.\"amt\"\n" +
            "WHEN NOT MATCHED THEN INSERT (\"id\", \"dt\", \"amt\") VALUES (source.\"id\", source.\"dt\", source.\"amt\")",
            sql);
    }

    [Fact]
    public void A_merge_predicate_that_is_not_a_predicate_is_refused_rather_than_concatenated()
    {
        // merge_predicate is appended to the ON clause. It must not be able to terminate the statement
        // and start another one.
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: "1=1; DROP TABLE x"), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Theory]
    // Qualified, because a merge has two sides and a bare column name resolves to neither. The
    // unqualified spelling of this predicate is the refusal case in
    // A_predicate_that_does_not_say_which_side_a_column_belongs_to_is_refused.
    [InlineData("target.dt >= '2026-01-01'")]
    [InlineData("source.dt >= '2026-01-01'")]
    public void A_well_formed_merge_predicate_is_accepted(string predicate) =>
        Assert.Contains(predicate, DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: predicate), null));

    [Fact]
    public void A_predicate_that_closes_the_wrapping_paren_early_is_refused()
    {
        // The predicate is interpolated as "AND (<predicate>)". "1=1) OR (1=1" closes that paren
        // right after the first "1=1" and opens a fresh, unconstrained one: the rendered ON clause
        // becomes "target."id" = source."id" AND (1=1) OR (1=1)", which is well-formed SQL that
        // matches every row in the table regardless of key equality. The terminator/comment blocklist
        // does not catch this — there is no ';', '--', '/*', or '*/' anywhere in it.
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: "1=1) OR (1=1"), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Fact]
    public void A_predicate_padded_with_quoted_parens_to_fake_balance_is_still_refused()
    {
        // Same escape as above, disguised so a quote-BLIND character count comes out balanced: one
        // quoted '(', one real ')', one real '(', one quoted ')' — net zero, never negative. Only a
        // scan that skips paren characters inside quoted regions sees that the two REAL parentheses
        // still close the wrapping group early and open an unconstrained one.
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(
                DeltaTestTable.Schema, Opts(["id"], mergePredicate: "col = '(' OR 1=1) OR (1=1 OR col = ')'"), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Fact]
    public void A_predicate_with_a_legitimately_unbalanced_open_paren_is_refused()
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: "(target.dt >= '2026-01-01'"), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Fact]
    public void A_predicate_with_balanced_nested_parens_is_accepted()
    {
        var sql = DeltaMergeSql.Build(
            DeltaTestTable.Schema, Opts(["id"], mergePredicate: "(target.dt >= '2026-01-01' AND target.amt > 0)"), null);
        Assert.Contains("(target.dt >= '2026-01-01' AND target.amt > 0)", sql);
    }

    [Fact]
    public void When_every_column_is_a_key_the_when_matched_clause_is_omitted_not_emitted_empty()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(Apache.Arrow.Types.StringType.Default).Nullable(false))
            .Build();
        var sql = DeltaMergeSql.Build(schema, Opts(["id", "dt"]), null);

        Assert.DoesNotContain("WHEN MATCHED", sql);
        Assert.Contains(
            "WHEN NOT MATCHED THEN INSERT (\"id\", \"dt\") VALUES (source.\"id\", source.\"dt\")", sql);
    }

    /// <summary>Every one of these was ACCEPTED by the previous guard and, except for the last,
    /// executed against a real Delta table: each rewrote every row in the target, or injected its own
    /// WHEN clause. They are here verbatim because a paraphrase would test a shape the attacker does
    /// not have to use.</summary>
    public static TheoryData<string> EscapingPayloads() =>
        new TheoryData<string>
        {
        // Dollar-quoted padding: "$$($$" is one string to the SQL lexer and a real open paren to a
        // scanner that does not model dollar quoting, so two of them fake any balance.
        "target.dt = $$($$ OR 1=1) OR (1=1 OR target.dt = $$)$$",

        // The same trick with a tagged dollar quote.
        "target.dt = $q$($q$ OR 1=1) OR (1=1 OR target.dt = $q$)$q$",

        // A prefixed string literal escapes its quotes with a backslash rather than by doubling it,
        // so the region a doubled-quote scanner measures ends somewhere else than the real one.
        @"target.dt = E'\'(\'' OR 1=1) OR (1=1 OR target.dt = E'\')\''",
        @"target.dt = e'\'(\'' OR 1=1) OR (1=1 OR target.dt = e'\')\''",

        // Reaches PAST the ON clause: the generated statement's own trailing ')' closes the VALUES '('
        // this opens, so the injected WHEN clauses precede — and therefore beat — the generated ones.
        """
        $$($$ IS NOT NULL AND 1=1) WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT ("id", "dt", "amt") VALUES (source."id", $$)$$ || source."dt", source."amt"
        """,

        // Backtick-delimited identifiers are another quoting form the scanner never modelled. This one
        // reaches name resolution and fails only because Delta forbids '(' in a column name — an
        // invariant owned by something else entirely.
            "target.dt = `(` OR 1=1) OR (1=1 OR target.dt = `)`",
        };

    [Theory]
    [MemberData(nameof(EscapingPayloads))]
    public void A_predicate_that_escapes_its_parenthesised_group_is_refused(string payload)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: payload), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);

        // The refusal names the rule, never the text that broke it: a predicate can carry anything a
        // user typed, and an error message travels into logs and run artifacts.
        Assert.DoesNotContain(payload, ex.Message);
    }

    [Theory]
    // Characters that open a lexical region this connector does not model, in the order they appear in
    // the guard: dollar quoting, a backtick identifier, a backslash escape, and NUL.
    [InlineData("target.dt = $$x$$")]
    [InlineData("target.dt = `x`")]
    [InlineData(@"target.dt = 'a\b'")]
    [InlineData("target.dt = 'a\0b'")]
    // String-literal prefixes, refused as a class by the "identifier character then quote" rule rather
    // than one letter at a time.
    [InlineData("target.dt = E'x'")]
    [InlineData("target.dt = e'x'")]
    [InlineData("target.dt = X'41'")]
    [InlineData("target.dt = N'x'")]
    [InlineData("target.dt = R'x'")]
    [InlineData("target.dt = B'0'")]
    // Characters outside the alphabet entirely.
    [InlineData("target.dt = U&'x'")]
    [InlineData("target.dt = 'a' # 'b'")]
    [InlineData("target.dt::text = 'a'")]
    [InlineData("target.dt = @x")]
    // An unterminated quoted region, and a lone '!' that is not an operator on its own.
    [InlineData("target.dt = 'a")]
    // The '"' half of the same rule, in the shapes that tell an unterminated region apart from a
    // terminated one however the region's end is computed: one whose remainder is not a column name,
    // one whose remainder IS, and one whose remainder is a column name with a character trimmed off.
    [InlineData("target.dt = \"a")]
    [InlineData("target.dt = \"dt")]
    [InlineData("target.dt = \"dtX")]
    [InlineData("target.\"dt = 'x'")]
    // The same shapes in a QUALIFIED position, where a truncated remainder that happens to name a
    // column would otherwise be accepted outright rather than merely refused for a different reason.
    [InlineData("target.\"dt")]
    [InlineData("target.\"dtX")]
    [InlineData("target.dt ! 'a'")]
    public void A_predicate_outside_the_permitted_alphabet_is_refused(string predicate)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: predicate), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Theory]
    [InlineData("   ")]
    [InlineData("\t\n")]
    [InlineData("")]
    public void A_blank_merge_predicate_is_refused_rather_than_treated_as_absent(string predicate)
    {
        // Rendering "AND (   )" would surface a configuration mistake as a runtime write failure, and
        // silently dropping the option would answer a question the user did not ask.
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: predicate), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Theory]
    [InlineData("target.id IN (1, 2)")]
    [InlineData("target.amt BETWEEN 1 AND 2")]
    [InlineData("target.dt LIKE 'a%'")]
    [InlineData("NOT (target.amt IS NULL) AND target.amt <> 0")]
    [InlineData("target.dt = 'a b' OR target.dt != 'x'")]
    [InlineData("target.amt >= -1.5")]
    [InlineData("target.amt * 2 + 1 <= 10 / 5")]
    public void An_ordinary_predicate_survives_the_alphabet_check(string predicate) =>
        Assert.Contains(predicate, DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: predicate), null));

    [Fact]
    public void A_column_name_containing_a_quote_is_refused_rather_than_doubled()
    {
        // Arrow column names come from the pipeline's own SQL, so a '"' in one is reachable without
        // anybody writing it on purpose -- 'select 1 as "say ""hi"""' mints one, verified through
        // DuckDB itself. Doubling it to quote it is what goes wrong: the merge path resolves the
        // doubled form to a different column, so the statement silently constrains, updates or inserts
        // the wrong one. MergeSafetyExecutionTests watches both consequences happen.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int64Type.Default).Nullable(false))
            .Field(f => f.Name("a\"b").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
            .Build();

        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(schema, Opts(["id"]), null));
        Assert.Contains(DeltaErrors.UnquotableColumnName, ex.Message);
        Assert.Contains("a\"b", ex.Message);
    }

    [Fact]
    public void Every_unquotable_name_is_reported_at_once_wherever_it_reaches_the_statement()
    {
        // Columns, keys and partition-filter columns all reach the quoting helper, so all three are
        // checked -- and reported together, because a user fixing one name per run is a user running
        // the pipeline once per column.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int64Type.Default).Nullable(false))
            .Field(f => f.Name("z\"col").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
            // Two names differing only in case. Both are legal Delta column names, and both have to be
            // reported: pooling them case-insensitively would name one, the user would rename it, rerun
            // and be refused again for the other -- the per-run loop the aggregation exists to prevent.
            .Field(f => f.Name("A\"b").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
            .Field(f => f.Name("a\"B").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
            // Sorts before 'a"B' ordinally and after it if case is folded -- the only pair here that
            // can tell the report's ordering rule from a case-insensitive one.
            .Field(f => f.Name("Z\"a").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
            .Build();

        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(
                schema, Opts(["id", "k\"key"], ["p\"part"]),
                [new PartitionFilter("p\"part", ["'1'"])]));

        Assert.Contains(DeltaErrors.UnquotableColumnName, ex.Message);
        Assert.Contains("'k\"key'", ex.Message);
        Assert.Contains("'p\"part'", ex.Message);
        Assert.Contains("'z\"col'", ex.Message);
        Assert.Contains("'A\"b'", ex.Message);
        Assert.Contains("'a\"B'", ex.Message);

        // Ordered by the names themselves, ordinally, so two runs over the same schema produce the
        // same message -- and so the order does not depend on which of columns, keys or partition
        // columns a name happened to arrive from.
        Assert.True(
            ex.Message.IndexOf("'Z\"a'", StringComparison.Ordinal)
                < ex.Message.IndexOf("'a\"B'", StringComparison.Ordinal),
            $"offenders are not in ordinal order: {ex.Message}");
    }

    [Fact]
    public void A_merge_predicate_naming_a_column_whose_name_carries_a_quote_is_refused_too()
    {
        // The predicate scanner un-doubles a quoted identifier the same way the generator doubles it,
        // so a predicate naming such a column would be accepted while constraining a different one.
        // It needs no rule of its own: the schema carrying the name is refused before the predicate is
        // ever inspected, which is why this test asserts the SCHEMA's code and not the predicate's.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int64Type.Default).Nullable(false))
            .Field(f => f.Name("a\"\"b").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
            .Build();

        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(
                schema, Opts(["id"], mergePredicate: "target.\"a\"\"\"\"b\" = 'x'"), null));
        Assert.Contains(DeltaErrors.UnquotableColumnName, ex.Message);
    }

    [Fact]
    public void Key_matching_is_case_sensitive_because_delta_column_names_are()
    {
        // "ID" does not name the column "id". Matching case-insensitively would drop "id" from the
        // UPDATE SET while joining on a column that does not exist.
        var sql = DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["ID"]), null);

        Assert.Contains("target.\"ID\" = source.\"ID\"", sql);
        Assert.Contains("target.\"id\" = source.\"id\"", sql);
    }

    [Fact]
    public void A_partition_filter_with_no_literals_is_refused_rather_than_rendered_as_an_empty_in_list()
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]), [new PartitionFilter("dt", [])]));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Theory]
    [InlineData("$$x$$")]
    // A trailing newline is not part of a number. It is called out because the anchor that refuses it
    // is easy to write as one that does not: '$' matches before a final newline.
    [InlineData("1\n")]
    [InlineData("'a', 'b'")]
    [InlineData("'a' OR 1=1")]
    [InlineData("'a")]
    [InlineData("`a`")]
    [InlineData("x")]
    [InlineData("")]
    // An interior quote or backslash is refused rather than read as an escape. These are the two
    // shapes a producer that escaped instead of refusing would hand over, and they are exactly the
    // ones the merge path mishandles -- see the doubled-quote theory below.
    [InlineData("'it''s'")]
    [InlineData("'O''''Brien'")]
    [InlineData("''''")]
    [InlineData(@"'back\'")]
    [InlineData(@"'a\''b'")]
    public void A_partition_literal_that_is_not_one_self_contained_value_is_refused(string literal)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(
                DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]), [new PartitionFilter("dt", [literal])]));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Theory]
    // A second value smuggled into one slot, and a value carrying the escape that is now refused.
    [InlineData("'{0}', 'b'")]
    [InlineData("'O''{0}'")]
    [InlineData("{0}")]
    public void The_partition_literal_refusal_does_not_echo_the_value_that_broke_it(string shape)
    {
        // A partition literal is derived from user DATA, which a merge_predicate is not: the message
        // names the column and states the rule, and must not carry the value into an exception, a run
        // artifact or a log. The marker is distinctive on purpose -- asserting this on a literal like
        // "x" would pass or fail on whether the message happens to contain that letter, which it does
        // ("Next step"), and a test that turns on a coincidence is not pinning anything.
        const string Marker = "confidential-tenant";
        var literal = string.Format(System.Globalization.CultureInfo.InvariantCulture, shape, Marker);

        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(
                DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]), [new PartitionFilter("dt", [literal])]));

        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
        Assert.Contains("'dt'", ex.Message);
        Assert.DoesNotContain(Marker, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    // A ';' or a paren INSIDE the value is legal content, not a second value: the closing quote is
    // still the final character.
    [InlineData("'a;b'")]
    [InlineData("'a)b('")]
    [InlineData("42")]
    [InlineData("-1.5e3")]
    public void A_self_contained_partition_literal_is_accepted(string literal)
    {
        var sql = DeltaMergeSql.Build(
            DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]), [new PartitionFilter("dt", [literal])]);
        Assert.Contains($"target.\"dt\" IN ({literal})", sql);
    }

    /// <summary>A schema whose column names exercise the shapes the word rules have to handle: one
    /// needing quotes, one ending in '_', one ending in a digit, and one whose name is not all lower
    /// case. A name containing a '"' is deliberately absent: such a schema cannot be merged at all
    /// now, so it belongs to the refusal test above rather than to the word rules.</summary>
    private static Apache.Arrow.Schema AwkwardSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(Apache.Arrow.Types.StringType.Default).Nullable(false))
            .Field(f => f.Name("region").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
            .Field(f => f.Name("weird col").DataType(Apache.Arrow.Types.Int64Type.Default).Nullable(true))
            .Field(f => f.Name("a_").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
            .Field(f => f.Name("a1").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
            .Field(f => f.Name("Amt").DataType(Apache.Arrow.Types.DoubleType.Default).Nullable(true))
            .Build();

    [Theory]
    [InlineData("target.region = 'eu' AND target.dt > '2026-01-01'")]
    [InlineData("source.region = 'eu'")]
    [InlineData("target.\"weird col\" >= 1")]
    [InlineData("source.\"weird col\" = 1")]
    [InlineData("target.a_ = 'x'")]
    [InlineData("target.a1 = 'x'")]
    // A column whose name is not all lower case is reachable by writing its name, in its own case,
    // either bare or quoted — which is what the merge path resolves.
    [InlineData("target.Amt >= 0")]
    [InlineData("target.\"Amt\" >= 0")]
    [InlineData("\"target\".Amt >= 0")]
    // Keywords are case-insensitive in this dialect; column names are not.
    [InlineData("target.dt Like 'd%' And target.Amt Is Not Null")]
    [InlineData("target.id In (1, 2) or TRUE")]
    public void A_predicate_over_this_outputs_own_columns_is_accepted(string predicate) =>
        Assert.Contains(
            predicate, DeltaMergeSql.Build(AwkwardSchema(), Opts(["id"], mergePredicate: predicate), null));

    [Theory]
    // Column names are matched ordinally because that is what the MERGE path does with them, measured
    // against the shipped library: against a column 'dt' it refuses 'target.DT' with "Column names are
    // case sensitive", and against a column 'Amt' it accepts 'target.Amt' and refuses 'target.AMT'.
    // Folding case here would accept predicates the merge then refuses. (The same engine's ordinary
    // SELECT planner behaves the OPPOSITE way — it lowercases an unquoted identifier first — which is
    // why this rule is measured on the path that actually runs it rather than reasoned about.)
    // Unqualified as well as qualified: an unqualified name that differs only in case names nothing, so
    // it must NOT be met with the add-a-qualifier advice — which is also what keeps the case rule
    // observable on the unqualified path, where both outcomes refuse.
    [InlineData("DT >= '2020-05-05'")]
    [InlineData("\"DT\" >= '2020-05-05'")]
    [InlineData("AMT >= 0")]
    [InlineData("source.DT >= '2026-01-01'")]
    [InlineData("source.\"DT\" >= '2026-01-01'")]
    [InlineData("source.AMT >= 0")]
    [InlineData("target.DT >= '2026-01-01'")]
    [InlineData("target.\"DT\" >= '2026-01-01'")]
    [InlineData("target.AMT >= 0")]
    [InlineData("target.\"amt\" >= 0")]
    // The two aliases delta-rs fixes for a merge are spelled in lower case, and it refuses any other
    // spelling of them too.
    [InlineData("TARGET.dt >= '2026-01-01'")]
    [InlineData("\"TARGET\".dt >= '2026-01-01'")]
    // A '"'-quoted token is an identifier, never a keyword, so a quoted keyword has to name a column.
    [InlineData("\"AND\" = 1")]
    [InlineData("target.dt = 'a' \"OR\" 1=1")]
    public void A_predicate_whose_names_differ_from_the_schema_only_in_case_is_refused(string predicate)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(AwkwardSchema(), Opts(["id"], mergePredicate: predicate), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);

        // A name that differs in case names NOTHING, so the refusal must not be the one that tells the
        // author to add a qualifier — following that advice would not help. This is also what keeps
        // the case rule observable now that both outcomes refuse.
        Assert.DoesNotContain(WhichSide, ex.Message);
    }

    /// <summary>The phrase that separates the two PZDL0107 refusals: a predicate that names a real
    /// column without a side, from one that names nothing at all.</summary>
    private const string WhichSide = "which side of the merge";

    [Theory]
    // A merge has an existing row and an incoming one, so an unqualified column name matches neither.
    // Measured: delta-rs answers one with "Ambiguous reference to unqualified field", every time, for
    // every column of the batch being written -- so this can never run and is refused up front.
    // The date differs from the one the refusal message uses as its example on purpose: the payload-free
    // assertion below would otherwise trip on the message quoting a correctly-qualified predicate.
    [InlineData("dt >= '2020-05-05'")]
    [InlineData("\"weird col\" = 1")]
    [InlineData("Amt >= 0")]
    [InlineData("\"Amt\" >= 0")]
    [InlineData("a_ = 'x'")]
    [InlineData("id IN (1, 2) AND target.dt > '2026-01-01'")]
    public void A_predicate_that_does_not_say_which_side_a_column_belongs_to_is_refused(string predicate)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(AwkwardSchema(), Opts(["id"], mergePredicate: predicate), null));

        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
        Assert.Contains(WhichSide, ex.Message);
        Assert.Contains("target.", ex.Message);
        Assert.Contains("source.", ex.Message);
        Assert.DoesNotContain(predicate, ex.Message);
    }

    [Fact]
    public void A_keyword_needs_no_qualifier_because_only_a_column_has_a_side()
    {
        const string Predicate = "NOT (target.Amt IS NULL) AND TRUE";
        Assert.Contains(
            Predicate, DeltaMergeSql.Build(AwkwardSchema(), Opts(["id"], mergePredicate: Predicate), null));
    }

    [Theory]
    // A subquery does not merely fail to run: it aborts delta-rs inside native code and the merge call
    // then never returns, so there is no error, no event and nothing for a retry to act on. It is
    // refused because SELECT and FROM are not in the vocabulary, not because they are blocked by name.
    [InlineData("target.id IN (SELECT id FROM target)")]
    [InlineData("target.id IN (SELECT id FROM nosuchtable)")]
    [InlineData("EXISTS (SELECT amt FROM source)")]
    // Function calls, casts and CASE are refused for the same reason, and this is a deliberate limit:
    // the vocabulary a foreign SQL engine accepts is not one this connector can enumerate.
    [InlineData("abs(target.amt) > 100")]
    [InlineData("CAST(target.amt AS INT) > 1")]
    [InlineData("CASE WHEN target.amt > 1 THEN 1 ELSE 0 END = 1")]
    // A column this output does not have, a qualifier that is not one of the two aliases delta-rs
    // fixes for a merge, and a three-part name.
    [InlineData("target.nosuchcolumn = 1")]
    [InlineData("nosuchcolumn = 1")]
    [InlineData("other.dt = 'a'")]
    [InlineData("target.dt.x = 1")]
    // The discriminating third part: 'dt' IS a column, so this is refused for being a three-part name
    // rather than for naming something unknown.
    [InlineData("target.dt.dt = 1")]
    // A trailing 'e' is only part of a number when digits follow it; otherwise it is a word, and an
    // unknown one.
    [InlineData("target.amt = 1e")]
    [InlineData("target.amt = 1ex")]
    public void A_predicate_naming_anything_but_a_column_or_a_keyword_is_refused(string predicate)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: predicate), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
        Assert.DoesNotContain(predicate, ex.Message);
        Assert.DoesNotContain(WhichSide, ex.Message);
    }

    [Theory]
    // A doubled quote is this dialect's own escape, and the region it delimits is measured correctly
    // here -- so this refusal is stricter than the dialect on purpose. Measured against the shipped
    // library: a predicate literal carrying ONE doubled quote is faithful (it compares as the value
    // with a single quote in it), but one carrying two in a row compares as though only one level of
    // doubling had been written, so the target row that should have matched is invisible and is
    // inserted a second time instead. Telling those two apart needs a model of a foreign parser's
    // unescaping which is already known to differ between that parser's own paths, so the escape is
    // refused outright rather than modelled. The cost is real and deliberate: a value containing a
    // quote cannot be named in a merge_predicate at all.
    [InlineData("target.dt = 'it''s'")]
    [InlineData("target.dt = 'a''''b'")]
    [InlineData("target.dt = ''''")]
    [InlineData("target.dt = 'a' OR target.dt = 'b''c'")]
    public void A_merge_predicate_carrying_a_doubled_quote_is_refused(string predicate)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: predicate), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
        Assert.DoesNotContain(predicate, ex.Message);
    }

    [Theory]
    // '-', '/' and '*' are all legal operator characters and a comment's own text can be made to sit
    // inside the permitted vocabulary, so the terminator blocklist is the ONLY thing refusing these.
    // Both were executed unguarded against a real table and rewrote all 20 rows: one comment hides a
    // '(' and a later one hides a ')', which leaves this connector's paren count balanced while the
    // SQL engine's dips negative and closes the wrapping group early.
    [InlineData("1=1 --(\n) OR (1=1 --)\nAND 1=1")]
    [InlineData("1=1 -- (\n) OR (1=1 -- )\nAND 1=1")]
    [InlineData("target.dt = 'a' /*(*/) OR (1=1 /*)*/ AND 1=1")]
    public void A_predicate_hiding_a_paren_in_a_comment_is_refused(string predicate)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: predicate), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Theory]
    // The string-prefix rule looks at the character before the quote, and every identifier character
    // counts: a digit and an underscore as much as a letter.
    [InlineData("target.dt = 1'a'")]
    [InlineData("a_'x' = 'y'")]
    [InlineData("a1'x' = 'y'")]
    public void A_quote_directly_after_any_identifier_character_is_refused(string predicate)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(AwkwardSchema(), Opts(["id"], mergePredicate: predicate), null));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Fact]
    public void The_refusal_says_what_to_write_instead_of_a_function_call()
    {
        // Refusing function calls is a deliberate limit rather than an oversight, so the message has to
        // leave the user somewhere to go.
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: "abs(target.amt) > 1"), null));
        Assert.Contains("Function calls", ex.Message);
        Assert.Contains("pipeline's SQL", ex.Message);
    }

    [Fact]
    public void The_all_key_shape_matches_its_golden()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(Apache.Arrow.Types.StringType.Default).Nullable(false))
            .Build();
        AssertGolden("merge-all-key.sql", DeltaMergeSql.Build(schema, Opts(["id", "dt"]), null));
    }
}
