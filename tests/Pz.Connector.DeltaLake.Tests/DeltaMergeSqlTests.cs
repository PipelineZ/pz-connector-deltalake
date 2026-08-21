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
    [InlineData("dt >= '2026-01-01'")]
    [InlineData("target.region = 'eu' AND target.dt > '2026-01-01'")]
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
    [InlineData("target.dt = 'it''s' OR target.dt != 'x'")]
    [InlineData("\"weird col\" = 1")]
    [InlineData("target.amt >= -1.5")]
    [InlineData("target.amt * 2 + 1 <= 10 / 5")]
    public void An_ordinary_predicate_survives_the_alphabet_check(string predicate) =>
        Assert.Contains(predicate, DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id"], mergePredicate: predicate), null));

    [Fact]
    public void Embedded_quotes_in_identifiers_are_doubled_not_passed_through()
    {
        // Arrow column names come from the pipeline's own SQL, so a '"' in one is reachable without
        // anybody writing it on purpose. Un-doubled, it would close the quoted identifier early.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int64Type.Default).Nullable(false))
            .Field(f => f.Name("a\"b").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
            .Build();
        var sql = DeltaMergeSql.Build(schema, Opts(["id"]), [new PartitionFilter("x\"y", ["'1'"])]);

        Assert.Contains("target.\"a\"\"b\" = source.\"a\"\"b\"", sql);
        Assert.Contains("target.\"x\"\"y\" IN ('1')", sql);
        Assert.DoesNotContain("\"a\"b\"", sql);
        Assert.DoesNotContain("\"x\"y\"", sql);
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
    [InlineData("'a', 'b'")]
    [InlineData("'a' OR 1=1")]
    [InlineData("'a")]
    [InlineData("`a`")]
    [InlineData("x")]
    [InlineData("")]
    public void A_partition_literal_that_is_not_one_self_contained_value_is_refused(string literal)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(
                DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]), [new PartitionFilter("dt", [literal])]));
        Assert.Contains(DeltaErrors.InvalidMergePredicate, ex.Message);
    }

    [Theory]
    // A ';' or a paren INSIDE the value is legal content, not a second value: the closing quote is
    // still the final character.
    [InlineData("'a;b'")]
    [InlineData("'a)b('")]
    [InlineData("'it''s'")]
    [InlineData("42")]
    [InlineData("-1.5e3")]
    public void A_self_contained_partition_literal_is_accepted(string literal)
    {
        var sql = DeltaMergeSql.Build(
            DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]), [new PartitionFilter("dt", [literal])]);
        Assert.Contains($"target.\"dt\" IN ({literal})", sql);
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
