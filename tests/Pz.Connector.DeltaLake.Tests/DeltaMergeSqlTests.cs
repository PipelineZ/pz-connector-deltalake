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
        var filters = new[] { new PartitionFilter("dt", ["'x'"]) };
        var a = DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]), filters);
        var b = DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]), filters);
        Assert.Equal(a, b);
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
}
