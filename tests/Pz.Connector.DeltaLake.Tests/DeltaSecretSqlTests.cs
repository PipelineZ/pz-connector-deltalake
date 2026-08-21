using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class DeltaSecretSqlTests
{
    private static ConnectorConfig Cfg(params (string Key, object? Value)[] pairs) =>
        new(pairs.ToDictionary(p => p.Key, p => p.Value));

    [Fact]
    public void A_local_root_needs_only_the_delta_extension()
    {
        var stmts = DeltaSecretSql.SetupStatements(Cfg(("root", "/mnt/lake")), "lake");
        Assert.Equal(["install delta", "load delta"], stmts);
    }

    [Fact]
    public void An_s3_root_loads_httpfs_and_creates_one_scoped_secret()
    {
        var stmts = DeltaSecretSql.SetupStatements(
            Cfg(("root", "s3://w/d"), ("access_key_id", "AK"), ("secret_access_key", "SK"), ("region", "eu-west-1")),
            "lake");

        Assert.Contains("install delta", stmts);
        Assert.Contains("load delta", stmts);
        Assert.Contains("install httpfs", stmts);
        Assert.Contains("load httpfs", stmts);
        Assert.Single(stmts, s => s.StartsWith("create or replace secret", StringComparison.Ordinal));
    }

    [Fact]
    public void An_azure_root_loads_the_azure_extension_not_httpfs()
    {
        var stmts = DeltaSecretSql.SetupStatements(
            Cfg(("root", "az://fs/d"), ("connection_string", "CS")), "lake");
        Assert.Contains("load azure", stmts);
        Assert.DoesNotContain("load httpfs", stmts);
    }

    [Fact]
    public void An_azure_root_with_only_account_name_and_key_synthesizes_a_connection_string_secret()
    {
        // DuckDB's azure secret type has no discrete account_key parameter (confirmed against a real
        // DuckDB 1.5.5: "Unknown parameter 'account_key' for secret type 'azure'"), so shared-key auth
        // must go through a synthesized connection string or it silently falls back to the ambient
        // credential chain — an advertised connection option that would otherwise do nothing.
        var sql = DeltaSecretSql.SetupStatements(
            Cfg(("root", "az://fs/d"), ("account_name", "myacct"), ("account_key", "bXlrZXk=")), "lake")
            .Single(s => s.StartsWith("create or replace secret", StringComparison.Ordinal));

        Assert.Contains("connection_string", sql);
        Assert.Contains("DefaultEndpointsProtocol=https", sql);
        Assert.Contains("AccountName=myacct", sql);
        Assert.Contains("AccountKey=bXlrZXk=", sql);
    }

    [Fact]
    public void A_connection_string_takes_precedence_over_account_name_and_key_when_both_are_given()
    {
        var sql = DeltaSecretSql.SetupStatements(
            Cfg(("root", "az://fs/d"), ("connection_string", "CS"),
                ("account_name", "myacct"), ("account_key", "bXlrZXk=")), "lake")
            .Single(s => s.StartsWith("create or replace secret", StringComparison.Ordinal));

        Assert.Contains("connection_string 'CS'", sql);
        Assert.DoesNotContain("AccountKey=", sql);
    }

    [Fact]
    public void Single_quotes_in_a_synthesized_azure_connection_string_are_escaped()
    {
        var sql = DeltaSecretSql.SetupStatements(
            Cfg(("root", "az://fs/d"), ("account_name", "acct"), ("account_key", "it's'a'key")), "lake")
            .Single(s => s.StartsWith("create or replace secret", StringComparison.Ordinal));

        Assert.Contains("it''s''a''key", sql);
    }

    [Fact]
    public void An_azure_root_with_only_account_name_emits_no_secret_so_the_ambient_chain_applies()
    {
        // A half credential pair (no matching account_key) is the same fallback shape as the S3
        // half-pair case: no secret, rather than a broken one.
        var stmts = DeltaSecretSql.SetupStatements(Cfg(("root", "az://fs/d"), ("account_name", "myacct")), "lake");
        Assert.DoesNotContain(stmts, s => s.Contains("secret", StringComparison.Ordinal));
    }

    [Fact]
    public void An_s3_root_with_no_credentials_emits_no_secret_so_the_ambient_chain_applies()
    {
        // Instance profiles and AWS_* environment credentials are a real deployment; forcing an empty
        // secret would break them.
        var stmts = DeltaSecretSql.SetupStatements(Cfg(("root", "s3://w/d")), "lake");
        Assert.DoesNotContain(stmts, s => s.Contains("secret", StringComparison.Ordinal));
    }

    [Fact]
    public void Secret_names_are_deterministic_and_do_not_collide_across_similar_connection_names()
    {
        Assert.Equal(DeltaSecretSql.SecretName("prod-db"), DeltaSecretSql.SecretName("prod-db"));
        Assert.NotEqual(DeltaSecretSql.SecretName("prod-db"), DeltaSecretSql.SecretName("prod_db"));
    }

    [Fact]
    public void Secret_names_are_valid_sql_identifiers()
    {
        Assert.Matches("^[a-z][a-z0-9_]*$", DeltaSecretSql.SecretName("Prod DB #1!"));
    }

    [Fact]
    public void Single_quotes_in_a_credential_are_escaped_rather_than_ending_the_literal()
    {
        var sql = DeltaSecretSql.SetupStatements(
            Cfg(("root", "s3://w/d"), ("access_key_id", "AK"), ("secret_access_key", "it's'a'secret")), "lake")
            .Single(s => s.StartsWith("create or replace secret", StringComparison.Ordinal));
        Assert.Contains("it''s''a''secret", sql);
    }

    [Fact]
    public void The_statement_list_is_idempotent_because_the_engine_repeats_it_per_node()
    {
        var cfg = Cfg(("root", "s3://w/d"), ("access_key_id", "AK"), ("secret_access_key", "SK"));
        var a = DeltaSecretSql.SetupStatements(cfg, "lake");
        Assert.Equal(a, DeltaSecretSql.SetupStatements(cfg, "lake"));
        Assert.All(a, s => Assert.True(
            s.StartsWith("install ", StringComparison.Ordinal) ||
            s.StartsWith("load ", StringComparison.Ordinal) ||
            s.StartsWith("create or replace ", StringComparison.Ordinal),
            $"not idempotent: {s}"));
    }
}
