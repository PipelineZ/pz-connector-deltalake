using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class DeltaSecretSqlTests
{
    private static ConnectorConfig Cfg(params (string Key, object? Value)[] pairs) =>
        new(pairs.ToDictionary(p => p.Key, p => p.Value));

    private static string SoleSecret(IReadOnlyList<string> stmts) =>
        stmts.Single(s => s.StartsWith("create or replace secret", StringComparison.Ordinal));

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
        var secret = SoleSecret(stmts);
        Assert.Contains("region 'eu-west-1'", secret);
    }

    [Fact]
    public void An_s3_root_with_a_full_credential_set_produces_the_exact_secret_statement()
    {
        // A real DuckDB 1.5.5 accepted this exact shape (CREATE SECRET succeeded) — this test pins
        // the generated text against a no-op regression: every Add() field must actually land in the
        // statement, in this fixed order, or the assertion below fails.
        var stmts = DeltaSecretSql.SetupStatements(
            Cfg(("root", "s3://w/d"), ("access_key_id", "AK"), ("secret_access_key", "SK"),
                ("session_token", "TOK"), ("region", "eu-west-1"), ("endpoint", "s3.eu-west-1.amazonaws.com"),
                ("url_style", "path"), ("use_ssl", true)),
            "lake");

        var name = DeltaSecretSql.SecretName("lake");
        Assert.Equal(
            $"create or replace secret {name} (type s3, key_id 'AK', secret 'SK', session_token 'TOK', " +
            "region 'eu-west-1', endpoint 's3.eu-west-1.amazonaws.com', url_style 'path', use_ssl true, " +
            "scope 's3://w/d')",
            SoleSecret(stmts));
    }

    [Fact]
    public void Use_ssl_false_is_rendered_literally_not_dropped()
    {
        var stmts = DeltaSecretSql.SetupStatements(
            Cfg(("root", "s3://w/d"), ("access_key_id", "AK"), ("secret_access_key", "SK"), ("use_ssl", false)),
            "lake");
        Assert.Contains("use_ssl false", SoleSecret(stmts));
    }

    [Fact]
    public void An_s3_root_with_no_key_pair_but_other_options_uses_credential_chain_instead_of_dropping_them()
    {
        // A MinIO connection typically sets endpoint/url_style/use_ssl in connections.yml with
        // credentials supplied via AWS_* environment variables rather than access_key_id/
        // secret_access_key. Dropping the secret entirely (the pre-fix behavior) meant DuckDB read
        // from real AWS using those env credentials instead of the configured MinIO endpoint —
        // silently. provider credential_chain resolves the key/secret from the same AWS_* chain
        // while still carrying the endpoint/url_style/use_ssl overrides. Confirmed against a real
        // DuckDB 1.5.5 with AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY set in the environment: this
        // exact shape succeeds. With no credential source discoverable anywhere in the chain, DuckDB
        // rejects the CREATE SECRET outright at setup-statement time rather than deferring to a
        // confusing read-time failure — the same "no silent failures" direction as every other error
        // in this connector, just enforced by DuckDB itself instead of by this connector's own code.
        var stmts = DeltaSecretSql.SetupStatements(
            Cfg(("root", "s3://w/d"), ("endpoint", "minio:9000"), ("url_style", "path"), ("use_ssl", false)),
            "lake");

        var name = DeltaSecretSql.SecretName("lake");
        Assert.Equal(
            $"create or replace secret {name} (type s3, provider credential_chain, " +
            "endpoint 'minio:9000', url_style 'path', use_ssl false, scope 's3://w/d')",
            SoleSecret(stmts));
    }

    [Fact]
    public void An_s3_root_with_no_credentials_and_no_other_options_emits_no_secret_so_the_ambient_chain_applies()
    {
        // Instance profiles and AWS_* environment credentials are a real deployment; forcing an empty
        // secret would break them.
        var stmts = DeltaSecretSql.SetupStatements(Cfg(("root", "s3://w/d")), "lake");
        Assert.DoesNotContain(stmts, s => s.Contains("secret", StringComparison.Ordinal));
    }

    [Fact]
    public void The_secret_is_scoped_to_this_connections_root()
    {
        var stmts = DeltaSecretSql.SetupStatements(
            Cfg(("root", "s3://prod-bucket/lake"), ("access_key_id", "AK"), ("secret_access_key", "SK")), "prod");
        Assert.Contains("scope 's3://prod-bucket/lake'", SoleSecret(stmts));
    }

    [Fact]
    public void Two_connections_with_different_roots_get_different_scopes_even_with_the_same_key_pair()
    {
        // Without an explicit scope, DuckDB resolves a secret by matching the query path against each
        // secret's SCOPE prefix list; two unscoped S3 secrets both default to the same scope
        // (s3://), and the tie breaks alphabetically by name — independent of creation order — so
        // whichever pz_delta_* name sorts first would silently authenticate every S3 read in the run.
        // Confirmed against a real DuckDB 1.5.5: which_secret() resolved 's3://bucket-one/...' to the
        // alphabetically-first unscoped secret regardless of which one was created second, and only
        // resolved correctly once each secret carried its own root as an explicit scope.
        var prod = SoleSecret(DeltaSecretSql.SetupStatements(
            Cfg(("root", "s3://prod-bucket/lake"), ("access_key_id", "AK"), ("secret_access_key", "SK")), "prod"));
        var staging = SoleSecret(DeltaSecretSql.SetupStatements(
            Cfg(("root", "s3://staging-bucket/lake"), ("access_key_id", "AK"), ("secret_access_key", "SK")),
            "staging"));

        Assert.Contains("scope 's3://prod-bucket/lake'", prod);
        Assert.Contains("scope 's3://staging-bucket/lake'", staging);
    }

    [Fact]
    public void A_single_quote_in_root_is_escaped_in_the_scope_clause_rather_than_ending_the_literal()
    {
        var sql = SoleSecret(DeltaSecretSql.SetupStatements(
            Cfg(("root", "s3://w/d'; drop secret x; --"), ("access_key_id", "AK"), ("secret_access_key", "SK")),
            "lake"));
        Assert.Contains("scope 's3://w/d''; drop secret x; --'", sql);
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
        var sql = SoleSecret(DeltaSecretSql.SetupStatements(
            Cfg(("root", "az://fs/d"), ("account_name", "myacct"), ("account_key", "bXlrZXk=")), "lake"));

        Assert.Contains("connection_string", sql);
        Assert.Contains("DefaultEndpointsProtocol=https", sql);
        Assert.Contains("AccountName=myacct", sql);
        Assert.Contains("AccountKey=bXlrZXk=", sql);
    }

    [Fact]
    public void A_connection_string_takes_precedence_over_account_name_and_key_when_both_are_given()
    {
        var sql = SoleSecret(DeltaSecretSql.SetupStatements(
            Cfg(("root", "az://fs/d"), ("connection_string", "CS"),
                ("account_name", "myacct"), ("account_key", "bXlrZXk=")), "lake"));

        Assert.Contains("connection_string 'CS'", sql);
        Assert.DoesNotContain("AccountKey=", sql);
    }

    [Fact]
    public void Service_principal_takes_precedence_over_account_key_when_both_are_given()
    {
        var sql = SoleSecret(DeltaSecretSql.SetupStatements(
            Cfg(("root", "az://fs/d"), ("account_name", "myacct"), ("tenant_id", "T"), ("client_id", "C"),
                ("client_secret", "CS"), ("account_key", "bXlrZXk=")), "lake"));

        Assert.Contains("provider service_principal", sql);
        Assert.DoesNotContain("AccountKey=", sql);
    }

    [Fact]
    public void Single_quotes_in_a_synthesized_azure_connection_string_are_escaped()
    {
        var sql = SoleSecret(DeltaSecretSql.SetupStatements(
            Cfg(("root", "az://fs/d"), ("account_name", "acct"), ("account_key", "it's'a'key")), "lake"));

        Assert.Contains("it''s''a''key", sql);
    }

    [Fact]
    public void An_azure_root_with_only_account_name_uses_the_credential_chain_provider()
    {
        // DuckDB's azure extension has NO ambient credential fallback the way S3 has AWS_* — confirmed
        // against a real DuckDB 1.5.5: a bare az:// read with no secret configured fails outright
        // ("Invalid Input Error: No valid Azure credentials found!"), even with
        // AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT set in the environment. A secret is
        // therefore mandatory for every az:// read; account_name alone is the managed-identity shape,
        // and "provider credential_chain, account_name '<account>'" is what DuckDB documents for it
        // (confirmed: this exact shape succeeds against a real DuckDB 1.5.5).
        var stmts = DeltaSecretSql.SetupStatements(Cfg(("root", "az://fs/d"), ("account_name", "myacct")), "lake");

        var name = DeltaSecretSql.SecretName("lake");
        Assert.Equal(
            $"create or replace secret {name} (type azure, provider credential_chain, " +
            "account_name 'myacct', scope 'az://fs/d')",
            SoleSecret(stmts));
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
        var sql = SoleSecret(DeltaSecretSql.SetupStatements(
            Cfg(("root", "s3://w/d"), ("access_key_id", "AK"), ("secret_access_key", "it's'a'secret")), "lake"));
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
