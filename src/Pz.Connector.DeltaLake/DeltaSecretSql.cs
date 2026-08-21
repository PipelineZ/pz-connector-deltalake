using System.Security.Cryptography;
using System.Text;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>Setup statements the engine runs on its DuckDB session before a delta_scan fragment: the
/// extension loads plus one scoped secret built from the connection config.
///
/// Secret names carry a hash suffix because <c>create or replace</c> is last-wins: two connections
/// whose names sanitize identically ("prod-db"/"prod_db") would otherwise silently share one secret
/// and one set of credentials. Secret names are internal — nothing in plan.json, Reason strings, or
/// events carries them, and no credential value ever leaves this file.</summary>
internal static class DeltaSecretSql
{
    public static string SecretName(string connectionName) =>
        $"pz_delta_{Sanitize(connectionName)}_{HashSuffix(connectionName)}";

    public static IReadOnlyList<string> SetupStatements(ConnectorConfig config, string connectionName)
    {
        var root = config.GetString("root") ?? string.Empty;
        var statements = new List<string> { "install delta", "load delta" };

        switch (DeltaLocation.ClassifyRoot(root))
        {
            case DeltaScheme.S3:
                statements.Add("install httpfs");
                statements.Add("load httpfs");
                if (S3Secret(config, SecretName(connectionName)) is { } s3)
                {
                    statements.Add(s3);
                }

                break;

            case DeltaScheme.Azure:
                statements.Add("install azure");
                statements.Add("load azure");
                if (AzureSecret(config, SecretName(connectionName)) is { } az)
                {
                    statements.Add(az);
                }

                break;

            case DeltaScheme.Local:
            default:
                break;
        }

        return statements;
    }

    /// <summary>Null when no explicit credentials are configured: DuckDB's own credential chain
    /// (instance profile, AWS_* environment) is a real deployment, and an empty secret would break it.</summary>
    private static string? S3Secret(ConnectorConfig config, string name)
    {
        var keyId = config.GetString("access_key_id");
        var secret = config.GetString("secret_access_key");
        if (string.IsNullOrEmpty(keyId) || string.IsNullOrEmpty(secret))
        {
            return null;
        }

        var parts = new List<string> { "type s3", $"key_id {Literal(keyId)}", $"secret {Literal(secret)}" };
        Add(parts, "session_token", config.GetString("session_token"));
        Add(parts, "region", config.GetString("region"));
        Add(parts, "endpoint", config.GetString("endpoint"));
        Add(parts, "url_style", config.GetString("url_style"));
        if (config.Values.ContainsKey("use_ssl"))
        {
            parts.Add($"use_ssl {(config.GetBool("use_ssl", true) ? "true" : "false")}");
        }

        return $"create or replace secret {name} ({string.Join(", ", parts)})";
    }

    private static string? AzureSecret(ConnectorConfig config, string name)
    {
        var connectionString = config.GetString("connection_string");
        if (!string.IsNullOrEmpty(connectionString))
        {
            return $"create or replace secret {name} (type azure, provider config, " +
                   $"connection_string {Literal(connectionString)})";
        }

        var account = config.GetString("account_name");
        var tenant = config.GetString("tenant_id");
        var clientId = config.GetString("client_id");
        var clientSecret = config.GetString("client_secret");
        if (!string.IsNullOrEmpty(account) && !string.IsNullOrEmpty(tenant) &&
            !string.IsNullOrEmpty(clientId) && !string.IsNullOrEmpty(clientSecret))
        {
            return $"create or replace secret {name} (type azure, provider service_principal, " +
                   $"tenant_id {Literal(tenant)}, client_id {Literal(clientId)}, " +
                   $"client_secret {Literal(clientSecret)}, account_name {Literal(account)})";
        }

        // DuckDB's azure secret type has no discrete account_key parameter — confirmed against a real
        // DuckDB 1.5.5 instance, which rejects "type azure, ..., account_key '...'" with "Binder
        // Error: Unknown parameter 'account_key' for secret type 'azure' with provider 'config'".
        // Shared-key auth is only reachable by synthesizing the connection string DuckDB does accept;
        // the synthesized string is itself a credential and goes through the same Literal() quoting
        // as every other secret value here.
        var accountKey = config.GetString("account_key");
        if (!string.IsNullOrEmpty(account) && !string.IsNullOrEmpty(accountKey))
        {
            var connectionStringFromKey =
                $"DefaultEndpointsProtocol=https;AccountName={account};AccountKey={accountKey};" +
                "EndpointSuffix=core.windows.net";
            return $"create or replace secret {name} (type azure, provider config, " +
                   $"connection_string {Literal(connectionStringFromKey)})";
        }

        return null;
    }

    private static void Add(List<string> parts, string key, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            parts.Add($"{key} {Literal(value)}");
        }
    }

    // Single-quote doubling: the one escaping rule that keeps a credential from ending its own literal.
    private static string Literal(string value) => $"'{value.Replace("'", "''")}'";

    private static string Sanitize(string name)
    {
        var sb = new StringBuilder();
        foreach (var c in name.ToLowerInvariant())
        {
            sb.Append(char.IsAsciiLetterOrDigit(c) ? c : '_');
        }

        var s = sb.ToString().Trim('_');
        return s.Length == 0 || !char.IsAsciiLetter(s[0]) ? $"c{s}" : s;
    }

    private static string HashSuffix(string name) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..8];
}
