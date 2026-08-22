using System.Security.Cryptography;
using System.Text;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>Setup statements the engine runs on its DuckDB session before a delta_scan fragment: the
/// extension loads plus one secret, scoped to this connection's own <c>root</c>, built from the
/// connection config.
///
/// Secret names carry a hash suffix because <c>create or replace</c> is last-wins: two connections
/// whose names sanitize identically ("prod-db"/"prod_db") would otherwise silently clobber each
/// other's CREATE SECRET *statement*. That solves a naming collision, not a selection one — DuckDB
/// never resolves which secret to use by name. It matches the query path against each secret's SCOPE
/// prefix list, and on a tie (two secrets whose scope both match, e.g. two S3 connections with no
/// explicit scope both defaulting to <c>s3://</c>) it picks alphabetically by name, independent of
/// creation order. Left unscoped, a project with a prod lake and a staging lake would have whichever
/// <c>pz_delta_*</c> name sorts first silently authenticate every S3 (or Azure) delta read in the run
/// — the other connection's credentials are simply never used, and it surfaces as a bucket-level 403
/// naming nothing. Every secret this file builds therefore also carries a <c>scope</c> derived from
/// <c>root</c> (trailing-slash normalized — DuckDB's scope match is a plain string prefix match,
/// so an unnormalized root would still cross-wire against a sibling root that merely shares its
/// string prefix, e.g. <c>s3://data-lake</c> against <c>s3://data-lake-archive</c>): the name
/// disambiguates the statement, the scope disambiguates which credentials DuckDB actually picks.
/// Secret names are internal — nothing in plan.json, Reason strings, or events carries them, and no
/// credential value ever leaves this file.</summary>
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
                if (S3Secret(config, SecretName(connectionName), root) is { } s3)
                {
                    statements.Add(s3);
                }

                break;

            case DeltaScheme.Azure:
                statements.Add("install azure");
                statements.Add("load azure");
                if (AzureSecret(config, SecretName(connectionName), root) is { } az)
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

    /// <summary>Null only when nothing s3-shaped is configured at all: DuckDB's own credential chain
    /// (instance profile, AWS_* environment) is a real deployment, and an empty secret would break it.
    /// When other options (endpoint, region, url_style, use_ssl, session_token) are set without an
    /// explicit key pair — the MinIO-with-ambient-credentials shape — those options still have to
    /// reach DuckDB, so the secret is built under <c>provider credential_chain</c> (which resolves the
    /// actual key/secret from the same AWS_* environment / instance profile at CREATE SECRET time)
    /// instead of being dropped outright and silently reading real AWS with the wrong endpoint.</summary>
    private static string? S3Secret(ConnectorConfig config, string name, string root)
    {
        var keyId = config.GetString("access_key_id");
        var secret = config.GetString("secret_access_key");
        var sessionToken = config.GetString("session_token");
        var region = config.GetString("region");
        var endpoint = config.GetString("endpoint");
        var urlStyle = config.GetString("url_style");
        var hasUseSsl = config.Values.ContainsKey("use_ssl");
        var hasKeyPair = !string.IsNullOrEmpty(keyId) && !string.IsNullOrEmpty(secret);

        if (!hasKeyPair && string.IsNullOrEmpty(sessionToken) && string.IsNullOrEmpty(region) &&
            string.IsNullOrEmpty(endpoint) && string.IsNullOrEmpty(urlStyle) && !hasUseSsl)
        {
            return null;
        }

        var parts = new List<string> { "type s3" };
        if (hasKeyPair)
        {
            parts.Add($"key_id {Literal(keyId!)}");
            parts.Add($"secret {Literal(secret!)}");
        }
        else
        {
            parts.Add("provider credential_chain");
        }

        Add(parts, "session_token", sessionToken);
        Add(parts, "region", region);
        Add(parts, "endpoint", StripScheme(endpoint));
        Add(parts, "url_style", urlStyle);
        if (hasUseSsl)
        {
            parts.Add($"use_ssl {(config.GetBool("use_ssl", true) ? "true" : "false")}");
        }

        parts.Add($"scope {Scope(root)}");

        return $"create or replace secret {name} ({string.Join(", ", parts)})";
    }

    private static string? AzureSecret(ConnectorConfig config, string name, string root)
    {
        var connectionString = config.GetString("connection_string");
        if (!string.IsNullOrEmpty(connectionString))
        {
            return $"create or replace secret {name} (type azure, provider config, " +
                   $"connection_string {Literal(connectionString)}, scope {Scope(root)})";
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
                   $"client_secret {Literal(clientSecret)}, account_name {Literal(account)}, " +
                   $"scope {Scope(root)})";
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
                   $"connection_string {Literal(connectionStringFromKey)}, scope {Scope(root)})";
        }

        // account_name alone, with no key and no service-principal quartet, is the managed-identity /
        // instance-metadata deployment shape. Unlike S3, DuckDB's azure extension has NO ambient
        // credential fallback at all — confirmed against a real DuckDB 1.5.5: a bare az:// read with
        // no secret configured (even with AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT set
        // in the environment) fails outright with "Invalid Input Error: No valid Azure credentials
        // found!". A secret is mandatory for every az:// read, so account_name alone must still
        // produce one: "type azure, provider credential_chain, account_name '<account>'" is the shape
        // DuckDB documents for managed identity, and account_name is that provider's one parameter.
        if (!string.IsNullOrEmpty(account))
        {
            return $"create or replace secret {name} (type azure, provider credential_chain, " +
                   $"account_name {Literal(account)}, scope {Scope(root)})";
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

    // DuckDB's S3 secret ENDPOINT is a bare host[:port] -- confirmed against a real DuckDB 1.5.5, which
    // rejects a scheme-prefixed value with "Invalid Input Error: URL needs to start with http:// or
    // https://" thrown from the WRONG place (it means the opposite: giving it one is the error). TLS is
    // controlled separately by the use_ssl parameter. delta-rs's AWS_ENDPOINT_URL storage option is the
    // mirror image -- it requires a full URL. A user's 'endpoint:' value is one string shared by both
    // translations (DeltaStorageOptions.Build normalizes the same value the other way), so this side
    // strips a scheme rather than assuming the user already wrote a bare host.
    private static string? StripScheme(string? value)
    {
        if (value is null)
        {
            return null;
        }

        var schemeEnd = value.IndexOf("://", StringComparison.Ordinal);
        return schemeEnd < 0 ? value : value[(schemeEnd + 3)..];
    }

    // Single-quote doubling: the one escaping rule that keeps a credential from ending its own literal.
    private static string Literal(string value) => $"'{value.Replace("'", "''")}'";

    // DuckDB's secret SCOPE match is a plain string prefix match, not path-boundary-aware: an
    // unnormalized scope of "s3://w/d" also matches the unrelated sibling "s3://w/d2/..." (confirmed
    // against a real DuckDB 1.5.5), so one connection's credential would be selected for another
    // connection's root -- the cross-wiring this scope exists to prevent, narrowed to roots that share
    // a string prefix. A trailing slash makes the prefix match only this root and its own subpaths --
    // reuses DeltaLocation's trim so root and scope agree on one rule rather than two.
    private static string Scope(string root) => Literal(DeltaLocation.TrimTrailingSlash(root) + "/");

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
