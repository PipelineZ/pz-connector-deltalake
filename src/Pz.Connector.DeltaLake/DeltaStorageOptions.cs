using DeltaLake.Interfaces;
using DeltaLake.Table;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>The second translation of one connection config: the same credentials the source turns
/// into a DuckDB CREATE SECRET (<see cref="DeltaSecretSql"/>) become delta-rs storage options here.
/// Two translations, one source of truth — every key below was confirmed against the literal strings
/// shipped in the real libdelta_rs_bridge.so for this package version, the same verification approach
/// DeltaErrors uses for its own delta-rs-originated wording, because a plausible-but-wrong key name is
/// silently ignored by delta-rs rather than rejected: the credential would simply never arrive.
///
/// AWS_S3_ALLOW_UNSAFE_RENAME is deliberately absent. It makes concurrent S3 writers lose commits, and
/// a connector that enables it silently would trade a loud failure for a quiet one.</summary>
internal static class DeltaStorageOptions
{
    public static IReadOnlyDictionary<string, string> Build(ConnectorConfig config)
    {
        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        var root = config.GetString("root") ?? string.Empty;

        switch (DeltaLocation.ClassifyRoot(root))
        {
            case DeltaScheme.S3:
                Put(options, "AWS_ACCESS_KEY_ID", config.GetString("access_key_id"));
                Put(options, "AWS_SECRET_ACCESS_KEY", config.GetString("secret_access_key"));
                Put(options, "AWS_SESSION_TOKEN", config.GetString("session_token"));
                Put(options, "AWS_REGION", config.GetString("region"));
                Put(options, "AWS_ENDPOINT_URL", config.GetString("endpoint"));

                // url_style and use_ssl are independent knobs on the DuckDB side (DeltaSecretSql), so
                // they stay independent here too: url_style => addressing style, use_ssl => TLS. object
                // store's default (allow_http unset) already requires TLS, matching pz's use_ssl
                // default of true, so only an explicit opt-out needs to say anything.
                if (config.GetString("url_style") == "path")
                {
                    options["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "false";
                }

                if (!config.GetBool("use_ssl", true))
                {
                    options["AWS_ALLOW_HTTP"] = "true";
                }

                break;

            case DeltaScheme.Azure:
                Put(options, "AZURE_STORAGE_ACCOUNT_NAME", config.GetString("account_name"));
                Put(options, "AZURE_STORAGE_ACCOUNT_KEY", config.GetString("account_key"));
                Put(options, "AZURE_STORAGE_CONNECTION_STRING", config.GetString("connection_string"));
                Put(options, "AZURE_STORAGE_TENANT_ID", config.GetString("tenant_id"));
                Put(options, "AZURE_STORAGE_CLIENT_ID", config.GetString("client_id"));
                Put(options, "AZURE_STORAGE_CLIENT_SECRET", config.GetString("client_secret"));
                break;

            case DeltaScheme.Local:
            default:
                break;
        }

        return options;
    }

    /// <summary>Loads a Delta table through delta-rs to read its metadata (schema, version, protocol) —
    /// never its data. <paramref name="version"/> is a plain <c>long?</c> at this connector's boundary
    /// (validated against the JSON dataset schema's non-negative-integer contract before it ever
    /// reaches here); <see cref="TableOptions.Version"/> is <c>ulong?</c>, so the conversion happens at
    /// the FFI boundary rather than pushing delta-rs's numeric type onto the rest of the connector.</summary>
    public static async Task<ITable> LoadAsync(
        IEngine engine, string location, ConnectorConfig config, long? version, string dataset, CancellationToken ct)
    {
        var options = new TableOptions
        {
            TableLocation = location,
            StorageOptions = Build(config).ToDictionary(kv => kv.Key, kv => kv.Value),
            Version = version is { } v ? checked((ulong)v) : null,
        };

        try
        {
            return await DeltaBigStack.RunAsync(() => engine.LoadTableAsync(options, ct)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // DeltaErrors.Translate rethrows OperationCanceledException before classifying anything
            // else, so a run that was stopped on purpose is never reported as a doomed one. Every other
            // failure lands on TableUnreadable (PZDL0201) here because "read" is in the operation name
            // — that branch exists specifically so read paths get a read code instead of WriteFailed.
            throw DeltaErrors.Translate(ex, $"read of dataset '{dataset}'", []);
        }
    }

    private static void Put(Dictionary<string, string> options, string key, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            options[key] = value;
        }
    }
}
