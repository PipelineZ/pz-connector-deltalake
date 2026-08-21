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
/// AZURE_STORAGE_CONNECTION_STRING is confirmed ABSENT from that binary (no "connection_string"
/// substring anywhere in it, while every other Azure alias is present as a contiguous literal), which
/// is why a configured connection_string is decomposed into the keys that ARE present rather than
/// forwarded as-is.
///
/// AWS_S3_ALLOW_UNSAFE_RENAME is deliberately absent. It makes concurrent S3 writers lose commits, and
/// a connector that enables it silently would trade a loud failure for a quiet one.
///
/// The Azure branch mirrors <see cref="DeltaSecretSql.AzureSecret"/>'s strict precedence (connection
/// string, then the service-principal quartet, then account name + key, then account name alone) one
/// mechanism at a time rather than emitting every configured key simultaneously: with a partial config
/// (e.g. both a connection string AND stray tenant/client fields left over from a previous edit),
/// DuckDB and delta-rs must pick the SAME mechanism or a read and a schema probe against the same
/// dataset can authenticate two different ways.</summary>
internal static class DeltaStorageOptions
{
    public static IReadOnlyDictionary<string, string> Build(ConnectorConfig config)
    {
        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        var root = config.GetString("root") ?? string.Empty;

        switch (DeltaLocation.ClassifyRoot(root))
        {
            case DeltaScheme.S3:
                ApplyS3Options(options, config);
                break;

            case DeltaScheme.Azure:
                ApplyAzureOptions(options, config);
                break;

            case DeltaScheme.Local:
            default:
                break;
        }

        return options;
    }

    /// <summary>Loads a Delta table through delta-rs to read its metadata (schema, version, protocol) —
    /// never its data. <paramref name="version"/> is a plain <c>long?</c> at this connector's boundary
    /// (validated as non-negative by the caller before it ever reaches here — see
    /// <c>DeltaLakeSource.ParseVersionOption</c>); <see cref="TableOptions.Version"/> is
    /// <c>ulong?</c>, so the conversion happens at the FFI boundary rather than pushing delta-rs's
    /// numeric type onto the rest of the connector. The whole construction, including the cast, sits
    /// inside the try: any failure here — an out-of-range version included — becomes a coded PZDL
    /// error through <see cref="DeltaErrors.Translate"/> rather than an unhandled exception.</summary>
    public static async Task<ITable> LoadAsync(
        IEngine engine, string location, ConnectorConfig config, long? version, string dataset, CancellationToken ct)
    {
        try
        {
            var options = new TableOptions
            {
                TableLocation = location,
                StorageOptions = Build(config).ToDictionary(kv => kv.Key, kv => kv.Value),

                // TableOptions.Version is deliberately left unset here, even when a version was
                // requested -- see the LoadVersionAsync call below for why.
            };

            return await DeltaBigStack.RunAsync(async () =>
            {
                // No ConfigureAwait(false) on either await in this delegate: DeltaBigStack.RunAsync
                // installs a pumping SynchronizationContext specifically so a plain await here resumes
                // on the same big-stack thread the delegate started on, not wherever the antecedent
                // task happened to complete. ConfigureAwait(false) would opt back out of that and put
                // the second delta-rs call -- LoadVersionAsync below -- back on a default-stack pool
                // thread, exactly the bug this connector's DeltaBigStackTests now pins.
                var table = await engine.LoadTableAsync(options, ct);
                if (version is { } v)
                {
                    try
                    {
                        // Confirmed against the real 0.33.0 package (a throwaway load against a table
                        // with a schema-widening second commit, reflection-driven since there is no
                        // public API surface for it otherwise): passing TableOptions.Version = 0 to
                        // LoadTableAsync pins table.Version() correctly to 0, but table.Schema()/
                        // table.Metadata() still reflect the table's LATEST commit, not version 0's --
                        // a library bug specific to requesting version 0 through TableOptions. Calling
                        // LoadVersionAsync AFTER a version-less load does not have this bug for any
                        // version tested (0, 1, 2), so every requested version goes through this call
                        // instead of TableOptions.Version, not only to work around version 0 but
                        // because a version-specific TableOptions code path already proved
                        // untrustworthy once.
                        await table.LoadVersionAsync(checked((ulong)v), ct);
                    }
                    catch
                    {
                        // LoadTableAsync above already allocated a native table handle; a version
                        // absent from the log (the ordinary case this catches) must not leak it just
                        // because the failure happened one call later than it used to.
                        if (table is IDisposable disposable)
                        {
                            disposable.Dispose();
                        }

                        throw;
                    }
                }

                return table;
            }).ConfigureAwait(false);
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

    private static void ApplyS3Options(Dictionary<string, string> options, ConnectorConfig config)
    {
        Put(options, "AWS_ACCESS_KEY_ID", config.GetString("access_key_id"));
        Put(options, "AWS_SECRET_ACCESS_KEY", config.GetString("secret_access_key"));
        Put(options, "AWS_SESSION_TOKEN", config.GetString("session_token"));
        Put(options, "AWS_REGION", config.GetString("region"));

        // object_store's AWS_ENDPOINT_URL requires a scheme; DuckDB's own ENDPOINT secret parameter
        // (DeltaSecretSql.S3Secret) requires the opposite -- a bare host[:port], confirmed against a
        // real DuckDB 1.5.5. One 'endpoint:' value has to satisfy both, so this side adds a scheme
        // (chosen by use_ssl, same default as DuckDB's own use_ssl default of true) rather than
        // assuming the user already wrote a full URL.
        var endpoint = config.GetString("endpoint");
        if (!string.IsNullOrEmpty(endpoint))
        {
            var useSsl = config.GetBool("use_ssl", true);
            options["AWS_ENDPOINT_URL"] = endpoint.Contains("://", StringComparison.Ordinal)
                ? endpoint
                : $"{(useSsl ? "https" : "http")}://{endpoint}";
        }

        // url_style and use_ssl are independent knobs on the DuckDB side (DeltaSecretSql), so they stay
        // independent here too: url_style => addressing style, use_ssl => TLS. object_store's default
        // (allow_http unset) already requires TLS, matching pz's use_ssl default of true, so only an
        // explicit opt-out needs to say anything.
        if (config.GetString("url_style") == "path")
        {
            options["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "false";
        }

        if (!config.GetBool("use_ssl", true))
        {
            options["AWS_ALLOW_HTTP"] = "true";
        }
    }

    private static void ApplyAzureOptions(Dictionary<string, string> options, ConnectorConfig config)
    {
        // Precedence 1: a connection string. DuckDB accepts one directly; delta-rs has no equivalent
        // key at all (see the type doc comment), so it is decomposed into the real aliases instead of
        // forwarded as an inert key nothing reads.
        var connectionString = config.GetString("connection_string");
        if (!string.IsNullOrEmpty(connectionString))
        {
            ApplyAzureConnectionString(options, connectionString);
            return;
        }

        var account = config.GetString("account_name");

        // Precedence 2: the service-principal quartet.
        var tenant = config.GetString("tenant_id");
        var clientId = config.GetString("client_id");
        var clientSecret = config.GetString("client_secret");
        if (!string.IsNullOrEmpty(account) && !string.IsNullOrEmpty(tenant) &&
            !string.IsNullOrEmpty(clientId) && !string.IsNullOrEmpty(clientSecret))
        {
            options["AZURE_STORAGE_ACCOUNT_NAME"] = account;
            options["AZURE_STORAGE_TENANT_ID"] = tenant;
            options["AZURE_STORAGE_CLIENT_ID"] = clientId;
            options["AZURE_STORAGE_CLIENT_SECRET"] = clientSecret;
            return;
        }

        // Precedence 3: account name + shared key.
        var accountKey = config.GetString("account_key");
        if (!string.IsNullOrEmpty(account) && !string.IsNullOrEmpty(accountKey))
        {
            options["AZURE_STORAGE_ACCOUNT_NAME"] = account;
            options["AZURE_STORAGE_ACCOUNT_KEY"] = accountKey;
            return;
        }

        // Precedence 4: account name alone (managed identity / ambient credential chain).
        if (!string.IsNullOrEmpty(account))
        {
            options["AZURE_STORAGE_ACCOUNT_NAME"] = account;
        }
    }

    // Decomposes a standard Azure Storage connection string ("Key1=Value1;Key2=Value2;...") into the
    // delta-rs storage-option keys confirmed present in the shipped binary. AccountKey values are
    // base64 and routinely contain their own '=' padding, so each segment splits on its FIRST '=' only
    // -- key names never contain '=', so this is unambiguous.
    private static void ApplyAzureConnectionString(Dictionary<string, string> options, string connectionString)
    {
        var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var segment in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var separator = segment.IndexOf('=');
            if (separator <= 0)
            {
                continue;
            }

            var key = segment[..separator].Trim();
            var value = segment[(separator + 1)..].Trim();
            if (key.Length > 0 && value.Length > 0)
            {
                fields[key] = value;
            }
        }

        Put(options, "AZURE_STORAGE_ACCOUNT_NAME", fields.GetValueOrDefault("AccountName"));
        Put(options, "AZURE_STORAGE_ACCOUNT_KEY", fields.GetValueOrDefault("AccountKey"));
        Put(options, "AZURE_STORAGE_SAS_KEY", fields.GetValueOrDefault("SharedAccessSignature"));
        Put(options, "AZURE_STORAGE_ENDPOINT", fields.GetValueOrDefault("BlobEndpoint"));
    }

    private static void Put(Dictionary<string, string> options, string key, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            options[key] = value;
        }
    }
}
