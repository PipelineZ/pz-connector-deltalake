using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>Offline cross-field validation of the CONNECTION block — the only config pz hands
/// ValidateAsync. Returns every error it finds rather than the first, and never throws: a malformed
/// value is an error to report, not an exception to propagate.
///
/// Dataset and output rules do not belong here and cannot: pz validates dataset options against the
/// JSON schema, and output options only inside the sink.</summary>
internal static class DeltaLakeValidation
{
    private static readonly string[] S3Only =
        ["region", "access_key_id", "secret_access_key", "session_token", "endpoint", "url_style", "use_ssl"];

    private static readonly string[] AzureOnly =
        ["account_name", "account_key", "connection_string", "tenant_id", "client_id", "client_secret"];

    public static IReadOnlyList<string> Check(ConnectorConfig connection)
    {
        var errors = new List<string>();

        var root = connection.GetString("root");
        DeltaScheme? scheme = null;
        if (string.IsNullOrWhiteSpace(root))
        {
            errors.Add($"{DeltaErrors.UnsupportedRoot}: deltalake connection requires 'root'. " +
                       "Next step: set 'root' to an s3:// URI, an az://-family URI, or an absolute local path");
        }
        else
        {
            try
            {
                scheme = DeltaLocation.ClassifyRoot(root);
            }
            catch (PzConnectorException ex)
            {
                errors.Add(ex.Message);
            }
        }

        if (scheme is DeltaScheme.Azure or DeltaScheme.Local)
        {
            Reject(errors, connection, S3Only, "an s3:// root", scheme.Value);
        }

        if (scheme is DeltaScheme.S3 or DeltaScheme.Local)
        {
            Reject(errors, connection, AzureOnly, "an az:// root", scheme.Value);
        }

        // Half a credential pair silently falls back to the ambient credential chain and then fails
        // much later with a permissions error that names nothing.
        var hasKeyId = !string.IsNullOrEmpty(connection.GetString("access_key_id"));
        var hasSecret = !string.IsNullOrEmpty(connection.GetString("secret_access_key"));
        if (hasKeyId != hasSecret)
        {
            var missing = hasKeyId ? "secret_access_key" : "access_key_id";
            errors.Add($"{DeltaErrors.SchemeOptionMismatch}: '{missing}' is required alongside the credential " +
                       "already given. Next step: supply both, or neither to use the ambient credential chain");
        }

        return errors;
    }

    private static void Reject(
        List<string> errors, ConnectorConfig connection, string[] keys, string appliesTo, DeltaScheme scheme)
    {
        foreach (var key in keys.Where(k => connection.Values.ContainsKey(k)))
        {
            errors.Add($"{DeltaErrors.SchemeOptionMismatch}: '{key}' applies only to {appliesTo}, but 'root' is " +
                       $"{scheme.ToString().ToLowerInvariant()}. Next step: remove '{key}'");
        }
    }
}
