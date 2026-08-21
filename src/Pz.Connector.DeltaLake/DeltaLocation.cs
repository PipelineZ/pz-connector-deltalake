using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>Which storage family a <c>root:</c> names. Decides both the DuckDB extension + secret the
/// source loads and the delta-rs storage options the sink builds — one connection config, two
/// translations, which is why the classification lives in one place.</summary>
internal enum DeltaScheme
{
    Local,
    S3,
    Azure,
}

/// <summary>Resolves a Delta table's URI from the connection's <c>root:</c> and the entity name, with
/// an optional <c>path:</c> override. Same composition rule the localfiles and s3 connectors follow —
/// a reader who knows one knows this one: the table directory is <c>&lt;root&gt;/&lt;entity&gt;</c>
/// unless <c>path:</c> says otherwise.</summary>
internal static class DeltaLocation
{
    public static DeltaScheme ClassifyRoot(string root)
    {
        if (root.StartsWith("s3://", StringComparison.OrdinalIgnoreCase) ||
            root.StartsWith("s3a://", StringComparison.OrdinalIgnoreCase))
        {
            return DeltaScheme.S3;
        }

        if (root.StartsWith("az://", StringComparison.OrdinalIgnoreCase) ||
            root.StartsWith("abfs://", StringComparison.OrdinalIgnoreCase) ||
            root.StartsWith("abfss://", StringComparison.OrdinalIgnoreCase) ||
            root.StartsWith("adl://", StringComparison.OrdinalIgnoreCase))
        {
            return DeltaScheme.Azure;
        }

        if (root.StartsWith("file://", StringComparison.OrdinalIgnoreCase) || Path.IsPathRooted(root))
        {
            return DeltaScheme.Local;
        }

        throw new PzConnectorException(
            $"PZDL0101: deltalake connection 'root' must be an s3:// URI, an az://-family URI, or an " +
            $"absolute local path (got '{root}'). Next step: set 'root' to one of those forms.",
            isTransient: false);
    }

    public static string Resolve(string root, string entity, string? path)
    {
        // An absolute override wins outright: the user named a table that does not live under root.
        if (path is not null && (HasScheme(path) || Path.IsPathRooted(path)))
        {
            return Trim(path);
        }

        var baseUri = Trim(root);
        var suffix = string.IsNullOrEmpty(path) ? entity : path.Trim('/');
        return $"{baseUri}/{suffix}";
    }

    private static bool HasScheme(string value) => value.Contains("://", StringComparison.Ordinal);

    private static string Trim(string value) => value.TrimEnd('/');
}
