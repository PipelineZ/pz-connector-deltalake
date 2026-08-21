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

        throw DeltaErrors.Fail(DeltaErrors.UnsupportedRoot,
            $"deltalake connection 'root' must be an s3:// URI, an az://-family URI, or an absolute " +
            $"local path (got '{root}')",
            "set 'root' to one of those forms");
    }

    public static string Resolve(string root, string entity, string? path)
    {
        // An absolute override wins outright: the user named a table that does not live under root.
        if (path is not null && (HasScheme(path) || Path.IsPathRooted(path)))
        {
            return TrimTrailingSlash(path);
        }

        var baseUri = TrimTrailingSlash(root);
        var suffix = string.IsNullOrEmpty(path) ? entity : path.Trim('/');
        return $"{baseUri}/{suffix}";
    }

    /// <summary>The one trailing-slash rule for a root/path fragment. Internal (not private) because
    /// DeltaSecretSql reuses it to normalize a SCOPE value — DuckDB's secret scope match is a plain
    /// string prefix match, not path-boundary-aware, so an unnormalized root and its own trailing-slash
    /// form must agree or two roots that are string prefixes of each other (s3://data-lake vs.
    /// s3://data-lake-archive) cross-wire exactly like an unscoped secret would.</summary>
    internal static string TrimTrailingSlash(string value) => value.TrimEnd('/');

    private static bool HasScheme(string value) => value.Contains("://", StringComparison.Ordinal);
}
