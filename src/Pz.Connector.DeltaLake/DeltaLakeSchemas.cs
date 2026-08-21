namespace Pz.Connector.DeltaLake;

/// <summary>The JSON Schemas pz validates a deltalake config against, offline, for a connector it has
/// never loaded. Both are strict (<c>additionalProperties: false</c>) so a typo'd option fails
/// validation instead of being silently ignored.
///
/// The dataset schema declares READ options only, because that is all pz validates it against — sink
/// output options are checked by this connector at BeginWriteAsync instead. WriteOptions below is
/// therefore not a schema; it is the authoritative list the sink validates against and the
/// documentation drift test compares the write reference page to.</summary>
internal static class DeltaLakeSchemas
{
    public static readonly IReadOnlyList<string> ConnectionOptions =
    [
        "root", "region", "access_key_id", "secret_access_key", "session_token", "endpoint",
        "url_style", "use_ssl", "account_name", "account_key", "connection_string", "tenant_id",
        "client_id", "client_secret",
    ];

    public static readonly IReadOnlyList<string> ReadOptions = ["path", "version", "union_by_name"];

    /// <summary>Write options this connector understands. Deliberately excludes every name pz strips
    /// before a connector sees it — strategy, keys, duplicates, on_delete, schema_policy, retry —
    /// and excludes the retired 'mode' spelling, which pz refuses outright.</summary>
    public static readonly IReadOnlyList<string> WriteOptions =
        ["path", "partition_by", "merge_predicate", "target_file_bytes", "max_rows_per_group"];

    /// <summary>A calendar token as pz's path templating spells it: a brace-delimited run of date
    /// format characters. Delta partitions declaratively by column value, so a templated read path has
    /// nothing to expand and must be refused rather than silently taken literally.</summary>
    public const string CalendarTokenPattern = @"\{[yMdHhms][yMdHhms\-_/:. ]*\}";

    public const string Connection =
        """
        { "type": "object", "required": ["root"], "properties": {
            "root": { "type": "string" },
            "region": { "type": "string" },
            "access_key_id": { "type": "string" },
            "secret_access_key": { "type": "string" },
            "session_token": { "type": "string" },
            "endpoint": { "type": "string" },
            "url_style": { "enum": ["vhost", "path"] },
            "use_ssl": { "type": "boolean" },
            "account_name": { "type": "string" },
            "account_key": { "type": "string" },
            "connection_string": { "type": "string" },
            "tenant_id": { "type": "string" },
            "client_id": { "type": "string" },
            "client_secret": { "type": "string" }
          }, "additionalProperties": false }
        """;

    public const string Dataset =
        """
        { "type": "object", "properties": {
            "path": { "type": "string", "not": { "pattern": "\\{[yMdHhms][yMdHhms\\-_/:. ]*\\}" } },
            "version": { "type": "integer", "minimum": 0 },
            "union_by_name": { "type": "boolean" }
          }, "additionalProperties": false }
        """;
}
