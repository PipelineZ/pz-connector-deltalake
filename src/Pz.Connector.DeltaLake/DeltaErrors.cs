using System.Text.RegularExpressions;
using DeltaLake.Errors;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>The connector's error registry and the one place a delta-rs failure becomes a pz error.
/// Codes carry a PZDL prefix rather than PZ: pz owns the PZ registry and wraps connector failures in
/// its own node-failure code, so a connector minting PZ codes would collide with it. Every code here
/// must appear in docs/troubleshooting.md — a test enforces that.</summary>
internal static class DeltaErrors
{
    // Configuration (offline validation).
    public const string UnsupportedRoot = "PZDL0101";
    public const string SchemeOptionMismatch = "PZDL0102";
    public const string MergeWithoutKeys = "PZDL0103";
    public const string KeysOverlapPartitions = "PZDL0104";
    public const string VersionOnWrite = "PZDL0105";
    public const string CalendarTokenInReadPath = "PZDL0106";
    public const string InvalidMergePredicate = "PZDL0107";

    // Read.
    public const string TableUnreadable = "PZDL0201";
    public const string VersionNotFound = "PZDL0202";

    // Write, pre-flight.
    public const string SchemaMismatch = "PZDL0301";
    public const string UnwritableArrowType = "PZDL0302";
    public const string MergeKeyNotInSchema = "PZDL0303";

    // Write, runtime.
    public const string CommitConflict = "PZDL0401";
    public const string DuplicateMergeKeys = "PZDL0402";
    public const string UnsafeConcurrentS3 = "PZDL0403";
    public const string WriteFailed = "PZDL0404";

    // Protocol.
    public const string UnsupportedProtocol = "PZDL0501";

    public static readonly IReadOnlyList<string> AllCodes =
    [
        UnsupportedRoot, SchemeOptionMismatch, MergeWithoutKeys, KeysOverlapPartitions, VersionOnWrite,
        CalendarTokenInReadPath, InvalidMergePredicate, TableUnreadable, VersionNotFound, SchemaMismatch,
        UnwritableArrowType, MergeKeyNotInSchema, CommitConflict, DuplicateMergeKeys, UnsafeConcurrentS3,
        WriteFailed, UnsupportedProtocol,
    ];

    /// <summary>Markers that identify an optimistic-concurrency loss. delta-rs owns conflict detection;
    /// this list only decides whether pz's retry policy gets a chance. Extend it when a real conflict
    /// surfaces with wording not covered here — the concurrency test is what catches that.</summary>
    private static readonly string[] ConflictMarkers =
        ["already exists", "concurrent", "conflict", "metadata changed", "version mismatch"];

    private const string DuplicateMergeMarker = "multiple source rows";

    /// <summary>Strips anything shaped like a credential out of a third-party message before it reaches
    /// a user-visible error. delta-rs is free to put storage options in its own error text; this
    /// connector is not free to pass them on. The keyword sits inside <c>[a-z0-9_]*...[a-z0-9_]*</c>
    /// rather than behind a word boundary, because object_store's option keys are env-var style
    /// (<c>AWS_SECRET_ACCESS_KEY</c>, <c>azure_storage_account_key</c>) — the sensitive word is a
    /// substring of the key, not the whole key, and <c>\b</c> does not fire mid-identifier across an
    /// underscore. The captured group is the key name, kept in the replacement so the message still
    /// says which field was redacted without saying what it held.</summary>
    private static readonly Regex SecretShapedKeyValue = new(
        @"(?i)\b([a-z0-9_]*(?:secret|password|passwd|access.?key|account.?key|client.?secret|" +
        @"session.?token|sas.?token|connection.?string|credential|bearer|api.?key|sig(?:nature)?|" +
        @"token)[a-z0-9_]*)\s*[=:]\s*(?:'[^']*'|""[^""]*""|\S+)",
        RegexOptions.Compiled);

    /// <summary>Matches userinfo embedded in a URL (<c>scheme://user:pass@host</c>) — the shape a
    /// misconfigured connection string comes back in. Greedy up to the LAST '@' before the next '/' or
    /// whitespace, because a raw (non-percent-encoded) '@' inside the password would otherwise end the
    /// match early and leak the password's tail.</summary>
    private static readonly Regex UrlEmbeddedCredential = new(@"://[^\s/]*@", RegexOptions.Compiled);

    /// <summary>Matches an HTTP bearer token, which is space-separated from its keyword rather than
    /// joined by '=' or ':' the way every other secret shape here is.</summary>
    private static readonly Regex BearerTokenValue = new(@"(?i)\bbearer\s+\S+", RegexOptions.Compiled);

    public static PzConnectorException Fail(string code, string what, string nextStep, Exception? inner = null) =>
        new($"{code}: {what}. Next step: {nextStep}", isTransient: false, retryAfter: null, innerException: inner);

    public static PzConnectorException Transient(string code, string what, string nextStep, Exception? inner = null) =>
        new($"{code}: {what}. Next step: {nextStep}", isTransient: true, retryAfter: null, innerException: inner);

    public static PzConnectorException Translate(Exception ex, string operation, IReadOnlyList<string> mergeKeys)
    {
        if (ex is PzConnectorException already)
        {
            return already;
        }

        var raw = Redact(ex.Message);

        if (raw.Contains(DuplicateMergeMarker, StringComparison.OrdinalIgnoreCase))
        {
            var keys = mergeKeys.Count == 0 ? "the merge keys" : string.Join(", ", mergeKeys);
            return Fail(DuplicateMergeKeys,
                $"the incoming batch contains more than one row per merge key ({keys}), so the merge cannot " +
                "decide which row wins",
                "deduplicate upstream — for example add a qualify/row_number filter in the pipeline SQL so " +
                "each key appears once", ex);
        }

        if (ConflictMarkers.Any(m => raw.Contains(m, StringComparison.OrdinalIgnoreCase)))
        {
            return Transient(CommitConflict,
                $"the delta {operation} lost a commit race against another writer ({raw})",
                "no action needed if retries are configured; otherwise re-run", ex);
        }

        return Fail(WriteFailed, $"the delta {operation} failed ({raw})",
            "check the table's protocol version and the incoming schema; see docs/troubleshooting.md", ex);
    }

    private static string Redact(string message)
    {
        var redacted = UrlEmbeddedCredential.Replace(message, "://<redacted>@");
        redacted = BearerTokenValue.Replace(redacted, "Bearer <redacted>");
        return SecretShapedKeyValue.Replace(redacted, "$1=<redacted>");
    }
}
