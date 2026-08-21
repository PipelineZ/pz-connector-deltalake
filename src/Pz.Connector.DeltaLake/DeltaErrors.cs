using System.Runtime.ExceptionServices;
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

    /// <summary>An output option this connector cannot act on — an unrecognized name, a value of the
    /// wrong shape, or one outside the range delta-rs accepts. Offline config, not a runtime write
    /// failure: it is decided from the OutputSpec alone, before anything is opened, so it belongs in
    /// this family rather than alongside PZDL0404's storage-layer causes.</summary>
    public const string InvalidWriteOption = "PZDL0108";

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
        CalendarTokenInReadPath, InvalidMergePredicate, InvalidWriteOption, TableUnreadable, VersionNotFound,
        SchemaMismatch,
        UnwritableArrowType, MergeKeyNotInSchema, CommitConflict, DuplicateMergeKeys, UnsafeConcurrentS3,
        WriteFailed, UnsupportedProtocol,
    ];

    /// <summary>Bare substrings that identify an optimistic-concurrency loss. "already exists" is
    /// deliberately absent: delta-rs uses that exact phrase both for a version-conflict retry AND for
    /// the permanent "A table already exists at: ..." create-time error (confirmed in the shipped
    /// delta-rs binary), so it cannot be a bare substring marker without misclassifying the permanent
    /// case as transient — see <see cref="VersionAlreadyExists"/>, which disambiguates the two.</summary>
    private static readonly string[] ConflictMarkers =
        ["concurrent", "conflict", "metadata changed", "version mismatch"];

    /// <summary>Matches delta-rs's version-conflict wording ("version 7 already exists", "version
    /// already exists") without matching the unrelated permanent create-time "table already exists"
    /// error, which never mentions "version". Confirmed against literal strings shipped in
    /// libdelta_rs_bridge.so: the conflict family ("version already exists, will retry", the kernel
    /// VersionAlreadyExists error) always pairs "version" with "already exists"; the permanent family
    /// ("A table already exists at: ", "Table already exists at path: ", "A Delta Lake table already
    /// exists at that location.") never does.</summary>
    private static readonly Regex VersionAlreadyExists = new(
        @"(?i)version\b.{0,40}?already exists", RegexOptions.Compiled);

    /// <summary>Bare substrings for storage-layer failures that are retryable on their own terms, not
    /// because of an optimistic-concurrency loss. Each is a literal string confirmed shipped in
    /// libdelta_rs_bridge.so: the network markers are Rust's own <c>std::io::ErrorKind</c> Display text
    /// (bubbled up through delta-rs's IOError variant), and the throttling markers are the DynamoDB
    /// lock client's literal AWS error text ("Provisioned table throughput exceeded",
    /// "ThrottlingException", "RequestLimitExceeded").</summary>
    private static readonly string[] TransientStorageMarkers =
    [
        "connection reset", "connection refused", "connection aborted", "network unreachable",
        "network down", "broken pipe", "not connected", "timed out",
        "throttl", "throughput exceeded", "requestlimitexceeded",
    ];

    private const string DuplicateMergeMarker = "multiple source rows";

    /// <summary>Truncates delta-rs's row-data preview, keeping the row count. Both halves matter: the
    /// count ("2 rows failed validation check") is the entire diagnostic and carries no data, while
    /// everything from "Preview of invalid data:" onward is an ASCII table of the offending ROWS —
    /// every column of them, id and amount and name alike — which a validation failure would otherwise
    /// carry into a PzConnectorException, a run artifact and a log. Reproduced against real delta-rs on
    /// the plainest write there is: a batch holding nulls in a column the table declares NOT NULL. No
    /// pre-flight guard can prevent that one, because an Arrow schema's nullability flag says nothing
    /// about whether the batch actually contains nulls — refusing on the declaration would refuse
    /// legitimate writes — so redaction here is the only place it can be stopped.</summary>
    private static readonly Regex InvalidDataPreview = new(
        @"(?i)(Preview of invalid data:).*", RegexOptions.Compiled | RegexOptions.Singleline);

    /// <summary>The single-cell sibling of the shape above: delta-rs reports a failed CHECK constraint
    /// or column invariant as "Invalid data found: validation check failed with value &lt;value&gt;",
    /// putting one offending cell in the message rather than a table of rows.</summary>
    private static readonly Regex InvalidDataValue = new(
        @"(?i)(validation check failed with value).*", RegexOptions.Compiled | RegexOptions.Singleline);

    /// <summary>A contiguous run of ASCII box-table lines, redacted whole. This is deliberate
    /// duplication of <see cref="InvalidDataPreview"/>: an enumeration of the two markers delta-rs uses
    /// today rots the moment upstream renames one, whereas the box table IS the format the data comes
    /// in. Over-redaction is the safe direction in this file, and a legitimate error line beginning
    /// with '|' or '+' followed by a rule character is not a shape any delta-rs message uses.</summary>
    private static readonly Regex BoxTableRun = new(
        @"(?m)(?:^[ \t]*[|+][-+| ][^\n]*\n?)+", RegexOptions.Compiled);

    /// <summary>Strips anything shaped like a credential out of a third-party message before it reaches
    /// a user-visible error. delta-rs is free to put storage options in its own error text; this
    /// connector is not free to pass them on. The keyword sits inside <c>[a-z0-9_]*...[a-z0-9_]*</c>
    /// rather than behind a word boundary, because object_store's option keys are env-var style
    /// (<c>AWS_SECRET_ACCESS_KEY</c>, <c>azure_storage_account_key</c>) — the sensitive word is a
    /// substring of the key, not the whole key, and <c>\b</c> does not fire mid-identifier across an
    /// underscore. The bare <c>key</c> alternative is a deliberate catch-all: over-redaction is the
    /// safe direction here, so any <c>*_key</c>-shaped option (present or future — e.g.
    /// <c>azure_storage_sas_key</c>) is covered without having to enumerate every spelling. The
    /// captured group is the key name, kept in the replacement so the message still says which field
    /// was redacted without saying what it held.</summary>
    private static readonly Regex SecretShapedKeyValue = new(
        @"(?i)\b([a-z0-9_]*(?:secret|password|passwd|client.?secret|session.?token|sas.?token|" +
        @"connection.?string|credential|bearer|sig(?:nature)?|token|key)[a-z0-9_]*)\s*[=:]\s*" +
        @"(?:'[^']*'|""[^""]*""|\S+)",
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

        // A cancelled run is not a delta failure. Rethrowing (rather than wrapping into a permanent
        // PZDL0404) lets the engine's own cancellation handling see it instead of reporting a run that
        // was stopped on purpose as a doomed one. ExceptionDispatchInfo preserves the original stack.
        if (ex is OperationCanceledException)
        {
            ExceptionDispatchInfo.Capture(ex).Throw();
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

        if (ConflictMarkers.Any(m => raw.Contains(m, StringComparison.OrdinalIgnoreCase)) ||
            VersionAlreadyExists.IsMatch(raw))
        {
            return Transient(CommitConflict,
                $"the delta {operation} lost a commit race against another writer ({raw})",
                "no action needed if retries are configured; otherwise re-run", ex);
        }

        if (TransientStorageMarkers.Any(m => raw.Contains(m, StringComparison.OrdinalIgnoreCase)))
        {
            return Transient(CommitConflict,
                $"the delta {operation} hit a transient storage error ({raw})",
                "no action needed if retries are configured; otherwise re-run", ex);
        }

        // TableUnreadable (PZDL0201) exists specifically for the read path; an unrecognized write
        // failure has nowhere else to land but WriteFailed (PZDL0404).
        var fallbackCode = operation.Contains("read", StringComparison.OrdinalIgnoreCase)
            ? TableUnreadable
            : WriteFailed;

        return Fail(fallbackCode, $"the delta {operation} failed ({raw})",
            "check the table's protocol version and the incoming schema; see docs/troubleshooting.md", ex);
    }

    private static string Redact(string message)
    {
        // User DATA first, credentials after: the data shapes are truncations to end-of-message, so
        // running them first also removes anything a later pattern would have had to scan.
        var redacted = InvalidDataPreview.Replace(message, "$1 <redacted>");
        redacted = InvalidDataValue.Replace(redacted, "$1 <redacted>");
        redacted = BoxTableRun.Replace(redacted, "<redacted>\n");

        redacted = UrlEmbeddedCredential.Replace(redacted, "://<redacted>@");
        redacted = BearerTokenValue.Replace(redacted, "Bearer <redacted>");
        return SecretShapedKeyValue.Replace(redacted, "$1=<redacted>");
    }
}
