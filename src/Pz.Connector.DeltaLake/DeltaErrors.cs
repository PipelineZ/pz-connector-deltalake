using System.Runtime.ExceptionServices;
using System.Text.RegularExpressions;
using DeltaLake.Errors;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>The connector's error registry and the one place a delta-rs failure becomes a pz error.
/// Codes carry a PZDL prefix rather than PZ: pz owns the PZ registry and wraps connector failures in
/// its own node-failure code, so a connector minting PZ codes would collide with it. Every code here
/// carries a next step in the message it is raised with; there is not yet a troubleshooting page
/// listing them, and nothing enforces that there is one.</summary>
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

    /// <summary>A write option this connector cannot act on — an unrecognized name, a value of the
    /// wrong shape or outside the range delta-rs accepts, or a <c>strategy:</c> it does not implement.
    /// Offline config, not a runtime write failure: every cause is decided from the OutputSpec alone,
    /// before anything is opened, so they belong here rather than alongside PZDL0404's storage-layer
    /// causes, which a reader hitting one of these has no way to act on.</summary>
    public const string InvalidWriteOption = "PZDL0108";

    // Read.
    public const string TableUnreadable = "PZDL0201";
    public const string VersionNotFound = "PZDL0202";

    // Write, pre-flight.
    public const string SchemaMismatch = "PZDL0301";
    public const string UnwritableArrowType = "PZDL0302";
    public const string MergeKeyNotInSchema = "PZDL0303";

    /// <summary>A column name a merge statement cannot carry. Delta column names come from the
    /// pipeline's own SQL and may contain a '"'; quoting one into a merge statement means doubling it,
    /// and the merge path unescapes a doubled quote one level further than the doubling put in — so the
    /// name resolves to a DIFFERENT column, or to none. Merge-only: append and replace hand the name to
    /// delta-rs directly and never build SQL from it.</summary>
    public const string UnquotableColumnName = "PZDL0304";

    // Write, runtime.
    public const string CommitConflict = "PZDL0401";
    public const string DuplicateMergeKeys = "PZDL0402";
    public const string UnsafeConcurrentS3 = "PZDL0403";
    public const string WriteFailed = "PZDL0404";

    /// <summary>A merge key value that cannot match anything, so the merge would insert a second copy
    /// of a row it should have updated and report success. Decided from the DATA, not the config, which
    /// is why it lives with the runtime codes rather than beside PZDL0103's empty-keys check.</summary>
    public const string UnmatchableMergeKey = "PZDL0405";

    /// <summary>A partition value this write cannot store. A partition value is not ordinary data: it
    /// becomes a directory name, so it inherits that layer's limits — a path-component length cap, and
    /// Delta's own inability to tell an empty partition value from a null one.
    ///
    /// Raised from two places, deliberately. An EMPTY value is refused pre-flight, per batch, by
    /// DeltaWriteSession: on a nullable partition column delta-rs accepts it and silently stores a null,
    /// so there is no failure to translate and no other point at which it can be caught. Everything
    /// else — an empty or null value in a NOT NULL partition column, a name too long for the
    /// filesystem — is a real delta-rs failure and is translated below.</summary>
    public const string UnusablePartitionValue = "PZDL0406";

    // Protocol.
    public const string UnsupportedProtocol = "PZDL0501";

    public static readonly IReadOnlyList<string> AllCodes =
    [
        UnsupportedRoot, SchemeOptionMismatch, MergeWithoutKeys, KeysOverlapPartitions, VersionOnWrite,
        CalendarTokenInReadPath, InvalidMergePredicate, InvalidWriteOption, TableUnreadable, VersionNotFound,
        SchemaMismatch,
        UnwritableArrowType, MergeKeyNotInSchema, UnquotableColumnName, CommitConflict, DuplicateMergeKeys,
        UnsafeConcurrentS3,
        WriteFailed, UnmatchableMergeKey, UnusablePartitionValue, UnsupportedProtocol,
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

    /// <summary>delta-rs's wording when a column the table's schema declares NOT NULL is absent from the
    /// data files already committed. Measured: it is what a write gets for ADDING a non-nullable column
    /// to a table that already has rows — those rows have no value for it, and none can be invented.
    /// Adding one to an EMPTY table succeeds, which is why this is mapped here rather than refused
    /// pre-flight: a pre-flight rule would have to guess at the row count and would cost the empty
    /// case.</summary>
    private const string MissingPhysicalColumnMarker = "missing from the physical schema";

    /// <summary>DataFusion's wording for a statement it could not parse. In a merge that means the
    /// merge_predicate: everything else in the statement is generated by this connector and is covered
    /// by golden tests, so a parse failure points at the one fragment the user supplied. Measured, the
    /// raw message carries at most a token of that fragment — never the generated statement and never a
    /// partition literal — so it is safe to pass through.</summary>
    private const string ParserErrorMarker = "parser error";

    /// <summary>delta-rs's wording when a partition column the table declares NOT NULL receives a value
    /// its directory encoding stores as a null. Measured against the shipped library: an empty value in
    /// a non-nullable partition column produces exactly this, while the same value in an ordinary
    /// non-nullable column produces a different message that names the column plainly, and a struct
    /// column holding nulls produces none at all.
    ///
    /// Still reachable after DeltaWriteSession's pre-flight refusal, which is why it stays: that guard
    /// walks the column encodings a value can be empty in, and an empty BINARY partition value produces
    /// this same message — measured. A genuine null in a NOT NULL partition column does NOT: delta-rs
    /// answers that one with "Column 'x' is declared as non-nullable but contains null values", which
    /// names the column and the cause plainly and needs no mapping of its own.</summary>
    private const string PartitionNullMarker = "found unmasked nulls for non-nullable";

    /// <summary>The filesystem refusing a path component. A partition value becomes a directory name, so
    /// a long one exceeds the 255-byte component cap every common local filesystem has. The raw message
    /// embeds the whole path, and therefore the partition VALUE — which is user data — so this branch is
    /// the one place in this file that must NOT pass the message through.</summary>
    private const string NameTooLongMarker = "file name too long";

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

    /// <summary><paramref name="mergePredicate"/> is the output's merge_predicate when one is
    /// configured. It changes no classification; it decides only whether a parse failure can honestly
    /// point the user at their own predicate or has to be reported as a connector bug.</summary>
    public static PzConnectorException Translate(
        Exception ex, string operation, IReadOnlyList<string> mergeKeys, string? mergePredicate = null)
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

        // Write operations only: the same message reaches a READ of a table some earlier write already
        // widened this way, and "would add a column" is not true of a read.
        if (raw.Contains(MissingPhysicalColumnMarker, StringComparison.OrdinalIgnoreCase)
            && !operation.Contains("read", StringComparison.OrdinalIgnoreCase))
        {
            return Fail(SchemaMismatch,
                $"the delta {operation} would add a column the table declares NOT NULL, and the rows " +
                $"already in the table have no value for it ({raw})",
                "make the added column nullable in the pipeline SQL, or write it to a new table and " +
                "backfill — Delta cannot invent a value for rows that were committed before the column " +
                "existed", ex);
        }

        if (raw.Contains(ParserErrorMarker, StringComparison.OrdinalIgnoreCase)
            && operation.Contains("merge", StringComparison.OrdinalIgnoreCase))
        {
            // The generic next step below names the protocol version and the schema, neither of which
            // has anything to do with a statement that did not parse.
            return Fail(InvalidMergePredicate,
                $"the delta {operation} produced a statement this SQL engine could not parse ({raw})",
                mergePredicate is null
                    ? "report this as a connector bug: no 'merge_predicate' is set on this output, so " +
                      "every part of the statement was generated by the connector"
                    : "check the 'merge_predicate' write option — it is interpolated into the generated " +
                      "statement and is the only part of it this connector did not write. It must be a " +
                      "complete boolean expression on its own", ex);
        }

        if (raw.Contains(PartitionNullMarker, StringComparison.OrdinalIgnoreCase))
        {
            return Fail(UnusablePartitionValue,
                $"the delta {operation} put an empty or null value in a partition column the table " +
                "declares NOT NULL. Delta encodes a partition value into a directory name, where an " +
                $"empty string and a null are the same thing, so neither can be stored there ({raw})",
                "filter those rows out in the pipeline SQL, coalesce the column to a non-empty " +
                "placeholder, or partition by a column that is never empty", ex);
        }

        if (raw.Contains(NameTooLongMarker, StringComparison.OrdinalIgnoreCase))
        {
            // Deliberately no raw message: it is a full path, and the partition value is a segment of
            // it. Nothing here names a value.
            return Fail(UnusablePartitionValue,
                $"the delta {operation} could not create a file because one component of its path was " +
                "longer than this filesystem allows — a partition value becomes a directory name, and " +
                "the common cap is 255 bytes; the value is percent-escaped on the way in, so a shorter " +
                "string can still exceed it",
                "shorten the partition column in the pipeline SQL (hash or truncate it), partition by a " +
                "narrower column, or write to a shorter root path. Object storage has no such limit", ex);
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
            "check the table's protocol version and the incoming schema against the write", ex);
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
