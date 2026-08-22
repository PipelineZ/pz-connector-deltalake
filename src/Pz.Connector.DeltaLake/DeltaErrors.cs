using System.Runtime.ExceptionServices;
using System.Text.RegularExpressions;
using DeltaLake.Errors;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>Which kind of operation a failure came from. Passed to <see cref="DeltaErrors.Translate"/>
/// explicitly rather than sniffed out of the human-readable operation text, because that text embeds
/// the user's own output name: an output called "spreadsheet", "threads" or "readings" contains "read",
/// and a write against it would otherwise be classified — and coded — as a read failure.</summary>
internal enum DeltaOperationKind
{
    Read,
    Write,
    Merge,
}

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

    /// <summary>A merge key whose Arrow type the duplicate-key resolver cannot compare row to row.
    /// Refused at BeginWriteAsync rather than at commit: without a comparison there is no way to tell
    /// whether one write names the same key twice, and an unresolved repeat is a silently duplicated
    /// row rather than an error.</summary>
    public const string UnresolvableMergeKeyType = "PZDL0305";

    // Write, runtime.
    public const string CommitConflict = "PZDL0401";
    public const string DuplicateMergeKeys = "PZDL0402";
    /// <summary>An S3 commit that did not go through the commit path this connector configures.
    /// delta-rs 0.33.0 commits to s3:// with a conditional PUT and never with a rename (measured — see
    /// <see cref="DeltaStorageOptions"/>), and that is what makes a second writer safe; anything that
    /// replaces or removes that path is this code. Two causes reach it, and they differ in how:
    /// <list type="bullet">
    /// <item><description>A commit path with no concurrency guarantee left —
    /// <see cref="UnsafeS3CommitMarkers"/>. Not reachable through the storage options this connector
    /// builds, and not through the environment either (the explicit pin wins — measured). It is a
    /// backstop against a delta-rs whose default moves.</description></item>
    /// <item><description>A commit DIVERTED to delta-rs's DynamoDB locking log store —
    /// <see cref="DivertedS3LogStoreMarkers"/>. Reachable, and the only measured way a user's
    /// environment can break every S3 read and write of this connector permanently:
    /// <c>AWS_S3_LOCKING_PROVIDER=dynamodb</c> selects that log store, and it then fails because this
    /// connector configures no lock table for it.</description></item>
    /// </list>
    /// A code of its own rather than another PZDL0404 for two independent reasons. PZDL0404's next
    /// step names the table's protocol version and the incoming schema, neither of which has anything
    /// to do with either cause. And the rename refusal's own wording says "concurrent writers", which
    /// the conflict markers below match — so without a branch ahead of them, a permanent configuration
    /// error is reported as a retryable commit race and the engine retries a run that can never
    /// succeed.</summary>
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
        UnwritableArrowType, MergeKeyNotInSchema, UnquotableColumnName, UnresolvableMergeKeyType,
        CommitConflict, DuplicateMergeKeys,
        UnsafeConcurrentS3,
        WriteFailed, UnmatchableMergeKey, UnusablePartitionValue, UnsupportedProtocol,
    ];

    /// <summary>Bare substrings identifying an S3 commit path that cannot be made safe. Both are
    /// literal strings confirmed shipped in libdelta_rs_bridge.so and both were reproduced against a
    /// real MinIO:
    /// <list type="bullet">
    /// <item><description>"requires a LockClient" is the tail of "Atomic rename requires a LockClient
    /// for S3 backends. Either configure the LockClient, or set AWS_S3_ALLOW_UNSAFE_RENAME=true to opt
    /// out of support for concurrent writers." — the rename path's refusal.</description></item>
    /// <item><description>"conditional put is disabled" is object_store's refusal to perform the
    /// PutMode::Create this connector's commits depend on; reproduced by setting
    /// AWS_CONDITIONAL_PUT=disabled, which is the one way left to reach a commit path with no
    /// concurrency guarantee at all.</description></item>
    /// </list>
    /// Matched BEFORE <see cref="ConflictMarkers"/>, and that order is load-bearing: the rename
    /// refusal contains the word "concurrent", so the conflict branch would otherwise claim it and
    /// report a permanent misconfiguration as a race worth retrying.</summary>
    private static readonly string[] UnsafeS3CommitMarkers =
        ["requires a lockclient", "conditional put is disabled"];

    /// <summary>Bare substrings identifying a commit diverted to delta-rs's DynamoDB locking log
    /// store. Both are literal strings confirmed shipped in libdelta_rs_bridge.so and both were
    /// reproduced against a real MinIO with <c>AWS_S3_LOCKING_PROVIDER=dynamodb</c> set: a CREATE
    /// answers "Transaction failed: … dynamodb client failed to write log entry", while an append and
    /// a plain table OPEN both answer "Generic error: error in DynamoDb". The open failing is why this
    /// branch is not gated on <see cref="DeltaOperationKind"/> — the variable breaks reads as well as
    /// writes, and the remedy is the same sentence either way.
    ///
    /// Matched AFTER <see cref="TransientStorageMarkers"/>, and that order is load-bearing in the
    /// opposite direction from <see cref="UnsafeS3CommitMarkers"/>'s: the DynamoDB lock client's own
    /// throttling wording ("ThrottlingException", "Provisioned table throughput exceeded") is
    /// genuinely retryable, and a DynamoDB deployment that IS configured correctly must keep getting
    /// the transient classification rather than this permanent one.</summary>
    private static readonly string[] DivertedS3LogStoreMarkers =
        ["error in dynamodb", "dynamodb client failed to write log entry"];

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
    /// DeltaLakeSink's Reconcile now refuses this before a session exists, for every strategy, so this
    /// mapping is a BACKSTOP rather than the main guard — it covers the one window Reconcile cannot: a
    /// concurrent writer changing the table between the schema read at BeginWriteAsync and this write's
    /// commit. That makes it unreachable from an integration test, so DeltaErrorsTests pins it
    /// directly.</summary>
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

    /// <summary>The words that make a field name credential-shaped, shared by the two shapes a
    /// credential reaches this connector in: <see cref="SecretShapedKeyValue"/> (an option echo, a
    /// connection string) and <see cref="SecretShapedXmlElement"/> (an S3 or Azure XML error body).
    /// One list, so a word added for one shape is never missing from the other — which is exactly how
    /// the XML shape came to be uncovered.</summary>
    private const string SecretWords =
        "secret|password|passwd|client.?secret|session.?token|sas.?token|connection.?string|" +
        "credential|bearer|sig(?:nature)?|token|key";

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
        $@"(?i)\b([a-z0-9_]*(?:{SecretWords})[a-z0-9_]*)\s*[=:]\s*(?:'[^']*'|""[^""]*""|\S+)",
        RegexOptions.Compiled);

    /// <summary>The same alphabet in the OTHER shape a credential arrives in: an XML element. S3 and
    /// Azure both answer a rejected request with an XML error body, and delta-rs passes that body
    /// through into its own message verbatim, so a redactor that only understands <c>name=value</c>
    /// covers one of the two shapes this connector actually meets.
    ///
    /// Real Amazon S3's 403 for a wrong secret carries
    /// <c>&lt;AWSAccessKeyId&gt;AKIA…&lt;/AWSAccessKeyId&gt;</c> and
    /// <c>&lt;StringToSign&gt;…&lt;/StringToSign&gt;</c>; MinIO's body omits both, which is why no
    /// container test in this repository can reach the shape and why
    /// <see cref="SecretShapedKeyValue"/> alone looked sufficient for as long as it did.
    /// DeltaErrorsTests feeds the real AWS body directly.
    ///
    /// The element-name character class is wider than the key=value one — element names carry
    /// namespace colons, dots and dashes that an env-var-style option key never does. Only the NAME is
    /// captured; an optional attribute run is matched but left outside the group, so <c>&lt;/\1&gt;</c>
    /// still closes on the bare name. Redaction is by backreference, so an opening tag only ever
    /// silences its OWN closing tag. Over-redaction is the safe direction, as everywhere else in this
    /// file: S3's own <c>&lt;Key&gt;</c> element (the object path, not a credential) matches the bare
    /// <c>key</c> catch-all and is silenced too. That costs nothing — every message carrying it also
    /// carries the same path inside the request URL, which is not an XML element and is left alone.
    ///
    /// THE PERIMETER, written down so the next author inherits it rather than rediscovering it. This
    /// pattern covers a well-formed element, with or without attributes, whose name contains a word
    /// from <see cref="SecretWords"/>. It deliberately does NOT cover:
    /// <list type="bullet">
    /// <item><description>a JSON-shaped body (<c>"AccountKey":"…"</c>) — the key=value pattern catches
    /// the unquoted form, not the quoted one;</description></item>
    /// <item><description>an unclosed or truncated element, where there is no <c>&lt;/name&gt;</c> for
    /// the backreference to find;</description></item>
    /// <item><description>whitespace or a newline between the name and its <c>&gt;</c>.</description></item>
    /// </list>
    /// Each is a deliberate limit, not an oversight: no service this connector talks to is known to
    /// produce any of them, and widening a security-boundary regex against bodies nobody has seen buys
    /// a speculative gain at the cost of real over-redaction and real backtracking. Measured, this
    /// pattern's backtracking is quadratic rather than catastrophic (100 KB in 137 ms, 1 MB in 9 s) —
    /// bounded, but not a budget to spend on shapes that do not exist. Add a shape here when a real
    /// message is observed carrying it, and bring the message with it.</summary>
    private static readonly Regex SecretShapedXmlElement = new(
        $@"(?is)<([a-z0-9_:.\-]*(?:{SecretWords})[a-z0-9_:.\-]*)(?:\s[^>]*)?>.*?</\1>",
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

    /// <summary><paramref name="kind"/> decides classification; <paramref name="operation"/> is only
    /// the phrase the message reads back to the user and is never inspected.
    /// <paramref name="mergePredicate"/> is the output's merge_predicate when one is configured — it
    /// changes no classification either, and decides only whether a parse failure can honestly point the
    /// user at their own predicate or has to be reported as a connector bug.</summary>
    public static PzConnectorException Translate(
        Exception ex, DeltaOperationKind kind, string operation, IReadOnlyList<string> mergeKeys,
        string? mergePredicate = null)
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
            // What delta-rs's marker means, by its own wording, is SOURCE-side multiplicity: two
            // incoming rows matching one target row. DeltaMergeDedup removes exactly that before the
            // statement runs, so reaching here means the resolver's notion of key equality disagreed
            // with DataFusion's — a defect in this connector, not something the user configured.
            //
            // The next step therefore asks for a report rather than naming a fix, and two other
            // wordings were rejected on measurement rather than taste. "Deduplicate upstream" is wrong
            // because the input IS deduplicated by the time delta-rs sees it. "Remove the duplicate
            // rows from the table" is wrong because a duplicated key in the TARGET does not produce
            // this error at all: measured against delta-rs 0.33.0, a merge into a table holding two
            // rows for one key commits, updates both copies and leaves the duplicate in place,
            // silently. That is a documented limit, not this code path.
            return Fail(DuplicateMergeKeys,
                $"delta-rs reports more than one incoming row per merge key ({keys}), which this " +
                "connector resolves before a merge runs — so reaching this means its duplicate-key " +
                "resolution did not recognise two of this write's own rows as sharing a key",
                "report this as a connector bug, naming the merge key column(s) and their types — " +
                "there is no configuration change that avoids it", ex);
        }

        // Write operations only: the same message reaches a READ of a table some earlier write already
        // widened this way, and "would add a column" is not true of a read.
        if (raw.Contains(MissingPhysicalColumnMarker, StringComparison.OrdinalIgnoreCase)
            && kind is not DeltaOperationKind.Read)
        {
            return Fail(SchemaMismatch,
                $"the delta {operation} would add a column the table declares NOT NULL, and the rows " +
                $"already in the table have no value for it ({raw})",
                "make the added column nullable in the pipeline SQL, or write it to a new table and " +
                "backfill — Delta cannot invent a value for rows that were committed before the column " +
                "existed", ex);
        }

        if (raw.Contains(ParserErrorMarker, StringComparison.OrdinalIgnoreCase)
            && kind is DeltaOperationKind.Merge)
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

        if (UnsafeS3CommitMarkers.Any(m => raw.Contains(m, StringComparison.OrdinalIgnoreCase)))
        {
            return Fail(UnsafeConcurrentS3,
                $"the delta {operation} could not commit through a mechanism that is safe against a " +
                $"second writer, so the table was left unchanged ({raw})",
                "leave AWS_S3_ALLOW_UNSAFE_RENAME unset — it replaces this failure with commits that " +
                "are silently overwritten — and remove any AWS_CONDITIONAL_PUT or " +
                "AWS_S3_LOCKING_PROVIDER value in the environment. This connector commits with a " +
                "conditional PUT, which needs no locking provider against an endpoint that supports " +
                "one", ex);
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

        if (DivertedS3LogStoreMarkers.Any(m => raw.Contains(m, StringComparison.OrdinalIgnoreCase)))
        {
            return Fail(UnsafeConcurrentS3,
                $"the delta {operation} was routed through delta-rs's DynamoDB locking log store, " +
                $"which this connector never configures and cannot supply a lock table for ({raw})",
                "remove AWS_S3_LOCKING_PROVIDER from the environment this run inherits — it is the " +
                "only thing that selects that log store, and this connector does not need it: its " +
                "commits use a conditional PUT, which is safe against a second writer with no lock " +
                "table at all", ex);
        }

        // TableUnreadable (PZDL0201) exists specifically for the read path; an unrecognized write
        // failure has nowhere else to land but WriteFailed (PZDL0404).
        var fallbackCode = kind is DeltaOperationKind.Read ? TableUnreadable : WriteFailed;

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

        // XML before the scalar shapes: an error body is a CONTAINER, and silencing the element whole
        // also removes anything inside it that a later pattern would otherwise have had to recognize.
        redacted = SecretShapedXmlElement.Replace(redacted, "<$1><redacted></$1>");

        redacted = UrlEmbeddedCredential.Replace(redacted, "://<redacted>@");
        redacted = BearerTokenValue.Replace(redacted, "Bearer <redacted>");
        return SecretShapedKeyValue.Replace(redacted, "$1=<redacted>");
    }
}
