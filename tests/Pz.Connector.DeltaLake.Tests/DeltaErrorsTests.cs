using DeltaLake.Errors;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class DeltaErrorsTests
{
    [Fact]
    public void Every_code_is_registered_exactly_once_and_well_formed()
    {
        Assert.Equal(DeltaErrors.AllCodes.Count, DeltaErrors.AllCodes.Distinct().Count());
        Assert.All(DeltaErrors.AllCodes, c => Assert.Matches(@"^PZDL\d{4}$", c));
    }

    [Fact]
    public void Fail_leads_with_the_code_names_the_cause_and_gives_a_next_step()
    {
        var ex = DeltaErrors.Fail(DeltaErrors.SchemaMismatch, "column 'amt' is double in the table but string in the input",
            "cast the column upstream, or set schema_policy: evolve");
        Assert.StartsWith("PZDL0301:", ex.Message);
        Assert.Contains("column 'amt'", ex.Message);
        Assert.Contains("Next step:", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Translate_maps_a_missing_non_nullable_column_to_PZDL0301_naming_nullability()
    {
        // Reconcile now refuses a NOT NULL column the write ADDS before any session exists, so this
        // mapping is a backstop for the one window it cannot cover: another writer changing the table
        // between BeginWriteAsync's schema read and this write's commit. Unreachable from an
        // integration test by construction, which is why it is pinned here instead.
        var raw = new DeltaLakeException(
            "Execution error: Non-nullable column 'note' is missing from the physical schema", 1);
        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Merge, "merge of output 'orders'", ["id"]);

        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);
        Assert.Contains("nullable", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.False(ex.IsTransient);
    }

    [Theory]
    // The operation phrase embeds the OUTPUT'S OWN NAME, so it cannot be what decides read-vs-write.
    // Each of these contains "read" as a substring while naming a perfectly ordinary output.
    [InlineData("spreadsheet")]
    [InlineData("threads")]
    [InlineData("readings")]
    public void Translate_classifies_a_write_by_its_kind_not_by_a_substring_of_the_output_name(string output)
    {
        var raw = new DeltaLakeException("some opaque delta-rs failure", 1);
        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, $"append of output '{output}'", []);

        // PZDL0201 is the READ code. A write that lands on it sends the user to the wrong half of the
        // reference for a failure that has nothing to do with reading.
        Assert.Contains(DeltaErrors.WriteFailed, ex.Message);
        Assert.DoesNotContain(DeltaErrors.TableUnreadable, ex.Message);
    }

    [Fact]
    public void Translate_still_reaches_the_nullability_branch_for_an_output_whose_name_contains_read()
    {
        // The same substring would have skipped the NOT NULL mapping entirely, leaving the generic
        // protocol-version next step on a failure whose remedy is a nullability change.
        var raw = new DeltaLakeException(
            "Execution error: Non-nullable column 'note' is missing from the physical schema", 1);
        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "merge of output 'spreadsheet'", []);

        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);
        Assert.Contains("nullable", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Translate_leaves_a_missing_non_nullable_column_on_a_READ_to_the_read_code()
    {
        // The same delta-rs message reaches a READ of a table some earlier write already broke this
        // way, and "would add a column" is not true of a read.
        var raw = new DeltaLakeException(
            "Execution error: Non-nullable column 'note' is missing from the physical schema", 1);
        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Read, "read of dataset 'orders'", []);

        Assert.Contains(DeltaErrors.TableUnreadable, ex.Message);
    }

    [Fact]
    public void Transient_marks_the_exception_so_the_engine_can_retry_it()
    {
        var ex = DeltaErrors.Transient(DeltaErrors.CommitConflict, "another writer committed first", "retry the run");
        Assert.True(ex.IsTransient);
    }

    [Fact]
    public void Translate_maps_a_duplicate_key_merge_failure_to_PZDL0402_and_asks_for_a_bug_report()
    {
        // delta-rs's marker names SOURCE-side multiplicity, and DeltaMergeDedup removes that before a
        // merge runs -- so the only way to reach this message is the resolver disagreeing with
        // DataFusion about key equality, which is a connector defect. The message must therefore ask
        // for a report, not send the user to edit something. Both wordings this replaced named causes
        // that provably do not produce the error: the pipeline's own SELECT (already resolved), and
        // duplicate rows in the TARGET (measured to commit silently instead of erroring).
        var raw = new DeltaLakeException(
            "MERGE matched a target row with multiple source rows that satisfy duplicate relevant WHEN MATCHED clauses", 1);
        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Merge, "merge", ["order_id", "region"]);
        Assert.Contains("PZDL0402", ex.Message);
        Assert.Contains("order_id, region", ex.Message);
        Assert.Contains("connector bug", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("pipeline SQL", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("remove the duplicate rows", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.False(ex.IsTransient);
        Assert.Same(raw, ex.InnerException);
    }

    [Theory]
    [InlineData("Delta transaction failed, version 7 already exists")]
    [InlineData("Failed to commit transaction: version already exists")]
    [InlineData("Metadata changed since last commit")]
    public void Translate_classifies_commit_races_as_transient(string message)
    {
        var ex = DeltaErrors.Translate(new DeltaLakeException(message, 1), DeltaOperationKind.Write, "commit", []);
        Assert.True(ex.IsTransient);
        Assert.Contains("PZDL0401", ex.Message);
    }

    // Real Amazon S3's 403 body for a wrong secret, verbatim. MinIO's body omits <AWSAccessKeyId> and
    // <StringToSign> entirely, so the integration tripwire in MinioTests CANNOT reach this shape --
    // it passed for as long as the redactor covered only name=value because the store it runs against
    // never produces the other shape. This is the fact that can fire.
    [Fact]
    public void Translate_never_leaks_an_access_key_id_out_of_an_s3_xml_error_body()
    {
        const string Body =
            "Error performing GET https://bucket.s3.amazonaws.com/t/_delta_log/_last_checkpoint - " +
            "Server returned non-2xx status code: 403 Forbidden: " +
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SignatureDoesNotMatch</Code>" +
            "<Message>The request signature we calculated does not match the signature you provided.</Message>" +
            "<AWSAccessKeyId>AKIAIOSFODNN7EXAMPLE</AWSAccessKeyId>" +
            "<StringToSign>AWS4-HMAC-SHA256\n20260822T000000Z\n20260822/us-east-1/s3/aws4_request</StringToSign>" +
            "<SignatureProvided>0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef</SignatureProvided>" +
            "<BucketName>bucket</BucketName><RequestId>18CE059D999CE9DA</RequestId></Error>";

        var ex = DeltaErrors.Translate(
            new DeltaLakeException(Body, 1), DeltaOperationKind.Write, "append of output 'orders'", []);

        Assert.DoesNotContain("AKIAIOSFODNN7EXAMPLE", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("AWS4-HMAC-SHA256", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("0123456789abcdef", ex.Message, StringComparison.Ordinal);

        // The operational half survives: which bucket, which code, which request. Without it a storage
        // failure names nothing an operator can act on, and run artifacts carry this message verbatim.
        Assert.Contains("SignatureDoesNotMatch", ex.Message, StringComparison.Ordinal);
        Assert.Contains("bucket", ex.Message, StringComparison.Ordinal);
        Assert.Contains("18CE059D999CE9DA", ex.Message, StringComparison.Ordinal);
    }

    // An element that carries ATTRIBUTES is the same shape with something between the name and its
    // '>', and the pattern required them to be adjacent -- so a body spelling the credential
    // <Credential type="aws4">...</Credential> went through untouched. Attributes are a plausible
    // variant of the XML bodies this connector demonstrably meets, so this one shape is closed; the
    // shapes that stay open (JSON, unclosed elements, whitespace before '>') are written down beside
    // the pattern rather than guessed at.
    [Theory]
    [InlineData("<Credential type=\"aws4\">AKIAIOSFODNN7EXAMPLE/20260822/us-east-1</Credential>")]
    [InlineData("<AccountKey xmlns=\"urn:x\" n=\"1\">c2VjcmV0dmFsdWU=</AccountKey>")]
    [InlineData("<sig  encoding='base64'>AbCdEf123</sig>")]
    public void Translate_never_leaks_a_credential_out_of_an_xml_element_with_attributes(string element)
    {
        var ex = DeltaErrors.Translate(
            new DeltaLakeException($"storage rejected the request: <Error>{element}</Error>", 1),
            DeltaOperationKind.Write, "append of output 'orders'", []);

        Assert.DoesNotContain("AKIAIOSFODNN7EXAMPLE", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("c2VjcmV0dmFsdWU=", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("AbCdEf123", ex.Message, StringComparison.Ordinal);

        // The attribute run is matched but never captured, so the closing tag still resolves and the
        // element name survives to say WHICH field was silenced.
        Assert.Contains("<redacted>", ex.Message, StringComparison.Ordinal);
    }

    // An Azure XML error body is the same shape with different element names. Nothing in it is a
    // credential today, so this pins that the XML rule does not eat the diagnosis.
    [Fact]
    public void Translate_keeps_the_diagnosis_in_an_azure_xml_error_body()
    {
        const string Body =
            "Server returned non-2xx status code: 403 Forbidden: <?xml version=\"1.0\"?><Error>" +
            "<Code>AuthorizationFailure</Code><Message>Server failed to authenticate the request. " +
            "RequestId:6c29cdce-54da-488d-8589-cac712cdf407</Message></Error>";

        var ex = DeltaErrors.Translate(
            new DeltaLakeException(Body, 1), DeltaOperationKind.Write, "append of output 'orders'", []);

        Assert.Contains("AuthorizationFailure", ex.Message, StringComparison.Ordinal);
        Assert.Contains("403", ex.Message, StringComparison.Ordinal);
    }

    // AWS_S3_LOCKING_PROVIDER=dynamodb is the one measured way a user's environment breaks every S3
    // read and write of this connector permanently. Both wordings are real: a CREATE says the first,
    // an append and a plain OPEN say the second (S3ConcurrencyTests reaches them end to end). Before
    // this branch both landed on PZDL0404, whose next step names the table's protocol version and the
    // incoming schema -- the wrong subsystem entirely.
    [Theory]
    [InlineData("Transaction failed: Transaction failed: dynamodb client failed to write log entry")]
    [InlineData("Generic error: error in DynamoDb")]
    public void Translate_names_the_locking_provider_that_diverted_the_commit(string message)
    {
        var ex = DeltaErrors.Translate(new DeltaLakeException(message, 1), DeltaOperationKind.Write, "append", []);
        Assert.Contains(DeltaErrors.UnsafeConcurrentS3, ex.Message, StringComparison.Ordinal);
        Assert.Contains("AWS_S3_LOCKING_PROVIDER", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }

    // The same diversion breaks a READ too -- the table cannot even be opened -- so the branch is not
    // gated on the operation kind, and this pins that a read does not fall through to PZDL0201 with no
    // mention of the variable that caused it.
    [Fact]
    public void Translate_names_the_locking_provider_on_a_read_as_well_as_a_write()
    {
        var ex = DeltaErrors.Translate(
            new DeltaLakeException("Generic error: error in DynamoDb", 1), DeltaOperationKind.Read, "read", []);
        Assert.Contains(DeltaErrors.UnsafeConcurrentS3, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(DeltaErrors.TableUnreadable, ex.Message, StringComparison.Ordinal);
    }

    // The ordering teeth for the branch above: a CORRECTLY configured DynamoDB deployment that is
    // merely throttled must keep the transient classification. Both markers appear in one message, and
    // the transient check runs first.
    [Theory]
    [InlineData("DynamoDb error: ThrottlingException in DynamoDb")]
    [InlineData("Generic error: error in DynamoDb: Provisioned table throughput exceeded")]
    public void Translate_keeps_a_throttled_dynamodb_failure_transient(string message)
    {
        var ex = DeltaErrors.Translate(new DeltaLakeException(message, 1), DeltaOperationKind.Write, "commit", []);
        Assert.True(ex.IsTransient, ex.Message);
        Assert.Contains(DeltaErrors.CommitConflict, ex.Message, StringComparison.Ordinal);
    }

    // The rename refusal's own wording ends "...to opt out of support for concurrent writers", and
    // "concurrent" is a bare conflict marker — so before PZDL0403 existed as a branch, this permanent
    // misconfiguration was reported as a retryable commit race and the engine would have retried a run
    // that can never succeed. Both messages below are literal delta-rs wording: the first is shipped
    // verbatim in libdelta_rs_bridge.so, the second was reproduced against a real MinIO by setting
    // AWS_CONDITIONAL_PUT=disabled (S3ConcurrencyTests pins that one end to end).
    [Theory]
    [InlineData("Atomic rename requires a LockClient for S3 backends. Either configure the LockClient, " +
                "or set AWS_S3_ALLOW_UNSAFE_RENAME=true to opt out of support for concurrent writers.")]
    [InlineData("Failed to read delta log object: Operation `put_opts` with mode `PutMode::Create` when " +
                "conditional put is disabled not yet implemented by AmazonS3(bucket).")]
    public void Translate_classifies_an_unsafe_s3_commit_path_as_permanent_not_a_commit_race(string message)
    {
        var ex = DeltaErrors.Translate(new DeltaLakeException(message, 1), DeltaOperationKind.Write, "append", []);
        Assert.Contains(DeltaErrors.UnsafeConcurrentS3, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(DeltaErrors.CommitConflict, ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
        Assert.Contains("AWS_S3_ALLOW_UNSAFE_RENAME", ex.Message, StringComparison.Ordinal);

        // The message says the TABLE was left unchanged, never that nothing was written. Measured
        // against a real MinIO: this branch fails at the log entry, which delta-rs commits last, so an
        // append that reaches it has already put its parquet in the bucket -- three objects before,
        // four after, the table still holding exactly its old rows. "Nothing was written" would send a
        // user looking for nothing to clean up.
        Assert.Contains("left unchanged", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("nothing was written", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Translate_classifies_an_unknown_delta_failure_as_permanent_not_transient()
    {
        // Guessing "transient" for an unrecognized failure would make the engine retry a permanent
        // error N times before reporting it. Unknown means permanent.
        var ex = DeltaErrors.Translate(new DeltaLakeException("schema mismatch: field 'x' not found", 1), DeltaOperationKind.Write, "insert", []);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Translate_passes_a_PzConnectorException_through_untouched()
    {
        var original = DeltaErrors.Fail(DeltaErrors.SchemaMismatch, "already mapped", "do the thing");
        Assert.Same(original, DeltaErrors.Translate(original, DeltaOperationKind.Write, "insert", []));
    }

    [Fact]
    public void Translate_never_leaks_a_storage_option_value()
    {
        var raw = new DeltaLakeException("failed with secret_access_key=AKIAWHOOPS in the message", 1);
        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "insert", []);
        Assert.DoesNotContain("AKIAWHOOPS", ex.Message);
    }

    // object_store (delta-rs's storage backend) reports option keys env-var style — uppercase, keyword
    // buried mid-identifier behind an underscore — not the lowercase standalone form the sample test
    // above uses. A regex anchored on a word boundary right before the keyword misses these.
    [Theory]
    [InlineData("put failed: AWS_SECRET_ACCESS_KEY=abcd1234efgh5678 rejected", "abcd1234efgh5678")]
    [InlineData("azure rejected azure_storage_account_key=Eby8vdM02xNOcqFbLTjXNsp4gYimSLGCK", "Eby8vdM02xNOcqFbLTjXNsp4gYimSLGCK")]
    [InlineData("bad options: AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE rejected", "AKIAIOSFODNN7EXAMPLE")]
    [InlineData("generic error: azure_storage_sas_key=SECRETSASVALUE111 rejected", "SECRETSASVALUE111")]
    [InlineData("generic error: sas_key=SECRETSASVALUE222 rejected", "SECRETSASVALUE222")]
    public void Translate_never_leaks_an_env_var_style_storage_option(string message, string secret)
    {
        var ex = DeltaErrors.Translate(new DeltaLakeException(message, 1), DeltaOperationKind.Write, "insert", []);
        Assert.DoesNotContain(secret, ex.Message);
    }

    // A misconfigured connection string can come back embedded in a URL rather than as key=value.
    [Fact]
    public void Translate_never_leaks_a_credential_embedded_in_a_url()
    {
        var raw = new DeltaLakeException(
            "PUT https://myaccount:zGl3AbG9NDGgAAcVpSimplePass@blob.core.windows.net/container/file failed", 1);
        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "insert", []);
        Assert.DoesNotContain("zGl3AbG9NDGgAAcVpSimplePass", ex.Message);
        Assert.DoesNotContain("myaccount:zGl3", ex.Message);
    }

    // Same as above, but the password itself contains a raw (non-percent-encoded) '@' — the shape most
    // likely to defeat a naive "stop at the first @" pattern.
    [Fact]
    public void Translate_never_leaks_a_url_credential_whose_password_contains_an_at_sign()
    {
        var raw = new DeltaLakeException(
            "PUT https://myaccount:P@ssw0rdZZZ@blob.core.windows.net/container/file failed", 1);
        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "insert", []);
        Assert.DoesNotContain("ssw0rdZZZ", ex.Message);
    }

    [Fact]
    public void Translate_never_leaks_a_bearer_token()
    {
        var raw = new DeltaLakeException(
            "request failed: Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig", 1);
        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "insert", []);
        Assert.DoesNotContain("eyJhbGciOiJIUzI1NiJ9.payload.sig", ex.Message);
    }

    [Fact]
    public void Translate_never_leaks_a_sas_signature_query_parameter()
    {
        var raw = new DeltaLakeException(
            "sas token rejected: https://a.blob.core.windows.net/c/b?sv=2021&sig=AbCdEf123%3D&se=2026-01-01", 1);
        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "insert", []);
        Assert.DoesNotContain("AbCdEf123", ex.Message);
    }

    // delta-rs uses "already exists" both for a version-conflict retry AND for the permanent
    // create-time "table already exists" error. An unanchored "already exists" marker would classify
    // this permanent failure as a transient commit race, so pz would retry a doomed create and then
    // report a misleading conflict message. Confirmed real delta-rs wording (from the shipped
    // libdelta_rs_bridge.so): "A table already exists at: ", "Table already exists at path: ", and
    // "A Delta Lake table already exists at that location." — none of them mention "version".
    [Theory]
    [InlineData("A table already exists at: s3://bucket/table")]
    [InlineData("Table already exists at path: /data/table")]
    [InlineData("A Delta Lake table already exists at that location.")]
    public void Translate_classifies_a_table_already_exists_create_failure_as_permanent(string message)
    {
        var ex = DeltaErrors.Translate(new DeltaLakeException(message, 1), DeltaOperationKind.Write, "create", []);
        Assert.False(ex.IsTransient);
    }

    // Storage-layer failures that are retryable on their own terms (not an optimistic-concurrency
    // loss) must still be offered to the engine's retry policy. Each message below is built from a
    // literal string confirmed shipped in libdelta_rs_bridge.so: the network wording is Rust's own
    // std::io::ErrorKind Display text, and the throttling wording is the DynamoDB lock client's literal
    // AWS error text.
    [Theory]
    [InlineData("request failed: connection reset")]
    [InlineData("request failed: connection refused")]
    [InlineData("request failed: connection aborted")]
    [InlineData("request failed: network unreachable")]
    [InlineData("request failed: broken pipe")]
    [InlineData("request failed: timed out")]
    [InlineData("DynamoDb error: ThrottlingException")]
    [InlineData("DynamoDb error: Provisioned table throughput exceeded")]
    public void Translate_classifies_a_storage_layer_transient_failure_as_transient(string message)
    {
        var ex = DeltaErrors.Translate(new DeltaLakeException(message, 1), DeltaOperationKind.Write, "commit", []);
        Assert.True(ex.IsTransient);
    }

    // TableUnreadable (PZDL0201) exists specifically for the read path; before this fix, every
    // unrecognized failure — including read failures — fell through to WriteFailed (PZDL0404).
    [Fact]
    public void Translate_classifies_an_unrecognized_read_failure_as_table_unreadable()
    {
        var ex = DeltaErrors.Translate(new DeltaLakeException("some opaque delta-rs read error", 1), DeltaOperationKind.Read, "read", []);
        Assert.Contains(DeltaErrors.TableUnreadable, ex.Message);
        Assert.False(ex.IsTransient);
    }

    // A cancelled run is not a delta failure. Wrapping it into a permanent PZDL0404 would report a run
    // stopped on purpose as a doomed one, and would hide cancellation from the engine's own handling
    // for it. Translate must let it propagate as cancellation instead of swallowing it into a result.
    [Fact]
    public void Translate_rethrows_cancellation_instead_of_wrapping_it()
    {
        var cancelled = new OperationCanceledException("the operation was canceled");
        var thrown = Assert.Throws<OperationCanceledException>(() => DeltaErrors.Translate(cancelled, DeltaOperationKind.Write, "insert", []));
        Assert.Same(cancelled, thrown);
    }

    [Fact]
    public void Translate_never_leaks_the_rows_delta_rs_previews_on_a_validation_failure()
    {
        // Verbatim from real delta-rs 0.33.0: a batch holding nulls in a column the table declares NOT
        // NULL. Reachable on the plainest append there is, and no pre-flight guard can stop it — an
        // Arrow schema's nullability flag says nothing about whether the batch contains nulls.
        var raw = new DeltaLakeException(
            "Generic DeltaTable error: External error: Invalid data found: 2 rows failed validation check.\n" +
            "Preview of invalid data:\n" +
            "\n" +
            "+-----+----+----------+\n" +
            "| id  | dt | amt      |\n" +
            "+-----+----+----------+\n" +
            "| 777 |    | 31337.5  |\n" +
            "| 778 |    | 42424.25 |\n" +
            "+-----+----+----------+", 1);

        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "append of output 'orders'", []);

        Assert.DoesNotContain("777", ex.Message);
        Assert.DoesNotContain("778", ex.Message);
        Assert.DoesNotContain("31337.5", ex.Message);
        Assert.DoesNotContain("42424.25", ex.Message);
        Assert.DoesNotContain("|", ex.Message);

        // The row count is the whole diagnostic and carries no data, so it survives.
        Assert.Contains("2 rows failed validation check", ex.Message);
    }

    [Fact]
    public void Translate_never_leaks_the_single_value_a_failed_check_constraint_reports()
    {
        // The other value-carrying shape shipped in libdelta_rs_bridge.so: one offending cell rather
        // than a table of rows.
        var raw = new DeltaLakeException(
            "Generic DeltaTable error: Invalid data found: validation check failed with value 31337.5", 1);
        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "append of output 'orders'", []);

        Assert.DoesNotContain("31337.5", ex.Message);
        Assert.Contains("validation check failed with value <redacted>", ex.Message);
    }

    [Fact]
    public void Translate_redacts_data_under_a_recognized_marker_that_is_not_an_ascii_table()
    {
        // The marker stage carries this one alone. Every other data test here happens to embed an ASCII
        // box table, which the box stripper catches independently — so without this the marker stage
        // could be deleted with the whole suite staying green. Anything delta-rs renders under that
        // marker in some other shape (Unicode box glyphs, a flat key=value line) is data all the same.
        var raw = new DeltaLakeException(
            "Generic DeltaTable error: External error: Invalid data found: 1 rows failed validation check.\n" +
            "Preview of invalid data: id=777 amt=31337.5", 1);

        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "append of output 'orders'", []);

        Assert.DoesNotContain("777", ex.Message);
        Assert.DoesNotContain("31337.5", ex.Message);
        Assert.Contains("1 rows failed validation check", ex.Message);
    }

    [Fact]
    public void Translate_strips_a_row_table_even_when_the_marker_above_it_is_not_recognized()
    {
        // Belt and braces: an enumeration of the markers delta-rs uses today rots the moment upstream
        // renames one, but the box table IS the format the data arrives in.
        var raw = new DeltaLakeException(
            "Generic DeltaTable error: some future wording nobody enumerated\n" +
            "+-----+----------+\n" +
            "| id  | amt      |\n" +
            "+-----+----------+\n" +
            "| 777 | 31337.5  |\n" +
            "+-----+----------+", 1);

        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "append of output 'orders'", []);

        Assert.DoesNotContain("777", ex.Message);
        Assert.DoesNotContain("31337.5", ex.Message);
        Assert.Contains("some future wording nobody enumerated", ex.Message);
    }

    [Fact]
    public void Translate_still_redacts_a_credential_that_precedes_a_row_preview()
    {
        // The data patterns run first and truncate to end-of-message, so a credential could only be
        // missed if it sat BEFORE the preview — this pins that the two stages compose.
        var raw = new DeltaLakeException(
            "Generic DeltaTable error: AWS_SECRET_ACCESS_KEY=hunter2 rejected: Invalid data found: " +
            "1 rows failed validation check.\nPreview of invalid data:\n| id |\n| 777 |", 1);

        var ex = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "append of output 'orders'", []);

        Assert.DoesNotContain("hunter2", ex.Message);
        Assert.DoesNotContain("777", ex.Message);
    }
}
