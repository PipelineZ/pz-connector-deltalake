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
    public void Transient_marks_the_exception_so_the_engine_can_retry_it()
    {
        var ex = DeltaErrors.Transient(DeltaErrors.CommitConflict, "another writer committed first", "retry the run");
        Assert.True(ex.IsTransient);
    }

    [Fact]
    public void Translate_maps_a_duplicate_source_key_merge_failure_to_PZDL0402_naming_the_keys()
    {
        var raw = new DeltaLakeException(
            "MERGE matched a target row with multiple source rows that satisfy duplicate relevant WHEN MATCHED clauses", 1);
        var ex = DeltaErrors.Translate(raw, "merge", ["order_id", "region"]);
        Assert.Contains("PZDL0402", ex.Message);
        Assert.Contains("order_id, region", ex.Message);
        Assert.Contains("deduplicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.False(ex.IsTransient);
        Assert.Same(raw, ex.InnerException);
    }

    [Theory]
    [InlineData("Delta transaction failed, version 7 already exists")]
    [InlineData("Failed to commit transaction: version already exists")]
    [InlineData("Metadata changed since last commit")]
    public void Translate_classifies_commit_races_as_transient(string message)
    {
        var ex = DeltaErrors.Translate(new DeltaLakeException(message, 1), "commit", []);
        Assert.True(ex.IsTransient);
        Assert.Contains("PZDL0401", ex.Message);
    }

    [Fact]
    public void Translate_classifies_an_unknown_delta_failure_as_permanent_not_transient()
    {
        // Guessing "transient" for an unrecognized failure would make the engine retry a permanent
        // error N times before reporting it. Unknown means permanent.
        var ex = DeltaErrors.Translate(new DeltaLakeException("schema mismatch: field 'x' not found", 1), "insert", []);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Translate_passes_a_PzConnectorException_through_untouched()
    {
        var original = DeltaErrors.Fail(DeltaErrors.SchemaMismatch, "already mapped", "do the thing");
        Assert.Same(original, DeltaErrors.Translate(original, "insert", []));
    }

    [Fact]
    public void Translate_never_leaks_a_storage_option_value()
    {
        var raw = new DeltaLakeException("failed with secret_access_key=AKIAWHOOPS in the message", 1);
        var ex = DeltaErrors.Translate(raw, "insert", []);
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
        var ex = DeltaErrors.Translate(new DeltaLakeException(message, 1), "insert", []);
        Assert.DoesNotContain(secret, ex.Message);
    }

    // A misconfigured connection string can come back embedded in a URL rather than as key=value.
    [Fact]
    public void Translate_never_leaks_a_credential_embedded_in_a_url()
    {
        var raw = new DeltaLakeException(
            "PUT https://myaccount:zGl3AbG9NDGgAAcVpSimplePass@blob.core.windows.net/container/file failed", 1);
        var ex = DeltaErrors.Translate(raw, "insert", []);
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
        var ex = DeltaErrors.Translate(raw, "insert", []);
        Assert.DoesNotContain("ssw0rdZZZ", ex.Message);
    }

    [Fact]
    public void Translate_never_leaks_a_bearer_token()
    {
        var raw = new DeltaLakeException(
            "request failed: Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig", 1);
        var ex = DeltaErrors.Translate(raw, "insert", []);
        Assert.DoesNotContain("eyJhbGciOiJIUzI1NiJ9.payload.sig", ex.Message);
    }

    [Fact]
    public void Translate_never_leaks_a_sas_signature_query_parameter()
    {
        var raw = new DeltaLakeException(
            "sas token rejected: https://a.blob.core.windows.net/c/b?sv=2021&sig=AbCdEf123%3D&se=2026-01-01", 1);
        var ex = DeltaErrors.Translate(raw, "insert", []);
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
        var ex = DeltaErrors.Translate(new DeltaLakeException(message, 1), "create", []);
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
        var ex = DeltaErrors.Translate(new DeltaLakeException(message, 1), "commit", []);
        Assert.True(ex.IsTransient);
    }

    // TableUnreadable (PZDL0201) exists specifically for the read path; before this fix, every
    // unrecognized failure — including read failures — fell through to WriteFailed (PZDL0404).
    [Fact]
    public void Translate_classifies_an_unrecognized_read_failure_as_table_unreadable()
    {
        var ex = DeltaErrors.Translate(new DeltaLakeException("some opaque delta-rs read error", 1), "read", []);
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
        var thrown = Assert.Throws<OperationCanceledException>(() => DeltaErrors.Translate(cancelled, "insert", []));
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

        var ex = DeltaErrors.Translate(raw, "append of output 'orders'", []);

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
        var ex = DeltaErrors.Translate(raw, "append of output 'orders'", []);

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

        var ex = DeltaErrors.Translate(raw, "append of output 'orders'", []);

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

        var ex = DeltaErrors.Translate(raw, "append of output 'orders'", []);

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

        var ex = DeltaErrors.Translate(raw, "append of output 'orders'", []);

        Assert.DoesNotContain("hunter2", ex.Message);
        Assert.DoesNotContain("777", ex.Message);
    }
}
