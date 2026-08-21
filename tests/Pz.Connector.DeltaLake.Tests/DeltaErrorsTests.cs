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
}
