using System.Text.Json;
using Json.Schema;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class ValidationTests
{
    private static ConnectorConfig Cfg(params (string Key, object? Value)[] pairs) =>
        new(pairs.ToDictionary(p => p.Key, p => p.Value));

    private static async Task<IReadOnlyList<string>> Validate(ConnectorConfig cfg) =>
        (await new DeltaLakeConnector().ValidateAsync(cfg, default)).Errors;

    [Fact]
    public async Task A_minimal_local_connection_is_valid() =>
        Assert.Empty(await Validate(Cfg(("root", "/mnt/lake"))));

    [Fact]
    public async Task A_missing_root_is_an_error() =>
        Assert.Contains(await Validate(Cfg()), e => e.Contains(DeltaErrors.UnsupportedRoot));

    [Fact]
    public async Task An_unsupported_root_scheme_reports_PZDL0101_rather_than_throwing()
    {
        // ValidateAsync must never throw for invalid config — it returns errors.
        Assert.Contains(await Validate(Cfg(("root", "gs://bucket/delta"))),
            e => e.Contains(DeltaErrors.UnsupportedRoot));
    }

    [Fact]
    public async Task S3_only_options_are_rejected_against_an_azure_root() =>
        Assert.Contains(await Validate(Cfg(("root", "az://fs/delta"), ("region", "eu-west-1"))),
            e => e.Contains(DeltaErrors.SchemeOptionMismatch) && e.Contains("region"));

    [Fact]
    public async Task Azure_only_options_are_rejected_against_an_s3_root() =>
        Assert.Contains(await Validate(Cfg(("root", "s3://w/d"), ("account_name", "acct"))),
            e => e.Contains(DeltaErrors.SchemeOptionMismatch) && e.Contains("account_name"));

    [Fact]
    public async Task S3_credentials_must_be_supplied_together_or_not_at_all()
    {
        // Half a credential pair silently falls back to the ambient chain and fails much later with a
        // permissions error that names nothing.
        Assert.Contains(await Validate(Cfg(("root", "s3://w/d"), ("access_key_id", "AK"))),
            e => e.Contains(DeltaErrors.SchemeOptionMismatch) && e.Contains("secret_access_key"));
    }

    [Fact]
    public async Task All_errors_are_reported_at_once_not_one_at_a_time()
    {
        var errors = await Validate(Cfg(("root", "az://fs/d"), ("region", "eu"), ("endpoint", "http://x")));
        Assert.True(errors.Count >= 2, $"expected every error, got: {string.Join(" | ", errors)}");
    }

    [Fact]
    public async Task Validation_never_throws_for_any_shape_of_garbage() =>
        Assert.NotEmpty((await new DeltaLakeConnector().ValidateAsync(Cfg(("root", 42)), default)).Errors);

    [Fact]
    public async Task CheckConnection_is_offline_and_reports_ok() =>
        Assert.True((await new DeltaLakeConnector().CheckConnectionAsync(Cfg(("root", "/mnt/lake")), default)).Ok);

    [Fact]
    public void Both_schemas_are_valid_json_with_additionalProperties_false()
    {
        foreach (var schema in new[] { DeltaLakeSchemas.Connection, DeltaLakeSchemas.Dataset })
        {
            using var doc = System.Text.Json.JsonDocument.Parse(schema);
            Assert.False(doc.RootElement.GetProperty("additionalProperties").GetBoolean());
        }
    }

    [Fact]
    public void The_dataset_schema_declares_read_options_only()
    {
        // pz validates DatasetConfigSchema against SOURCE dataset options only. Listing write options
        // here would loosen read validation and document a surface pz never checks.
        using var doc = System.Text.Json.JsonDocument.Parse(DeltaLakeSchemas.Dataset);
        var props = doc.RootElement.GetProperty("properties");
        Assert.Equal(DeltaLakeSchemas.ReadOptions.Order().ToArray(),
            props.EnumerateObject().Select(p => p.Name).Order().ToArray());
    }

    [Fact]
    public void No_schema_mentions_the_retired_mode_keyword()
    {
        // pz refuses `mode:` on a write and tells the user to write `strategy:`. Declaring it here
        // would make a config pz rejects look schema-valid.
        Assert.DoesNotContain("\"mode\"", DeltaLakeSchemas.Dataset);
        Assert.DoesNotContain("mode", DeltaLakeSchemas.WriteOptions);
    }

    [Fact]
    public void Write_options_exclude_every_name_pz_strips_before_the_connector_sees_it()
    {
        foreach (var pzOwned in new[] { "strategy", "keys", "duplicates", "on_delete", "schema_policy", "retry" })
        {
            Assert.DoesNotContain(pzOwned, DeltaLakeSchemas.WriteOptions);
        }
    }

    [Fact]
    public void Every_declared_connection_option_appears_in_the_connection_schema()
    {
        using var conn = System.Text.Json.JsonDocument.Parse(DeltaLakeSchemas.Connection);
        var props = conn.RootElement.GetProperty("properties");
        Assert.All(DeltaLakeSchemas.ConnectionOptions, o => Assert.True(props.TryGetProperty(o, out _), o));
    }

    [Theory]
    [InlineData("data/{yyyy}/{MM}")]
    [InlineData("orders_{yyyy-MM-dd}")]
    public void A_calendar_token_in_a_read_path_is_refused_by_the_dataset_schema(string path)
    {
        // The connector declares PathTemplating for its SINK meaning only. Accepting a templated read
        // path would silently reference a literal-token folder — the exact silent no-op the capability
        // flag exists to prevent.
        Assert.Matches(DeltaLakeSchemas.CalendarTokenPattern, path);
    }

    [Theory]
    [InlineData("curated/orders")]
    [InlineData("orders")]
    public void An_ordinary_read_path_is_not_mistaken_for_a_template(string path) =>
        Assert.DoesNotMatch(DeltaLakeSchemas.CalendarTokenPattern, path);

    // The two theory tests above exercise CalendarTokenPattern as a bare .NET Regex -- they would
    // still pass even if the pattern's escaping came out wrong once embedded in the Dataset JSON
    // string (a dropped "\\", say), because they never touch the JSON text at all. These two evaluate
    // the SHIPPED DeltaLakeSchemas.Dataset constant through JsonSchema.Net using the exact call shape
    // pz's own ConnectorConfigValidator.ValidateSchema uses (schema.Evaluate with
    // OutputFormat.List), so a future edit that breaks the embedded pattern's escaping fails here
    // even though the bare-regex tests above would keep passing.
    [Theory]
    [InlineData("data/{yyyy}/{MM}")]
    [InlineData("orders_{yyyy-MM-dd}")]
    public void The_shipped_dataset_schema_rejects_a_calendar_token_path_when_evaluated(string path) =>
        Assert.False(EvaluateDatasetPath(path).IsValid);

    [Theory]
    [InlineData("curated/orders")]
    [InlineData("orders")]
    public void The_shipped_dataset_schema_accepts_an_ordinary_path_when_evaluated(string path) =>
        Assert.True(EvaluateDatasetPath(path).IsValid);

    private static EvaluationResults EvaluateDatasetPath(string path)
    {
        var schema = JsonSchema.FromText(DeltaLakeSchemas.Dataset);
        var instance = JsonSerializer.Deserialize<JsonElement>(
            JsonSerializer.Serialize(new Dictionary<string, object?> { ["path"] = path }));
        return schema.Evaluate(instance, new EvaluationOptions { OutputFormat = OutputFormat.List });
    }
}
