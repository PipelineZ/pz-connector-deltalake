using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class SchemaTests
{
    private static ConnectorConfig Cfg(params (string Key, object? Value)[] pairs) =>
        new(pairs.ToDictionary(p => p.Key, p => p.Value));

    [Fact]
    public void S3_credentials_become_delta_rs_storage_options_under_their_canonical_names()
    {
        var opts = DeltaStorageOptions.Build(Cfg(
            ("root", "s3://w/d"), ("access_key_id", "AK"), ("secret_access_key", "SK"),
            ("region", "eu-west-1"), ("endpoint", "http://localhost:9000")));

        Assert.Equal("AK", opts["AWS_ACCESS_KEY_ID"]);
        Assert.Equal("SK", opts["AWS_SECRET_ACCESS_KEY"]);
        Assert.Equal("eu-west-1", opts["AWS_REGION"]);
        Assert.Equal("http://localhost:9000", opts["AWS_ENDPOINT_URL"]);
    }

    [Fact]
    public void Azure_credentials_become_delta_rs_storage_options()
    {
        var opts = DeltaStorageOptions.Build(Cfg(
            ("root", "az://fs/d"), ("account_name", "acct"), ("account_key", "KEY")));
        Assert.Equal("acct", opts["AZURE_STORAGE_ACCOUNT_NAME"]);
        Assert.Equal("KEY", opts["AZURE_STORAGE_ACCOUNT_KEY"]);
    }

    [Fact]
    public void A_local_root_produces_no_storage_options() =>
        Assert.Empty(DeltaStorageOptions.Build(Cfg(("root", "/mnt/lake"))));

    [Fact]
    public void The_unsafe_s3_rename_escape_hatch_is_never_set_implicitly()
    {
        // Silently enabling AWS_S3_ALLOW_UNSAFE_RENAME loses commits under concurrent writers.
        // Whatever the S3 safety regime turns out to be, it is never turned on behind the user's back.
        var opts = DeltaStorageOptions.Build(Cfg(("root", "s3://w/d"), ("access_key_id", "A"), ("secret_access_key", "B")));
        Assert.False(opts.ContainsKey("AWS_S3_ALLOW_UNSAFE_RENAME"));
    }

    [Fact]
    public async Task GetSchemaAsync_returns_the_tables_declared_arrow_schema()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-schema").FullName;
        var location = await DeltaTestTable.CreateLocalAsync(dir, rows: 5);

        var source = await ((ISourceConnector)new DeltaLakeConnector())
            .OpenAsync(Cfg(("root", Path.GetDirectoryName(location)!)), default);
        var schema = await source.GetSchemaAsync(new DatasetSpec("lake", "orders", new Dictionary<string, object?>()), default);

        Assert.Equal(["id", "dt", "amt"], schema.Schema.FieldsList.Select(f => f.Name).ToArray());
    }

    [Fact]
    public async Task GetSchemaAsync_on_a_missing_table_fails_with_PZDL0201_naming_the_dataset()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-missing").FullName;
        var source = await ((ISourceConnector)new DeltaLakeConnector()).OpenAsync(Cfg(("root", dir)), default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.GetSchemaAsync(new DatasetSpec("lake", "nope", new Dictionary<string, object?>()), default));
        Assert.Contains(DeltaErrors.TableUnreadable, ex.Message);
        Assert.Contains("nope", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task GetSchemaAsync_honors_the_version_option()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-tt").FullName;
        var location = await DeltaTestTable.CreateLocalAsync(dir, rows: 5);
        var source = await ((ISourceConnector)new DeltaLakeConnector())
            .OpenAsync(Cfg(("root", Path.GetDirectoryName(location)!)), default);

        var schema = await source.GetSchemaAsync(
            new DatasetSpec("lake", "orders", new Dictionary<string, object?> { ["version"] = 0L }), default);
        Assert.Equal(3, schema.Schema.FieldsList.Count);
    }

    [Fact]
    public async Task GetSchemaAsync_never_leaks_a_credential_into_its_error()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-leak").FullName;
        var source = await ((ISourceConnector)new DeltaLakeConnector())
            .OpenAsync(Cfg(("root", dir), ("access_key_id", "AK"), ("secret_access_key", "LEAKME")), default);

        // A local root with s3 keys is a validation error, but a run that reaches here must still not
        // print the key when the table turns out to be missing.
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.GetSchemaAsync(new DatasetSpec("lake", "nope", new Dictionary<string, object?>()), default));
        Assert.DoesNotContain("LEAKME", ex.ToString());
    }
}
