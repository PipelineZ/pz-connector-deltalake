using DeltaLake.Errors;
using DeltaLake.Interfaces;
using DeltaLake.Table;
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
    public void Session_token_becomes_a_delta_rs_storage_option()
    {
        var opts = DeltaStorageOptions.Build(Cfg(
            ("root", "s3://w/d"), ("access_key_id", "A"), ("secret_access_key", "B"), ("session_token", "TOK")));
        Assert.Equal("TOK", opts["AWS_SESSION_TOKEN"]);
    }

    [Fact]
    public void Use_ssl_false_permits_a_non_tls_s3_endpoint()
    {
        var opts = DeltaStorageOptions.Build(Cfg(("root", "s3://w/d"), ("use_ssl", false)));
        Assert.Equal("true", opts["AWS_ALLOW_HTTP"]);
    }

    [Fact]
    public void Use_ssl_absent_leaves_the_non_tls_escape_hatch_unset()
    {
        var opts = DeltaStorageOptions.Build(Cfg(("root", "s3://w/d"), ("access_key_id", "A"), ("secret_access_key", "B")));
        Assert.False(opts.ContainsKey("AWS_ALLOW_HTTP"));
    }

    [Fact]
    public void Url_style_path_sets_addressing_style_only_not_the_unrelated_TLS_knob()
    {
        // url_style (addressing style) and use_ssl (TLS) are independent on the DuckDB side too
        // (DeltaSecretSql) -- setting one must not silently flip the other.
        var opts = DeltaStorageOptions.Build(Cfg(("root", "s3://w/d"), ("url_style", "path")));
        Assert.Equal("false", opts["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"]);
        Assert.False(opts.ContainsKey("AWS_ALLOW_HTTP"));
    }

    [Theory]
    [InlineData("minio:9000", true, "https://minio:9000")]
    [InlineData("minio:9000", false, "http://minio:9000")]
    [InlineData("http://localhost:9000", true, "http://localhost:9000")]
    [InlineData("https://s3.eu-west-1.amazonaws.com", false, "https://s3.eu-west-1.amazonaws.com")]
    public void Endpoint_is_normalized_to_a_full_URL_for_delta_rs(string endpoint, bool useSsl, string expected)
    {
        // object_store's AWS_ENDPOINT_URL requires a scheme; DuckDB's ENDPOINT secret parameter
        // (DeltaSecretSqlTests) requires the opposite. One 'endpoint:' value must satisfy both.
        var opts = DeltaStorageOptions.Build(Cfg(("root", "s3://w/d"), ("endpoint", endpoint), ("use_ssl", useSsl)));
        Assert.Equal(expected, opts["AWS_ENDPOINT_URL"]);
    }

    [Fact]
    public void Azure_service_principal_quartet_becomes_delta_rs_storage_options()
    {
        var opts = DeltaStorageOptions.Build(Cfg(
            ("root", "az://fs/d"), ("account_name", "acct"), ("tenant_id", "tenant"),
            ("client_id", "cid"), ("client_secret", "csecret")));

        Assert.Equal("acct", opts["AZURE_STORAGE_ACCOUNT_NAME"]);
        Assert.Equal("tenant", opts["AZURE_STORAGE_TENANT_ID"]);
        Assert.Equal("cid", opts["AZURE_STORAGE_CLIENT_ID"]);
        Assert.Equal("csecret", opts["AZURE_STORAGE_CLIENT_SECRET"]);
        Assert.False(opts.ContainsKey("AZURE_STORAGE_ACCOUNT_KEY"));
    }

    [Fact]
    public void Azure_service_principal_takes_precedence_over_account_key_when_both_are_given()
    {
        // Mirrors DeltaSecretSql.AzureSecret's precedence: DuckDB and delta-rs must pick the same
        // mechanism from the same partial config, or they authenticate two different ways.
        var opts = DeltaStorageOptions.Build(Cfg(
            ("root", "az://fs/d"), ("account_name", "acct"), ("tenant_id", "tenant"),
            ("client_id", "cid"), ("client_secret", "csecret"), ("account_key", "shouldnotappear")));

        Assert.False(opts.ContainsKey("AZURE_STORAGE_ACCOUNT_KEY"));
        Assert.Equal("csecret", opts["AZURE_STORAGE_CLIENT_SECRET"]);
    }

    [Fact]
    public void Azure_connection_string_decomposes_into_account_name_and_key_because_delta_rs_has_no_connection_string_key()
    {
        // AZURE_STORAGE_CONNECTION_STRING is confirmed absent from the shipped delta-rs binary (no
        // "connection_string" substring anywhere in it), unlike every other Azure alias here. Forwarding
        // it verbatim would be an inert key -- decompose it into the aliases that are actually present.
        var opts = DeltaStorageOptions.Build(Cfg(
            ("root", "az://fs/d"),
            ("connection_string",
                "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=bXlrZXk==;EndpointSuffix=core.windows.net")));

        Assert.Equal("acct", opts["AZURE_STORAGE_ACCOUNT_NAME"]);
        Assert.Equal("bXlrZXk==", opts["AZURE_STORAGE_ACCOUNT_KEY"]);
        Assert.False(opts.ContainsKey("AZURE_STORAGE_CONNECTION_STRING"));
    }

    [Fact]
    public void Azure_connection_string_decomposes_a_SAS_and_blob_endpoint_too()
    {
        var opts = DeltaStorageOptions.Build(Cfg(
            ("root", "az://fs/d"),
            ("connection_string",
                "BlobEndpoint=https://acct.blob.core.windows.net/;SharedAccessSignature=sv=2020-08-04&sig=abc%3D%3D")));

        Assert.Equal("https://acct.blob.core.windows.net/", opts["AZURE_STORAGE_ENDPOINT"]);
        Assert.Equal("sv=2020-08-04&sig=abc%3D%3D", opts["AZURE_STORAGE_SAS_KEY"]);
    }

    [Fact]
    public void Azure_connection_string_takes_precedence_over_account_name_and_key_when_both_are_given()
    {
        var opts = DeltaStorageOptions.Build(Cfg(
            ("root", "az://fs/d"), ("connection_string", "AccountName=fromcs;AccountKey=fromcskey"),
            ("account_name", "fromfields"), ("account_key", "fromfieldskey")));

        Assert.Equal("fromcs", opts["AZURE_STORAGE_ACCOUNT_NAME"]);
        Assert.Equal("fromcskey", opts["AZURE_STORAGE_ACCOUNT_KEY"]);
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
        // Version 0 has the narrower 3-column schema; a second, schema-evolving commit widens the
        // latest version to 4 columns. Only a real schema DIFFERENCE across versions can prove
        // 'version' actually drives what comes back rather than being silently accepted and ignored --
        // a single-commit fixture where every version has the same schema cannot fail this way.
        var dir = Directory.CreateTempSubdirectory("pz-delta-tt").FullName;
        var location = await DeltaTestTable.CreateLocalWithSchemaEvolutionAsync(dir, rows: 5);
        var source = await ((ISourceConnector)new DeltaLakeConnector())
            .OpenAsync(Cfg(("root", Path.GetDirectoryName(location)!)), default);

        var v0 = await source.GetSchemaAsync(
            new DatasetSpec("lake", "orders", new Dictionary<string, object?> { ["version"] = 0L }), default);
        var latest = await source.GetSchemaAsync(
            new DatasetSpec("lake", "orders", new Dictionary<string, object?>()), default);

        Assert.Equal(["id", "dt", "amt"], v0.Schema.FieldsList.Select(f => f.Name).ToArray());
        Assert.Equal(["id", "dt", "amt", "note"], latest.Schema.FieldsList.Select(f => f.Name).ToArray());
    }

    [Fact]
    public async Task GetSchemaAsync_on_a_version_absent_from_the_tables_history_fails_without_crashing()
    {
        // Exercises the path where engine.LoadTableAsync succeeds (allocating a native table handle)
        // and the SUBSEQUENT table.LoadVersionAsync then fails -- the ordinary "version not found"
        // case, and the one path where a successfully-loaded table handle could leak if nothing
        // disposed it on the way out. A single xunit run cannot observe a leaked native handle
        // directly; this test's job is coverage of the path (so the try/catch/dispose/rethrow in
        // DeltaStorageOptions.LoadAsync actually runs, with no secondary exception of its own), not
        // measuring the leak.
        var dir = Directory.CreateTempSubdirectory("pz-delta-badversion").FullName;
        var location = await DeltaTestTable.CreateLocalAsync(dir, rows: 5);
        var source = await ((ISourceConnector)new DeltaLakeConnector())
            .OpenAsync(Cfg(("root", Path.GetDirectoryName(location)!)), default);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.GetSchemaAsync(
                new DatasetSpec("lake", "orders", new Dictionary<string, object?> { ["version"] = 99L }), default));

        Assert.Contains("orders", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task GetSchemaAsync_never_leaks_a_credential_into_its_error()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-leak").FullName;
        var source = await ((ISourceConnector)new DeltaLakeConnector())
            .OpenAsync(Cfg(("root", dir), ("access_key_id", "AK"), ("secret_access_key", "LEAKME")), default);

        // A local root with s3 keys is a validation error, but a run that reaches here must still not
        // print the key when the table turns out to be missing. This proves the structural half of the
        // guarantee: Build() never even hands a local-root run a credential to leak in the first place
        // (DeltaStorageOptions.Build's Local branch adds nothing). LoadAsync_never_leaks_a_credential_
        // shaped_value_from_a_delta_rs_failure below proves the other half -- that the redaction
        // machinery actually fires when delta-rs's own error text DOES contain one.
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.GetSchemaAsync(new DatasetSpec("lake", "nope", new Dictionary<string, object?>()), default));
        Assert.DoesNotContain("LEAKME", ex.ToString());
    }

    [Fact]
    public async Task LoadAsync_never_leaks_a_credential_shaped_value_from_a_delta_rs_failure()
    {
        // Unlike the local-root test above, this drives DeltaStorageOptions.LoadAsync's own catch
        // block directly with a fake IEngine whose LoadTableAsync fails with a message that DOES
        // contain a credential-shaped value -- the scenario a real S3/Azure root would hit if delta-rs
        // ever echoed a storage option back in its own error text. No real delta-rs FFI call is made;
        // this is pure C#, so it needs neither network nor Docker.
        var engine = new FakeDeltaEngine
        {
            OnLoadTableAsync = (_, _) =>
                throw new DeltaLakeException("put failed: AWS_SECRET_ACCESS_KEY=LEAKME rejected", 1),
        };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            DeltaStorageOptions.LoadAsync(
                engine, "s3://w/d/orders", Cfg(("root", "s3://w/d")), version: null, dataset: "orders", ct: default));

        Assert.DoesNotContain("LEAKME", ex.Message);
        Assert.Contains(DeltaErrors.TableUnreadable, ex.Message);
    }

    [Fact]
    public async Task LoadAsync_disposes_the_loaded_table_on_the_big_stack_thread_when_the_version_pin_fails()
    {
        // A fake IEngine/ITable proves disposal directly, with no native code and no reliance on
        // observing a leaked handle (which a single xunit run cannot do): the fake's Dispose() sets a
        // flag AND records which thread called it, and LoadTableAsync succeeds (so there is a real
        // table to fail to dispose) while LoadVersionAsync fails (the ordinary "version not found"
        // case DeltaStorageOptions.LoadAsync's try/catch/dispose/rethrow exists to serve).
        string? disposedOnThreadName = null;
        var versionFailure = new DeltaLakeException("version 99 not found", 1);
        var table = new FakeDeltaTable
        {
            OnLoadVersionAsync = (_, _) => throw versionFailure,
            OnDispose = () => disposedOnThreadName = Thread.CurrentThread.Name,
        };
        var engine = new FakeDeltaEngine { OnLoadTableAsync = (_, _) => Task.FromResult<ITable>(table) };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            DeltaStorageOptions.LoadAsync(
                engine, "s3://w/d/orders", Cfg(("root", "s3://w/d")), version: 0L, dataset: "orders", ct: default));

        // Disposal happening on "pz-deltalake", not the calling thread, is what distinguishes disposing
        // INSIDE the DeltaBigStack.RunAsync delegate (correct -- ITable.Dispose() is itself an FFI
        // call, decompiled down to a native table_free) from an alternative fix that disposed in
        // LoadAsync's OUTER catch instead, which would still satisfy "the table gets disposed" but on
        // the wrong thread.
        Assert.Equal("pz-deltalake", disposedOnThreadName);

        // The user needs to see the LoadVersionAsync failure, not a replacement produced somewhere
        // along the way.
        Assert.Same(versionFailure, ex.InnerException);
    }

    [Fact]
    public async Task A_throwing_Dispose_does_not_replace_the_original_LoadVersionAsync_failure()
    {
        // Pins the deliberate swallow in LoadAsync's catch-dispose-rethrow: Dispose() failing (low
        // likelihood given a real SafeHandle-backed ITable, but not impossible) must not surface
        // INSTEAD of the LoadVersionAsync failure the user actually needs to see. Without this test,
        // "simplifying" the inner try/catch back to a bare disposable.Dispose() call reintroduces the
        // bug with every other test in this file still green -- OnDispose never throws anywhere else.
        var versionFailure = new DeltaLakeException("version 99 not found", 1);
        var table = new FakeDeltaTable
        {
            OnLoadVersionAsync = (_, _) => throw versionFailure,
            OnDispose = () => throw new InvalidOperationException("dispose boom"),
        };
        var engine = new FakeDeltaEngine { OnLoadTableAsync = (_, _) => Task.FromResult<ITable>(table) };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            DeltaStorageOptions.LoadAsync(
                engine, "s3://w/d/orders", Cfg(("root", "s3://w/d")), version: 0L, dataset: "orders", ct: default));

        Assert.Same(versionFailure, ex.InnerException);
    }

    [Fact]
    public async Task LoadAsync_never_lets_its_own_ConfigureAwaitFalse_regression_land_silently()
    {
        // This is the exact historical bug DeltaBigStack's doc comment names: DeltaStorageOptions.
        // LoadAsync's delegate once ended its two internal awaits in .ConfigureAwait(false), which
        // opts back OUT of the pump DeltaBigStack.RunAsync installs -- the second delta-rs call then
        // resumes on a default-stack pool thread instead of "pz-deltalake". DeltaBigStackTests pins
        // the gate generically, but nothing pinned THIS delegate specifically, and the disposal test
        // above cannot catch it either: both fakes there complete synchronously, so the delegate never
        // actually yields and the pump is never exercised.
        //
        // Making OnLoadTableAsync complete asynchronously (a real await, not an already-completed
        // Task.FromResult) forces a genuine resumption; OnLoadVersionAsync then records which thread
        // it runs on. If either internal await in LoadAsync's delegate ever regains
        // .ConfigureAwait(false), this fails with ".NET TP Worker" instead of "pz-deltalake".
        string? loadVersionAsyncThreadName = null;
        var table = new FakeDeltaTable
        {
            OnLoadVersionAsync = (_, _) =>
            {
                loadVersionAsyncThreadName = Thread.CurrentThread.Name;
                return Task.CompletedTask;
            },
        };
        var engine = new FakeDeltaEngine
        {
            OnLoadTableAsync = async (_, _) =>
            {
                await Task.Yield();
                return table;
            },
        };

        await DeltaStorageOptions.LoadAsync(
            engine, "s3://w/d/orders", Cfg(("root", "s3://w/d")), version: 0L, dataset: "orders", ct: default);

        Assert.Equal("pz-deltalake", loadVersionAsyncThreadName);
    }
}
