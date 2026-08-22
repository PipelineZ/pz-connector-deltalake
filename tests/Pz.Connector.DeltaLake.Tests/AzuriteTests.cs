using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>The Azure backend, against a real Azurite. Same two-translation problem the MinIO suite
/// exists for, with a different shape on each side: DuckDB takes the connection string whole, while
/// delta-rs has no connection-string key at all and gets the string decomposed into the account, key
/// and endpoint options it does recognize.
///
/// Coverage here is narrower than the S3 suite's, for a measured reason rather than an editorial one,
/// and docs/compatibility.md states the gaps rather than implying they are covered:
/// <list type="bullet">
/// <item><description>ONE commit per table. A second commit to the same Azurite table fails with
/// "Invalid table version: N" — from raw delta-rs, with none of this connector's code in the path.
/// That is an emulator ADDRESSING artifact, not a property of the Azure write path, and the proof is
/// that the same three commits all succeed the moment object_store is put into its emulator mode
/// (<c>AZURE_STORAGE_USE_EMULATOR=true</c>) instead of being pointed at the same server through a
/// generic endpoint. Azurite's BlobEndpoint carries the account as a PATH segment
/// (<c>http://host:port/devstoreaccount1</c>); a real Azure account's carries it in the HOST and has
/// no path at all, which is the shape object_store's non-emulator client is written for. The
/// connector does not set emulator mode, and should not: it makes object_store ignore the configured
/// endpoint entirely and resolve 127.0.0.1:10000, which would break any user running Azurite
/// anywhere else. <see cref="A_second_commit_to_one_azure_table_is_refused_by_this_emulator"/> pins
/// the limit so a delta-rs or Azurite bump that lifts it is noticed.</description></item>
/// <item><description>Merge is not re-proven here. It is one MERGE statement handed to delta-rs,
/// proven on local disk and again on S3, and nothing in it is backend-specific — and with one commit
/// per table available there is no way to seed a target to merge into.</description></item>
/// <item><description>ADLS Gen2 (<c>abfss://</c>) is covered by nothing at all. Azurite emulates the
/// Blob endpoint only.</description></item>
/// </list></summary>
[Collection("azurite")]
public class AzuriteTests(AzuriteLake lake)
{
    /// <summary>Write through delta-rs, read back through DuckDB's <c>delta_scan</c> — the two
    /// credential translations proven to describe the same account, container and endpoint. One
    /// commit, which is all this emulator takes.</summary>
    [SkippableFact]
    public async Task A_table_written_to_azure_is_readable_by_duckdb_delta_scan()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        lake.SkipIfUnavailable();
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(),
            "the duckdb delta extension could not be installed");
        Skip.IfNot(lake.WellKnownBlobPort,
            "azurite is not on port 10000, which the duckdb delta extension's kernel resolves for an " +
            "emulator connection string regardless of the endpoint it was given");

        const string Entity = "az_roundtrip";
        var config = lake.Config();
        var written = await ObjectStoreLake.WriteAsync(config, Spec(Entity, "append"),
            DeltaTestTable.Rows(0, 120), DeltaTestTable.Rows(120, 80));

        Assert.Equal(200, written.RowsWritten);
        Assert.Equal(Enumerable.Range(0, 200).Select(i => (long)i), await ObjectStoreLake.ScanIdsAsync(config, Entity));
    }

    /// <summary>The same write read back through delta-rs, which needs nothing off the network and
    /// nothing off a well-known port — so what an azure write actually COMMITS is asserted on every
    /// machine with docker, not only on one where port 10000 was free.</summary>
    [SkippableFact]
    public async Task An_azure_write_commits_the_rows_it_reports()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        lake.SkipIfUnavailable();

        const string Entity = "az_commit";
        var config = lake.Config();
        var written = await ObjectStoreLake.WriteAsync(config, Spec(Entity, "append"), DeltaTestTable.Rows(0, 40));

        Assert.Equal(40, written.RowsWritten);
        var rows = await ObjectStoreLake.RowsAsync(AzuriteLake.Location(Entity), config);
        Assert.Equal(Enumerable.Range(0, 40).Select(i => (long)i), rows.Select(r => r.Id));
    }

    /// <summary>The emulator limit, pinned. This is an OBSERVED fact about Azurite 3.35.0 driven
    /// through a generic endpoint, not a claim about Azure — see the type doc for what was measured and
    /// why the connector does not work around it. When this fact starts failing, the limit has been
    /// lifted and docs/compatibility.md is what needs updating.</summary>
    [SkippableFact]
    public async Task A_second_commit_to_one_azure_table_is_refused_by_this_emulator()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        lake.SkipIfUnavailable();

        const string Entity = "az_second";
        var config = lake.Config();
        await ObjectStoreLake.WriteAsync(config, Spec(Entity, "append"), DeltaTestTable.Rows(0, 5));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ObjectStoreLake.WriteAsync(config, Spec(Entity, "append"), DeltaTestTable.Rows(5, 5)));

        // Loud and coded, whatever else it is. The failure mode that would matter is a second commit
        // that reported success and wrote nothing.
        Assert.StartsWith("PZDL", ex.Message, StringComparison.Ordinal);
        Assert.Equal(5, (await ObjectStoreLake.RowsAsync(AzuriteLake.Location(Entity), config)).Count);
    }

    /// <summary>An http BlobEndpoint is what an emulator and an on-prem gateway both look like, and
    /// object_store's Azure client refuses one unless allow_http is set — with a message
    /// ("HTTP error: builder error") that names neither http nor the endpoint. There is no
    /// <c>use_ssl</c> on an azure root to hang the decision on (validation refuses it, PZDL0102), so
    /// the scheme the user already wrote into the connection string is what decides. This fact is the
    /// teeth: every other azure fact in this file fails outright without that option.</summary>
    [Fact]
    public void An_http_blob_endpoint_opts_into_plain_http_and_an_https_one_does_not()
    {
        var http = DeltaStorageOptions.Build(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["root"] = "az://c/delta",
            ["connection_string"] =
                "DefaultEndpointsProtocol=http;AccountName=a;AccountKey=k;BlobEndpoint=http://127.0.0.1:10000/a",
        }));
        Assert.Equal("true", http["AZURE_ALLOW_HTTP"]);

        var https = DeltaStorageOptions.Build(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["root"] = "az://c/delta",
            ["connection_string"] =
                "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=k;BlobEndpoint=https://a.blob.core.windows.net",
        }));
        Assert.False(https.ContainsKey("AZURE_ALLOW_HTTP"));

        // No endpoint at all is the ordinary Azure deployment: object_store resolves the account's own
        // https URL, and nothing here should be loosening that.
        var plain = DeltaStorageOptions.Build(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["root"] = "az://c/delta", ["account_name"] = "a", ["account_key"] = "k",
        }));
        Assert.False(plain.ContainsKey("AZURE_ALLOW_HTTP"));
    }

    /// <summary>The azure half of the secret-hygiene tripwire, with teeth of its own rather than
    /// borrowed from a sibling fact. Asserting only "some PZDL error without the key in it" is not
    /// enough: that also holds when the write never leaves the process — measured, the
    /// AZURE_ALLOW_HTTP mutation makes every azure write fail at the transport builder, and this fact
    /// passed under it. So it additionally asserts the failure is a 403 from the SERVICE, which is
    /// what proves the wrong key was actually presented and rejected rather than never sent.</summary>
    [SkippableFact]
    public async Task A_rejected_credential_is_reported_without_the_account_key_in_the_message()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        lake.SkipIfUnavailable();

        const string WrongKey = "cHpkbC1ub3QtdGhlLXJlYWwtYWNjb3VudC1rZXk=";
        var wrong = System.Text.RegularExpressions.Regex.Replace(
            lake.ConnectionString, @"AccountKey=[^;]+", $"AccountKey={WrongKey}");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ObjectStoreLake.WriteAsync(lake.Config(wrong), Spec("az_badcred", "append"), DeltaTestTable.Rows(0, 5)));

        for (Exception? e = ex; e is not null; e = e.InnerException)
        {
            Assert.DoesNotContain(WrongKey, e.Message, StringComparison.Ordinal);
        }

        Assert.StartsWith("PZDL", ex.Message, StringComparison.Ordinal);

        // Reached Azurite and was refused there. "builder error" is what a request that never left the
        // process reports, and it is the outcome this assertion exists to tell apart from a genuine
        // authentication rejection.
        Assert.Contains("403", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("builder error", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static OutputSpec Spec(string entity, string mode) =>
        new("lake", entity, mode, "fail_on_change", new Dictionary<string, object?>());
}
