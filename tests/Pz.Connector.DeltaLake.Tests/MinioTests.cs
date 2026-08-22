using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>The S3 backend, against a real MinIO. Everything up to this suite ran on local disk, where
/// one config value — <c>root</c> — reaches delta-rs and nothing reaches DuckDB. On an object store
/// the same connection block is translated TWICE, into two entirely different vocabularies: delta-rs
/// storage options for the write (<see cref="DeltaStorageOptions"/>) and a DuckDB CREATE SECRET for
/// the read (<see cref="DeltaSecretSql"/>). Nothing checks that the two agree, and a disagreement is
/// invisible on local disk — which is why the round-trip fact below is the important one in this file.
///
/// Every fact skips without docker and under PZ_TESTS_OFFLINE=1; the container is shared across the
/// collection, so each fact writes to its own entity name.</summary>
[Collection("minio")]
public class MinioTests(MinioLake lake)
{
    /// <summary>Write through delta-rs, read back through DuckDB's <c>delta_scan</c> — the two
    /// credential translations, proven to describe the same bucket, the same endpoint, the same
    /// addressing style and the same credentials. Either half alone passes with a broken other half:
    /// the write can succeed while every read 403s, and vice versa.</summary>
    [SkippableFact]
    public async Task A_table_written_to_s3_is_readable_by_duckdb_delta_scan()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        lake.SkipIfUnavailable();
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(),
            "the duckdb delta extension could not be installed");

        const string Entity = "s3_roundtrip";
        var config = lake.Config();
        var written = await ObjectStoreLake.WriteAsync(config, Spec(Entity, "append"),
            DeltaTestTable.Rows(0, 120), DeltaTestTable.Rows(120, 80));

        Assert.Equal(200, written.RowsWritten);
        Assert.Equal(Enumerable.Range(0, 200).Select(i => (long)i), await ObjectStoreLake.ScanIdsAsync(config, Entity));
    }

    /// <summary>All three strategies, in sequence, against one S3 table. Read back through delta-rs
    /// rather than DuckDB so the assertion about what was COMMITTED does not depend on an extension
    /// download — the round-trip fact above is where the DuckDB half is proven.</summary>
    [SkippableFact]
    public async Task Append_replace_and_merge_all_work_against_s3()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        lake.SkipIfUnavailable();

        const string Entity = "s3_strategies";
        var config = lake.Config();
        var location = MinioLake.Location(Entity);

        await ObjectStoreLake.WriteAsync(config, Spec(Entity, "append"), DeltaTestTable.Rows(0, 10));
        Assert.Equal(10, (await ObjectStoreLake.RowsAsync(location, config)).Count);

        await ObjectStoreLake.WriteAsync(config, Spec(Entity, "append"), DeltaTestTable.Rows(10, 10));
        Assert.Equal(20, (await ObjectStoreLake.RowsAsync(location, config)).Count);

        await ObjectStoreLake.WriteAsync(config, Spec(Entity, "replace"), DeltaTestTable.Rows(0, 5));
        var afterReplace = await ObjectStoreLake.RowsAsync(location, config);
        Assert.Equal([0L, 1, 2, 3, 4], afterReplace.Select(r => r.Id));

        // One key the table already holds (id 4, amount rewritten) and one it does not (id 99).
        var merge = Spec(Entity, "merge") with { Keys = ["id"] };
        await ObjectStoreLake.WriteAsync(config, merge, DeltaTestTable.RowsWithAmounts(
            [(4, DeltaTestTable.Partition(4), 4242d), (99, DeltaTestTable.Partition(99), 99d)]));

        var afterMerge = await ObjectStoreLake.RowsAsync(location, config);
        Assert.Equal([0L, 1, 2, 3, 4, 99], afterMerge.Select(r => r.Id));
        Assert.Equal(4242d, Assert.Single(afterMerge, r => r.Id == 4).Amt);
    }

    /// <summary>The first place in this build where a real credential flows through the connector, and
    /// therefore the first place the no-secrets-in-errors rule is exercised on a real failure rather
    /// than on a hand-fed string. A wrong secret key makes S3 reject the request; whatever delta-rs
    /// says about that must reach the user without the key in it.
    ///
    /// This is a TRIPWIRE, not a demonstration that redaction fired. Measured: what delta-rs says for
    /// a rejected S3 credential is MinIO's own SignatureDoesNotMatch body, which names the bucket, the
    /// key and the request id and carries neither the secret nor the access key id — so today this
    /// fact would pass with redaction removed. It is here because the message is third-party text this
    /// connector does not control, a future delta-rs or object_store is free to start echoing the
    /// storage options it was given, and the assertion that would catch that has to already exist.
    /// DeltaErrorsTests carries the facts that prove Redact actually removes a credential — including
    /// <c>Translate_never_leaks_an_access_key_id_out_of_an_s3_xml_error_body</c>, which feeds real
    /// Amazon S3's 403 body. That shape carries the access key id in an XML ELEMENT, which the
    /// name=value redactor never covered and which MinIO's own 403 body does not contain, so it could
    /// only ever be reached by a hand-fed message.</summary>
    [SkippableFact]
    public async Task A_rejected_credential_is_reported_without_the_secret_in_the_message()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        lake.SkipIfUnavailable();

        const string WrongSecret = "pzdl-not-the-real-secret-3f8a1c9e";
        var config = lake.Config(WrongSecret);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ObjectStoreLake.WriteAsync(config, Spec("s3_badcred", "append"), DeltaTestTable.Rows(0, 5)));

        Assert.DoesNotContain(WrongSecret, Flatten(ex), StringComparison.Ordinal);
        Assert.DoesNotContain(lake.AccessKey, Flatten(ex), StringComparison.Ordinal);
        Assert.StartsWith("PZDL", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>The teeth for the fact above: the wrong secret really is what fails the write, so the
    /// assertion is about redaction rather than about a message that never had a chance to carry the
    /// key. Same config, right secret, same table name — it commits.</summary>
    [SkippableFact]
    public async Task The_same_write_succeeds_once_the_secret_is_right()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        lake.SkipIfUnavailable();

        var written = await ObjectStoreLake.WriteAsync(
            lake.Config(), Spec("s3_goodcred", "append"), DeltaTestTable.Rows(0, 5));

        Assert.Equal(5, written.RowsWritten);
    }

    private static OutputSpec Spec(string entity, string mode) =>
        new("lake", entity, mode, "fail_on_change", new Dictionary<string, object?>());

    /// <summary>Message plus every inner message. Only the OUTER message reaches anything a user sees:
    /// pz writes <c>Error.Message</c> into run_results.json and publishes <c>Error?.Message</c> onto
    /// the NDJSON stream, and nothing in it walks InnerException. The inner ones are asserted anyway
    /// because they cost nothing and because the outer message is BUILT from the inner one — a
    /// credential visible in the inner text is a credential that the next wording change upstream can
    /// carry outward.</summary>
    private static string Flatten(Exception ex)
    {
        var text = new System.Text.StringBuilder();
        for (Exception? e = ex; e is not null; e = e.InnerException)
        {
            text.AppendLine(e.Message);
        }

        return text.ToString();
    }
}
