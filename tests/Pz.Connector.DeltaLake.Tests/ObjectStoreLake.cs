using Amazon.S3;
using Apache.Arrow;
using Azure.Storage.Blobs;
using DuckDB.NET.Data;
using Pz.Connectors.Abstractions;
using Testcontainers.Azurite;
using Testcontainers.Minio;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>The two object-store emulators the remote suites run against, plus the handful of
/// operations those suites share. One container per collection, not per fact: starting one is the
/// expensive part, and every fact writes to its own entity name so nothing a fact commits is visible
/// to another.
///
/// Neither fixture skips. A fixture cannot: a Skip thrown while xunit is constructing a collection
/// fixture is reported as a collection-level ERROR, not as skipped facts. So each one asks
/// <see cref="DockerFacts.CanStartContainers"/>, does nothing when the answer is no, and leaves every
/// fact to skip itself through <see cref="DockerFacts.SkipUnlessDocker"/> and
/// <see cref="DockerFacts.SkipIfOffline"/>.</summary>
internal static class ObjectStoreLake
{
    /// <summary>Runs one whole write session against a sink built from <paramref name="config"/>,
    /// disposing each batch after the call that consumed it — the engine's own contract: a batch handed
    /// to WriteBatchAsync is valid only until the call returns, and a session that retained it instead
    /// of cloning is reading recycled memory afterwards.</summary>
    public static async Task<WriteResult> WriteAsync(
        ConnectorConfig config, OutputSpec spec, params RecordBatch[] batches)
    {
        await using var sink = new DeltaLakeSink(config);
        await using var session = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default);
        foreach (var batch in batches)
        {
            using (batch)
            {
                await session.WriteBatchAsync(batch, default);
            }
        }

        return await session.CommitAsync(default);
    }

    /// <summary>The table read back through the SOURCE's own native scan, executed by a real DuckDB:
    /// the scan's <c>SetupStatements</c> first (extension loads plus the CREATE SECRET this connection
    /// translates its credentials into), then its fragment. This is the only place the connector's two
    /// credential translations meet — the write went through delta-rs storage options, the read through
    /// a DuckDB secret, and both were built from one config — so a drift between them shows up here and
    /// nowhere else.</summary>
    public static async Task<IReadOnlyList<long>> ScanIdsAsync(ConnectorConfig config, string entity)
    {
        var source = new DeltaLakeSource(config);
        await using (source)
        {
            Assert.True(source.TryGetNativeScan(new DatasetSpec("lake", entity, new Dictionary<string, object?>()),
                out var scan));

            using var conn = new DuckDBConnection("DataSource=:memory:");
            await conn.OpenAsync();
            foreach (var statement in scan.SetupStatements)
            {
                using var setup = conn.CreateCommand();
                setup.CommandText = statement;
                await setup.ExecuteNonQueryAsync();
            }

            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"select id from {scan.SqlFragment} order by id";
            using var reader = await cmd.ExecuteReaderAsync();
            var ids = new List<long>();
            while (await reader.ReadAsync())
            {
                ids.Add(reader.GetInt64(0));
            }

            return ids;
        }
    }

    /// <summary>The table read back through delta-rs, using the same storage options the write used.
    /// Needs nothing off the network, so the facts that assert what a write actually committed do not
    /// depend on a DuckDB extension download.</summary>
    public static Task<IReadOnlyList<(long Id, string Dt, double Amt)>> RowsAsync(
        string location, ConnectorConfig config) =>
        DeltaReader.RowsAsync(location, DeltaStorageOptions.Build(config).ToDictionary(kv => kv.Key, kv => kv.Value));
}

/// <summary>MinIO, the S3 backend under test. The image tag matches the one pz's own S3 suite pins, so
/// this connector is measured against the same server build the rest of the ecosystem is — and the
/// version matters here more than usual: whether concurrent Delta commits to S3 are safe depends on
/// the STORE honouring the conditional PUT delta-rs issues, and MinIO has not always done so
/// (ConcurrencyTests documents what was measured on which build).</summary>
public sealed class MinioLake : IAsyncLifetime
{
    private const string Image = "minio/minio:RELEASE.2025-09-07T16-13-09Z";

    /// <summary>Created up front through the AWS S3 SDK: neither delta-rs nor DuckDB can create a
    /// bucket, and every root below lives inside this one.</summary>
    public const string Bucket = "pzdl-e2e";

    private MinioContainer? container;

    public string AccessKey { get; private set; } = string.Empty;

    public string SecretKey { get; private set; } = string.Empty;

    /// <summary>"host:port" — the bare shape a DuckDB s3 secret's <c>endpoint</c> takes. The delta-rs
    /// side needs a full URL and adds the scheme itself from <c>use_ssl</c>; one value has to serve
    /// both, which is exactly the agreement the round-trip fact exists to prove.</summary>
    public string Endpoint { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        if (!DockerFacts.CanStartContainers)
        {
            return;
        }

        this.container = new MinioBuilder(Image).Build();
        await this.container.StartAsync();

        var connectionString = this.container.GetConnectionString();
        var uri = new Uri(connectionString);
        this.Endpoint = $"{uri.Host}:{uri.Port}";
        this.AccessKey = this.container.GetAccessKey();
        this.SecretKey = this.container.GetSecretKey();

        using var s3 = new AmazonS3Client(this.AccessKey, this.SecretKey, new AmazonS3Config
        {
            ServiceURL = connectionString,
            ForcePathStyle = true,
            AuthenticationRegion = "us-east-1",
        });
        await s3.PutBucketAsync(Bucket);
    }

    public async Task DisposeAsync()
    {
        if (this.container is not null)
        {
            await this.container.DisposeAsync();
        }
    }

    public ConnectorConfig Config(string? secretAccessKey = null) => new(new Dictionary<string, object?>
    {
        ["root"] = $"s3://{Bucket}/delta",
        ["access_key_id"] = this.AccessKey,
        ["secret_access_key"] = secretAccessKey ?? this.SecretKey,
        ["endpoint"] = this.Endpoint,
        ["region"] = "us-east-1",
        ["url_style"] = "path",
        ["use_ssl"] = false,
    });

    public static string Location(string entity) => $"s3://{Bucket}/delta/{entity}";
}

[CollectionDefinition("minio")]
public sealed class MinioCollection : ICollectionFixture<MinioLake>;

/// <summary>Azurite, the Azure backend under test — same image tag pz's own azure suite pins. Azurite
/// emulates the Blob endpoint; ADLS Gen2 (<c>abfss://</c>) is not covered by anything here, which
/// docs/compatibility.md says plainly rather than leaving a reader to assume the az:// family was
/// proven whole.</summary>
public sealed class AzuriteLake : IAsyncLifetime
{
    private const string Image = "mcr.microsoft.com/azure-storage/azurite:3.35.0";

    public const string BlobContainer = "pzdl-e2e";

    private const int BlobPort = 10000;

    private AzuriteContainer? container;

    public string ConnectionString { get; private set; } = string.Empty;

    /// <summary>True when the blob endpoint came up on 10000, the Azure emulator's well-known port.
    /// It matters for exactly one fact. DuckDB's delta extension reads through delta-kernel-rs, whose
    /// object store recognizes an emulator connection string and resolves 127.0.0.1:10000 for it —
    /// measured: against a container on a mapped port every scan fails on a GET to :10000, and with
    /// the port bound the same scan reads the table. So the cross-engine read is provable only on the
    /// well-known port, and the fact that needs it says so in its skip reason rather than passing on a
    /// machine where the port was free and vanishing on one where it was not.</summary>
    public bool WellKnownBlobPort { get; private set; }

    public async Task InitializeAsync()
    {
        if (!DockerFacts.CanStartContainers)
        {
            return;
        }

        try
        {
            this.container = new AzuriteBuilder(Image).WithPortBinding(BlobPort, BlobPort).Build();
            await this.container.StartAsync();
            this.WellKnownBlobPort = true;
        }
        catch (Exception)
        {
            // Something already holds 10000. Every fact that does not need the well-known port still
            // has to run, so start again on a mapped one; a start that fails for any OTHER reason
            // fails again here and propagates, rather than being turned into a skip.
            await DisposeContainerAsync(this.container);
            this.container = new AzuriteBuilder(Image).Build();
            await this.container.StartAsync();
            this.WellKnownBlobPort = false;
        }

        this.ConnectionString = this.container.GetConnectionString();
        await new BlobServiceClient(this.ConnectionString).CreateBlobContainerAsync(BlobContainer);
    }

    public Task DisposeAsync() => DisposeContainerAsync(this.container);

    public ConnectorConfig Config(string? connectionString = null) => new(new Dictionary<string, object?>
    {
        ["root"] = $"az://{BlobContainer}/delta",
        ["connection_string"] = connectionString ?? this.ConnectionString,
    });

    public static string Location(string entity) => $"az://{BlobContainer}/delta/{entity}";

    private static async Task DisposeContainerAsync(AzuriteContainer? container)
    {
        if (container is not null)
        {
            await container.DisposeAsync();
        }
    }
}

[CollectionDefinition("azurite")]
public sealed class AzuriteCollection : ICollectionFixture<AzuriteLake>;
