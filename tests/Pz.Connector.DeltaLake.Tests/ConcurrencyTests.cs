using DeltaLake.Table;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>What happens when two writers commit to one Delta table at the same time — the question
/// this connector's design refused to guess at, because the wrong answer on S3 is the only failure it
/// can produce that is SILENT: a commit that is overwritten, a run that reports success, and rows that
/// are simply not there afterwards.
///
/// What was measured, against delta-rs 0.33.0 (DeltaLake.Net 0.33.0) on 2026-08-22:
/// <list type="bullet">
/// <item><description>A commit to an s3:// root is an object_store put with <c>PutMode::Create</c> — a
/// conditional PUT — and never a rename. Reached by setting <c>AWS_CONDITIONAL_PUT=disabled</c>, which
/// fails the write with "Operation `put_opts` with mode `PutMode::Create` when conditional put is
/// disabled"; a rename-based path would instead have failed every write with "Atomic rename requires a
/// LockClient for S3 backends", which no write in this suite has ever produced. No locking provider is
/// required, and <c>AWS_S3_ALLOW_UNSAFE_RENAME</c> is neither needed nor set.</description></item>
/// <item><description>Four writers committing simultaneously to one table, ten rounds, on MinIO
/// RELEASE.2025-09-07T16-13-09Z: forty successes, zero failures, zero rows lost.</description></item>
/// <item><description>The same forty commits on MinIO RELEASE.2023-01-31T02-24-19Z — a build that does
/// not enforce If-None-Match — reported forty successes and kept ONE commit per round. Thirty of forty
/// commits vanished, silently. The safety is the store's, not the library's; docs/limitations.md
/// carries this.</description></item>
/// <item><description>On local disk the same race also loses nothing: delta-rs resolves an append
/// conflict internally and commits both, so a lost-race exception is not reachable through the sink at
/// all. What is asserted below is therefore the property that matters and not the mechanism — no
/// committer may lose rows, and any committer that DOES fail must be transient, or the engine would
/// turn a normal race into a failed run.</description></item>
/// </list></summary>
public class ConcurrencyTests
{
    /// <summary>Every S3 config shape this connector can build, checked for the one option that would
    /// make a lost commit silent. Not a container test: this must hold on every machine, on every leg,
    /// forever — it is the single assertion standing between a user and undetectable data loss, and it
    /// must not be skippable.</summary>
    [Theory]
    [MemberData(nameof(S3Configs))]
    public void No_s3_configuration_ever_asks_delta_rs_for_an_unsafe_rename(Dictionary<string, object?> values)
    {
        var options = DeltaStorageOptions.Build(new ConnectorConfig(values));

        Assert.DoesNotContain(options, kv =>
            kv.Key.Contains("UNSAFE", StringComparison.OrdinalIgnoreCase) ||
            kv.Value.Contains("UNSAFE", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>The other half: the commit path is pinned to the conditional PUT rather than left to a
    /// library default. <see cref="A_commit_path_with_no_concurrency_guarantee_is_refused"/> is what
    /// proves the key is actually read by delta-rs — this only proves the connector sets it.</summary>
    [Theory]
    [MemberData(nameof(S3Configs))]
    public void Every_s3_configuration_pins_the_commit_to_a_conditional_put(Dictionary<string, object?> values)
    {
        Assert.Equal("etag", DeltaStorageOptions.Build(new ConnectorConfig(values))["AWS_CONDITIONAL_PUT"]);
    }

    public static TheoryData<Dictionary<string, object?>> S3Configs() =>
    [
        new() { ["root"] = "s3://b/d" },
        new() { ["root"] = "s3://b/d", ["access_key_id"] = "k", ["secret_access_key"] = "s" },
        new()
        {
            ["root"] = "s3a://b/d", ["access_key_id"] = "k", ["secret_access_key"] = "s",
            ["session_token"] = "t", ["region"] = "eu-west-1", ["endpoint"] = "minio:9000",
            ["url_style"] = "path", ["use_ssl"] = false,
        },
        new() { ["root"] = "s3://b/d", ["region"] = "us-east-1", ["use_ssl"] = true },
    ];

    /// <summary>The same race the S3 suite runs, on local disk, where it needs no docker and runs on
    /// every leg.</summary>
    [Fact]
    public async Task Concurrent_appends_to_one_local_table_never_lose_a_commit()
    {
        var root = Directory.CreateTempSubdirectory("pz-delta-race").FullName;
        try
        {
            var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = root });
            for (var round = 0; round < 3; round++)
            {
                var entity = $"race{round}";
                await AssertRaceKeepsEveryCommitAsync(config, entity, Path.Combine(root, entity), null);
            }
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>Opens four sessions against one table, fills them, then releases every commit at once
    /// so they contend for the same next version rather than queueing. Gate-based, not clock-based:
    /// nothing here sleeps, and the overlap comes from a TaskCompletionSource every committer is
    /// already waiting on.
    ///
    /// The assertion is deliberately not "all four succeed": that would pin delta-rs's internal retry
    /// rather than the property a user depends on. What is pinned is that the table afterwards holds
    /// exactly the rows of the committers that reported success — no commit reported and then lost —
    /// and that a committer that failed did so transiently.</summary>
    internal static async Task AssertRaceKeepsEveryCommitAsync(
        ConnectorConfig config, string entity, string location, ConnectorConfig? remote)
    {
        const int Writers = 4;
        const int RowsEach = 5;

        var spec = new OutputSpec("lake", entity, "append", "fail_on_change", new Dictionary<string, object?>());
        await ObjectStoreLake.WriteAsync(config, spec, DeltaTestTable.Rows(0, RowsEach));

        var sinks = new List<DeltaLakeSink>();
        var sessions = new List<ISinkWriteSession>();
        try
        {
            for (var w = 0; w < Writers; w++)
            {
                var sink = new DeltaLakeSink(config);
                sinks.Add(sink);
                var session = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default);
                sessions.Add(session);
                using var batch = DeltaTestTable.Rows(1000 * (w + 1), RowsEach);
                await session.WriteBatchAsync(batch, default);
            }

            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var commits = sessions.Select(async session =>
            {
                await gate.Task;
                try
                {
                    await session.CommitAsync(default);
                    return (Committed: true, Failure: (PzConnectorException?)null);
                }
                catch (PzConnectorException ex)
                {
                    return (Committed: false, Failure: ex);
                }
            }).ToArray();

            gate.SetResult();
            var outcomes = await Task.WhenAll(commits);

            foreach (var failure in outcomes.Select(o => o.Failure).OfType<PzConnectorException>())
            {
                // A lost commit race is a normal event on a multi-writer table. Classified permanent it
                // would fail the whole run instead of costing one retry.
                Assert.Contains(DeltaErrors.CommitConflict, failure.Message, StringComparison.Ordinal);
                Assert.True(failure.IsTransient, failure.Message);
            }

            var committed = outcomes.Count(o => o.Committed);
            var rows = remote is null
                ? await DeltaReader.RowsAsync(location)
                : await ObjectStoreLake.RowsAsync(location, remote);

            Assert.Equal(RowsEach + (RowsEach * committed), rows.Count);
            for (var w = 0; w < Writers; w++)
            {
                var mine = rows.Count(r => r.Id >= 1000 * (w + 1) && r.Id < (1000 * (w + 1)) + RowsEach);
                Assert.True(mine is 0 or RowsEach, $"writer {w} left {mine} of its {RowsEach} rows behind");
            }
        }
        finally
        {
            foreach (var session in sessions)
            {
                await session.DisposeAsync();
            }

            foreach (var sink in sinks)
            {
                await sink.DisposeAsync();
            }
        }
    }

    /// <summary>Loads an existing table and appends to it, through storage options the caller chose.
    /// The path a user actually meets: <c>CreateAsync</c> only ever exercises the version-0 commit,
    /// and the two failure modes this suite provokes both behave differently on an existing
    /// table.</summary>
    internal static Task AppendAsync(string location, Dictionary<string, string> storage) =>
        DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(
                new TableOptions { TableLocation = location, StorageOptions = storage }, default);
            try
            {
                using var batch = DeltaTestTable.Rows(100, 5);
                await table.InsertAsync([batch], DeltaTestTable.Schema,
                    new InsertOptions { SaveMode = SaveMode.Append }, default);
            }
            finally
            {
                (table as IDisposable)?.Dispose();
            }

            return 0;
        });

    internal static Task CreateAsync(string location, Dictionary<string, string> storage) =>
        DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.CreateTableAsync(
                new TableCreateOptions(location, DeltaTestTable.Schema)
                {
                    SaveMode = SaveMode.ErrorIfExists,
                    StorageOptions = storage,
                },
                default);
            (table as IDisposable)?.Dispose();
            return 0;
        });
}

/// <summary>The S3 half of the concurrency determination, against the shared MinIO container.
/// A class of its own because the collection that owns that container is declared per class, and the
/// facts next door must keep running with no docker at all.</summary>
[Collection("minio")]
public class S3ConcurrencyTests(MinioLake lake)
{
    /// <summary>Four sinks, one S3 table, one gate — the measurement this task exists for, kept as a
    /// standing assertion so a delta-rs bump that moves the commit path off the conditional PUT is
    /// caught here rather than by a user missing rows.</summary>
    [SkippableFact]
    public async Task Concurrent_appends_to_one_s3_table_never_lose_a_commit()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        lake.SkipIfUnavailable();

        var config = lake.Config();
        for (var round = 0; round < 3; round++)
        {
            var entity = $"s3_race{round}";
            await ConcurrencyTests.AssertRaceKeepsEveryCommitAsync(
                config, entity, MinioLake.Location(entity), config);
        }
    }

    /// <summary>The only measured way a user's environment breaks every S3 read and write of this
    /// connector, permanently. <c>AWS_S3_LOCKING_PROVIDER=dynamodb</c> — a variable that lives on in
    /// shell profiles and container images at any team that used delta-rs's DynamoDB log store before
    /// — selects that log store, which then fails because this connector supplies no lock table for
    /// it. It is passed here as a storage option rather than exported into the environment because
    /// the two reach the same code and only one of them is deterministic in-process: .NET's
    /// Environment.SetEnvironmentVariable does not reach the native environ on Linux, so an in-process
    /// env probe measures nothing at all.
    ///
    /// Before PZDL0403 covered it this landed on PZDL0404, whose next step names the table's protocol
    /// version and the incoming schema — so a user whose shell profile broke their run was told to
    /// inspect their data.</summary>
    [SkippableFact]
    public async Task A_locking_provider_in_the_environment_is_named_as_the_cause()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        lake.SkipIfUnavailable();

        var clean = DeltaStorageOptions.Build(lake.Config()).ToDictionary(kv => kv.Key, kv => kv.Value);
        var location = MinioLake.Location("s3_dynamolock");
        await ConcurrencyTests.CreateAsync(location, clean);

        var diverted = new Dictionary<string, string>(clean) { ["AWS_S3_LOCKING_PROVIDER"] = "dynamodb" };
        var raw = await Assert.ThrowsAnyAsync<Exception>(() => ConcurrencyTests.AppendAsync(location, diverted));

        var mapped = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "append of output 'orders'", []);
        Assert.Contains(DeltaErrors.UnsafeConcurrentS3, mapped.Message, StringComparison.Ordinal);
        Assert.Contains("AWS_S3_LOCKING_PROVIDER", mapped.Message, StringComparison.Ordinal);
        Assert.False(mapped.IsTransient);
    }

    /// <summary>The one way left, in 0.33.0, to reach an S3 commit path with no concurrency guarantee:
    /// take the conditional PUT away. delta-rs refuses rather than falling back to an unconditional
    /// write, and this pins BOTH halves of that — the refusal is real, so the option this connector
    /// sets is genuinely read rather than a plausible-looking string delta-rs ignores, and it is
    /// classified as PZDL0403 rather than as the retryable commit race its wording would otherwise be
    /// mistaken for.</summary>
    [SkippableFact]
    public async Task A_commit_path_with_no_concurrency_guarantee_is_refused()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        lake.SkipIfUnavailable();

        var clean = DeltaStorageOptions.Build(lake.Config()).ToDictionary(kv => kv.Key, kv => kv.Value);
        var storage = new Dictionary<string, string>(clean) { ["AWS_CONDITIONAL_PUT"] = "disabled" };

        // Both the version-0 commit and a later one. Only the second says anything about the path an
        // ordinary run takes -- a create could have had its own mechanism -- and a rename fallback,
        // if delta-rs had one, is exactly what a version>0 commit would have used here.
        var created = MinioLake.Location("s3_nocondput_create");
        var existing = MinioLake.Location("s3_nocondput_append");
        await ConcurrencyTests.CreateAsync(existing, clean);

        foreach (var attempt in new Func<Task>[]
        {
            () => ConcurrencyTests.CreateAsync(created, storage),
            () => ConcurrencyTests.AppendAsync(existing, storage),
        })
        {
            var raw = await Assert.ThrowsAnyAsync<Exception>(attempt);
            var mapped = DeltaErrors.Translate(raw, DeltaOperationKind.Write, "append of output 'orders'", []);
            Assert.Contains(DeltaErrors.UnsafeConcurrentS3, mapped.Message, StringComparison.Ordinal);
            Assert.False(mapped.IsTransient);
            Assert.Contains("AWS_S3_ALLOW_UNSAFE_RENAME", mapped.Message, StringComparison.Ordinal);
        }
    }
}
