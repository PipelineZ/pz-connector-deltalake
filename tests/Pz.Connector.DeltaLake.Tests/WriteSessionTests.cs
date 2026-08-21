using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Errors;
using DeltaLake.Table;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class WriteSessionTests
{
    private static ConnectorConfig Cfg(string root) => new(new Dictionary<string, object?> { ["root"] = root });

    /// <summary>"fail_on_change" is pz's own default <c>schema_policy</c>, so it is what an ordinary
    /// output actually carries; pz never validates the value against a list, so the connector, not pz,
    /// decides which spellings mean anything.</summary>
    private static OutputSpec Out(string mode = "append", params (string Key, object? Value)[] options) =>
        new("lake", "orders", mode, "fail_on_change", options.ToDictionary(p => p.Key, p => p.Value));

    private static async Task<ISink> OpenSink(string root) =>
        await ((ISinkConnector)new DeltaLakeConnector()).OpenAsync(Cfg(root), default);

    private static string TempDir(string name) => Directory.CreateTempSubdirectory(name).FullName;

    [Fact]
    public async Task Native_copy_is_refused_because_duckdb_cannot_write_delta()
    {
        await using var sink = await OpenSink("/mnt/lake");
        Assert.False(sink.TryGetNativeCopy(Out(), out var copy));
        Assert.Null(copy);
    }

    [Fact]
    public async Task Abort_semantics_are_best_effort_not_discards_all()
    {
        // An append session flushes bounded generations, and a flushed generation is committed. Claiming
        // DiscardsAll would make a failed write report a cleanup that did not happen — and the engine
        // surfaces this value in run artifacts.
        await using var sink = await OpenSink("/mnt/lake");
        Assert.Equal(AbortSemantics.BestEffort, sink.AbortSemantics);
    }

    [Fact]
    public async Task Begin_creates_the_table_when_it_does_not_exist()
    {
        var dir = TempDir("pz-delta-create");
        await using var sink = await OpenSink(dir);
        await using (var session = await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default))
        {
            await session.CommitAsync(default);
        }

        Assert.True(Directory.Exists(Path.Combine(dir, "orders", "_delta_log")));
    }

    [Fact]
    public async Task Begin_creates_the_table_partitioned_when_partition_by_is_declared()
    {
        var dir = TempDir("pz-delta-part");
        await using var sink = await OpenSink(dir);
        var spec = Out("append", ("partition_by", new List<object?> { "dt" }));
        await using (var session = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default))
        {
            await session.WriteBatchAsync(DeltaTestTable.Rows(0, 16), default);
            await session.CommitAsync(default);
        }

        // Partition columns live in directory names, not in the data files.
        Assert.NotEmpty(Directory.GetDirectories(Path.Combine(dir, "orders"), "dt=*"));
        Assert.Equal(16, await DeltaReader.CountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task Begin_refuses_an_unwritable_arrow_type_before_any_data_moves()
    {
        var dir = TempDir("pz-delta-badtype");
        await using var sink = await OpenSink(dir);
        var schema = new Schema.Builder()
            .Field(f => f.Name("span").DataType(new IntervalType(IntervalUnit.YearMonth)).Nullable(true))
            .Build();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out(), schema, default));
        Assert.Contains(DeltaErrors.UnwritableArrowType, ex.Message);
        Assert.False(Directory.Exists(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task Begin_refuses_an_incompatible_incoming_schema_naming_the_column_and_both_types()
    {
        var dir = TempDir("pz-delta-mismatch");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);

        await using var sink = await OpenSink(dir);
        var incompatible = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(false))
            .Field(f => f.Name("amt").DataType(StringType.Default).Nullable(true))
            .Build();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out(), incompatible, default));
        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);
        Assert.Contains("amt", ex.Message);
        // The names Arrow itself produces, not the SQL spellings: Apache.Arrow calls a string column
        // "utf8", and an assertion on "string" would pass only by being weakened.
        Assert.Contains("double", ex.Message, StringComparison.Ordinal);
        Assert.Contains("utf8", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Begin_refuses_two_decimals_that_differ_only_in_precision()
    {
        // Both sides are Decimal128, so a comparison on ArrowTypeId alone would let this through to a
        // delta-rs error that names neither the output nor the column.
        var dir = TempDir("pz-delta-decimal");
        var table = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("price").DataType(new Decimal128Type(38, 9)).Nullable(true))
            .Build();
        await CreateLocalAsync(dir, table);

        await using var sink = await OpenSink(dir);
        var incoming = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("price").DataType(new Decimal128Type(10, 2)).Nullable(true))
            .Build();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out(), incoming, default));
        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);
        Assert.Contains("price", ex.Message);
        Assert.Contains("decimal128(38, 9)", ex.Message, StringComparison.Ordinal);
        Assert.Contains("decimal128(10, 2)", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Begin_refuses_a_column_the_table_does_not_have_under_the_default_schema_policy()
    {
        var dir = TempDir("pz-delta-newcol");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);

        await using var sink = await OpenSink(dir);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out(), DeltaTestTable.WiderSchema, default));
        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);
        Assert.Contains("note", ex.Message);
    }

    [Fact]
    public async Task Schema_policy_evolve_lets_the_write_add_the_column()
    {
        // The connector, not pz, decides that "evolve" means this; delta-rs widens the table's schema
        // on an append whose batch carries an extra column, with no further flag.
        var dir = TempDir("pz-delta-evolve");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);

        await using var sink = await OpenSink(dir);
        var spec = Out() with { SchemaPolicy = "evolve" };
        await using (var session = await sink.BeginWriteAsync(spec, DeltaTestTable.WiderSchema, default))
        {
            await session.CommitAsync(default);
        }

        Assert.Equal(4, await DeltaReader.CountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task Begin_refuses_a_merge_key_that_is_not_in_the_schema()
    {
        // Keys arrive on OutputSpec.Keys, not in the option bag — pz strips `keys` before a connector
        // ever sees the options.
        var dir = TempDir("pz-delta-badkey");
        await using var sink = await OpenSink(dir);
        var spec = Out("merge") with { Keys = ["order_id"] };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default));
        Assert.Contains(DeltaErrors.MergeKeyNotInSchema, ex.Message);
        Assert.Contains("order_id", ex.Message);
        Assert.False(Directory.Exists(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task Merge_without_keys_is_refused_at_begin()
    {
        var dir = TempDir("pz-delta-nokeys");
        await using var sink = await OpenSink(dir);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out("merge"), DeltaTestTable.Schema, default));
        Assert.Contains(DeltaErrors.MergeWithoutKeys, ex.Message);
    }

    [Fact]
    public void Write_options_default_target_file_bytes_and_read_every_declared_option()
    {
        var opts = DeltaWriteOptions.From(new OutputSpec("lake", "orders", "merge", "fail_on_change",
            new Dictionary<string, object?>
            {
                ["partition_by"] = new List<object?> { "dt" },
                ["merge_predicate"] = "dt >= '2026-01-01'",
                ["max_rows_per_group"] = 65536L,
            })
        { Keys = ["id"] });

        Assert.Equal("merge", opts.Mode);
        Assert.Equal(["id"], opts.Keys);
        Assert.Equal(["dt"], opts.PartitionBy);
        Assert.Equal("dt >= '2026-01-01'", opts.MergePredicate);
        Assert.Equal(65536L, opts.MaxRowsPerGroup);
        Assert.True(opts.TargetFileBytes > 0);
    }

    [Fact]
    public void Merge_keys_come_from_the_OutputSpec_Keys_property_not_from_options()
    {
        // pz strips `keys` from the option bag and stamps it on OutputSpec.Keys; reading it only from
        // Options would silently merge on nothing.
        var opts = DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "merge", "fail_on_change", new Dictionary<string, object?>())
            { Keys = ["id"] });
        Assert.Equal(["id"], opts.Keys);
    }

    [Fact]
    public void An_unknown_write_option_is_refused_rather_than_silently_ignored()
    {
        // Nothing upstream validates output options, so a typo here would otherwise do nothing at all.
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["partiton_by"] = new List<object?> { "dt" } })));
        Assert.Contains("partiton_by", ex.Message);
        Assert.Contains("partition_by", ex.Message);
    }

    [Fact]
    public void Every_invalid_write_option_is_named_not_only_the_first()
    {
        // Aggregate reporting, not fail-one-at-a-time: a user fixing options one run at a time is a
        // user who runs the pipeline four times to learn about four typos.
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?>
                {
                    ["partiton_by"] = new List<object?> { "dt" },
                    ["targt_file_bytes"] = 1L,
                    ["max_rows_per_group"] = "lots",
                })));
        Assert.Contains("partiton_by", ex.Message);
        Assert.Contains("targt_file_bytes", ex.Message);
        Assert.Contains("max_rows_per_group", ex.Message);
    }

    [Fact]
    public void The_retired_mode_option_is_refused_with_a_pointer_to_strategy()
    {
        // pz refuses `mode:` itself, but a sink() kwarg spelled `mode` passes straight through to the
        // connector, where it would otherwise be a silent no-op that looks like it set the write mode.
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["mode"] = "merge" })));
        Assert.Contains("strategy", ex.Message);
    }

    [Fact]
    public void A_version_option_on_a_write_is_refused_because_time_travel_is_read_only()
    {
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["version"] = 12L })));
        Assert.Contains(DeltaErrors.VersionOnWrite, ex.Message);
    }

    [Fact]
    public void A_non_numeric_max_rows_per_group_is_a_coded_refusal_not_a_format_exception()
    {
        // Convert.ToInt64 would raise FormatException, which pz's `catch (PzConnectorException)` does
        // not carry — the failure would reach the user without a code, a node name or a next step.
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["max_rows_per_group"] = "lots" })));
        Assert.Contains("max_rows_per_group", ex.Message);
        Assert.Contains("lots", ex.Message);
        Assert.Contains("PZDL", ex.Message);
    }

    [Fact]
    public void A_max_rows_per_group_beyond_int_range_is_carried_not_truncated()
    {
        // Narrowing to int would silently turn 4294967296 into 0 and hand delta-rs a value its Rust
        // writer panics on.
        var opts = DeltaWriteOptions.From(new OutputSpec("lake", "orders", "append", "fail_on_change",
            new Dictionary<string, object?> { ["max_rows_per_group"] = 4294967296L }));
        Assert.Equal(4294967296L, opts.MaxRowsPerGroup);
    }

    [Fact]
    public void A_zero_max_rows_per_group_is_refused_because_the_rust_writer_panics_on_it()
    {
        // Observed against DeltaLake.Net 0.33.0: MaxRowsPerGroup = 0 aborts the write with a Rust
        // panic ("assertion failed: step != 0") rather than an error the engine can report.
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["max_rows_per_group"] = 0L })));
        Assert.Contains("max_rows_per_group", ex.Message);
    }

    [Fact]
    public void A_non_numeric_target_file_bytes_is_a_coded_refusal()
    {
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["target_file_bytes"] = "128MB" })));
        Assert.Contains("target_file_bytes", ex.Message);
        Assert.Contains("128MB", ex.Message);
    }

    [Fact]
    public void A_bare_string_partition_by_is_refused_rather_than_silently_unpartitioned()
    {
        // Returning an empty list for a non-list value would produce an unpartitioned table from a
        // declaration that asked for a partitioned one — a silent, and permanent, layout change.
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["partition_by"] = "dt" })));
        Assert.Contains("partition_by", ex.Message);
        Assert.Contains("dt", ex.Message);
    }

    [Fact]
    public async Task Commit_persists_every_written_batch()
    {
        var dir = TempDir("pz-delta-append");
        await using var sink = await OpenSink(dir);
        await using (var session = await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default))
        {
            await session.WriteBatchAsync(DeltaTestTable.Rows(0, 100), default);
            await session.WriteBatchAsync(DeltaTestTable.Rows(100, 100), default);
            var result = await session.CommitAsync(default);
            Assert.Equal(200, result.RowsWritten);
            Assert.Equal(2, result.BatchesWritten);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(200, rows.Count);
        Assert.Equal(Enumerable.Range(0, 200).Select(i => (long)i), rows.Select(r => r.Id));
    }

    [SkippableFact]
    public async Task Committed_rows_are_readable_by_duckdb_the_other_engine()
    {
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(), "delta extension unavailable offline");

        var dir = TempDir("pz-delta-duckdb");
        await using var sink = await OpenSink(dir);
        await using (var session = await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default))
        {
            await session.WriteBatchAsync(DeltaTestTable.Rows(0, 100), default);
            await session.CommitAsync(default);
        }

        var rows = await DuckDbReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(100, rows.Count);
        Assert.Equal(Enumerable.Range(0, 100).Select(i => (long)i), rows.Select(r => r.Id));
    }

    [Fact]
    public async Task The_session_clones_rather_than_retains_the_engine_owned_batch()
    {
        var dir = TempDir("pz-delta-clone");
        await using var sink = await OpenSink(dir);
        using var handed = new EngineOwnedBatch(DeltaTestTable.Rows(0, 50), DeltaTestTable.Rows(1000, 50));

        await using (var session = await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default))
        {
            await session.WriteBatchAsync(handed.Batch, default);

            // The engine is done with the batch the instant WriteBatchAsync returns, and its pooled
            // buffers can be handed straight to an unrelated batch. Recycle() is that reuse, and the
            // assertion right after it proves the recycle landed rather than quietly doing nothing.
            handed.Recycle();
            Assert.Equal(1000L, ((Int64Array)handed.Batch.Column(0)).GetValue(0));

            await session.CommitAsync(default);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(50, rows.Count);
        Assert.Equal(Enumerable.Range(0, 50).Select(i => (long)i), rows.Select(r => r.Id));
    }

    [Fact]
    public async Task An_append_flushes_bounded_generations_rather_than_buffering_the_whole_write()
    {
        var dir = TempDir("pz-delta-gen");
        await using var sink = await OpenSink(dir);
        // A tiny target forces a flush partway through, so the generation logic is exercised
        // deterministically rather than by writing gigabytes.
        var spec = Out("append", ("target_file_bytes", 65536L));

        await using (var session = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default))
        {
            for (var i = 0; i < 8; i++)
            {
                await session.WriteBatchAsync(DeltaTestTable.Rows(i * 5000, 5000), default);
            }

            Assert.Equal(40_000, (await session.CommitAsync(default)).RowsWritten);
        }

        var location = Path.Combine(dir, "orders");
        Assert.Equal(40_000, await DeltaReader.CountAsync(location));

        // Row count alone cannot tell a generation-flushing session from one that buffered all 40,000
        // rows and committed once. The transaction log can: create + one commit per flushed generation.
        Assert.True(DeltaReader.CommitCount(location) > 2,
            $"expected more than one insert commit, saw {DeltaReader.CommitCount(location)} log entries");
    }

    [Fact]
    public async Task Replace_overwrites_the_prior_commit_rather_than_appending()
    {
        var dir = TempDir("pz-delta-replace");
        await using var sink = await OpenSink(dir);

        await using (var first = await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default))
        {
            await first.WriteBatchAsync(DeltaTestTable.Rows(0, 100), default);
            await first.CommitAsync(default);
        }

        await using (var second = await sink.BeginWriteAsync(Out("replace"), DeltaTestTable.Schema, default))
        {
            await second.WriteBatchAsync(DeltaTestTable.Rows(500, 10), default);
            await second.CommitAsync(default);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Equal(10, rows.Count);
        Assert.Equal(500L, rows[0].Id);
    }

    [Fact]
    public async Task Replace_with_no_batches_empties_the_table()
    {
        // Observed against DeltaLake.Net 0.33.0: InsertAsync with an EMPTY batch collection and
        // SaveMode.Overwrite returns successfully and changes nothing at all, so a replace that
        // produced no rows would silently leave the previous run's data in place.
        var dir = TempDir("pz-delta-replace-empty");
        await using var sink = await OpenSink(dir);

        await using (var first = await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default))
        {
            await first.WriteBatchAsync(DeltaTestTable.Rows(0, 100), default);
            await first.CommitAsync(default);
        }

        await using (var second = await sink.BeginWriteAsync(Out("replace"), DeltaTestTable.Schema, default))
        {
            var result = await second.CommitAsync(default);
            Assert.Equal(0, result.RowsWritten);
        }

        Assert.Equal(0, await DeltaReader.CountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task Abort_before_any_flush_leaves_the_table_empty()
    {
        var dir = TempDir("pz-delta-abort");
        await using var sink = await OpenSink(dir);
        await using (var session = await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default))
        {
            await session.WriteBatchAsync(DeltaTestTable.Rows(0, 100), default);
            await session.AbortAsync(default);
        }

        Assert.Equal(0, await DeltaReader.CountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task A_second_commit_is_rejected()
    {
        var dir = TempDir("pz-delta-double");
        await using var sink = await OpenSink(dir);
        await using var session = await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default);
        await session.WriteBatchAsync(DeltaTestTable.Rows(0, 10), default);
        await session.CommitAsync(default);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.CommitAsync(default));
    }

    [Fact]
    public async Task Abort_after_commit_is_rejected_because_commits_outcome_is_unknowable()
    {
        var dir = TempDir("pz-delta-abortafter");
        await using var sink = await OpenSink(dir);
        await using var session = await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default);
        await session.WriteBatchAsync(DeltaTestTable.Rows(0, 10), default);
        await session.CommitAsync(default);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.AbortAsync(default));
    }

    [Fact]
    public async Task Committing_zero_batches_succeeds_and_reports_zero_rows()
    {
        var dir = TempDir("pz-delta-empty");
        await using var sink = await OpenSink(dir);
        await using var session = await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default);
        var result = await session.CommitAsync(default);
        Assert.Equal(0, result.RowsWritten);
        Assert.Equal(0, result.BatchesWritten);
    }

    [Fact]
    public async Task An_absent_delta_table_is_the_one_load_failure_that_means_create_it()
    {
        // The discriminator OpenOrCreateAsync uses, pinned against real delta-rs: DeltaLake.Net 0.33.0
        // does not expose its error-code enum publicly, so the connector carries the NotATable value as
        // a literal. If an upstream release renumbers it, this fails here rather than silently turning
        // every open failure — a wrong credential, an unreachable bucket — into a create attempt.
        var dir = TempDir("pz-delta-absent");
        var ex = await Assert.ThrowsAsync<DeltaRuntimeException>(async () =>
            await DeltaBigStack.RunAsync(async () =>
            {
                using var engine = new DeltaEngine(EngineOptions.Default);
                return await engine.LoadTableAsync(
                    new TableOptions { TableLocation = Path.Combine(dir, "orders") }, default);
            }));

        Assert.Equal(DeltaLakeSink.TableAbsentErrorCode, ex.ErrorCode);
    }

    [Fact]
    public async Task An_open_failure_that_is_not_an_absent_table_is_reported_rather_than_retried_as_a_create()
    {
        // A corrupt transaction log stands in for every non-absent open failure (a wrong credential, an
        // unreachable endpoint): swallowing it and creating instead would report a CREATE failure for a
        // table that exists, and — for a transient storage error — would do so permanently.
        var dir = TempDir("pz-delta-corrupt");
        var location = await DeltaTestTable.CreateLocalAsync(dir, rows: 4);
        var log = Directory.GetFiles(Path.Combine(location, "_delta_log"), "*.json").Order().First();
        await File.WriteAllTextAsync(log, "{ not json at all\n");

        await using var sink = await OpenSink(dir);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default));
        Assert.Contains("open of output 'orders'", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("create of output", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_cancelled_begin_neither_creates_a_table_nor_reports_a_delta_failure()
    {
        var dir = TempDir("pz-delta-cancel");
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await using var sink = await OpenSink(dir);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, cts.Token));
        Assert.False(Directory.Exists(Path.Combine(dir, "orders")));
    }

    private static Task<string> CreateLocalAsync(string dir, Schema schema) =>
        DeltaBigStack.RunAsync(async () =>
        {
            var location = Path.Combine(dir, "orders");
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.CreateTableAsync(
                new TableCreateOptions(location, schema) { SaveMode = SaveMode.ErrorIfExists }, default);
            if (table is IDisposable disposable)
            {
                disposable.Dispose();
            }

            return location;
        });
}
