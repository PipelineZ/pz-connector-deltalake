using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Errors;
using DeltaLake.Interfaces;
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
    public async Task A_path_option_decides_where_the_table_is_written()
    {
        // The one option that decides where the data LANDS. Nothing else in this file passes it, so
        // without this a regression writes into <root>/<output> — appending rows into a different Delta
        // table entirely, with no diagnostic anywhere.
        var dir = TempDir("pz-delta-path");
        await using var sink = await OpenSink(dir);
        var spec = Out("append", ("path", "curated/orders"));

        await using (var session = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default))
        {
            await session.WriteBatchAsync(DeltaTestTable.Rows(0, 12), default);
            await session.CommitAsync(default);
        }

        Assert.True(Directory.Exists(Path.Combine(dir, "curated", "orders", "_delta_log")));
        Assert.False(Directory.Exists(Path.Combine(dir, "orders")));
        Assert.Equal(12, await DeltaReader.CountAsync(Path.Combine(dir, "curated", "orders")));
    }

    [Fact]
    public async Task A_validation_failure_never_carries_the_offending_rows_into_the_error()
    {
        // The leak reproduced end to end on the append path this task ships, through the real sink and
        // real delta-rs — not a hand-written message. The table declares 'dt' NOT NULL; the incoming
        // schema declares it nullable, which is true of the SCHEMA and says nothing about whether the
        // batch holds nulls, so Reconcile cannot refuse it without refusing legitimate writes. delta-rs
        // then fails the insert with a preview of the offending rows — every column of them.
        var dir = TempDir("pz-delta-leak");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);

        await using var sink = await OpenSink(dir);
        var nullableDt = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(true))
            .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
            .Build();

        var id = new Int64Array.Builder();
        var dt = new StringArray.Builder();
        var amt = new DoubleArray.Builder();
        id.Append(777);
        dt.AppendNull();
        amt.Append(31337.5);
        var batch = new RecordBatch(nullableDt, [id.Build(), dt.Build(), amt.Build()], 1);

        await using var session = await sink.BeginWriteAsync(Out(), nullableDt, default);
        await session.WriteBatchAsync(batch, default);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await session.CommitAsync(default));

        Assert.Contains(DeltaErrors.WriteFailed, ex.Message);
        Assert.DoesNotContain("777", ex.Message);
        Assert.DoesNotContain("31337.5", ex.Message);
        Assert.DoesNotContain("|", ex.Message);
        Assert.Contains("1 rows failed validation check", ex.Message);
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
            // Committing no batches would flush nothing, and the count below would just be the
            // fixture's starting rows — proving only that the guard stood aside, not that the write
            // landed through the widened schema.
            await session.WriteBatchAsync(DeltaTestTable.WiderRows(3), default);
            Assert.Equal(3, (await session.CommitAsync(default)).RowsWritten);
        }

        // The row count alone would still read 7 if delta-rs silently DROPPED the extra column instead
        // of widening the table, which is the one thing this test is named for.
        Assert.Equal(7, await DeltaReader.CountAsync(Path.Combine(dir, "orders")));
        Assert.Contains("note", await DeltaReader.ColumnsAsync(Path.Combine(dir, "orders")));
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
                ["target_file_bytes"] = 65536L,
            })
        { Keys = ["id"] });

        Assert.Equal("merge", opts.Mode);
        Assert.Equal(["id"], opts.Keys);
        Assert.Equal(["dt"], opts.PartitionBy);
        Assert.Equal("dt >= '2026-01-01'", opts.MergePredicate);
        Assert.Equal(65536L, opts.TargetFileBytes);

        // A tripwire on the list's CONTENTS, not on whether each name is acted on — 'path' is not read
        // by From at all; the sink reads it when it resolves where the table lives. Adding a name here
        // without a test that watches it DO something is how an option becomes validated-and-ignored,
        // the failure this whole surface exists to prevent. All four are watched: partition_by,
        // merge_predicate and target_file_bytes by the assertions above, path by
        // A_path_option_decides_where_the_table_is_written.
        Assert.Equal(
            ["merge_predicate", "partition_by", "path", "target_file_bytes"],
            DeltaLakeSchemas.WriteOptions.Order(StringComparer.Ordinal));
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
        Assert.Contains(DeltaErrors.InvalidWriteOption, ex.Message);
        Assert.Contains("partiton_by", ex.Message);
        Assert.Contains("partition_by", ex.Message);
    }

    [Fact]
    public void An_option_problem_carries_a_config_code_not_the_runtime_write_code()
    {
        // PZDL0404 is the runtime family — a storage-layer write failure. An option problem is decided
        // from the OutputSpec alone, before anything is opened, and one code that has to explain both
        // causes at once helps nobody.
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["nonsense"] = 1L })));
        Assert.Contains(DeltaErrors.InvalidWriteOption, ex.Message);
        Assert.DoesNotContain(DeltaErrors.WriteFailed, ex.Message);
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
                    ["merge_predicat"] = "x",
                    ["target_file_bytes"] = "lots",
                })));
        Assert.Contains("partiton_by", ex.Message);
        Assert.Contains("merge_predicat", ex.Message);
        Assert.Contains("target_file_bytes", ex.Message);
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
    public void A_non_numeric_target_file_bytes_is_a_coded_refusal_not_a_format_exception()
    {
        // Convert.ToInt64 would raise FormatException, which pz's `catch (PzConnectorException)` does
        // not carry — the failure would reach the user without a code, a node name or a next step.
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["target_file_bytes"] = "128MB" })));
        Assert.Contains(DeltaErrors.InvalidWriteOption, ex.Message);
        Assert.Contains("target_file_bytes", ex.Message);
        Assert.Contains("128MB", ex.Message);
    }

    [Fact]
    public void A_target_file_bytes_beyond_int_range_is_carried_not_truncated()
    {
        // A byte count routinely exceeds int.MaxValue; narrowing would silently turn 4 GiB into 0 and
        // flush a generation after every batch.
        var opts = DeltaWriteOptions.From(new OutputSpec("lake", "orders", "append", "fail_on_change",
            new Dictionary<string, object?> { ["target_file_bytes"] = 4294967296L }));
        Assert.Equal(4294967296L, opts.TargetFileBytes);
    }

    [Fact]
    public void A_zero_target_file_bytes_is_refused_rather_than_flushing_after_every_batch()
    {
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["target_file_bytes"] = 0L })));
        Assert.Contains(DeltaErrors.InvalidWriteOption, ex.Message);
        Assert.Contains("target_file_bytes", ex.Message);
    }

    [Fact]
    public void Max_rows_per_group_is_refused_because_setting_it_was_measured_to_do_nothing()
    {
        // Measured against DeltaLake.Net 0.33.0: 100,000 rows land in one parquet row group of 100,000
        // whether InsertOptions.MaxRowsPerGroup is unset or 1000. Accepting the option would make it a
        // validated no-op that reads like a working setting — the same silent failure as an unvalidated
        // one, just relocated. It comes back when it demonstrably shapes a row group.
        var ex = Assert.Throws<PzConnectorException>(() => DeltaWriteOptions.From(
            new OutputSpec("lake", "orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["max_rows_per_group"] = 1000L })));
        Assert.Contains(DeltaErrors.InvalidWriteOption, ex.Message);
        Assert.Contains("unknown write option 'max_rows_per_group'", ex.Message, StringComparison.Ordinal);
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
    public async Task A_cancelled_begin_surfaces_cancellation_rather_than_a_permanent_write_failure()
    {
        // Named for what it actually guards. It is NOT evidence about the open-versus-create decision:
        // with an already-cancelled token a create attempt would fail identically, so a sink that
        // swallowed every load failure and created regardless would pass this test unchanged. The
        // seam-driven tests below are what discriminate that.
        var dir = TempDir("pz-delta-cancel");
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await using var sink = await OpenSink(dir);
        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, cts.Token));
        Assert.IsNotAssignableFrom<PzConnectorException>(ex);
        Assert.False(Directory.Exists(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task An_absent_table_is_created()
    {
        var (engine, created) = EngineThatFails(
            new DeltaRuntimeException("Not a Delta table: …", DeltaLakeSink.TableAbsentErrorCode));
        await using var sink = new DeltaLakeSink(Cfg("/mnt/lake"), () => engine);

        await using var session = await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default);
        Assert.True(created.Value, "an absent table must lead to a create");
    }

    [Fact]
    public async Task A_storage_failure_at_open_is_reported_transiently_rather_than_retried_as_a_create()
    {
        // The stake in ruling 4's discriminator: a bare catch turns this retryable failure into a
        // create attempt, and reports whatever the create said — permanently.
        var (engine, created) = EngineThatFails(
            new DeltaRuntimeException("Kernel error: object store error: connection refused", 30));
        await using var sink = new DeltaLakeSink(Cfg("/mnt/lake"), () => engine);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, default));
        Assert.False(created.Value, "a non-absent load failure must never reach a create");
        Assert.Contains("open of output 'orders'", ex.Message, StringComparison.Ordinal);
        Assert.True(ex.IsTransient);
    }

    [Fact]
    public async Task Cancellation_arriving_during_the_open_never_reaches_a_create()
    {
        // Cancelling AS the load fails is what an already-cancelled token cannot show: the create is
        // left able to succeed, so a sink that reached it would return a session instead of throwing,
        // and would flip the flag.
        using var cts = new CancellationTokenSource();
        var (engine, created) = EngineThatFails(new OperationCanceledException(), onLoad: () => cts.Cancel());
        await using var sink = new DeltaLakeSink(Cfg("/mnt/lake"), () => engine);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await sink.BeginWriteAsync(Out(), DeltaTestTable.Schema, cts.Token));
        Assert.False(created.Value, "a cancelled open must never reach a create");
    }

    /// <summary>An engine whose load always fails with <paramref name="failure"/> and whose create
    /// always succeeds, returning a table shaped like the fixture. The flag records whether the create
    /// was reached at all — the one thing no arrangement of real files can observe, because a create
    /// over a location delta-rs refuses to load always fails too.</summary>
    private static (FakeDeltaEngine Engine, StrongBox<bool> Created) EngineThatFails(
        Exception failure, Action? onLoad = null)
    {
        var created = new StrongBox<bool>(false);
        var engine = new FakeDeltaEngine
        {
            OnLoadTableAsync = (_, _) =>
            {
                onLoad?.Invoke();
                throw failure;
            },
            OnCreateTableAsync = (_, _) =>
            {
                created.Value = true;
                return Task.FromResult<ITable>(new FakeDeltaTable
                {
                    OnSchema = () => DeltaTestTable.Schema,
                    // TableMetadata.PartitionColumns has no public setter, so this double reports the
                    // null a caller must already tolerate; no test through this seam declares
                    // partition_by, so the comparison is not what is under test here.
                    OnMetadata = () => new TableMetadata(),
                });
            },
        };

        return (engine, created);
    }

    [Fact]
    public async Task Begin_refuses_partition_by_on_a_table_that_was_created_unpartitioned()
    {
        // partition_by only ever reaches CreateTableAsync, so on an existing table it is inert: without
        // this guard the run succeeds, writes no partition directories, says nothing, and every
        // partition-pruned read against the table silently full-scans from then on.
        var dir = TempDir("pz-delta-repartition");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);

        await using var sink = await OpenSink(dir);
        var spec = Out("append", ("partition_by", new List<object?> { "dt" }));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default));

        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);
        Assert.Contains("partition_by declares [dt]", ex.Message, StringComparison.Ordinal);
        Assert.Contains("not partitioned", ex.Message, StringComparison.Ordinal);
        Assert.Empty(Directory.GetDirectories(Path.Combine(dir, "orders"), "dt=*"));
    }

    [Fact]
    public async Task Begin_refuses_partition_by_that_names_different_columns_than_the_table()
    {
        var dir = TempDir("pz-delta-repartition2");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4, partitionBy: ["dt"]);

        await using var sink = await OpenSink(dir);
        var spec = Out("append", ("partition_by", new List<object?> { "id" }));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default));

        Assert.Contains("partition_by declares [id]", ex.Message, StringComparison.Ordinal);
        Assert.Contains("partitioned by [dt]", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_partition_by_matching_the_existing_table_is_accepted()
    {
        // The refusal above must be a mismatch guard, not a blanket ban on declaring partition_by
        // against a table that already exists — which every run after the first one does.
        var dir = TempDir("pz-delta-repartition3");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4, partitionBy: ["dt"]);

        await using var sink = await OpenSink(dir);
        var spec = Out("append", ("partition_by", new List<object?> { "dt" }));
        await using (var session = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default))
        {
            await session.WriteBatchAsync(DeltaTestTable.Rows(100, 8), default);
            await session.CommitAsync(default);
        }

        Assert.Equal(12, await DeltaReader.CountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task Begin_refuses_a_write_that_omits_a_nullable_table_column()
    {
        // delta-rs accepts this and null-fills, silently: a pipeline that stops selecting a column
        // would keep writing, filling it with nulls forever, with no diagnostic anywhere.
        var dir = TempDir("pz-delta-omit-nullable");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);

        await using var sink = await OpenSink(dir);
        var withoutAmt = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(false))
            .Build();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out(), withoutAmt, default));
        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);
        Assert.Contains("amt", ex.Message);
    }

    [Fact]
    public async Task Schema_policy_evolve_allows_omitting_a_nullable_column_but_never_a_non_nullable_one()
    {
        var dir = TempDir("pz-delta-omit-evolve");
        await DeltaTestTable.CreateLocalAsync(dir, rows: 4);
        await using var sink = await OpenSink(dir);

        var withoutAmt = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(false))
            .Build();
        await using (var session = await sink.BeginWriteAsync(
            Out() with { SchemaPolicy = "evolve" }, withoutAmt, default))
        {
            await session.CommitAsync(default);
        }

        // 'dt' is NOT NULL in the table. delta-rs refuses that one itself — but only at insert time,
        // with a preview of the offending rows, after the table exists and after any earlier flushed
        // generation has already committed. "evolve" cannot wave through what delta-rs will not accept.
        var withoutDt = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
            .Build();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out() with { SchemaPolicy = "evolve" }, withoutDt, default));
        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);
        Assert.Contains("dt", ex.Message);
        Assert.Contains("NOT NULL", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Merge_opens_a_session_and_creates_the_table_it_will_write_to()
    {
        // This test replaces the one that pinned merge's refusal at BeginWriteAsync. The refusal had a
        // reason worth keeping visible: a strategy refused at COMMIT has already created a table and
        // buffered the whole pipeline, leaving an orphan behind. Now that merge writes, the same
        // property is asserted the other way round — begin creates the table, and the merge is what
        // fills it.
        var dir = TempDir("pz-delta-merge-begin");
        await using var sink = await OpenSink(dir);

        await using (var session = await sink.BeginWriteAsync(
            Out("merge") with { Keys = ["id"] }, DeltaTestTable.Schema, default))
        {
            Assert.True(Directory.Exists(Path.Combine(dir, "orders")));
            await session.WriteBatchAsync(DeltaTestTable.Rows(0, 3), default);
            Assert.Equal(3, (await session.CommitAsync(default)).RowsWritten);
        }

        Assert.Equal(3, await DeltaReader.CountAsync(Path.Combine(dir, "orders")));
    }

    [Fact]
    public async Task Begin_refuses_two_timestamps_that_differ_only_in_unit_and_timezone()
    {
        // Delta stores exactly one timestamp shape, so the table's is always microseconds in UTC; a
        // comparison on ArrowTypeId alone would let a millisecond, zoneless column straight through.
        var dir = TempDir("pz-delta-ts");
        await CreateLocalAsync(dir, new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("at").DataType(new TimestampType(TimeUnit.Microsecond, "UTC")).Nullable(true))
            .Build());

        await using var sink = await OpenSink(dir);
        var incoming = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("at").DataType(new TimestampType(TimeUnit.Millisecond, (string?)null)).Nullable(true))
            .Build();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out(), incoming, default));
        Assert.Contains("timestamp[Microsecond, tz=UTC]", ex.Message, StringComparison.Ordinal);
        Assert.Contains("timestamp[Millisecond]", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Begin_refuses_a_list_whose_element_type_differs()
    {
        var dir = TempDir("pz-delta-list");
        await CreateLocalAsync(dir, new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("tags").DataType(new ListType(new Field("element", Int64Type.Default, true)))
                .Nullable(true))
            .Build());

        await using var sink = await OpenSink(dir);
        var incoming = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("tags").DataType(new ListType(new Field("element", StringType.Default, true)))
                .Nullable(true))
            .Build();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out(), incoming, default));
        Assert.Contains("list<int64>", ex.Message, StringComparison.Ordinal);
        Assert.Contains("list<utf8>", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Begin_refuses_a_struct_whose_field_type_differs()
    {
        var dir = TempDir("pz-delta-struct");
        await CreateLocalAsync(dir, new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("who").DataType(new StructType(
                [new Field("name", StringType.Default, true), new Field("age", Int64Type.Default, true)]))
                .Nullable(true))
            .Build());

        await using var sink = await OpenSink(dir);
        var incoming = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("who").DataType(new StructType(
                [new Field("name", StringType.Default, true), new Field("age", StringType.Default, true)]))
                .Nullable(true))
            .Build();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Out(), incoming, default));
        Assert.Contains("struct<name: utf8, age: int64>", ex.Message, StringComparison.Ordinal);
        Assert.Contains("struct<name: utf8, age: utf8>", ex.Message, StringComparison.Ordinal);
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
