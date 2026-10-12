using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class TelemetryTests
{
    private static OutputSpec Out(string mode, params (string Key, object? Value)[] options) =>
        new("lake", "orders", mode, "fail_on_change", options.ToDictionary(p => p.Key, p => p.Value))
        {
            Keys = mode == "merge" ? ["id"] : [],
        };

    private static async Task<ISink> OpenSink(string root) =>
        await ((ISinkConnector)new DeltaLakeConnector()).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["root"] = root }), default);

    [Fact]
    public void The_connector_declares_ReportsStats()
    {
        Assert.True(new DeltaLakeConnector().Capabilities.HasFlag(ConnectorCapabilities.ReportsStats));
    }

    [Fact]
    public async Task Each_flushed_generation_is_one_request_and_one_commit_with_its_arrow_bytes()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-tel").FullName;
        var acc = new ConnectorStatsAccumulator();
        var first = DeltaTestTable.Rows(0, 8);
        var second = DeltaTestTable.Rows(8, 8);
        using (ConnectorStats.Use(acc))
        {
            await using var sink = await OpenSink(dir);
            // One byte per generation forces a flush on every batch.
            await using var session = await sink.BeginWriteAsync(
                Out("append", ("target_file_bytes", 1L)), DeltaTestTable.Schema, default);
            await session.WriteBatchAsync(first, default);
            await session.WriteBatchAsync(second, default);
            await session.CommitAsync(default);
        }

        var s = acc.Snapshot();
        Assert.Equal(2, s.Requests);
        Assert.Equal(DeltaTelemetry.PayloadBytes([first, second]), s.WireBytes);
        Assert.True(s.WireBytes > 0);
        Assert.Contains(new KeyValuePair<string, double>("deltalake.commits", 2), s.Extras);
        Assert.Equal(0, s.RemoteMs);
        Assert.Equal(0, s.Throttled);
        Assert.Null(s.CostUnit);
    }

    [Fact]
    public async Task A_buffered_append_is_one_request_and_one_commit()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-tel").FullName;
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            await using var sink = await OpenSink(dir);
            await using var session = await sink.BeginWriteAsync(Out("append"), DeltaTestTable.Schema, default);
            await session.WriteBatchAsync(DeltaTestTable.Rows(0, 8), default);
            await session.WriteBatchAsync(DeltaTestTable.Rows(8, 8), default);
            await session.CommitAsync(default);
        }

        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.Contains(new KeyValuePair<string, double>("deltalake.commits", 1), s.Extras);
    }

    [Fact]
    public async Task A_merge_reports_one_request_and_one_commit()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-tel").FullName;
        await DeltaTestTable.CreateAtAsync(Path.Combine(dir, "orders"), 4);
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            await using var sink = await OpenSink(dir);
            await using var session = await sink.BeginWriteAsync(Out("merge"), DeltaTestTable.Schema, default);
            await session.WriteBatchAsync(DeltaTestTable.Rows(2, 6), default);
            await session.CommitAsync(default);
        }

        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.True(s.WireBytes > 0);
        Assert.Contains(new KeyValuePair<string, double>("deltalake.commits", 1), s.Extras);
    }

    [Fact]
    public async Task Reporting_from_the_big_stack_thread_lands_in_the_captured_handle()
    {
        var captured = new ConnectorStatsAccumulator();
        var decoy = new ConnectorStatsAccumulator();

        await DeltaTelemetry.RunWriteAsync(captured, 100, () =>
        {
            // Whatever is ambient on the big-stack thread is not the handle the caller captured.
            _ = ConnectorStats.Use(decoy);
            return Task.CompletedTask;
        });

        var s = captured.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.Equal(100, s.WireBytes);
        Assert.Contains(new KeyValuePair<string, double>("deltalake.commits", 1), s.Extras);
        Assert.True(decoy.Snapshot().IsEmpty);
    }

    [Fact]
    public async Task A_failed_call_counts_a_request_but_not_a_commit()
    {
        var acc = new ConnectorStatsAccumulator();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            DeltaTelemetry.RunWriteAsync(acc, 10, () => throw new InvalidOperationException("boom")));

        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.Empty(s.Extras);
    }

    [Fact]
    public async Task Reporting_outside_an_operation_is_a_no_op()
    {
        var dir = Directory.CreateTempSubdirectory("pz-delta-tel").FullName;
        await using var sink = await OpenSink(dir);
        await using var session = await sink.BeginWriteAsync(Out("append"), DeltaTestTable.Schema, default);
        await session.WriteBatchAsync(DeltaTestTable.Rows(0, 4), default);
        await session.CommitAsync(default);
    }
}
