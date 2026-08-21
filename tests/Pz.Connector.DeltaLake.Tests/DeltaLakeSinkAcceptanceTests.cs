using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>The ABI's own sink acceptance suite. Its batch-ownership fact is the one this connector
/// most needs: the write session buffers batches for a terminal delta-rs call, which is precisely the
/// shape that gets ownership wrong.
///
/// Reads go back through delta-rs so the whole suite runs offline —
/// <see cref="DeltaLakeSinkAcceptanceThroughDuckDbTests"/> is the identical suite read through the
/// other engine, and skips when the extension cannot be fetched.</summary>
public class DeltaLakeSinkAcceptanceTests : SinkConnectorAcceptanceTests, IDisposable
{
    private readonly LocalLakeFixture lake = LocalLakeFixture.Empty();

    /// <summary>How a committed table is read back. Overridable so the cross-engine subclass below can
    /// swap the READER without owning a second fixture — both legs must observe the same lake, or the
    /// second one would be asserting against a table the first one never wrote.
    ///
    /// The fixture type itself stays internal (xunit needs the test class public, which a protected
    /// member of an internal type could not satisfy), so the seam is this method rather than the
    /// fixture.</summary>
    protected virtual Task<IReadOnlyList<RecordBatch>> ReadBackAsync(string entity) =>
        this.lake.ReadBatchesAsync(entity);

    private protected Task<IReadOnlyList<RecordBatch>> ReadBackThroughDuckDbAsync(string entity) =>
        this.lake.ReadBatchesThroughDuckDbAsync(entity);

    protected override ISinkConnector CreateSink() => new DeltaLakeConnector();

    protected override ConnectorConfig ValidConfig =>
        new(new Dictionary<string, object?> { ["root"] = this.lake.Root });

    protected override OutputSpec SmallOutput =>
        new("lake", "appended", "append", "fail_on_change", new Dictionary<string, object?>());

    protected override OutputSpec? ReplaceOutput =>
        new("lake", "replaced", "replace", "fail_on_change", new Dictionary<string, object?>());

    protected override OutputSpec? MergeOutput =>
        new("lake", "merged", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };

    /// <summary>Each merge fact starts from a clean table: the base suite writes to a persistent
    /// destination, and a leftover from the previous fact would make an upsert assertion pass or fail
    /// for the wrong reason.</summary>
    protected override Task ResetMergeTargetAsync() => this.lake.DropAsync("merged");

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(
        ISinkConnector connector, OutputSpec spec) =>
        await this.ReadBackAsync(spec.Output);

    public void Dispose()
    {
        this.lake.Dispose();
        GC.SuppressFinalize(this);
    }
}

/// <summary>The same suite, verified through DuckDB's delta extension instead of delta-rs. A write is
/// only correct if the OTHER engine can see it, and reading back through the writer would hide exactly
/// the interop bugs worth catching — but installing the extension is a real download, so this leg
/// SKIPs offline while <see cref="DeltaLakeSinkAcceptanceTests"/> keeps the ownership and commit
/// assertions running on every leg.</summary>
public sealed class DeltaLakeSinkAcceptanceThroughDuckDbTests : DeltaLakeSinkAcceptanceTests
{
    protected override void GateFact() =>
        Skip.IfNot(
            TestNetwork.CanInstallDuckDbExtensionsAsync().GetAwaiter().GetResult(),
            "DuckDB's delta extension could not be installed (offline)");

    protected override Task<IReadOnlyList<RecordBatch>> ReadBackAsync(string entity) =>
        this.ReadBackThroughDuckDbAsync(entity);
}
