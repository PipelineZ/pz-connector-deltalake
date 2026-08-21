using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>The ABI's own source acceptance suite, wired to a real local Delta table — and skipped in
/// full, because every data-plane fact in it reads through <c>ISource.PlanReadAsync</c> and this source
/// has no universal tier to read on. It is an <see cref="INativeOnlySource"/>: DuckDB executes the
/// scan, the rows never enter .NET, and pz's own planner refuses the universal tier for such a
/// connector before a run starts. <c>PlanReadAsync</c> answering PZ0312 is the contract, not a gap, so
/// there is nothing here to fix on the connector side — the suite has no shape for a source of this
/// kind. Confirmed against the TestKit 0.2.2 assembly: none of its facts consult
/// <see cref="INativeOnlySource"/> or <c>TryGetNativeScan</c>, and none of pz's own four native-only
/// source connectors subclasses this suite, so no native-only source has ever run it.
///
/// The five facts that failed — schema/batch agreement, read determinism, partition-union
/// completeness, and the BoundedWindow and InclusiveWatermarkBound capability contracts — are real
/// properties this connector owes. <see cref="NativeScanAcceptanceTests"/> carries them against the
/// tier the connector actually uses, so skipping here loses coverage of nothing.
///
/// The gate is a live condition, not a constant: the day this connector grows a universal read path
/// and stops being <see cref="INativeOnlySource"/>, the whole suite starts running again by itself
/// rather than staying quietly skipped. The fixtures below stay wired for the same reason.
///
/// <c>GateFact()</c> is the only lever the base class offers — its facts are non-virtual by design
/// ("the provided [Fact]s are the executable spec — do not override them") and it is called first by
/// every one of them, with nothing to say which. So the skip is all-or-nothing, and one fact that
/// would have passed (<c>Validate_accepts_valid_config</c>) goes with it; it is re-asserted in
/// <see cref="NativeScanAcceptanceTests"/>.</summary>
public class DeltaLakeSourceAcceptanceTests : SourceConnectorAcceptanceTests, IDisposable
{
    private readonly LocalLakeFixture lake = LocalLakeFixture.WithTable("orders", rows: 250);

    protected override void GateFact() =>
        Skip.If(new DeltaLakeConnector() is INativeOnlySource,
            "deltalake is a native-only source: every data-plane fact in this suite reads through " +
            "PlanReadAsync, which it refuses (PZ0312). See NativeScanAcceptanceTests.");

    protected override ISourceConnector CreateSource() => new DeltaLakeConnector();

    protected override ConnectorConfig ValidConfig =>
        new(new Dictionary<string, object?> { ["root"] = this.lake.Root });

    protected override DatasetSpec SmallDataset => new("lake", "orders", new Dictionary<string, object?>());

    /// <summary>The lower bound is EXCLUSIVE and the upper INCLUSIVE, and the base fact asserts the
    /// exact cursor set 4,5,6,7 against a seed producing 0..10 — so 3/7 are the only values that mean
    /// anything here, whatever a spec draft may have said.</summary>
    protected override DatasetSpec? BoundedWindowDataset => this.SmallDataset with
    {
        WatermarkCursor = "id",
        WatermarkValue = "3",
        WatermarkUpperBound = "7",
    };

    public void Dispose()
    {
        this.lake.Dispose();
        GC.SuppressFinalize(this);
    }
}
