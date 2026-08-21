using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class NativeScanTests
{
    private static ISource Open(params (string Key, object? Value)[] pairs)
    {
        var connector = (ISourceConnector)new DeltaLakeConnector();
        return connector.OpenAsync(new ConnectorConfig(pairs.ToDictionary(p => p.Key, p => p.Value)), default)
            .AsTask().GetAwaiter().GetResult();
    }

    private static DatasetSpec Spec(params (string Key, object? Value)[] options) =>
        new("lake", "orders", options.ToDictionary(p => p.Key, p => p.Value));

    [Fact]
    public void A_plain_read_is_a_bare_delta_scan_of_root_slash_entity()
    {
        var source = Open(("root", "/mnt/lake"));
        Assert.True(source.TryGetNativeScan(Spec(), out var scan));
        Assert.StartsWith("delta_scan('/mnt/lake/orders'", scan!.SqlFragment);
    }

    [Fact]
    public void The_setup_statements_load_the_delta_extension()
    {
        var source = Open(("root", "/mnt/lake"));
        source.TryGetNativeScan(Spec(), out var scan);
        Assert.Contains("load delta", scan!.SetupStatements);
    }

    [Fact]
    public void A_version_option_time_travels()
    {
        var source = Open(("root", "/mnt/lake"));
        source.TryGetNativeScan(Spec(("version", 12L)), out var scan);
        Assert.Contains("version => 12", scan!.SqlFragment);
    }

    [Fact]
    public void A_path_option_overrides_the_entity_default()
    {
        var source = Open(("root", "/mnt/lake"));
        source.TryGetNativeScan(Spec(("path", "curated/orders")), out var scan);
        Assert.Contains("'/mnt/lake/curated/orders'", scan!.SqlFragment);
    }

    [Fact]
    public void A_watermark_window_is_wrapped_around_the_scan()
    {
        var source = Open(("root", "/mnt/lake"));
        var spec = Spec() with { WatermarkCursor = "updated_at", WatermarkValue = "2026-01-01" };
        source.TryGetNativeScan(spec, out var scan);
        Assert.StartsWith("(select * from delta_scan(", scan!.SqlFragment);
        Assert.Contains("\"updated_at\" > '2026-01-01'", scan.SqlFragment);
    }

    [Fact]
    public void Schema_is_not_inferred_because_delta_declares_its_own()
    {
        // The engine's integer-inference lint would be a false positive against a declared schema.
        var source = Open(("root", "/mnt/lake"));
        source.TryGetNativeScan(Spec(), out var scan);
        Assert.False(scan!.SchemaInferred);
        Assert.Null(scan.SniffFragment);
    }

    [Fact]
    public void The_mechanism_string_names_delta_scan_and_carries_no_credential()
    {
        var source = Open(("root", "s3://w/d"), ("access_key_id", "AK"), ("secret_access_key", "SUPERSECRET"));
        source.TryGetNativeScan(Spec(), out var scan);
        Assert.Equal("delta_scan", scan!.Mechanism);
        Assert.DoesNotContain("SUPERSECRET", scan.Mechanism);
        Assert.DoesNotContain("SUPERSECRET", scan.SqlFragment);
    }

    [Fact]
    public async Task PlanReadAsync_refuses_the_universal_tier_with_PZ0312()
    {
        var source = Open(("root", "/mnt/lake"));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.PlanReadAsync(Spec(), ReadHints.None, default));
        Assert.Contains("PZ0312", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void The_connector_is_native_only_so_the_planner_refuses_force_universal_before_run_time() =>
        Assert.IsAssignableFrom<INativeOnlySource>(new DeltaLakeConnector());
}
