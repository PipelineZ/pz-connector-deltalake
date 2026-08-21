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

    [Fact]
    public void A_union_by_name_option_is_emitted_as_a_named_argument()
    {
        var source = Open(("root", "/mnt/lake"));
        source.TryGetNativeScan(Spec(("union_by_name", true)), out var scan);
        Assert.Contains("union_by_name => true", scan!.SqlFragment);
    }

    [Fact]
    public void A_union_by_name_option_left_unset_or_false_is_not_emitted()
    {
        var source = Open(("root", "/mnt/lake"));

        source.TryGetNativeScan(Spec(), out var unset);
        Assert.DoesNotContain("union_by_name", unset!.SqlFragment);

        source.TryGetNativeScan(Spec(("union_by_name", false)), out var setFalse);
        Assert.DoesNotContain("union_by_name", setFalse!.SqlFragment);
    }

    [Fact]
    public void An_out_of_range_version_is_refused_with_PZDL0202_not_an_unhandled_overflow()
    {
        // DeltaLakeSchemas.Dataset declares "integer, minimum 0" with no maximum, so a JSON-valid
        // integer wider than Int64 reaches TryGetNativeScan; it must fail as a coded
        // PzConnectorException, not an unguarded OverflowException the planner cannot map to PZDL0202.
        var source = Open(("root", "/mnt/lake"));
        var ex = Assert.Throws<PzConnectorException>(
            () => source.TryGetNativeScan(Spec(("version", "99999999999999999999")), out _));
        Assert.Contains(DeltaErrors.VersionNotFound, ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void The_fragment_and_setup_statements_are_byte_identical_for_identical_inputs()
    {
        // Node ids are content-addressed from this output, so two calls with the same spec must
        // produce the same strings, not merely equivalent ones.
        var source = Open(("root", "s3://w/d"), ("access_key_id", "AK"), ("secret_access_key", "SK"), ("region", "eu-west-1"));
        var spec = Spec(("version", 12L), ("union_by_name", true)) with
        {
            WatermarkCursor = "updated_at", WatermarkValue = "2026-01-01", WatermarkUpperBound = "2026-02-01",
        };

        source.TryGetNativeScan(spec, out var first);
        source.TryGetNativeScan(spec, out var second);

        Assert.Equal(first!.SqlFragment, second!.SqlFragment);
        Assert.Equal(first.SetupStatements, second.SetupStatements);
    }
}
