using Pz.Connector.DeltaLake;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class ConnectorIdentityTests
{
    [Fact]
    public void Info_names_the_connector_and_matches_the_host_protocol_major()
    {
        var c = new DeltaLakeConnector();
        Assert.Equal("deltalake", c.Info.Name);
        Assert.Equal(ProtocolVersion.Major, c.Info.ProtocolMajor);
    }

    [Fact]
    public void Capabilities_are_the_single_declared_union_for_both_directions()
    {
        var expected =
            ConnectorCapabilities.NativeScan | ConnectorCapabilities.ColumnPruning |
            ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.BoundedWindow |
            ConnectorCapabilities.InclusiveWatermarkBound | ConnectorCapabilities.Merge |
            ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Transactional |
            ConnectorCapabilities.ColumnPartitionedWrites;
        Assert.Equal(expected, new DeltaLakeConnector().Capabilities);
    }

    [Theory]
    [InlineData(ConnectorCapabilities.NativeCopy)]
    [InlineData(ConnectorCapabilities.ApplyDeletes)]
    [InlineData(ConnectorCapabilities.ChangeCapture)]
    [InlineData(ConnectorCapabilities.GatedOperations)]
    [InlineData(ConnectorCapabilities.PartitionedRead)]
    [InlineData(ConnectorCapabilities.StreamingPartitions)]
    [InlineData(ConnectorCapabilities.CheckpointableWrites)]
    [InlineData(ConnectorCapabilities.StablePartitionIds)]
    [InlineData(ConnectorCapabilities.TextLengthStats)]
    [InlineData(ConnectorCapabilities.SyncState)]
    // PathTemplating means the connector renders pz's calendar tokens into a path. It was declared
    // once, for a meaning it does not have: it was the only flag that got partition_by past pz, which
    // read the option as one column substituting those tokens. pz now splits the two -- tokens mean
    // pz lays the partitions out, their absence means the destination does -- so the honest flag is
    // ColumnPartitionedWrites and this one goes back to being withheld. Nothing here renders a path
    // in either direction.
    [InlineData(ConnectorCapabilities.PathTemplating)]
    public void Capabilities_deliberately_withheld_stay_withheld(ConnectorCapabilities withheld)
    {
        // Each of these makes pz refuse a config with a targeted error rather than degrade silently.
        // Declaring one before its feature exists is worse than leaving it off.
        Assert.False(new DeltaLakeConnector().Capabilities.HasFlag(withheld));
    }

    [Fact]
    public void Source_is_native_only()
    {
        // Turns engine.force_universal / files_per_partition into PZ0312 instead of a doomed run.
        // The connector's declared interfaces are this file's subject, so this is the one copy of the
        // assertion -- NativeScanTests carried an identical body under a different name, which is two
        // tests failing together and telling a reader nothing the first did not.
        Assert.IsAssignableFrom<INativeOnlySource>(new DeltaLakeConnector());
    }

    [Fact]
    public void Manifest_ships_next_to_the_assembly_and_declares_protocol_1()
    {
        var dir = Path.GetDirectoryName(typeof(DeltaLakeConnector).Assembly.Location)!;
        var json = File.ReadAllText(Path.Combine(dir, "pz.connector.json"));
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        Assert.Equal("deltalake", doc.RootElement.GetProperty("name").GetString());
        Assert.Equal(1, doc.RootElement.GetProperty("protocolMajorMin").GetInt32());
        Assert.Equal(1, doc.RootElement.GetProperty("protocolMajorMax").GetInt32());
    }
}
