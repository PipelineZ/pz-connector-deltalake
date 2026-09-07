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
    public async Task Manifest_mode_writes_a_process_manifest_from_the_connector_itself()
    {
        // The manifest is no longer a hand-written file: `dotnet pack` runs the published binary in
        // --pz-manifest mode and ships what it prints, so the manifest and the PCP handshake come
        // from the same object. The test-time build output holds the same entry assembly, so it is
        // driven the same way here; what it writes is what `pz restore` reads.
        var assembly = typeof(DeltaLakeConnector).Assembly.Location;
        var outFile = Path.Combine(Path.GetTempPath(), $"pz-deltalake-manifest-{Guid.NewGuid():N}.json");
        try
        {
            using var process = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
            {
                FileName = "dotnet",
                ArgumentList = { assembly, "--pz-manifest", "--out", outFile,
                    "--entrypoint", "linux-x64=native/Pz.Connector.DeltaLake" },
                RedirectStandardError = true,
                RedirectStandardOutput = true,
            })!;
            var stderr = await process.StandardError.ReadToEndAsync();
            await process.WaitForExitAsync();
            Assert.True(process.ExitCode == 0, $"manifest mode exited {process.ExitCode}: {stderr}");

            using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(outFile));
            var root = doc.RootElement;
            Assert.Equal("deltalake", root.GetProperty("name").GetString());
            Assert.Equal(1, root.GetProperty("protocolMajorMin").GetInt32());
            Assert.Equal(1, root.GetProperty("protocolMajorMax").GetInt32());
            Assert.Equal("process", root.GetProperty("runtime").GetString());
            Assert.Equal("native/Pz.Connector.DeltaLake",
                root.GetProperty("entrypoints").GetProperty("linux-x64").GetString());
            // root: stays absolute (PZDL0101), so no project-directory anchor is declared.
            Assert.False(root.TryGetProperty("projectDirectoryAnchor", out _));
            var capabilities = root.GetProperty("capabilities").EnumerateArray().Select(c => c.GetString()).ToList();
            Assert.Contains("NativeScan", capabilities);
            Assert.Contains("Merge", capabilities);
            Assert.DoesNotContain("SyncState", capabilities);
        }
        finally
        {
            File.Delete(outFile);
        }
    }
}
