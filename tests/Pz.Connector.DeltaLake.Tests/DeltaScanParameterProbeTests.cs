using DuckDB.NET.Data;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class DeltaScanParameterProbeTests
{
    private static bool Accepts(string namedArgument, string tablePath)
    {
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        foreach (var s in new[] { "install delta", "load delta" })
        {
            using var setup = conn.CreateCommand();
            setup.CommandText = s;
            setup.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"select count(*) from delta_scan('{tablePath}'{namedArgument})";
        try
        {
            cmd.ExecuteScalar();
            return true;
        }
        catch (DuckDBException)
        {
            return false;
        }
    }

    [SkippableFact]
    public async Task Record_which_named_parameters_delta_scan_accepts()
    {
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(), "delta extension unavailable offline");
        var dir = Directory.CreateTempSubdirectory("pz-delta-probe").FullName;
        var table = await DeltaTestTable.CreateLocalAsync(dir, rows: 10);

        Assert.True(Accepts("", table));
        Assert.True(Accepts(", version => 0", table));

        // Not assertions about what SHOULD exist — a record of what DOES, pinned so a DuckDB bump
        // that changes the surface fails here instead of at a user's run.
        var pushdown = Accepts(", pushdown_filters => true", table);
        var unionByName = Accepts(", union_by_name => true", table);
        Assert.True(pushdown || !pushdown);       // record only
        Assert.True(unionByName || !unionByName); // record only
        Assert.Equal(DeltaScanFragment.SupportsPushdownFilters, pushdown);
        Assert.Equal(DeltaScanFragment.SupportsUnionByName, unionByName);
    }
}
