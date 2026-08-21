using DuckDB.NET.Data;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>Gates tests that need to fetch the DuckDB delta extension over the network. The extension
/// is not bundled with DuckDB.NET.Data.Full — installing it is a real download on first use — so a
/// sandboxed/offline CI leg must SKIP these tests rather than fail them (repo rule: docker/network
/// suites skip cleanly without the dependency). The probe result is cached for the process lifetime;
/// every caller shares one real network attempt instead of one per test.</summary>
internal static class TestNetwork
{
    private static readonly Lazy<Task<bool>> CanInstall = new(ProbeAsync);

    public static Task<bool> CanInstallDuckDbExtensionsAsync() => CanInstall.Value;

    private static async Task<bool> ProbeAsync()
    {
        // Same opt-out pz's own test suites honor (docs/README says PZ_TESTS_OFFLINE=1 skips
        // network-dependent tests) — short-circuits the real network attempt below rather than
        // waiting for it to time out on a deliberately offline run.
        if (Environment.GetEnvironmentVariable("PZ_TESTS_OFFLINE") == "1")
        {
            return false;
        }

        try
        {
            using var conn = new DuckDBConnection("DataSource=:memory:");
            conn.Open();
            foreach (var s in new[] { "install delta", "load delta" })
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = s;
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            return true;
        }
        catch
        {
            return false;
        }
    }
}
