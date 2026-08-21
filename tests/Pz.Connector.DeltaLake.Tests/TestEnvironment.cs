namespace Pz.Connector.DeltaLake.Tests;

/// <summary>The gate for tests that measure rather than assert. A cost regression needs a table big
/// enough for the cost to be visible, which is minutes of work — too slow for every run and too
/// valuable to delete, so it is opt-in and CI does not pay for it.</summary>
internal static class TestEnvironment
{
    public static bool RunSlowBenchmarks => Environment.GetEnvironmentVariable("PZDL_SLOW_TESTS") == "1";
}
