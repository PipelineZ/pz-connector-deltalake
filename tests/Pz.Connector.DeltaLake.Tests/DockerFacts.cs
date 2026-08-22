using System.Diagnostics;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>The two gates every container-backed fact passes through, mirroring pz's own
/// <c>Pz.TestSupport.DockerFacts</c> so a run of this suite skips under exactly the conditions a run
/// of pz's does.
///
/// Both are needed, not one. <see cref="SkipUnlessDocker"/> covers a machine with no daemon.
/// <see cref="SkipIfOffline"/> covers the deliberate offline leg (<c>PZ_TESTS_OFFLINE=1</c>), which a
/// docker probe cannot see: starting a MinIO or Azurite container PULLS an image, so a suite gated on
/// the daemon alone still reaches for the network on the leg that exists to prove it does not.
///
/// The probe waits 5 seconds for <c>docker info</c> — pz's own timeout, chosen over a longer one so
/// the two suites agree about what "docker is not available" means rather than differing by an
/// interval nobody would think to compare.</summary>
internal static class DockerFacts
{
    private static readonly Lazy<bool> Available = new(Probe);

    public static bool DockerAvailable => Available.Value;

    public static bool Offline => Environment.GetEnvironmentVariable("PZ_TESTS_OFFLINE") == "1";

    /// <summary>What a FIXTURE asks before touching Testcontainers. A fixture cannot skip — a
    /// <c>Skip</c> thrown from fixture construction is a collection-level error, not a skipped fact —
    /// so it checks this, does nothing, and leaves each fact to skip itself through the two gates
    /// below.</summary>
    public static bool CanStartContainers => DockerAvailable && !Offline;

    public static void SkipUnlessDocker() => Skip.IfNot(DockerAvailable, "docker is not available");

    public static void SkipIfOffline() => Skip.If(Offline, "PZ_TESTS_OFFLINE=1");

    private static bool Probe()
    {
        try
        {
            using var process = Process.Start(new ProcessStartInfo("docker", "info")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            });

            return process is not null && process.WaitForExit(5_000) && process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}
