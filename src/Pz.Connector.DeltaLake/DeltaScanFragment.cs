namespace Pz.Connector.DeltaLake;

/// <summary>What DuckDB's delta extension actually accepts as named arguments to delta_scan, against
/// the pinned DuckDB. These are OBSERVATIONS, pinned by DeltaScanParameterProbeTests — a DuckDB bump
/// that changes the surface fails that test rather than a user's run. Set each flag to what the probe
/// reported; do not set one hopefully.</summary>
internal static class DeltaScanFragment
{
    // Rejected against a real DuckDB 1.5.5 + delta extension: "pushdown_filters => true" fails with
    // "Invalid Input Error: Unknown Filter pushdown mode: true". The parameter name IS recognized (the
    // error names a specific mode, not "unrecognized named parameter"), but no value tried — 'true',
    // 'false', 'constant', 'dynamic', 'random', the bare integer 1 — was accepted as a valid mode, so
    // there is no known-good spelling to emit. Filter pushdown still happens; it just is not something
    // this fragment can request explicitly.
    public const bool SupportsPushdownFilters = false;

    // Accepted against the same DuckDB 1.5.5 + delta extension: "union_by_name => true" scans without
    // error and returns the expected row count.
    public const bool SupportsUnionByName = true;
}
