using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>The read half: a DuckDB delta_scan and nothing else. Reads deliberately do not go through
/// delta-rs — routing them there would pull Arrow batches into .NET only to hand them straight back to
/// DuckDB, and DuckDB's own delta reader already does projection and predicate pushdown against the
/// transaction log's file statistics. DuckDB owns the read data plane; delta-rs owns writes and table
/// metadata.
///
/// No <c>pushdown_filters =&gt;</c> argument is ever emitted: confirmed against a real DuckDB 1.5.5 +
/// delta extension (<see cref="DeltaScanFragment"/>), the parameter name is recognized but every value
/// tried (boolean and string) is rejected as an "Unknown Filter pushdown mode" — there is no known-good
/// spelling, so the fragment omits it rather than guess one that fails at scan time against real
/// data.</summary>
internal sealed class DeltaLakeSource(ConnectorConfig config) : ISource
{
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        throw new NotImplementedException();

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        var root = config.GetString("root") ?? string.Empty;
        var location = DeltaLocation.Resolve(root, spec.Dataset, Option(spec, "path"));

        var args = new List<string> { $"'{location.Replace("'", "''")}'" };
        if (Option(spec, "version") is { } version)
        {
            // Schema validation (DeltaLakeSchemas.Dataset) checks "integer, minimum 0" but declares no
            // maximum, so a JSON-valid integer wider than Int64 reaches here -- an unguarded long.Parse
            // would throw OverflowException/FormatException, which the planner's TryGetNativeScan catch
            // (Pz.Engine ExecutionPlanner) only maps to a coded error for PzConnectorException. Reusing
            // VersionNotFound: a version value that cannot even be represented as a Delta version number
            // can, by definition, never be found in the table's history -- same user-facing outcome as
            // the design's other VersionNotFound use (a version absent from the transaction log), just
            // caught one step earlier.
            if (!long.TryParse(version, NumberStyles.Integer, CultureInfo.InvariantCulture, out var versionNumber))
            {
                throw DeltaErrors.Fail(DeltaErrors.VersionNotFound,
                    $"dataset '{spec.Dataset}': 'version' value '{version}' is not a valid delta table version",
                    "set 'version' to a non-negative integer that fits in a 64-bit signed integer");
            }

            args.Add($"version => {versionNumber.ToString(CultureInfo.InvariantCulture)}");
        }

        if (DeltaScanFragment.SupportsUnionByName && OptionBool(spec, "union_by_name"))
        {
            args.Add("union_by_name => true");
        }

        var fragment = $"delta_scan({string.Join(", ", args)})";

        scan = new NativeScan(DeltaWindowSql.Wrap(fragment, spec), DeltaSecretSql.SetupStatements(config, spec.Source))
        {
            // Delta declares its own schema in the transaction log, so nothing here is invented.
            SchemaInferred = false,
            SniffFragment = null,
            Mechanism = "delta_scan",
        };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(
        DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': deltalake reads are native-scan only; they cannot run on " +
            "the universal tier (remove engine.force_universal / files_per_partition)", isTransient: false);

    public ValueTask DisposeAsync() => default;

    private static string? Option(DatasetSpec spec, string key) =>
        spec.Options.TryGetValue(key, out var v) ? v?.ToString() : null;

    private static bool OptionBool(DatasetSpec spec, string key) =>
        spec.Options.TryGetValue(key, out var v) && v is not null && Convert.ToBoolean(v, CultureInfo.InvariantCulture);
}
