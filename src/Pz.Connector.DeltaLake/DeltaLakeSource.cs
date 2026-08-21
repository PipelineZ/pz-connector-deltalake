using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using DeltaLake.Table;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>The read half. Data always comes from a DuckDB delta_scan and never enters .NET — routing
/// it through delta-rs would pull Arrow batches into .NET only to hand them straight back to DuckDB,
/// and DuckDB's own delta reader already does projection and predicate pushdown against the
/// transaction log's file statistics. But the DAG needs the table's declared schema before a run can
/// even be planned, and that is metadata DuckDB's delta extension has no way to hand back ahead of a
/// scan — so <see cref="GetSchemaAsync"/> is the one place this source calls delta-rs instead. DuckDB
/// owns the read data plane; delta-rs owns writes and table metadata.
///
/// No <c>pushdown_filters =&gt;</c> argument is ever emitted: confirmed against a real DuckDB 1.5.5 +
/// delta extension (<see cref="DeltaScanFragment"/>), the parameter name is recognized but every value
/// tried (boolean and string) is rejected as an "Unknown Filter pushdown mode" — there is no known-good
/// spelling, so the fragment omits it rather than guess one that fails at scan time against real
/// data.</summary>
internal sealed class DeltaLakeSource(ConnectorConfig config) : ISource
{
    // Lazy<Task<T>> rather than Lazy<T>: construction itself is a delta-rs call and so must run on
    // DeltaBigStack's oversized stack like every other one, but Lazy<T>'s factory has to be
    // synchronous. Wrapping the Task, not the engine, keeps construction on the big-stack thread while
    // still amortizing it to one attempt under concurrent first access (default ExecutionAndPublication
    // thread-safety: every caller observes the same in-flight Task).
    private readonly Lazy<Task<DeltaEngine>> engine =
        new(() => DeltaBigStack.RunAsync(() => Task.FromResult(new DeltaEngine(EngineOptions.Default))));

    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var root = config.GetString("root") ?? string.Empty;
        var location = DeltaLocation.Resolve(root, spec.Dataset, Option(spec, "path"));
        var version = ParseVersionOption(spec);
        var engine = await this.engine.Value.ConfigureAwait(false);

        var table = await DeltaStorageOptions
            .LoadAsync(engine, location, config, version, spec.Dataset, ct).ConfigureAwait(false);

        // Schema() and Dispose() are delta-rs calls too, so they run on DeltaBigStack's thread just
        // like the load that produced the table -- there is no delta-rs call small enough to risk on a
        // default-sized stack.
        return await DeltaBigStack.RunAsync(() =>
        {
            try
            {
                return Task.FromResult(new DatasetSchema(table.Schema()));
            }
            finally
            {
                // ITable is not documented as IDisposable in the 0.33.0 API docs, so the interface is
                // tested rather than assumed -- a defensive cast costs nothing if the assumption is
                // wrong in either direction.
                if (table is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
        }).ConfigureAwait(false);
    }

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        var root = config.GetString("root") ?? string.Empty;
        var location = DeltaLocation.Resolve(root, spec.Dataset, Option(spec, "path"));

        var args = new List<string> { $"'{location.Replace("'", "''")}'" };
        if (ParseVersionOption(spec) is { } versionNumber)
        {
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

    public async ValueTask DisposeAsync()
    {
        if (!this.engine.IsValueCreated)
        {
            return;
        }

        DeltaEngine engine;
        try
        {
            engine = await this.engine.Value.ConfigureAwait(false);
        }
        catch
        {
            // Construction itself never completed (the failure already surfaced to whichever
            // GetSchemaAsync call awaited it first), so there is no engine here to release.
            return;
        }

        await DeltaBigStack.RunAsync(() =>
        {
            engine.Dispose();
            return Task.CompletedTask;
        }).ConfigureAwait(false);
    }

    // Schema validation (DeltaLakeSchemas.Dataset) checks "integer, minimum 0" but declares no
    // maximum, so a JSON-valid integer wider than Int64 reaches here -- an unguarded long.Parse would
    // throw OverflowException/FormatException instead of a coded error. Reusing VersionNotFound: a
    // version value that cannot even be represented as a Delta version number can, by definition,
    // never be found in the table's history -- same user-facing outcome as the design's other
    // VersionNotFound use (a version absent from the transaction log), just caught one step earlier.
    // Shared by both the native-scan path and the schema probe so an invalid 'version' fails the same
    // way regardless of which one runs first.
    private static long? ParseVersionOption(DatasetSpec spec)
    {
        if (Option(spec, "version") is not { } version)
        {
            return null;
        }

        // versionNumber < 0 is rejected here, not left for delta-rs to reject: TableOptions.Version is
        // ulong, so an unchecked negative would wrap into a huge, meaningless version number at the FFI
        // boundary (DeltaStorageOptions.LoadAsync), and DuckDB's delta_scan would receive a literal
        // "version => -1" it has no sensible reading for either. Rejecting it here, where the message
        // already promises "non-negative", makes both consumers provably never see one.
        if (!long.TryParse(version, NumberStyles.Integer, CultureInfo.InvariantCulture, out var versionNumber) ||
            versionNumber < 0)
        {
            throw DeltaErrors.Fail(DeltaErrors.VersionNotFound,
                $"dataset '{spec.Dataset}': 'version' value '{version}' is not a valid delta table version",
                "set 'version' to a non-negative integer that fits in a 64-bit signed integer");
        }

        return versionNumber;
    }

    private static string? Option(DatasetSpec spec, string key) =>
        spec.Options.TryGetValue(key, out var v) ? v?.ToString() : null;

    private static bool OptionBool(DatasetSpec spec, string key) =>
        spec.Options.TryGetValue(key, out var v) && v is not null && Convert.ToBoolean(v, CultureInfo.InvariantCulture);
}
