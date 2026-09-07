using Pz.Connectors.Abstractions;


namespace Pz.Connector.DeltaLake;

/// <summary>Delta Lake source + sink connector. Reads are a DuckDB <c>delta_scan</c> native scan and
/// never enter .NET; writes go through delta-rs on the universal Arrow path, because DuckDB's delta
/// extension cannot write. One capability union covers both directions — the ABI has a single
/// <see cref="ConnectorCapabilities"/> value per connector, not one per direction.</summary>
public sealed class DeltaLakeConnector : ISourceConnector, ISinkConnector, INativeOnlySource
{
    public ConnectorInfo Info => new("deltalake", "0.1.0", ProtocolVersion.Major);

    /// <summary>ColumnPartitionedWrites is what a Delta table's partitioning actually is: the columns
    /// are recorded in the table's own metadata and there is no path to route rows into. PathTemplating
    /// is deliberately NOT declared — it means the connector renders pz's calendar tokens into a path,
    /// which nothing here does in either direction. Validation rejects calendar tokens in a read
    /// <c>path:</c> for the same reason, so neither flag can become a silent no-op.</summary>
    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.ColumnPruning |
        ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.BoundedWindow |
        ConnectorCapabilities.InclusiveWatermarkBound | ConnectorCapabilities.Merge |
        ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Transactional |
        ConnectorCapabilities.ColumnPartitionedWrites;

    public string ConnectionConfigSchema => DeltaLakeSchemas.Connection;

    public string DatasetConfigSchema => DeltaLakeSchemas.Dataset;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = DeltaLakeValidation.Check(config);
        return new ValueTask<ValidationResult>(
            errors.Count == 0 ? ValidationResult.Success : new ValidationResult(errors));
    }

    /// <summary>No deep probe: opening a Delta table is the connectivity test, and it happens at run
    /// time with a PZDL-coded failure that says more than a generic reachability check would.</summary>
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true,
            "deltalake connectivity is verified at run time when the table is opened"));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new DeltaLakeSource(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new DeltaLakeSink(config));
}
