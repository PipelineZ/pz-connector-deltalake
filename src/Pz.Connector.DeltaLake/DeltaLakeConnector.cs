using Pz.Connectors.Abstractions;

[assembly: PzConnector("deltalake", typeof(Pz.Connector.DeltaLake.DeltaLakeConnector))]

namespace Pz.Connector.DeltaLake;

/// <summary>Delta Lake source + sink connector. Reads are a DuckDB <c>delta_scan</c> native scan and
/// never enter .NET; writes go through delta-rs on the universal Arrow path, because DuckDB's delta
/// extension cannot write. One capability union covers both directions — the ABI has a single
/// <see cref="ConnectorCapabilities"/> value per connector, not one per direction.</summary>
public sealed class DeltaLakeConnector : ISourceConnector, ISinkConnector, INativeOnlySource
{
    public ConnectorInfo Info => new("deltalake", "0.1.0", ProtocolVersion.Major);

    /// <summary>PathTemplating is declared for its SINK meaning only — fanning rows out to partition
    /// buckets via <c>partition_by</c>, which pz refuses with PZ0314 without the flag. Its SOURCE
    /// meaning (calendar-token path pruning) is not implemented: Delta partitions declaratively by
    /// column value, so there is no templated read path. Validation rejects calendar tokens in a read
    /// <c>path:</c> so the flag can never become the silent no-op it exists to prevent.</summary>
    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.ColumnPruning |
        ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.BoundedWindow |
        ConnectorCapabilities.InclusiveWatermarkBound | ConnectorCapabilities.Merge |
        ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Transactional |
        ConnectorCapabilities.PathTemplating;

    public string ConnectionConfigSchema => throw new NotImplementedException();

    public string DatasetConfigSchema => throw new NotImplementedException();

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();
}
