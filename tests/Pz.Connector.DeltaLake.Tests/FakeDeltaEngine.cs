using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DeltaLake.Interfaces;
using DeltaLake.Kernel.Core;
using DeltaLake.Table;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>A configurable <see cref="IEngine"/> test double: tests that need to control exactly what
/// LoadTableAsync/CreateTableAsync return or throw, without touching real delta-rs FFI, set the
/// relevant delegate; calling an unconfigured member throws so a test that reaches a member it never
/// meant to exercise fails loudly instead of quietly returning a default. Reusable across the read-path
/// tests here and the write-path tests that need the same seam.</summary>
internal sealed class FakeDeltaEngine : IEngine
{
    public Func<TableOptions, CancellationToken, Task<ITable>> OnLoadTableAsync { get; set; } =
        (_, _) => throw NotConfigured();

    public Func<TableCreateOptions, CancellationToken, Task<ITable>> OnCreateTableAsync { get; set; } =
        (_, _) => throw NotConfigured();

    public Task<ITable> LoadTableAsync(TableOptions options, CancellationToken ct) =>
        this.OnLoadTableAsync(options, ct);

    public Task<ITable> CreateTableAsync(TableCreateOptions options, CancellationToken ct) =>
        this.OnCreateTableAsync(options, ct);

    public void Dispose()
    {
    }

    private static NotSupportedException NotConfigured([CallerMemberName] string member = "") =>
        new($"{nameof(FakeDeltaEngine)}.{member} was not configured for this test.");
}

/// <summary>An <see cref="ITable"/> test double where every one of its 33 members throws by default —
/// deliberately mechanical, since a real test typically exercises one or two of them and the rest exist
/// only because the interface requires an implementation. Override the specific members a test needs
/// via the public delegate properties. <see cref="FilesAsync"/>/<see cref="FileUrisAsync"/> are
/// declared only because <see cref="ITable"/> requires them — this connector's production code never
/// calls either (they corrupt the heap on tables with a few hundred files; see
/// <c>DeltaLakeSource</c>'s doc comment) and no test should configure them either. Reusable across the
/// read-path tests here and the write-path tests that need the same seam.</summary>
internal sealed class FakeDeltaTable : ITable
{
    public Func<Schema> OnSchema { get; set; } = () => throw NotConfigured();

    public Func<ulong, CancellationToken, Task> OnLoadVersionAsync { get; set; } =
        (_, _) => throw NotConfigured();

    public Func<TableMetadata> OnMetadata { get; set; } = () => throw NotConfigured();

    public Action OnDispose { get; set; } = () => { };

    public string Location() => throw NotConfigured();

    public ulong? Version() => throw NotConfigured();

    public Schema Schema() => this.OnSchema();

    public TableMetadata Metadata() => this.OnMetadata();

    public ProtocolInfo ProtocolVersions() => throw NotConfigured();

    public Task<string[]> FilesAsync() => throw NotConfigured();

    public Task<string[]> FileUrisAsync() => throw NotConfigured();

    public Task<CommitInfo[]> HistoryAsync(ulong? limit, CancellationToken ct) => throw NotConfigured();

    public Task LoadVersionAsync(ulong version, CancellationToken ct) => this.OnLoadVersionAsync(version, ct);

    public Task LoadDateTimeAsync(DateTimeOffset timestamp, CancellationToken ct) => throw NotConfigured();

    public Task LoadDateTimeAsync(long timestamp, CancellationToken ct) => throw NotConfigured();

    public Task UpdateIncrementalAsync(long? version, CancellationToken ct) => throw NotConfigured();

    public Task RestoreAsync(RestoreOptions options, CancellationToken ct) => throw NotConfigured();

    public IAsyncEnumerable<RecordBatch> QueryAsync(SelectQuery query, CancellationToken ct) => throw NotConfigured();

    public Task<OwnedArrowTable> ReadAsArrowTableAsync(CancellationToken ct) => throw NotConfigured();

    public Task<OwnedDataFrame> ReadAsDataFrameAsync(CancellationToken ct) => throw NotConfigured();

    public IAsyncEnumerable<RecordBatch> QueryTableChangesAsync(TableChangesOptions options, CancellationToken ct) =>
        throw NotConfigured();

    public Task InsertAsync(
        IReadOnlyCollection<RecordBatch> data, Schema schema, InsertOptions options, CancellationToken ct) =>
        throw NotConfigured();

    public Task InsertAsync(IArrowArrayStream stream, InsertOptions options, CancellationToken ct) =>
        throw NotConfigured();

    public Task MergeAsync(
        string predicate, IReadOnlyCollection<RecordBatch> data, Schema schema, CancellationToken ct) =>
        throw NotConfigured();

    public Task<string> UpdateAsync(string predicate, CancellationToken ct) => throw NotConfigured();

    public Task DeleteAsync(string predicate, CancellationToken ct) => throw NotConfigured();

    public Task DeleteAsync(CancellationToken ct) => throw NotConfigured();

    public Task AddConstraintsAsync(
        IReadOnlyDictionary<string, string> nameToExpression,
        IReadOnlyDictionary<string, string>? nameToDescription,
        CancellationToken ct) =>
        throw NotConfigured();

    public Task AddConstraintsAsync(IReadOnlyDictionary<string, string> nameToExpression, CancellationToken ct) =>
        throw NotConfigured();

    public Task CheckpointAsync(CancellationToken ct) => throw NotConfigured();

    public Task CheckpointAsync(CheckpointOptions options, CancellationToken ct) => throw NotConfigured();

    public Task OptimizeAsync(OptimizeOptions options, CancellationToken ct) => throw NotConfigured();

    public Task VacuumAsync(VacuumOptions options, CancellationToken ct) => throw NotConfigured();

    public Task<long> CreateWriteTransactionAsync(IReadOnlyList<AddAction> actions, CancellationToken ct) =>
        throw NotConfigured();

    public Task<long> CreateWriteTransactionAsync(
        IReadOnlyList<AddAction> actions, CommitOptions options, CancellationToken ct) =>
        throw NotConfigured();

    public Task<long?> GetLatestTransactionVersionAsync(string appId, CancellationToken ct) => throw NotConfigured();

    public void Dispose() => this.OnDispose();

    private static NotSupportedException NotConfigured([CallerMemberName] string member = "") =>
        new($"{nameof(FakeDeltaTable)}.{member} was not configured for this test.");
}
