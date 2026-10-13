using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>What a delta-rs write reports to pz's per-node remote stats.
///
/// Only WRITES report. A read is a DuckDB <c>delta_scan</c> that pz runs itself, so no byte of it
/// passes through this process and there is nothing here to count; a read node carries no remote
/// stats by construction, not by omission.
///
/// Each mutating delta-rs call (insert, merge, delete) is one request AND, when it succeeds, one
/// commit in the table's log. The call runs on the <see cref="DeltaBigStack"/> thread, and the
/// ambient <see cref="ConnectorStats.Current"/> belongs to the caller's async flow, so the handle is
/// captured by the caller before the hop and reported into explicitly from inside it.</summary>
internal static class DeltaTelemetry
{
    internal const string CommitsExtra = "deltalake.commits";

    /// <summary>Runs <paramref name="work"/> on the big stack and reports it into
    /// <paramref name="stats"/>. A call that fails still counts as a request; only a call that
    /// returns counts as a commit.</summary>
    internal static async Task RunWriteAsync(IConnectorStats stats, long payloadBytes, Func<Task> work)
    {
        await DeltaBigStack.RunAsync(async () =>
        {
            // No ConfigureAwait(false) on the await below: DeltaBigStack.RunAsync documents that a
            // delegate's own awaits must stay on its pump.
            var succeeded = false;
            try
            {
                await work();
                succeeded = true;
            }
            finally
            {
                stats.AddRequests(1);
                stats.AddWireBytes(payloadBytes);
                if (succeeded)
                {
                    stats.AddExtra(CommitsExtra, 1);
                }
            }
        }).ConfigureAwait(false);
    }

    /// <summary>The Arrow payload size of a write: the sum of every buffer length, children included.
    /// Null buffers and children are what Apache.Arrow leaves on arrays that have none.</summary>
    internal static long PayloadBytes(IEnumerable<RecordBatch> batches) =>
        batches.Sum(b => b.Arrays.Sum(a => ArrayBytes(a.Data)));

    private static long ArrayBytes(ArrayData data)
    {
        var total = data.Buffers is null ? 0L : data.Buffers.Sum(b => (long)b.Length);
        if (data.Children is not null)
        {
            total += data.Children.Where(c => c is not null).Sum(ArrayBytes);
        }

        return total;
    }
}
