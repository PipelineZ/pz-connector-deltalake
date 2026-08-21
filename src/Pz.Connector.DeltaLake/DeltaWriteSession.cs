using Apache.Arrow;
using DeltaLake.Interfaces;
using DeltaLake.Table;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>One output's write. Batches are CLONED on the way in: delta-rs write entry points take a
/// collection, so the session must retain batches past WriteBatchAsync — and the batch handed in is
/// engine-owned, backed by pooled native memory that may be recycled the moment the call returns.
/// Retaining the instance instead of a clone is the worst bug available in this file.
///
/// An append flushes in bounded generations so memory tracks a file rather than the whole write.
/// Replace and merge cannot: their semantics are defined over the entire input, so for those the
/// buffer IS the write.</summary>
internal sealed class DeltaWriteSession(
    ITable table, Schema schema, DeltaWriteOptions options, string output) : ISinkWriteSession
{
    private readonly List<RecordBatch> buffered = [];
    private long bufferedBytes;
    private long rows;
    private long batches;
    private bool terminated;

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        this.ThrowIfTerminated();

        var clone = batch.Clone();
        this.buffered.Add(clone);
        this.bufferedBytes += EstimateBytes(clone);
        this.rows += batch.Length;
        this.batches++;

        if (options.Mode == "append" && this.bufferedBytes >= options.TargetFileBytes)
        {
            await this.FlushAsync(SaveMode.Append, ct).ConfigureAwait(false);
        }
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        this.ThrowIfTerminated();
        this.terminated = true;

        try
        {
            switch (options.Mode)
            {
                case "append":
                    await this.FlushAsync(SaveMode.Append, ct).ConfigureAwait(false);
                    break;

                case "replace":
                    // Overwrite is one atomic commit against the whole buffer; there is no partial
                    // replace to flush early.
                    await this.ReplaceAsync(ct).ConfigureAwait(false);
                    break;

                case "merge":
                    await this.MergeAsync(ct).ConfigureAwait(false);
                    break;

                default:
                    throw DeltaErrors.Fail(DeltaErrors.WriteFailed,
                        $"output '{output}': unsupported write strategy '{options.Mode}'",
                        "use strategy: append, replace, or merge");
            }
        }
        finally
        {
            this.Clear();
        }

        return new WriteResult(this.rows, this.batches);
    }

    public ValueTask AbortAsync(CancellationToken ct)
    {
        this.ThrowIfTerminated();
        this.terminated = true;
        // Generations an append already flushed are committed and are NOT rolled back — which is
        // exactly why this sink declares AbortSemantics.BestEffort.
        this.Clear();
        return default;
    }

    public async ValueTask DisposeAsync()
    {
        this.Clear();

        // Dispose() releases a native table handle through delta-rs, so it runs on the big stack like
        // every other call into that library.
        await DeltaBigStack.RunAsync(() =>
        {
            // ITable is not documented as IDisposable in the 0.33.0 API docs, so the interface is
            // tested rather than assumed.
            if (table is IDisposable disposable)
            {
                disposable.Dispose();
            }

            return Task.CompletedTask;
        }).ConfigureAwait(false);
    }

    /// <summary>Approximate in-memory size, used only to decide when to flush a generation. Exactness
    /// does not matter; monotonicity does — which is why child buffers count too, or a batch of nested
    /// columns would measure as nearly nothing and never trigger a flush.</summary>
    private static long EstimateBytes(RecordBatch batch) => batch.Arrays.Sum(a => EstimateBytes(a.Data));

    private static long EstimateBytes(ArrayData data)
    {
        // Apache.Arrow leaves Buffers/Children null rather than empty on arrays that have neither, so
        // both are null-checked: an unguarded Sum() throws on every flat column there is.
        var total = data.Buffers is null ? 0L : data.Buffers.Sum(b => (long)b.Length);
        if (data.Children is not null)
        {
            total += data.Children.Where(c => c is not null).Sum(EstimateBytes);
        }

        return total;
    }

    private async Task FlushAsync(SaveMode mode, CancellationToken ct)
    {
        if (this.buffered.Count == 0)
        {
            return;
        }

        var payload = this.buffered.ToArray();
        // MaxRowsPerGroup is init-only, and leaving it unset is not the same as setting it: an
        // unconfigured output must keep whatever default DeltaLake.Net applies, so the two shapes are
        // constructed separately rather than assigned after the fact.
        var insert = options.MaxRowsPerGroup is { } maxRows
            ? new InsertOptions { SaveMode = mode, MaxRowsPerGroup = (ulong)maxRows }
            : new InsertOptions { SaveMode = mode };

        try
        {
            await DeltaBigStack.RunAsync(() => table.InsertAsync(payload, schema, insert, ct)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw DeltaErrors.Translate(ex, $"{options.Mode} of output '{output}'", options.Keys);
        }
        finally
        {
            this.Clear();
        }
    }

    /// <summary>Replace is one Overwrite commit — except when the write produced nothing. delta-rs's
    /// InsertAsync accepts an EMPTY batch collection with SaveMode.Overwrite, returns successfully, and
    /// changes nothing at all, so a replace with no rows would silently leave the previous run's data
    /// in place. Deleting every row instead is the only shape that keeps "replace" meaning what it
    /// says, and it is still one commit.</summary>
    private async Task ReplaceAsync(CancellationToken ct)
    {
        if (this.buffered.Count > 0)
        {
            await this.FlushAsync(SaveMode.Overwrite, ct).ConfigureAwait(false);
            return;
        }

        try
        {
            await DeltaBigStack.RunAsync(() => table.DeleteAsync(ct)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw DeltaErrors.Translate(ex, $"{options.Mode} of output '{output}'", options.Keys);
        }
    }

    private Task MergeAsync(CancellationToken ct) => throw new NotImplementedException();

    private void Clear()
    {
        foreach (var b in this.buffered)
        {
            b.Dispose();
        }

        this.buffered.Clear();
        this.bufferedBytes = 0;
    }

    private void ThrowIfTerminated()
    {
        if (this.terminated)
        {
            throw new InvalidOperationException(
                $"output '{output}': this write session has already been committed or aborted");
        }
    }
}
