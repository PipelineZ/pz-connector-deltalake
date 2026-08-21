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
    ITable table, Schema schema, IReadOnlyList<string> targetColumns,
    IReadOnlyList<string> tablePartitions, DeltaWriteOptions options, string output) : ISinkWriteSession
{
    private readonly List<RecordBatch> buffered = [];
    private long bufferedBytes;
    private long rows;
    private long batches;
    private bool terminated;

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        this.ThrowIfTerminated();
        RefuseEmptyPartitionValues(batch, tablePartitions, output);

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
                    throw DeltaErrors.Fail(DeltaErrors.InvalidWriteOption,
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
        // MaxRowsPerGroup is left at DeltaLake.Net's own default: setting it was measured to change
        // nothing about the parquet the writer emits, so this connector does not expose a knob for it
        // (see DeltaLakeSchemas.WriteOptions).
        var insert = new InsertOptions { SaveMode = mode };

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

    /// <summary>The most recent statement this session handed to delta-rs, and the reason no partition
    /// predicate was derived for it — both are TEST SEAMS, and only that.
    ///
    /// <see cref="LastMergeSql"/> carries partition LITERALS, which are user data, so it must never
    /// reach an event, an exception message, a log line or a run artifact; nothing in this assembly
    /// reads it, and nothing may start to.
    ///
    /// <see cref="LastSkipReason"/> is the opposite by construction — the deriver builds it from column
    /// names and character classes and never from a value — so it WOULD be safe to surface as the
    /// explanation for a merge that scanned more than it had to. There is nowhere to surface it:
    /// connector ABI 0.2.2 gives a sink no note, warning or logger channel, and WriteResult carries only
    /// a row and a batch count. Until the ABI grows one additively, a skipped derivation is invisible to
    /// the user, which costs less than it sounds — the derived predicate measures as worth nothing
    /// (docs/reference/write.md), so what a skip loses is a hedge rather than the speed itself.</summary>
    internal string? LastMergeSql { get; private set; }

    internal string? LastSkipReason { get; private set; }

    /// <summary>One MERGE against the whole buffer. Merge semantics are defined over the entire input —
    /// a key that appears in a later batch has to be able to match a row an earlier batch inserted — so
    /// there is no partial merge to flush early and the buffer IS the write.
    ///
    /// A HANG IN HERE CANNOT BE CANCELLED, and that is a documented limit rather than an oversight.
    /// A malformed predicate used to abort delta-rs inside native Rust and this call then never
    /// returned: measured at 150s, process alive, 0% CPU, flat RSS. The vocabulary allowlist in
    /// <see cref="DeltaMergeSql"/> is what removes that input shape; a timeout here would not, and
    /// would make things worse. The batches below are pinned native memory the Rust side is reading:
    /// returning early on a timer means <c>Clear()</c> disposes them while that read is still in
    /// flight, trading a stopped run for a use-after-free — and the native thread would go on holding
    /// the table handle either way, because a synchronous FFI call in progress cannot be interrupted
    /// from managed code. So the guard is upstream, in what is allowed to reach this statement, and a
    /// merge that hangs is a bug to be reported with the predicate that provoked it.</summary>
    private async Task MergeAsync(CancellationToken ct)
    {
        if (this.buffered.Count == 0)
        {
            // Nothing to merge against, and no statement to build: an empty batch collection would
            // make delta-rs plan a merge with no source rows, which can only be a no-op. Skipping it
            // keeps the transaction log free of a commit that changed nothing.
            return;
        }

        this.RefuseUnmatchableKeys();

        // Safe to hand the buffer straight to the deriver: these are the session's own CLONES, not the
        // engine-owned batches WriteBatchAsync was called with.
        var derivation = DeltaPartitionPredicate.Derive(this.buffered, options);
        this.LastSkipReason = derivation.SkipReason;

        var sql = DeltaMergeSql.Build(schema, targetColumns, options, derivation.Filters);
        this.LastMergeSql = sql;

        var payload = this.buffered.ToArray();
        try
        {
            await DeltaBigStack.RunAsync(() => table.MergeAsync(sql, payload, schema, ct)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw DeltaErrors.Translate(ex, $"merge of output '{output}'", options.Keys);
        }
    }

    /// <summary>Refuses a merge whose keys hold a NULL, rather than letting it report success over a
    /// duplicated row. Measured against real delta-rs on a real table: the ON clause is
    /// <c>target.k = source.k</c>, and SQL equality against a null is null, never true — so the incoming
    /// row matches nothing, WHEN NOT MATCHED fires, and a row whose key is already in the table gains a
    /// second copy. Nothing errors, and no row count looks wrong.
    ///
    /// The other value that cannot match — an empty partition value, which Delta stores as a null — is
    /// not checked here. It is refused for every strategy, per batch, in
    /// <see cref="RefuseEmptyPartitionValues"/>, so it cannot reach a merge commit; catching it a second
    /// time here would only mean two messages for one cause.
    ///
    /// This check is bounded: nulls go through Arrow's own NullCount, so a key column with none costs
    /// one read for the whole batch. The message names columns, never values. It sees only the buffer,
    /// not the target table — a null key already sitting in the table that no incoming row touches is
    /// outside its reach, and reading the whole target on every merge would cost more than the operation
    /// it protects.</summary>
    private void RefuseUnmatchableKeys()
    {
        var nullable = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var batch in this.buffered)
        {
            foreach (var key in options.Keys)
            {
                // Ordinal, and it has to stay Ordinal: Delta column names are case-sensitive. A key
                // naming no column of the batch was already refused at BeginWriteAsync.
                var index = batch.Schema.GetFieldIndex(key, StringComparer.Ordinal);
                if (index >= 0 && batch.Column(index).NullCount > 0)
                {
                    nullable.Add(key);
                }
            }
        }

        if (nullable.Count == 0)
        {
            return;
        }

        throw DeltaErrors.Fail(DeltaErrors.UnmatchableMergeKey,
            $"output '{output}': merge key column(s) " +
            $"{string.Join(", ", nullable.Select(c => $"'{c}'"))} contain null values, and a null never " +
            "equals anything — those rows would be inserted a second time instead of updating the rows " +
            "they belong to",
            "filter or coalesce those columns in the pipeline SQL so every key value is present, or " +
            "choose keys that are");
    }

    /// <summary>Refuses a batch carrying an empty value in a partition column, on EVERY strategy.
    ///
    /// Measured against real delta-rs: Delta writes a partition value into a directory name, and an
    /// empty value produces the directory <c>col=</c>, which it cannot tell from a null. On a NULLABLE
    /// partition column the write therefore SUCCEEDS and the row reads back with null in that column —
    /// a silent change to the user's data, on the plainest append there is, with no error anywhere. On a
    /// NOT NULL one delta-rs fails the insert instead, but only after the table exists and after any
    /// generation an append already flushed has committed.
    ///
    /// So it is checked per batch, before the batch is buffered, rather than at commit: an append
    /// flushes bounded generations, and a generation is a commit that cannot be unwound.
    ///
    /// The authority is the TABLE's partition columns, not <c>partition_by</c>. A run against a table an
    /// earlier run partitioned need not declare partition_by at all, so the option is not what decides
    /// which columns become directory names.
    ///
    /// BINARY columns are covered as well as string ones, and that is measured, not defensive: a
    /// nullable binary partition column holding zero bytes reads back null exactly as an empty string
    /// does. Every string encoding derives from one of the three binary array types below, so those
    /// three cover all six. Fixed-width types have no empty value and are not walked.</summary>
    private static void RefuseEmptyPartitionValues(
        RecordBatch batch, IReadOnlyList<string> partitions, string output)
    {
        if (partitions.Count == 0)
        {
            return;
        }

        var offenders = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var column in partitions)
        {
            // Ordinal, and it has to stay Ordinal: Delta column names are case-sensitive. A partition
            // column the batch does not carry is Reconcile's problem, not this one.
            var index = batch.Schema.GetFieldIndex(column, StringComparer.Ordinal);
            if (index >= 0 && HasEmptyValue(batch.Column(index)))
            {
                offenders.Add(column);
            }
        }

        if (offenders.Count == 0)
        {
            return;
        }

        // Every offending column at once: a user fixing one column per run is a user running the
        // pipeline once per column. The message names columns and never a value.
        throw DeltaErrors.Fail(DeltaErrors.UnusablePartitionValue,
            $"output '{output}': partition column(s) " +
            $"{string.Join(", ", offenders.Select(c => $"'{c}'"))} contain an empty value. Delta writes " +
            "a partition value into a directory name, where an empty value is indistinguishable from a " +
            "null, so those rows would read back as null in that column — and a merge keyed on such a " +
            "column would insert a second copy of them on every run",
            "filter those rows out in the pipeline SQL, coalesce the column to a non-empty placeholder, " +
            "or partition by a column that is never empty");
    }

    /// <summary>Whether any non-null row of a variable-length column holds a zero-length value. The
    /// length comes from the offsets rather than from a materialized value, so this costs no allocation
    /// per row — it runs on every batch of every partitioned write.
    ///
    /// Nulls are skipped, and they have to be: measured, <c>GetValueLength</c> reports 0 for a null
    /// entry in every one of these encodings, so an unguarded length test would report a legitimately
    /// null partition value as an empty one. A null written is a null read back; only an empty value
    /// changes on the way through.</summary>
    private static bool HasEmptyValue(IArrowArray array)
    {
        // StringArray/LargeStringArray/StringViewArray derive from these three, so the three cases
        // cover all six encodings a batch may arrive in — which one it is depends on the plan that
        // produced the batch, not on the column's Delta type.
        Func<int, int>? length = array switch
        {
            BinaryArray a => a.GetValueLength,
            LargeBinaryArray a => a.GetValueLength,
            BinaryViewArray a => a.GetValueLength,
            _ => null,
        };

        if (length is null)
        {
            return false;
        }

        for (var row = 0; row < array.Length; row++)
        {
            if (!array.IsNull(row) && length(row) == 0)
            {
                return true;
            }
        }

        return false;
    }

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
