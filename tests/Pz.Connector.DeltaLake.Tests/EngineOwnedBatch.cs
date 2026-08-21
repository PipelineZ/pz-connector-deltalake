using System.Buffers;
using System.Runtime.InteropServices;
using Apache.Arrow;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>Models the ABI's batch-ownership rule literally so a test can catch a session that breaks
/// it. The batch handed to <c>WriteBatchAsync</c> is engine-owned and may be backed by pooled
/// off-heap memory that a LATER, unrelated batch reuses once the engine is done with it;
/// <see cref="Recycle"/> performs that reuse, overwriting the buffers in place with a second batch's
/// bytes. A session that retained the instance instead of cloning therefore commits the recycled
/// values and fails the assertion on the committed rows; a session that cloned is untouched.
///
/// Recycling is the whole mechanism, and the calling test deliberately does NOT dispose the batch
/// before committing. Both halves were measured against a session rewritten to retain the instance:
/// disposing first makes delta-rs read memory Apache.Arrow has already released and the TEST HOST dies
/// with an AccessViolationException inside table_insert, aborting the entire run instead of failing
/// one assertion; recycling without disposing turns the same bug into a readable assertion failure on
/// the committed values. Disposing ALONE discriminates nothing at all — Apache.Arrow's dispose neither
/// zeroes nor poisons anything, so a retaining session can still commit the right numbers.</summary>
internal sealed class EngineOwnedBatch : IDisposable
{
    private readonly RecordBatch replacement;

    public EngineOwnedBatch(RecordBatch batch, RecordBatch replacement)
    {
        this.Batch = batch;
        this.replacement = replacement;
    }

    public RecordBatch Batch { get; }

    /// <summary>Overwrites every buffer of <see cref="Batch"/> with the corresponding bytes of the
    /// replacement batch. Any structural difference between the two throws rather than silently
    /// copying nothing — a recycle that quietly did nothing would make the calling test hollow.</summary>
    public void Recycle()
    {
        if (this.Batch.ColumnCount != this.replacement.ColumnCount)
        {
            throw new InvalidOperationException("the recycled batch must have the same columns as the original.");
        }

        for (var i = 0; i < this.Batch.ColumnCount; i++)
        {
            RecycleData(this.Batch.Column(i).Data, this.replacement.Column(i).Data);
        }
    }

    public void Dispose()
    {
        this.Batch.Dispose();
        this.replacement.Dispose();
    }

    private static void RecycleData(ArrayData target, ArrayData source)
    {
        // Apache.Arrow leaves Buffers/Children null rather than empty on arrays that have neither.
        var targetBuffers = target.Buffers ?? [];
        var sourceBuffers = source.Buffers ?? [];
        var targetChildren = target.Children ?? [];
        var sourceChildren = source.Children ?? [];
        if (targetBuffers.Length != sourceBuffers.Length || targetChildren.Length != sourceChildren.Length)
        {
            throw new InvalidOperationException("the recycled batch must have the same buffer layout as the original.");
        }

        for (var i = 0; i < targetBuffers.Length; i++)
        {
            if (targetBuffers[i].Length != sourceBuffers[i].Length)
            {
                throw new InvalidOperationException(
                    "the recycled batch must have the same buffer lengths as the original.");
            }

            sourceBuffers[i].Span.CopyTo(Writable(targetBuffers[i]));
        }

        for (var i = 0; i < targetChildren.Length; i++)
        {
            RecycleData(targetChildren[i], sourceChildren[i]);
        }
    }

    private static Span<byte> Writable(ArrowBuffer buffer)
    {
        if (buffer.Length == 0)
        {
            return Span<byte>.Empty;
        }

        // Arrow's default allocator hands out native memory through a MemoryManager, so that case comes
        // first; the array case covers a buffer built over a managed array. Neither succeeding means
        // the probe cannot reach the memory, which must fail loudly rather than skip the overwrite.
        if (MemoryMarshal.TryGetMemoryManager<byte, MemoryManager<byte>>(
                buffer.Memory, out var manager, out var start, out var length))
        {
            return manager.Memory.Span.Slice(start, length);
        }

        if (MemoryMarshal.TryGetArray(buffer.Memory, out var segment) && segment.Array is not null)
        {
            return segment.Array.AsSpan(segment.Offset, segment.Count);
        }

        throw new InvalidOperationException("this buffer's memory cannot be reached to recycle it.");
    }
}
