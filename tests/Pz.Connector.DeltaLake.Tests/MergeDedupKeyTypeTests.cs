using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>Every key type the duplicate-key guard ADMITS must actually resolve. The suite next door
/// drives the resolver through the sink, which reaches only the types a real write produces; this one
/// drives it directly, so a type the guard admits and the resolver cannot read fails here instead of
/// surfacing later as an uncoded NotSupportedException in the middle of a merge.
///
/// The variable-width binary family is why this file exists. BinaryArray, LargeBinaryArray,
/// BinaryViewArray and FixedSizeBinaryArray each derive straight from Apache.Arrow's Array — four
/// unrelated classes, not one hierarchy — so an arm written for one covers none of the others, and the
/// compiler cannot say so. The mismatch was invisible for a second reason: delta-rs maps every binary
/// width to Delta 'binary', so a write declaring one is refused at reconcile with PZDL0301 before a
/// merge can run. Latent rather than absent is exactly the kind that becomes live later.</summary>
public class MergeDedupKeyTypeTests
{
    private static readonly byte[] First = [0x01, 0x02];
    private static readonly byte[] Second = [0x03, 0x04];

    public static TheoryData<string, IArrowType, IArrowArray> BinaryKeyColumns()
    {
        // Three rows per column, keyed First / Second / First: rows 0 and 2 repeat. A resolver that
        // read the column correctly keeps two; one that compared boxed byte arrays by reference would
        // keep three, and one with no arm at all would throw.
        var binary = new BinaryArray.Builder();
        var large = new LargeBinaryArray.Builder();
        var view = new BinaryViewArray.Builder();
        foreach (var value in new[] { First, Second, First })
        {
            binary.Append(value.AsSpan());
            large.Append(value.AsSpan());
            view.Append(value.AsSpan());
        }

        return new TheoryData<string, IArrowType, IArrowArray>
        {
            { "binary", BinaryType.Default, binary.Build() },
            { "large_binary", LargeBinaryType.Default, large.Build() },
            { "binary_view", BinaryViewType.Default, view.Build() },
            { "fixed_size_binary", new FixedSizeBinaryType(2), FixedSizeBinary() },
        };
    }

    [Theory]
    [MemberData(nameof(BinaryKeyColumns))]
    public void An_admitted_binary_key_type_resolves_a_repeat(string name, IArrowType type, IArrowArray column)
    {
        var schema = new Schema([new Field("k", type, nullable: false)], null);

        // Admitted by the guard: AssertResolvableKeys returning is the guard saying it can compare
        // this type, which is the promise the resolver below has to keep.
        DeltaMergeDedup.AssertResolvableKeys(schema, ["k"], "out");

        var batch = new RecordBatch(schema, [column], 3);
        var resolved = DeltaMergeDedup.LastWriterWins([batch], ["k"]);

        Assert.True(resolved.Sum(b => b.Length) == 2,
            $"{name}: expected the repeated key to resolve to one row, leaving 2 of 3");

        // Rebuilt, not the buffer handed straight back: reaching the right count while returning the
        // input unchanged would mean the repeat was never recognised at all.
        Assert.DoesNotContain(batch, resolved);
    }

    /// <summary>The guard's own refusal, on the family it does NOT admit — the other half of the
    /// agreement. A type that reaches the resolver unrefused is the defect; a type refused here can
    /// never reach it.</summary>
    [Fact]
    public void A_nested_key_type_is_refused_by_the_guard_rather_than_reaching_the_resolver()
    {
        var schema = new Schema([new Field("k", new ListType(Int64Type.Default), nullable: false)], null);

        var ex = Assert.Throws<PzConnectorException>(
            () => DeltaMergeDedup.AssertResolvableKeys(schema, ["k"], "out"));

        Assert.Contains(DeltaErrors.UnresolvableMergeKeyType, ex.Message);
    }

    /// <summary>Apache.Arrow 23.0.0 ships no concrete <c>FixedSizeBinaryArray.Builder</c> — only an
    /// abstract BuilderBase — so the array is assembled from its buffers directly. Two bytes per
    /// value, no validity buffer needed because no value is null.</summary>
    private static IArrowArray FixedSizeBinary()
    {
        var values = new ArrowBuffer.Builder<byte>();
        foreach (var value in new[] { First, Second, First })
        {
            values.Append(value.AsSpan());
        }

        return new FixedSizeBinaryArray(new ArrayData(
            new FixedSizeBinaryType(2), length: 3, nullCount: 0, offset: 0,
            buffers: [ArrowBuffer.Empty, values.Build()]));
    }
}
