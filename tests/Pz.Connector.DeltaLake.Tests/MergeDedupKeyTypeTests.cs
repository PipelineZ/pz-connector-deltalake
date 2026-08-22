using System.Numerics;
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
/// compiler cannot say so.
///
/// All four reach a real merge. delta-rs maps every binary width to Delta 'binary', and Reconcile
/// compares the write against what Delta STORES rather than against the type offered, so a write
/// declaring any of them is accepted and its key column arrives here in exactly the encoding it was
/// written in. While Reconcile compared the offered type instead, none of them could get past
/// BeginWriteAsync at all and this file was the only thing standing between the resolver and an
/// uncoded NotSupportedException in the middle of a merge.</summary>
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

    /// <summary>A decimal key too large for <see cref="decimal"/> resolves instead of throwing.
    ///
    /// Delta and DataFusion allow decimal(38, s), whose range exceeds System.Decimal's, and
    /// DeltaTypeSupport accepts the type for writes — so this is a live shape, not an exotic one.
    /// Reading it through <c>Decimal128Array.GetValue</c> raises an OverflowException from outside
    /// MergeAsync's try block, which would escape CommitAsync as an uncoded exception: no PZDL####, no
    /// named cause, no next step. Comparing the raw 16-byte payload removes the failure rather than
    /// reporting it, and is exact — precision and scale are fixed within a column, so equal payloads
    /// are equal values.</summary>
    [Fact]
    public void A_decimal_key_too_large_for_System_Decimal_resolves_instead_of_overflowing()
    {
        var type = new Decimal128Type(38, 0);
        var schema = new Schema([new Field("k", type, nullable: false)], null);
        DeltaMergeDedup.AssertResolvableKeys(schema, ["k"], "out");

        // 10^30 -- outside System.Decimal's range, comfortably inside decimal(38, 0)'s. Rows 0 and 2
        // repeat it; row 1 carries a different over-range value, so the resolver must also tell two
        // unrepresentable values APART rather than collapsing both to one unreadable key.
        var batch = new RecordBatch(
            schema, [Decimals(type, BigInteger.Pow(10, 30), BigInteger.Pow(10, 31), BigInteger.Pow(10, 30))], 3);

        var resolved = DeltaMergeDedup.LastWriterWins([batch], ["k"]);

        Assert.Equal(2, resolved.Sum(b => b.Length));
        Assert.DoesNotContain(batch, resolved);
    }

    /// <summary>A date or timestamp key outside <see cref="DateTime"/>'s range resolves instead of
    /// throwing — the same defect the decimal key above carries, in the arms next to it.
    ///
    /// An Arrow DATE is a day count and an Arrow TIMESTAMP a unit count, both reaching far past year
    /// 9999, and pz's own hub supports dates past it, so this is a live shape. Reading one through
    /// <c>GetDateTime</c> raises ArgumentOutOfRangeException from outside MergeAsync's try block, which
    /// would escape CommitAsync as an uncoded exception: no PZDL####, no named cause, no next step.
    ///
    /// Each column repeats its first value in row 2 and carries a DIFFERENT out-of-range value in row
    /// 1, so a resolver that collapsed every unrepresentable value onto one identity would leave one
    /// row rather than two — which is how the timestamp arm failed. <c>GetTimestamp</c> does not throw
    /// on an extreme value, it WRAPS: microseconds become ticks by an unchecked multiplication by ten,
    /// so long.MinValue microseconds comes back as the epoch itself. Two distinct keys shared one
    /// identity and the loser was dropped silently — a lost row with nothing to report, which is worse
    /// than the date arms' uncoded throw.</summary>
    public static TheoryData<string, IArrowType, IArrowArray> CalendarKeyColumnsBeyondDateTime()
    {
        var micros = new TimestampType(TimeUnit.Microsecond, "UTC");
        return new TheoryData<string, IArrowType, IArrowArray>
        {
            {
                "date32", Date32Type.Default,
                new Date32Array(Fixed(Date32Type.Default, 3_000_000, int.MinValue, 3_000_000))
            },
            {
                "date64", Date64Type.Default,
                new Date64Array(Fixed(Date64Type.Default, long.MaxValue, long.MinValue, long.MaxValue))
            },
            {
                // long.MinValue microseconds and 0 are the measured COLLISION: Apache.Arrow converts
                // microseconds to ticks by multiplying by ten, unchecked, and -2^63 * 10 is exactly
                // -5 * 2^64, so it wraps to the same tick count the epoch has. Two extreme values
                // alone do not reproduce it -- long.MaxValue and long.MinValue land ten ticks apart
                // and stay distinct, which is why this needs the epoch beside it.
                "timestamp_us_utc", micros,
                new TimestampArray(Fixed(micros, long.MinValue, 0L, long.MinValue))
            },
        };
    }

    /// <summary>A fixed-width primitive column assembled from its buffers. Apache.Arrow's own builders
    /// for these three types take a DateTime/DateTimeOffset and so cannot express a value outside
    /// DateTime's range at all — which is the whole point of the columns above.</summary>
    private static ArrayData Fixed<T>(IArrowType type, params T[] values)
        where T : struct
    {
        var buffer = new ArrowBuffer.Builder<T>();
        foreach (var value in values)
        {
            buffer.Append(value);
        }

        return new ArrayData(
            type, values.Length, nullCount: 0, offset: 0, buffers: [ArrowBuffer.Empty, buffer.Build()]);
    }

    [Theory]
    [MemberData(nameof(CalendarKeyColumnsBeyondDateTime))]
    public void A_calendar_key_outside_DateTimes_range_resolves_instead_of_throwing(
        string name, IArrowType type, IArrowArray column)
    {
        var schema = new Schema([new Field("k", type, nullable: false)], null);
        DeltaMergeDedup.AssertResolvableKeys(schema, ["k"], "out");

        var batch = new RecordBatch(schema, [column], 3);
        var resolved = DeltaMergeDedup.LastWriterWins([batch], ["k"]);

        Assert.True(resolved.Sum(b => b.Length) == 2,
            $"{name}: expected the repeated key to resolve to one row and the two distinct " +
            $"out-of-range keys to stay apart, leaving 2 of 3");
        Assert.DoesNotContain(batch, resolved);
    }

    private static IArrowArray Decimals(Decimal128Type type, params BigInteger[] values)
    {
        var buffer = new ArrowBuffer.Builder<byte>();
        foreach (var value in values)
        {
            // Two's-complement little-endian, zero-padded to the type's width -- the layout Arrow
            // stores a decimal in, so the bytes below are the same ones a real column would carry.
            var payload = new byte[type.ByteWidth];
            value.ToByteArray(isUnsigned: true, isBigEndian: false).CopyTo(payload, 0);
            buffer.Append(payload.AsSpan());
        }

        return new Decimal128Array(new ArrayData(
            type, values.Length, nullCount: 0, offset: 0, buffers: [ArrowBuffer.Empty, buffer.Build()]));
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
