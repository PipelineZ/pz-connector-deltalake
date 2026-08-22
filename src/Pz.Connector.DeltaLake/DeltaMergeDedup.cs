using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Pz.Connector.DeltaLake;

/// <summary>Resolves a merge input that names the same key more than once, last-writer-wins.
///
/// A raw Delta MERGE cannot. Its ON clause matches the TARGET against the source, so two source rows
/// carrying one key are two independent matches: if the target already holds that key delta-rs refuses
/// the whole statement, and if it does NOT, both rows fall to WHEN NOT MATCHED and both are INSERTED —
/// one commit, no error, and a table holding a duplicate of a key the output declared unique. Measured
/// against DeltaLake.Net 0.33.0, both halves; the second is the dangerous one, because it looks exactly like
/// a successful merge.
///
/// Last-writer-wins — the LATER row for a key is the one that lands — is the merge contract the rest of
/// the ecosystem implements: the connector ABI's own reference sink absorbs a repeated key, and the
/// first-party postgres and sql server sinks each carry an arrival-order tiebreaker to reproduce it.
/// Those resolve it in SQL over a staging table. Delta's merge source is a batch collection with no
/// arrival-order column to sort on, so this connector resolves it in Arrow instead, before the
/// statement ever runs.
///
/// What the common case costs, stated rather than waved at: no DATA is copied and no batch is rebuilt
/// — an input with no repeated key is handed back as the SAME list — but the scan that establishes
/// that is not free. Every key value of every row is boxed into a fresh <c>object[]</c> and inserted
/// into a dictionary before the resolver can know whether anything repeats, so every merge pays one
/// managed allocation and one hash insert per row, duplicate-free or not. That is affordable for a
/// reason specific to this operation: a merge already buffers its entire input in memory, so the row
/// set is bounded by something the session is holding anyway.</summary>
internal static class DeltaMergeDedup
{
    /// <summary>Refuses, at BeginWriteAsync, a merge key whose type this resolver cannot compare —
    /// before a session exists and therefore before anything can commit. Every offending column is
    /// named in one message, with its Arrow type; the message names columns and types, never values.
    ///
    /// Refusing is the only honest answer available. Letting an uncomparable key through would mean
    /// skipping the resolution for it, and the outcome of skipping is precisely the silent duplicate
    /// this type exists to prevent — a failure that surfaces later, in someone else's read, with
    /// nothing to trace it back to.</summary>
    public static void AssertResolvableKeys(Schema schema, IReadOnlyList<string> keys, string output)
    {
        var bad = new List<Field>();
        foreach (var key in keys)
        {
            // Ordinal, and it has to stay Ordinal: Delta column names are case-sensitive. A key naming
            // no column at all is DeltaLakeSink's own refusal (PZDL0303) and is already raised by the
            // time this runs.
            var index = schema.GetFieldIndex(key, StringComparer.Ordinal);
            if (index >= 0 && !IsComparable(schema.FieldsList[index].DataType))
            {
                bad.Add(schema.FieldsList[index]);
            }
        }

        if (bad.Count == 0)
        {
            return;
        }

        var named = string.Join(", ", bad.Select(f => $"'{f.Name}' ({f.DataType.Name})"));
        throw DeltaErrors.Fail(DeltaErrors.UnresolvableMergeKeyType,
            $"output '{output}': merge key(s) {named} have Arrow types this connector cannot compare " +
            "row to row, so it cannot tell whether one write names the same key twice",
            "use a scalar column as the merge key — a number, a string, a boolean, a date or a " +
            "timestamp — and derive it in the pipeline SQL if the source column is nested");
    }

    /// <summary>The batches to hand delta-rs: <paramref name="buffered"/> itself when no key repeats,
    /// otherwise the surviving rows.
    ///
    /// Survivors come back as SLICES of the buffered batches, which share the parent's buffers rather
    /// than copying them. Two consequences, and both are load-bearing: a slice must never be disposed
    /// (disposing one frees the parent's memory, and the parent is a batch this session still owns and
    /// still has to hand to the merge), and the returned list must not outlive
    /// <paramref name="buffered"/>.</summary>
    public static IReadOnlyList<RecordBatch> LastWriterWins(
        IReadOnlyList<RecordBatch> buffered, IReadOnlyList<string> keys)
    {
        if (keys.Count == 0 || buffered.Count == 0)
        {
            return buffered;
        }

        var lastAt = new Dictionary<RowKey, (int Batch, int Row)>();
        var rows = 0;
        for (var b = 0; b < buffered.Count; b++)
        {
            var columns = KeyColumns(buffered[b], keys);
            for (var r = 0; r < buffered[b].Length; r++)
            {
                rows++;
                var values = new object[columns.Count];
                for (var k = 0; k < columns.Count; k++)
                {
                    values[k] = Value(columns[k], r);
                }

                // Plain assignment, not TryAdd: the LAST write for a key is the one that wins, so a
                // repeat must overwrite the position recorded for the earlier row.
                lastAt[new RowKey(values)] = (b, r);
            }
        }

        if (lastAt.Count == rows)
        {
            return buffered;
        }

        var keep = new bool[buffered.Count][];
        for (var b = 0; b < buffered.Count; b++)
        {
            keep[b] = new bool[buffered[b].Length];
        }

        foreach (var (batch, row) in lastAt.Values)
        {
            keep[batch][row] = true;
        }

        // Maximal contiguous runs, so a merge that dropped one row out of a hundred thousand hands
        // delta-rs two slices rather than a hundred thousand single-row batches.
        var payload = new List<RecordBatch>();
        for (var b = 0; b < buffered.Count; b++)
        {
            var start = -1;
            for (var r = 0; r <= buffered[b].Length; r++)
            {
                var kept = r < buffered[b].Length && keep[b][r];
                if (kept && start < 0)
                {
                    start = r;
                }
                else if (!kept && start >= 0)
                {
                    payload.Add(buffered[b].Slice(start, r - start));
                    start = -1;
                }
            }
        }

        return payload;
    }

    private static IReadOnlyList<IArrowArray> KeyColumns(RecordBatch batch, IReadOnlyList<string> keys)
    {
        var columns = new List<IArrowArray>(keys.Count);
        foreach (var key in keys)
        {
            // Ordinal, and it has to stay Ordinal: Delta column names are case-sensitive. A key the
            // batch does not carry cannot take part in the identity, and it cannot happen either —
            // DeltaLakeSink refuses a key that names no column of the write's schema, and every batch
            // in the buffer carries that schema.
            var index = batch.Schema.GetFieldIndex(key, StringComparer.Ordinal);
            if (index >= 0)
            {
                columns.Add(batch.Column(index));
            }
        }

        return columns;
    }

    /// <summary>Which key types can be compared row to row. Deliberately the SCALAR types only: a key
    /// of a nested type would need an equality this connector would be defining rather than mirroring,
    /// and the SQL side's answer for one is not something this connector can vouch for.</summary>
    private static bool IsComparable(IArrowType type) => type.TypeId switch
    {
        ArrowTypeId.Boolean or ArrowTypeId.Int8 or ArrowTypeId.Int16 or ArrowTypeId.Int32 or
            ArrowTypeId.Int64 or ArrowTypeId.UInt8 or ArrowTypeId.UInt16 or ArrowTypeId.UInt32 or
            ArrowTypeId.UInt64 or ArrowTypeId.Float or ArrowTypeId.Double or ArrowTypeId.Decimal128 or
            ArrowTypeId.String or ArrowTypeId.StringView or ArrowTypeId.LargeString or
            ArrowTypeId.Binary or ArrowTypeId.BinaryView or ArrowTypeId.LargeBinary or
            ArrowTypeId.FixedSizedBinary or ArrowTypeId.Date32 or ArrowTypeId.Date64 or
            ArrowTypeId.Timestamp => true,
        ArrowTypeId.Dictionary => IsComparable(((DictionaryType)type).ValueType),
        _ => false,
    };

    /// <summary>One key value, boxed, as the identity the resolver compares. A column has ONE type, so
    /// two boxes only ever meet when they came from the same column — an Int64 identity can never
    /// collide with a string one.
    ///
    /// Sorted by what a key column actually is, not by Arrow's enum order. A null returns the sentinel
    /// rather than throwing: it cannot arrive (RefuseUnmatchableKeys refuses a null key before this
    /// runs, because a null key cannot match itself in the ON clause either) and reaching for
    /// <c>.Value</c> on one would turn a guard that has already spoken into a
    /// NullReferenceException.</summary>
    private static object Value(IArrowArray array, int row)
    {
        if (array.IsNull(row))
        {
            return NullKey;
        }

        return array switch
        {
            Int64Array a => a.GetValue(row)!.Value,
            Int32Array a => a.GetValue(row)!.Value,
            StringArray a => a.GetString(row)!,
            StringViewArray a => a.GetString(row)!,
            LargeStringArray a => a.GetString(row)!,
            Int16Array a => a.GetValue(row)!.Value,
            Int8Array a => a.GetValue(row)!.Value,
            UInt64Array a => a.GetValue(row)!.Value,
            UInt32Array a => a.GetValue(row)!.Value,
            UInt16Array a => a.GetValue(row)!.Value,
            UInt8Array a => a.GetValue(row)!.Value,
            // NaN cannot arrive: RefuseUnmatchableKeys refuses it first, for the same reason it refuses
            // a null. -0.0 does arrive and must compare EQUAL to 0.0, which is what SQL says of it and
            // what Double.Equals/Single.Equals say too.
            DoubleArray a => a.GetValue(row)!.Value,
            FloatArray a => a.GetValue(row)!.Value,
            // Compared as its raw 16-byte payload, NOT as a System.Decimal, and that is a correctness
            // fix rather than a micro-optimisation. Delta and DataFusion allow decimal(38, s), whose
            // range exceeds System.Decimal's: measured against Apache.Arrow 23.0.0, GetValue() on a
            // decimal(38, 0) holding 10^30 throws OverflowException — uncoded, and raised from outside
            // MergeAsync's try, so it would escape CommitAsync carrying no PZDL#### at all. GetBytes()
            // cannot overflow, and the payload is exact: within one column precision and scale are
            // fixed, so two values are equal exactly when their two's-complement payloads are, which
            // is the same equality DataFusion's ON clause applies.
            //
            // Decimal128Array derives from FixedSizeBinaryArray, so this arm MUST precede that one.
            // Reordering is not a silent hazard: the compiler answers a subsumed arm with CS8510
            // (unreachable pattern), which TreatWarningsAsErrors turns into a build failure.
            Decimal128Array a => Convert.ToHexString(a.GetBytes(row)),
            BooleanArray a => a.GetValue(row)!.Value,
            Date32Array a => a.GetDateTime(row)!.Value,
            Date64Array a => a.GetDateTime(row)!.Value,
            TimestampArray a => a.GetTimestamp(row)!.Value,
            // Hex rather than the byte array itself: two equal byte arrays are different objects and
            // would compare unequal, which would leave every binary key looking distinct.
            //
            // Four UNRELATED classes, not one hierarchy: BinaryArray, LargeBinaryArray,
            // BinaryViewArray and FixedSizeBinaryArray each derive straight from Array, so one arm
            // does not cover the others and every binary encoding IsComparable admits needs its own
            // here or it falls to the throw below.
            //
            // Where derivation DOES exist it dictates arm order, in both directions. StringArray
            // derives from BinaryArray, StringViewArray from BinaryViewArray and LargeStringArray
            // from LargeBinaryArray, so the string arms must precede these or a string column would
            // be compared as hex rather than as text; Decimal128Array derives from
            // FixedSizeBinaryArray, so its arm must precede that one. None of it can regress
            // silently — a subsumed arm is CS8510, which TreatWarningsAsErrors makes a build error.
            BinaryArray a => Convert.ToHexString(a.GetBytes(row)),
            LargeBinaryArray a => Convert.ToHexString(a.GetBytes(row)),
            BinaryViewArray a => Convert.ToHexString(a.GetBytes(row)),
            FixedSizeBinaryArray a => Convert.ToHexString(a.GetBytes(row)),
            DictionaryArray a => Value(a.Dictionary, DictionaryIndex(a.Indices, row)),
            _ => throw new NotSupportedException(
                $"merge key column of type {array.GetType().Name} reached the duplicate-key resolver; " +
                "AssertResolvableKeys should have refused it at BeginWriteAsync"),
        };
    }

    /// <summary>The index type of a dictionary-encoded column is the producer's choice, so every
    /// integer width is accepted rather than one guessed one.</summary>
    private static int DictionaryIndex(IArrowArray indices, int row) => indices switch
    {
        Int8Array a => a.GetValue(row)!.Value,
        UInt8Array a => a.GetValue(row)!.Value,
        Int16Array a => a.GetValue(row)!.Value,
        UInt16Array a => a.GetValue(row)!.Value,
        Int32Array a => a.GetValue(row)!.Value,
        UInt32Array a => checked((int)a.GetValue(row)!.Value),
        Int64Array a => checked((int)a.GetValue(row)!.Value),
        UInt64Array a => checked((int)a.GetValue(row)!.Value),
        _ => throw new NotSupportedException($"unexpected dictionary index type {indices.GetType().Name}"),
    };

    private static readonly object NullKey = new();

    /// <summary>One row's key values, compared and hashed as a tuple. A plain <c>object[]</c> would
    /// compare by reference and make every row look distinct.</summary>
    private readonly struct RowKey(object[] values) : IEquatable<RowKey>
    {
        private readonly object[] values = values;

        public bool Equals(RowKey other)
        {
            if (this.values.Length != other.values.Length)
            {
                return false;
            }

            for (var i = 0; i < this.values.Length; i++)
            {
                if (!this.values[i].Equals(other.values[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => obj is RowKey other && this.Equals(other);

        public override int GetHashCode()
        {
            var hash = default(HashCode);
            foreach (var value in this.values)
            {
                hash.Add(value);
            }

            return hash.ToHashCode();
        }
    }
}
