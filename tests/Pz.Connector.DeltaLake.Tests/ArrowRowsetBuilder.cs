using System.Globalization;
using Apache.Arrow;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>Turns whatever shape a reader hands back into the ONE canonical Arrow shape the acceptance
/// assertions cast against: an <see cref="Int64Array"/>, a <see cref="StringArray"/>, or a
/// <see cref="DoubleArray"/> per column, never a view- or dictionary-encoded stand-in. Both readers
/// funnel through here so a table read through delta-rs and the same table read through DuckDB are
/// comparable object-for-object instead of merely equal in value.
///
/// An unrecognized column type is a loud <see cref="NotSupportedException"/>, never a dropped column:
/// a reader that silently omitted what it could not decode would turn a schema regression into a
/// passing test.</summary>
internal sealed class ArrowRowsetBuilder
{
    private readonly List<string> names = [];
    private readonly List<object> builders = [];

    public int RowCount { get; private set; }

    public void DeclareColumn(string name, Type clrType)
    {
        this.names.Add(name);
        this.builders.Add(clrType switch
        {
            _ when clrType == typeof(long) => new Int64Array.Builder(),
            _ when clrType == typeof(string) => new StringArray.Builder(),
            _ when clrType == typeof(double) => new DoubleArray.Builder(),
            _ => throw new NotSupportedException(
                $"column '{name}' has unsupported type {clrType} in the acceptance rowset builder"),
        });
    }

    /// <summary>Appends one value to column <paramref name="column"/>. A null is appended as a real
    /// Arrow null rather than coerced to a default — a reader that cannot represent a null in a string
    /// column is the known gap this builder exists not to reproduce.</summary>
    public void Append(int column, object? value)
    {
        switch (this.builders[column])
        {
            case Int64Array.Builder b:
                _ = value is null ? b.AppendNull() : b.Append(Convert.ToInt64(value, CultureInfo.InvariantCulture));
                break;
            case StringArray.Builder b:
                _ = value is null ? b.AppendNull() : b.Append((string)value);
                break;
            case DoubleArray.Builder b:
                _ = value is null ? b.AppendNull() : b.Append(Convert.ToDouble(value, CultureInfo.InvariantCulture));
                break;
            default:
                throw new NotSupportedException($"no appender for column {column}");
        }
    }

    public void CompleteRow() => this.RowCount++;

    /// <summary>The accumulated rows as a single batch, or null when there were none. Null rather than
    /// an empty batch: the acceptance suites sum lengths over the returned list and an empty table must
    /// read back as an empty LIST, which is what <c>Assert.Empty</c> checks after an abort.</summary>
    public RecordBatch? Build()
    {
        if (this.RowCount == 0)
        {
            return null;
        }

        var arrays = new List<IArrowArray>(this.builders.Count);
        var fields = new List<Field>(this.builders.Count);
        for (var i = 0; i < this.builders.Count; i++)
        {
            IArrowArray array = this.builders[i] switch
            {
                Int64Array.Builder b => b.Build(),
                StringArray.Builder b => b.Build(),
                DoubleArray.Builder b => b.Build(),
                var other => throw new NotSupportedException($"no build for {other.GetType().Name}"),
            };
            arrays.Add(array);
            fields.Add(new Field(this.names[i], array.Data.DataType, nullable: true));
        }

        return new RecordBatch(new Schema(fields, null), arrays, this.RowCount);
    }
}
