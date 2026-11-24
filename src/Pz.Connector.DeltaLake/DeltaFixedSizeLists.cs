using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.DeltaLake;

/// <summary>Rewrites fixed-size-list columns (DuckDB's ARRAY) as variable-size lists, to match the schema
/// <see cref="DeltaArrowTypes.Canonical(Schema)"/> produced. Delta has one list type, and delta-rs
/// rejects the fixed-size shape on insert. No value buffer is copied: only the offsets are new, because a
/// fixed-size list's offsets are implicit (<c>i * size</c>). A fixed-size list inside a map is not
/// converted and is refused up front by <see cref="DeltaTypeSupport"/>.</summary>
internal static class DeltaFixedSizeLists
{
    public static RecordBatch Apply(RecordBatch batch, Schema canonical)
    {
        if (!batch.Schema.FieldsList.Any(f => DeltaArrowTypes.ContainsFixedSizeList(f.DataType)))
        {
            return batch;
        }

        var columns = new IArrowArray[batch.ColumnCount];
        for (var i = 0; i < columns.Length; i++)
        {
            var column = batch.Column(i);
            columns[i] = DeltaArrowTypes.ContainsFixedSizeList(column.Data.DataType)
                ? ArrowArrayFactory.BuildArray(Convert(column.Data, canonical.FieldsList[i].DataType))
                : column;
        }

        return new RecordBatch(canonical, columns, batch.Length);
    }

    private static ArrayData Convert(ArrayData data, IArrowType target)
    {
        if (!DeltaArrowTypes.ContainsFixedSizeList(data.DataType))
        {
            return data;
        }

        switch (data.DataType)
        {
            case FixedSizeListType fixedSize:
            {
                var offsets = new ArrowBuffer.Builder<int>(data.Offset + data.Length + 1);
                for (var i = 0; i <= data.Offset + data.Length; i++)
                {
                    offsets.Append(checked(i * fixedSize.ListSize));
                }

                var child = Convert(data.Children[0], ((ListType)target).ValueDataType);
                return new ArrayData(target, data.Length, data.NullCount, data.Offset,
                    [data.Buffers[0], offsets.Build()], [child]);
            }
            case ListType:
            {
                var child = Convert(data.Children[0], ((ListType)target).ValueDataType);
                return new ArrayData(target, data.Length, data.NullCount, data.Offset, data.Buffers, [child]);
            }
            case StructType:
            {
                var fields = ((StructType)target).Fields;
                var children = data.Children.Select((c, i) => Convert(c, fields[i].DataType)).ToArray();
                return new ArrayData(target, data.Length, data.NullCount, data.Offset, data.Buffers, children);
            }
            default:
                return data;
        }
    }
}
