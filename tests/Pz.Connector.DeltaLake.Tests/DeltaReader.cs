using Apache.Arrow;
using DeltaLake.Table;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>Reads a Delta table back through delta-rs. This is the reader the write tests assert
/// against by default, because it needs nothing off the network: <see cref="DuckDbReader"/> proves the
/// stronger cross-engine property but depends on a DuckDB extension download, so it has to SKIP on an
/// offline run — and the ownership and commit assertions here are too important to be skippable.
///
/// Everything below goes through <see cref="DeltaBigStack"/> for the same reason production code does:
/// delta-kernel-rs can exhaust a default .NET thread stack on Unix and take the whole test process
/// down with it. <c>ReadAsArrowTableAsync</c> is deliberately not used — it throws
/// "Arrow Context must contain at least one RecordBatch" on a table with no rows, which is exactly the
/// case the abort and empty-commit tests need to read.</summary>
internal static class DeltaReader
{
    public static async Task<long> CountAsync(string location) =>
        (await RowsAsync(location).ConfigureAwait(false)).Count;

    public static Task<IReadOnlyList<(long Id, string Dt, double Amt)>> RowsAsync(string location) =>
        DeltaBigStack.RunAsync(async () =>
        {
            // No ConfigureAwait(false) on this delegate's own awaits: DeltaBigStack pumps plain awaits
            // back onto the big-stack thread, and opting out would put the query enumeration on a
            // default-stack pool thread. The enumeration is fully drained before returning, so no
            // delta-rs continuation outlives the delegate.
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
            try
            {
                var rows = new List<(long, string, double)>();
                var query = new SelectQuery("select id, dt, amt from tbl order by id") { TableAlias = "tbl" };
                await foreach (var batch in table.QueryAsync(query, default))
                {
                    using (batch)
                    {
                        var id = (Int64Array)batch.Column(0);
                        var dt = batch.Column(1);
                        var amt = (DoubleArray)batch.Column(2);
                        for (var i = 0; i < batch.Length; i++)
                        {
                            rows.Add((id.GetValue(i) ?? 0, Text(dt, i), amt.GetValue(i) ?? 0));
                        }
                    }
                }

                return (IReadOnlyList<(long, string, double)>)rows;
            }
            finally
            {
                if (table is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
        });

    /// <summary>The whole table as Arrow batches, ordered by its first column, decoded through
    /// delta-rs so the caller needs nothing off the network — <see cref="DuckDbReader.BatchesAsync"/> is
    /// the same read through the OTHER engine, and depends on an extension download.
    ///
    /// Unlike <see cref="RowsAsync"/> this decodes a NULL in any column: it appends a real Arrow null
    /// rather than squeezing the value into a non-nullable tuple field, which is what made RowsAsync
    /// unable to read a table with a null partition value.</summary>
    public static Task<IReadOnlyList<RecordBatch>> BatchesAsync(string location) =>
        DeltaBigStack.RunAsync(async () =>
        {
            // No ConfigureAwait(false) on this delegate's own awaits — see RowsAsync above.
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
            try
            {
                ArrowRowsetBuilder? rowset = null;
                var query = new SelectQuery("select * from tbl order by 1") { TableAlias = "tbl" };
                await foreach (var batch in table.QueryAsync(query, default))
                {
                    using (batch)
                    {
                        rowset ??= Declare(batch);
                        for (var row = 0; row < batch.Length; row++)
                        {
                            for (var col = 0; col < batch.ColumnCount; col++)
                            {
                                rowset.Append(col, Scalar(batch.Column(col), row));
                            }

                            rowset.CompleteRow();
                        }
                    }
                }

                return rowset?.Build() is { } built ? (IReadOnlyList<RecordBatch>)[built] : [];
            }
            finally
            {
                if (table is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
        });

    private static ArrowRowsetBuilder Declare(RecordBatch batch)
    {
        var rowset = new ArrowRowsetBuilder();
        for (var col = 0; col < batch.ColumnCount; col++)
        {
            rowset.DeclareColumn(batch.Schema.FieldsList[col].Name, ClrTypeOf(batch.Column(col)));
        }

        return rowset;
    }

    /// <summary>Which CLR type a column decodes to. Driven by the ARRAY that arrived rather than by the
    /// table's declared schema: DataFusion picks the encoding (a partition column arrives
    /// dictionary-encoded, an ordinary string column as a view), and the encoding is what determines
    /// how a value must be read back out.</summary>
    private static Type ClrTypeOf(IArrowArray array) => array switch
    {
        Int64Array => typeof(long),
        DoubleArray => typeof(double),
        StringArray or StringViewArray or LargeStringArray => typeof(string),
        DictionaryArray d => ClrTypeOf(d.Dictionary),
        _ => throw new NotSupportedException($"unexpected array type {array.GetType().Name}"),
    };

    private static object? Scalar(IArrowArray array, int index)
    {
        if (array.IsNull(index))
        {
            return null;
        }

        return array switch
        {
            Int64Array a => a.GetValue(index),
            DoubleArray a => a.GetValue(index),
            // Every string encoding, including the dictionary-encoded shape a partition column takes.
            _ => Text(array, index),
        };
    }

    /// <summary>delta-rs answers a query through DataFusion, whose string columns arrive in whichever
    /// encoding the plan produced: a StringViewArray for an ordinary column, and a dictionary-encoded
    /// array for a PARTITION column, whose values live in directory names rather than in the data
    /// files. A straight cast to StringArray fails on both.</summary>
    internal static string Text(IArrowArray array, int index) => array switch
    {
        StringArray s => s.GetString(index),
        StringViewArray v => v.GetString(index),
        LargeStringArray l => l.GetString(index),
        DictionaryArray d => Text(d.Dictionary, Index(d.Indices, index)),
        _ => throw new InvalidOperationException($"unexpected string array type {array.GetType().Name}"),
    };

    /// <summary>The index type of a dictionary-encoded column is chosen by the producer (DataFusion
    /// hands back UInt16 for a partition column with a handful of distinct values), so every integer
    /// width is accepted rather than one guessed one.</summary>
    private static int Index(IArrowArray indices, int at) => indices switch
    {
        Int8Array a => a.GetValue(at)!.Value,
        UInt8Array a => a.GetValue(at)!.Value,
        Int16Array a => a.GetValue(at)!.Value,
        UInt16Array a => a.GetValue(at)!.Value,
        Int32Array a => a.GetValue(at)!.Value,
        UInt32Array a => checked((int)a.GetValue(at)!.Value),
        Int64Array a => checked((int)a.GetValue(at)!.Value),
        UInt64Array a => checked((int)a.GetValue(at)!.Value),
        _ => throw new InvalidOperationException($"unexpected dictionary index type {indices.GetType().Name}"),
    };

    /// <summary>A row count that decodes no values. <see cref="CountAsync"/> goes through
    /// <see cref="RowsAsync"/> and therefore through <see cref="Text"/>, which cannot represent a NULL
    /// in a string column — its tuple carries a non-nullable string. A null PARTITION value is
    /// legitimate and delta-rs returns it as a dictionary-encoded column with a null index, so a test
    /// that writes one has to count without reading.</summary>
    public static Task<long> RowCountAsync(string location) =>
        DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
            try
            {
                var count = 0L;
                var query = new SelectQuery("select count(*) from tbl") { TableAlias = "tbl" };
                await foreach (var batch in table.QueryAsync(query, default))
                {
                    using (batch)
                    {
                        count = ((Int64Array)batch.Column(0)).GetValue(0) ?? 0;
                    }
                }

                return count;
            }
            finally
            {
                if (table is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
        });

    /// <summary>The table's column names, as delta-rs reports them after the write. A row count cannot
    /// tell a schema that widened from one that quietly dropped the extra column.</summary>
    public static Task<IReadOnlyList<string>> ColumnsAsync(string location) =>
        DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
            try
            {
                return (IReadOnlyList<string>)[.. table.Schema().FieldsList.Select(f => f.Name)];
            }
            finally
            {
                if (table is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
        });

    /// <summary>How many commits the table's transaction log holds. An append that flushes bounded
    /// generations produces more of them than one that buffers the whole write and commits once, which
    /// is the only externally visible difference between the two — a row count cannot tell them
    /// apart.</summary>
    public static int CommitCount(string location) =>
        Directory.GetFiles(Path.Combine(location, "_delta_log"), "*.json").Length;
}
