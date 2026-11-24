using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Table;
using DuckDB.NET.Data;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>List, struct, map and fixed-size-list columns, with a null at every level, written through the
/// sink and read back by both engines. A fixed-size list is stored as a Delta list.</summary>
public sealed class NestedRoundTripTests : IDisposable
{
    private readonly LocalLakeFixture lake = LocalLakeFixture.Empty();

    public void Dispose() => this.lake.Dispose();

    private static RecordBatch Nested()
    {
        const int Rows = 4;
        var batch = BuildNested();
        Assert.Equal(Rows, batch.Length);
        return batch;
    }

    private static RecordBatch BuildNested()
    {
        var ids = new Int64Array.Builder().AppendRange([0, 1, 2, 3]).Build();

        // rec: {n: 7, tags: [a, null]}, null, {n: null, tags: []}, {n: 9, tags: null}
        var tags = new ListArray.Builder(StringType.Default);
        var tagValues = (StringArray.Builder)tags.ValueBuilder;
        tags.Append();
        tagValues.Append("a");
        tagValues.AppendNull();
        tags.AppendNull(); // under the null struct
        tags.Append();
        tags.AppendNull();
        var recType = new StructType([
            new Field("n", Int32Type.Default, true),
            new Field("tags", new ListType(new Field("item", StringType.Default, true)), true),
        ]);
        var rec = new StructArray(recType, 4,
            [new Int32Array.Builder().Append(7).AppendNull().AppendNull().Append(9).Build(), tags.Build()],
            new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Append(true).Build(), nullCount: 1);

        // xs: [1, 2], null, [], [null]
        var xs = new ListArray.Builder(Int64Type.Default);
        var xValues = (Int64Array.Builder)xs.ValueBuilder;
        xs.Append();
        xValues.Append(1);
        xValues.Append(2);
        xs.AppendNull();
        xs.Append();
        xs.Append();
        xValues.AppendNull();
        var xsArray = xs.Build();

        // pair: [1, 2], null, [null, 4], [5, 6]
        var pairType = new FixedSizeListType(new Field("item", Int32Type.Default, true), 2);
        var pairValues = new Int32Array.Builder().Append(1).Append(2).Append(0).Append(0).AppendNull().Append(4).Append(5).Append(6).Build();
        var pair = new FixedSizeListArray(new ArrayData(pairType, 4, 1, 0,
            [new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Append(true).Build()], [pairValues.Data]));

        // m: {k1: 1, k2: null}, null, {}, {k3: 3}
        var map = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default));
        var keys = (StringArray.Builder)map.KeyBuilder;
        var values = (Int32Array.Builder)map.ValueBuilder;
        map.Append();
        keys.Append("k1");
        values.Append(1);
        keys.Append("k2");
        values.AppendNull();
        map.AppendNull();
        map.Append();
        map.Append();
        keys.Append("k3");
        values.Append(3);
        var mapArray = map.Build();

        // items: [{k: x, v: 1.5}], null, [], [null, {k: null, v: 2.5}]
        var itemType = new StructType([new Field("k", StringType.Default, true), new Field("v", DoubleType.Default, true)]);
        var itemStructs = new StructArray(itemType, 3,
            [new StringArray.Builder().Append("x").AppendNull().AppendNull().Build(),
             new DoubleArray.Builder().Append(1.5).AppendNull().Append(2.5).Build()],
            new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Build(), nullCount: 1);
        var itemsType = new ListType(new Field("item", itemType, true));
        var items = new ListArray(itemsType, 4,
            new ArrowBuffer.Builder<int>().AppendRange([0, 1, 1, 1, 3]).Build(), itemStructs,
            new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Append(true).Build(), nullCount: 1);

        var schema = new Schema([
            new Field("id", Int64Type.Default, false),
            new Field("rec", recType, true),
            new Field("xs", xsArray.Data.DataType, true),
            new Field("pair", pairType, true),
            new Field("m", mapArray.Data.DataType, true),
            new Field("items", itemsType, true),
        ], null);
        return new RecordBatch(schema, [ids, rec, xsArray, pair, mapArray, items], 4);
    }

    private async Task WriteAsync(string output, string mode, RecordBatch batch, string[]? keys = null)
    {
        var sink = await ((ISinkConnector)new DeltaLakeConnector()).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["root"] = this.lake.Root }), default);
        await using var _ = sink;
        var spec = new OutputSpec("lake", output, mode, "fail_on_change", new Dictionary<string, object?>())
        {
            Keys = keys ?? [],
        };
        await using var session = await sink.BeginWriteAsync(spec, batch.Schema, default);
        await session.WriteBatchAsync(batch, default);
        await session.CommitAsync(default);
    }

    private static string[] Json(RecordBatch batch, int column) =>
        Enumerable.Range(0, batch.Length).Select(r => Plain(batch.Column(column), r)).ToArray();

    /// <summary>A canonical text form of one cell. DataFusion returns utf8 as string views, which
    /// ArrowCell has no encoding for, so the comparison cannot go through it.</summary>
    private static string Plain(IArrowArray array, int row)
    {
        if (array.IsNull(row))
        {
            return "null";
        }

        switch (array)
        {
            case StringArray a: return "\"" + a.GetString(row) + "\"";
            case StringViewArray a: return "\"" + a.GetString(row) + "\"";
            case Int32Array a: return a.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            case Int64Array a: return a.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            case DoubleArray a: return a.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            case MapArray m:
            {
                var keys = m.Keys;
                var values = m.Values;
                var parts = new List<string>();
                for (var i = m.ValueOffsets[row]; i < m.ValueOffsets[row + 1]; i++)
                {
                    parts.Add(Plain(keys, i) + ":" + Plain(values, i));
                }

                return "{" + string.Join(",", parts) + "}";
            }
            case FixedSizeListArray f:
            {
                var size = ((FixedSizeListType)f.Data.DataType).ListSize;
                var parts = new List<string>();
                for (var i = 0; i < size; i++)
                {
                    parts.Add(Plain(f.Values, (f.Offset + row) * size + i));
                }

                return "[" + string.Join(",", parts) + "]";
            }
            case ListArray l:
            {
                var parts = new List<string>();
                for (var i = l.ValueOffsets[row]; i < l.ValueOffsets[row + 1]; i++)
                {
                    parts.Add(Plain(l.Values, i));
                }

                return "[" + string.Join(",", parts) + "]";
            }
            case StructArray st:
                return "{" + string.Join(",", ((StructType)st.Data.DataType).Fields.Select(
                    (f, i) => f.Name + ":" + Plain(st.Fields[i], row))) + "}";
            default:
                throw new NotSupportedException(array.GetType().Name);
        }
    }

    private async Task<RecordBatch> ReadAllAsync(string entity)
    {
        var location = Path.Combine(this.lake.Root, entity);
        var batches = await DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
            var all = new List<RecordBatch>();
            await foreach (var b in table.QueryAsync(new SelectQuery("select * from tbl order by id") { TableAlias = "tbl" }, default))
            {
                all.Add(b);
            }

            return all;
        });
        return Assert.Single(batches);
    }

    [Fact]
    public async Task Every_nested_shape_round_trips_with_nulls_at_every_level_through_delta_rs()
    {
        using var written = Nested();
        await this.WriteAsync("nested", "append", written);

        using var read = await this.ReadAllAsync("nested");

        Assert.Equal(written.Length, read.Length);
        foreach (var name in new[] { "rec", "xs", "pair", "items" })
        {
            Assert.Equal(
                Json(written, written.Schema.GetFieldIndex(name)),
                Json(read, read.Schema.GetFieldIndex(name)));
        }

        // The fixed-size list is a Delta list, and row 1's whole-list null survives.
        Assert.IsType<ListType>(read.Column(read.Schema.GetFieldIndex("pair")).Data.DataType);
        Assert.Equal(1, read.Column(read.Schema.GetFieldIndex("pair")).IsNull(1) ? 1 : 0);
        Assert.IsType<MapType>(read.Column(read.Schema.GetFieldIndex("m")).Data.DataType);
        Assert.Equal(Json(written, written.Schema.GetFieldIndex("m")), Json(read, read.Schema.GetFieldIndex("m")));
    }

    [SkippableFact]
    public async Task The_nested_table_reads_back_through_duckdb()
    {
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(), "delta extension unavailable offline");
        using var written = Nested();
        await this.WriteAsync("nested", "append", written);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        await conn.OpenAsync();
        foreach (var s in new[] { "install delta", "load delta" })
        {
            using var c = conn.CreateCommand();
            c.CommandText = s;
            await c.ExecuteNonQueryAsync();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "select cast(to_json(rec) as varchar), cast(to_json(xs) as varchar), cast(to_json(pair) as varchar), " +
            "cast(to_json(m) as varchar), cast(to_json(items) as varchar) " +
            $"from delta_scan('{Path.Combine(this.lake.Root, "nested").Replace("'", "''")}') order by id";
        using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<string?[]>();
        while (await reader.ReadAsync())
        {
            rows.Add(Enumerable.Range(0, 5).Select(i => reader.IsDBNull(i) ? null : reader.GetString(i)).ToArray());
        }

        Assert.Equal(4, rows.Count);
        Assert.Equal("{\"n\":7,\"tags\":[\"a\",null]}", rows[0][0]);
        Assert.Null(rows[1][0]);
        Assert.Equal("[1,2]", rows[0][1]);
        Assert.Equal("[]", rows[2][1]);
        Assert.Equal("[null]", rows[3][1]);
        Assert.Equal("[1,2]", rows[0][2]);
        Assert.Null(rows[1][2]);
        Assert.Equal("[null,4]", rows[2][2]);
        Assert.Equal("{\"k1\":1,\"k2\":null}", rows[0][3]);
        Assert.Null(rows[1][3]);
        Assert.Equal("{}", rows[2][3]);
        Assert.Equal("[null,{\"k\":null,\"v\":2.5}]", rows[3][4]);
    }

    [Fact]
    public async Task A_second_write_with_the_same_nested_schema_reconciles()
    {
        using var first = Nested();
        await this.WriteAsync("nested", "append", first);
        using var second = Nested();

        await this.WriteAsync("nested", "append", second);

        using var read = await this.ReadAllAsync("nested");
        Assert.Equal(8, read.Length);
    }

    [Fact]
    public async Task A_changed_nested_leaf_type_is_refused_naming_the_column()
    {
        using var first = Nested();
        await this.WriteAsync("nested", "append", first);
        var changed = new Schema([
            new Field("id", Int64Type.Default, false),
            new Field("xs", new ListType(new Field("item", StringType.Default, true)), true),
        ], null);
        using var second = new RecordBatch(changed, [
            new Int64Array.Builder().Append(9).Build(),
            new ListArray.Builder(StringType.Default).Append().Build()], 1);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            this.WriteAsync("nested", "append", second));

        Assert.Contains("'xs'", ex.Message);
        Assert.Contains(DeltaErrors.SchemaMismatch, ex.Message);
    }

    [Fact]
    public async Task A_nested_merge_key_is_refused_before_the_table_is_touched()
    {
        using var batch = Nested();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            this.WriteAsync("merged", "merge", batch, ["rec"]));

        Assert.Contains("'rec'", ex.Message);
        Assert.Contains("a merge key must be a flat value", ex.Message);
        Assert.False(Directory.Exists(Path.Combine(this.lake.Root, "merged")));
    }

    [Fact]
    public async Task A_fixed_size_list_inside_a_map_is_refused_naming_the_column()
    {
        var type = new MapType(StringType.Default, new FixedSizeListType(Int32Type.Default, 2));
        var schema = new Schema([new Field("id", Int64Type.Default, false), new Field("m", type, true)], null);
        var mapBuilder = new MapArray.Builder(type);
        mapBuilder.AppendNull();
        using var batch = new RecordBatch(schema, [new Int64Array.Builder().Append(1).Build(), mapBuilder.Build()], 1);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => this.WriteAsync("mapfsl", "append", batch));

        Assert.Contains("'m'", ex.Message);
    }
}
