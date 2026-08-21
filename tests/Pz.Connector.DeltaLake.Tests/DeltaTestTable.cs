using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Table;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>Shared fixtures: one small Arrow schema and the builders every write/read test uses, plus a
/// helper that materializes a real Delta table on local disk through delta-rs.</summary>
internal static class DeltaTestTable
{
    public static readonly Schema Schema = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
        .Build();

    public static string Partition(long id) => $"2026-01-{(int)(id % 8) + 1:D2}";

    public static RecordBatch Rows(long from, long count)
    {
        var id = new Int64Array.Builder();
        var dt = new StringArray.Builder();
        var amt = new DoubleArray.Builder();
        for (var i = from; i < from + count; i++)
        {
            id.Append(i);
            dt.Append(Partition(i));
            amt.Append(i);
        }

        return new RecordBatch(Schema, [id.Build(), dt.Build(), amt.Build()], (int)count);
    }

    public static RecordBatch RowsWithAmounts(IReadOnlyList<(long Id, string Dt, double Amt)> rows)
    {
        var id = new Int64Array.Builder();
        var dt = new StringArray.Builder();
        var amt = new DoubleArray.Builder();
        foreach (var r in rows)
        {
            id.Append(r.Id);
            dt.Append(r.Dt);
            amt.Append(r.Amt);
        }

        return new RecordBatch(Schema, [id.Build(), dt.Build(), amt.Build()], rows.Count);
    }

    // One column wider than Schema -- version-probe tests need a real schema DIFFERENCE across
    // commits, not just a different version number, to prove GetSchemaAsync's 'version' option drives
    // what comes back rather than always returning latest.
    public static readonly Schema WiderSchema = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
        .Field(f => f.Name("note").DataType(StringType.Default).Nullable(true))
        .Build();

    public static RecordBatch WiderRows(long count)
    {
        var id = new Int64Array.Builder();
        var dt = new StringArray.Builder();
        var amt = new DoubleArray.Builder();
        var note = new StringArray.Builder();
        for (var i = 0; i < count; i++)
        {
            id.Append(i);
            dt.Append(Partition(i));
            amt.Append(i);
            note.Append($"n{i}");
        }

        return new RecordBatch(WiderSchema, [id.Build(), dt.Build(), amt.Build(), note.Build()], (int)count);
    }

    // Every delta-rs call runs on DeltaBigStack's dedicated big-stack thread: delta-kernel-rs can
    // exhaust a default .NET thread stack on Unix and take the process down with it (see
    // DeltaBigStack's own doc comment) — a risk that applies just as much to a test helper calling
    // straight into the same native FFI as it does to production code.
    public static Task<string> CreateLocalAsync(string dir, long rows, string[]? partitionBy = null) =>
        DeltaBigStack.RunAsync(async () =>
        {
            var location = Path.Combine(dir, "orders");
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.CreateTableAsync(
                new TableCreateOptions(location, Schema) { PartitionBy = partitionBy ?? [], SaveMode = SaveMode.ErrorIfExists },
                default);
            if (rows > 0)
            {
                await table.InsertAsync([Rows(0, rows)], Schema, new InsertOptions { SaveMode = SaveMode.Append }, default);
            }

            return location;
        });

    // The same as above for a fixture that needs particular ROWS rather than a count -- partition
    // values a test chose on purpose, most of the time.
    public static Task<string> CreateLocalFromAsync(
        string dir, IReadOnlyList<(long Id, string Dt, double Amt)> rows, string[]? partitionBy = null,
        string name = "orders") =>
        DeltaBigStack.RunAsync(async () =>
        {
            var location = Path.Combine(dir, name);
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.CreateTableAsync(
                new TableCreateOptions(location, Schema)
                { PartitionBy = partitionBy ?? [], SaveMode = SaveMode.ErrorIfExists },
                default);
            await table.InsertAsync(
                [RowsWithAmounts(rows)], Schema, new InsertOptions { SaveMode = SaveMode.Append }, default);
            return location;
        });

    // Version 0 has the 3-column Schema; a second, schema-evolving overwrite commit makes version 1 a
    // 4-column table -- the one fixture shape that lets a test prove 'version: 0' actually changes what
    // GetSchemaAsync returns, rather than merely accepting the option without acting on it.
    public static Task<string> CreateLocalWithSchemaEvolutionAsync(string dir, long rows) =>
        DeltaBigStack.RunAsync(async () =>
        {
            var location = Path.Combine(dir, "orders");
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.CreateTableAsync(
                new TableCreateOptions(location, Schema) { SaveMode = SaveMode.ErrorIfExists }, default);
            await table.InsertAsync([Rows(0, rows)], Schema, new InsertOptions { SaveMode = SaveMode.Append }, default);
            await table.InsertAsync(
                [WiderRows(rows)], WiderSchema,
                new InsertOptions { SaveMode = SaveMode.Overwrite, OverwriteSchema = true }, default);

            return location;
        });
}
