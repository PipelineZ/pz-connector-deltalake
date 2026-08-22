using Apache.Arrow;
using DuckDB.NET.Data;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>Reads a Delta table back through DuckDB's delta extension. Tests assert against DuckDB
/// rather than against delta-rs deliberately: a write is only correct if the OTHER engine can read it.
/// Installing the extension is a real download, so every caller must gate on
/// <see cref="TestNetwork.CanInstallDuckDbExtensionsAsync"/> and SKIP when it is unavailable —
/// <see cref="DeltaReader"/> carries the assertions that must run offline.</summary>
internal static class DuckDbReader
{
    public static async Task<IReadOnlyList<(long Id, string Dt, double Amt)>> RowsAsync(string location)
    {
        using var conn = await OpenAsync().ConfigureAwait(false);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"select id, dt, amt from delta_scan('{Quote(location)}') order by id";
        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        var rows = new List<(long, string, double)>();
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            rows.Add((reader.GetInt64(0), reader.GetString(1), reader.GetDouble(2)));
        }

        return rows;
    }

    /// <summary>The whole table as Arrow batches, ordered by its first column. Column 0 is the
    /// acceptance suites' identity column, and their content assertions read the FIRST and LAST row of
    /// the returned sequence, so an unordered scan would make them assert against whichever file
    /// delta_scan happened to open first.</summary>
    public static async Task<IReadOnlyList<RecordBatch>> BatchesAsync(string location)
    {
        using var conn = await OpenAsync().ConfigureAwait(false);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"select * from delta_scan('{Quote(location)}') order by 1";
        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

        var rowset = new ArrowRowsetBuilder();
        for (var i = 0; i < reader.FieldCount; i++)
        {
            rowset.DeclareColumn(reader.GetName(i), reader.GetFieldType(i));
        }

        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                rowset.Append(i, reader.IsDBNull(i) ? null : reader.GetValue(i));
            }

            rowset.CompleteRow();
        }

        return rowset.Build() is { } batch ? [batch] : [];
    }

    private static string Quote(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    private static async Task<DuckDBConnection> OpenAsync()
    {
        var conn = new DuckDBConnection("DataSource=:memory:");
        await conn.OpenAsync().ConfigureAwait(false);
        foreach (var s in new[] { "install delta", "load delta" })
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = s;
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        return conn;
    }
}
