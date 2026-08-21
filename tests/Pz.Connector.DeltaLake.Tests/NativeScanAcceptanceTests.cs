using System.Globalization;
using Apache.Arrow.Types;
using DuckDB.NET.Data;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>The source acceptance contract, carried on the tier this connector actually reads on.
///
/// <see cref="DeltaLakeSourceAcceptanceTests"/> — the TestKit's own source suite — skips in full,
/// because every data-plane fact in it reads through <c>PlanReadAsync</c> and this source has no
/// universal tier. The properties those facts assert are still owed, so each one is restated here
/// against a real <c>delta_scan</c> executed by a real DuckDB, which is exactly what pz's engine does
/// with the fragment <c>TryGetNativeScan</c> hands back.
///
/// The mapping, fact for fact:
/// <list type="bullet">
/// <item><description><c>Validate_accepts_valid_config</c> → <see cref="A_valid_config_validates"/>,
/// the one skipped fact that needed no read tier at all.</description></item>
/// <item><description><c>Schema_matches_produced_batches</c> →
/// <see cref="The_declared_schema_matches_what_the_scan_produces"/>. The declared schema comes from
/// delta-rs and the scan from DuckDB, so this compares two independent implementations rather than one
/// against itself.</description></item>
/// <item><description><c>Read_is_deterministic_across_two_reads</c> →
/// <see cref="Two_reads_of_the_same_scan_agree"/>.</description></item>
/// <item><description><c>Partitions_union_equals_single_partition_read</c> →
/// <see cref="One_scan_returns_every_row_of_the_table_exactly_once"/>. A native scan has no partitions
/// to union — DuckDB plans the read — so the property that survives is the one the union fact exists
/// to protect: no row lost and none duplicated, measured against the other engine's own
/// count.</description></item>
/// <item><description><c>BoundedWindow_filters_to_lower_exclusive_upper_inclusive_cursor_range</c> →
/// <see cref="A_bounded_window_returns_lower_exclusive_upper_inclusive"/>, with the same 0..10 seed and
/// the same expected 4,5,6,7.</description></item>
/// <item><description><c>Inclusive_watermark_bound_returns_boundary_row</c> →
/// <see cref="An_inclusive_lower_bound_returns_the_boundary_row_a_strict_one_omits"/>.</description></item>
/// </list>
///
/// Every fact that executes a scan needs DuckDB's delta extension, which is a real download, so those
/// SKIP offline. That is not a weakening of the guarantee — there is no offline way to execute a scan
/// whose whole definition is "DuckDB reads it".</summary>
public class NativeScanAcceptanceTests
{
    private static ConnectorConfig Cfg(string root) => new(new Dictionary<string, object?> { ["root"] = root });

    private static DatasetSpec Spec(string entity) => new("lake", entity, new Dictionary<string, object?>());

    [Fact]
    public async Task A_valid_config_validates()
    {
        var result = await new DeltaLakeConnector().ValidateAsync(Cfg("/tmp/pz-delta-any"), default);
        Assert.True(result.IsValid);
    }

    [SkippableFact]
    public async Task The_declared_schema_matches_what_the_scan_produces()
    {
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(), "delta extension unavailable offline");
        var (root, connector) = await SeedAsync("schema", rows: 120);

        await using var source = await ((ISourceConnector)connector).OpenAsync(Cfg(root), default);
        var declared = await source.GetSchemaAsync(Spec("orders"), default);
        Assert.True(source.TryGetNativeScan(Spec("orders"), out var scan));

        var produced = await ColumnsAsync(scan!);

        Assert.Equal(
            declared.Schema.FieldsList.Select(f => f.Name),
            produced.Select(c => c.Name));

        // DECLARED type against PRODUCED type, field by field — not against a literal. What the
        // TestKit fact compares is the declared DataType.TypeId against the produced batch's, so a
        // replacement that only pinned the produced side against a hard-coded triple would go green on
        // a GetSchemaAsync that started declaring the wrong types, which is the regression that
        // matters: pz types a pipeline's SQL against this schema at compile time, so a declared-type
        // drift ships silently and surfaces as SQL checked against a shape the data does not have.
        Assert.Equal(
            declared.Schema.FieldsList.Select(f => ClrTypeOf(f.DataType)),
            produced.Select(c => c.Type));

        // And the seed's own shape, so the pairwise check above cannot be satisfied by BOTH sides
        // drifting together — a schema that declared everything as a string and a scan that produced
        // strings would agree with each other and be wrong.
        Assert.Equal(
            new[] { typeof(long), typeof(string), typeof(double) },
            produced.Select(c => c.Type));
    }

    /// <summary>The CLR type DuckDB's reader hands back for a given Arrow type — the only vocabulary
    /// the two engines share, since delta-rs declares Arrow and DuckDB reports .NET types. Covers the
    /// types this connector's own fixtures produce; anything else is a loud failure rather than a
    /// silent pass, because a mapping that quietly returned null would make the comparison above
    /// vacuous for exactly the field that drifted.</summary>
    private static Type ClrTypeOf(IArrowType type) => type.TypeId switch
    {
        ArrowTypeId.Int64 => typeof(long),
        ArrowTypeId.Int32 => typeof(int),
        ArrowTypeId.Double => typeof(double),
        ArrowTypeId.Float => typeof(float),
        ArrowTypeId.Boolean => typeof(bool),
        ArrowTypeId.String or ArrowTypeId.LargeString or ArrowTypeId.StringView => typeof(string),
        ArrowTypeId.Date32 or ArrowTypeId.Date64 or ArrowTypeId.Timestamp => typeof(DateTime),
        ArrowTypeId.Decimal128 => typeof(decimal),
        _ => throw new NotSupportedException(
            $"no DuckDB CLR type known for Arrow {type.Name}; add it rather than letting the " +
            "declared-vs-produced comparison skip the field"),
    };

    [SkippableFact]
    public async Task Two_reads_of_the_same_scan_agree()
    {
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(), "delta extension unavailable offline");
        var (root, connector) = await SeedAsync("determinism", rows: 150);

        await using var source = await ((ISourceConnector)connector).OpenAsync(Cfg(root), default);
        Assert.True(source.TryGetNativeScan(Spec("orders"), out var scan));

        var first = await DigestAsync(scan!);
        var second = await DigestAsync(scan!);

        Assert.Equal(150, first.Rows);
        Assert.Equal(first.Rows, second.Rows);
        Assert.Equal(first.Digest, second.Digest);
    }

    [SkippableFact]
    public async Task One_scan_returns_every_row_of_the_table_exactly_once()
    {
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(), "delta extension unavailable offline");
        var (root, connector) = await SeedAsync("union", rows: 137);

        await using var source = await ((ISourceConnector)connector).OpenAsync(Cfg(root), default);
        Assert.True(source.TryGetNativeScan(Spec("orders"), out var scan));

        var scanned = await CursorsAsync(scan!);

        // Ground truth from the OTHER engine: delta-rs reads the same table through its own planner, so
        // a row DuckDB's scan lost or doubled shows up as a disagreement rather than as a number that
        // merely looks plausible.
        var truth = await DeltaReader.RowsAsync(Path.Combine(root, "orders"));

        Assert.Equal(truth.Select(r => r.Id).Order(), scanned.Order());
        Assert.Equal(scanned.Count, scanned.Distinct().Count());
    }

    [SkippableFact]
    public async Task A_bounded_window_returns_lower_exclusive_upper_inclusive()
    {
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(), "delta extension unavailable offline");
        // Cursor values 0..10, the seed the TestKit's own BoundedWindow fact is specified against.
        var (root, connector) = await SeedAsync("window", rows: 11);

        await using var source = await ((ISourceConnector)connector).OpenAsync(Cfg(root), default);
        var windowed = Spec("orders") with
        {
            WatermarkCursor = "id",
            WatermarkValue = "3",
            WatermarkUpperBound = "7",
        };
        Assert.True(source.TryGetNativeScan(windowed, out var scan));

        Assert.Equal([4L, 5L, 6L, 7L], (await CursorsAsync(scan!)).Order());
    }

    [SkippableFact]
    public async Task An_inclusive_lower_bound_returns_the_boundary_row_a_strict_one_omits()
    {
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(), "delta extension unavailable offline");
        var (root, connector) = await SeedAsync("inclusive", rows: 11);

        await using var source = await ((ISourceConnector)connector).OpenAsync(Cfg(root), default);
        var strictSpec = Spec("orders") with { WatermarkCursor = "id", WatermarkValue = "4" };
        var inclusiveSpec = strictSpec with { WatermarkLowerInclusive = true };

        Assert.True(source.TryGetNativeScan(strictSpec, out var strict));
        Assert.True(source.TryGetNativeScan(inclusiveSpec, out var inclusive));

        var strictCursors = await CursorsAsync(strict!);
        var inclusiveCursors = await CursorsAsync(inclusive!);

        // Both halves, or the fact would pass on a connector that ignored the flag and returned the
        // boundary row either way.
        Assert.DoesNotContain(4L, strictCursors);
        Assert.Contains(4L, inclusiveCursors);
        Assert.Equal(strictCursors.Count + 1, inclusiveCursors.Count);
    }

    private static async Task<(string Root, DeltaLakeConnector Connector)> SeedAsync(string name, int rows)
    {
        var root = Directory.CreateTempSubdirectory($"pz-delta-native-{name}").FullName;
        await DeltaTestTable.CreateAtAsync(Path.Combine(root, "orders"), rows);
        return (root, new DeltaLakeConnector());
    }

    /// <summary>Opens DuckDB, runs the scan's own SetupStatements, and hands back a connection ready to
    /// execute its fragment — the same two steps pz's engine performs, in the same order.</summary>
    private static async Task<DuckDBConnection> OpenAsync(NativeScan scan)
    {
        var conn = new DuckDBConnection("DataSource=:memory:");
        await conn.OpenAsync();
        foreach (var statement in scan.SetupStatements)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = statement;
            await cmd.ExecuteNonQueryAsync();
        }

        return conn;
    }

    private static async Task<IReadOnlyList<(string Name, Type Type)>> ColumnsAsync(NativeScan scan)
    {
        using var conn = await OpenAsync(scan);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"select * from {scan.SqlFragment} limit 0";
        using var reader = await cmd.ExecuteReaderAsync();
        return [.. Enumerable.Range(0, reader.FieldCount).Select(i => (reader.GetName(i), reader.GetFieldType(i)))];
    }

    private static async Task<IReadOnlyList<long>> CursorsAsync(NativeScan scan)
    {
        using var conn = await OpenAsync(scan);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"select id from {scan.SqlFragment}";
        using var reader = await cmd.ExecuteReaderAsync();
        var values = new List<long>();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetInt64(0));
        }

        return values;
    }

    /// <summary>Row count plus an order-insensitive digest over every column of every row — the same
    /// shape the TestKit's own determinism check uses, so a read that returned the right COUNT of the
    /// wrong rows is still caught.</summary>
    private static async Task<(long Rows, string Digest)> DigestAsync(NativeScan scan)
    {
        using var conn = await OpenAsync(scan);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"select * from {scan.SqlFragment}";
        using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<string>();
        while (await reader.ReadAsync())
        {
            var cells = Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.IsDBNull(i)
                    ? "\u0000NULL"
                    : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? string.Empty);
            rows.Add(string.Join('\u0001', cells));
        }

        rows.Sort(StringComparer.Ordinal);
        var bytes = System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(string.Join('\n', rows)));
        return (rows.Count, Convert.ToHexString(bytes));
    }
}
