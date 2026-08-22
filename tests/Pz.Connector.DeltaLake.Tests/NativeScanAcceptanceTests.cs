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

        // WHAT THIS DOES NOT COVER, stated because a check that looks complete is worse than one whose
        // edges are known: the comparison is over CLR types, so a drift WITHIN one CLR type is
        // invisible. A GetSchemaAsync that started declaring String as LargeString, or Date32 as
        // Date64, still maps to the same CLR type and stays green here, where the TestKit's own fact —
        // which compares Arrow DataType.TypeId directly — would have failed.
        //
        // That residue is inherent, not an oversight. The two sides speak different vocabularies:
        // delta-rs declares Arrow types and DuckDB reports .NET ones, and the produced side has no
        // Arrow type ID to compare against. Comparing declared Arrow IDs to each other would prove
        // nothing about the scan, and DuckDB legitimately produces an encoding of its own choosing for
        // a given Delta type, so demanding they match would fail spuriously. The CLR type is the
        // widest common vocabulary that is honest about both.
    }

    /// <summary>Pins <see cref="ClrTypeOf"/> against the DuckDB.NET version actually referenced, for
    /// every Arrow type it claims to know. Needs no delta extension and therefore no network: it asks
    /// DuckDB to produce each type directly rather than scanning a Delta table.
    ///
    /// This exists because the mapping's only job is to be the thing that notices a declared-schema
    /// drift, and it once claimed DATE arrives as DateTime when DuckDB.NET 1.5.5 reports DateOnly — an
    /// error that fails loud rather than silently, but that still defeats the fact it serves.</summary>
    [Fact]
    public async Task DuckDB_reports_the_CLR_types_this_mapping_claims()
    {
        // One column per Arrow type ClrTypeOf knows, in that order. TIMESTAMP appears four times
        // because DuckDB has four widths and the mapping folds them into one arm.
        var columns = new (string Sql, IArrowType Arrow)[]
        {
            ("CAST(1 AS BIGINT)", Int64Type.Default),
            ("CAST(1 AS INTEGER)", Int32Type.Default),
            ("CAST(1 AS DOUBLE)", DoubleType.Default),
            ("CAST(1 AS FLOAT)", FloatType.Default),
            ("true", BooleanType.Default),
            ("'x'", StringType.Default),
            ("'x'", LargeStringType.Default),
            ("'x'", StringViewType.Default),
            ("DATE '2026-01-01'", Date32Type.Default),
            ("DATE '2026-01-01'", Date64Type.Default),
            ("TIMESTAMP '2026-01-01'", new TimestampType(TimeUnit.Microsecond, timezone: (string?)null)),
            ("TIMESTAMP_S '2026-01-01'", new TimestampType(TimeUnit.Second, timezone: (string?)null)),
            ("TIMESTAMP_MS '2026-01-01'", new TimestampType(TimeUnit.Millisecond, timezone: (string?)null)),
            ("TIMESTAMP_NS '2026-01-01'", new TimestampType(TimeUnit.Nanosecond, timezone: (string?)null)),
            ("CAST(1 AS DECIMAL(18,2))", new Decimal128Type(18, 2)),
            ("CAST(1 AS DECIMAL(38,0))", new Decimal128Type(38, 0)),
        };

        using var conn = new DuckDBConnection("DataSource=:memory:");
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "select " + string.Join(", ",
            columns.Select((c, i) => $"{c.Sql} as c{i.ToString(CultureInfo.InvariantCulture)}"));
        using var reader = await cmd.ExecuteReaderAsync();

        for (var i = 0; i < columns.Length; i++)
        {
            Assert.Equal(ClrTypeOf(columns[i].Arrow), reader.GetFieldType(i));
        }
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
        // DATE and TIMESTAMP differ, and the difference is measured rather than assumed: DuckDB.NET
        // 1.5.5's reader reports DateOnly for DATE and DateTime for every TIMESTAMP width
        // (TIMESTAMP_S/_MS/_NS included). Pinned by
        // DuckDB_reports_the_CLR_types_this_mapping_claims below, because a mapping whose whole job is
        // to notice drift is worthless if it is itself wrong.
        ArrowTypeId.Date32 or ArrowTypeId.Date64 => typeof(DateOnly),
        ArrowTypeId.Timestamp => typeof(DateTime),
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
