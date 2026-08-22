using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Errors;
using DeltaLake.Table;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>What actually happens to a partition VALUE on its way into a directory NAME, measured
/// rather than assumed. Two user-facing claims rest on this and neither had a test until now: that a
/// value is percent-escaped on the way in (so a short string can still exceed the filesystem's
/// path-component cap), and that an empty value comes back as null.
///
/// The second claim's OUTCOME is right and its usual EXPLANATION is wrong, which is why the mechanism
/// is pinned here: an empty value and a null land in different directories and are recorded
/// differently in the log. The value is lost on the READ, by both engines, not by an ambiguous
/// directory name.</summary>
public class PartitionValueEncodingTests
{
    private static readonly Schema NullableDt = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(true))
        .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
        .Build();

    private static readonly Schema NotNullDt = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
        .Build();

    /// <summary>Every measured input paired with the directory name it produced. A plain alphanumeric
    /// value is in the list on purpose: escaping is selective, and a test that only fed it characters
    /// needing escapes could not tell selective escaping from blanket escaping.</summary>
    public static TheoryData<string, string> Escapes => new()
    {
        { "abc", "dt=abc" },
        { "a b", "dt=a%20b" },
        { "a/b", "dt=a%2Fb" },
        { "a%b", "dt=a%25b" },
        { "a=b", "dt=a%3Db" },
        { "a:b", "dt=a%3Ab" },
        { "a+b", "dt=a%2Bb" },
        { "a\\b", "dt=a%5Cb" },
        { "a'b", "dt=a%27b" },
        { "a\nb", "dt=a%0Ab" },
        { "a\tb", "dt=a%09b" },
        { "café", "dt=caf%C3%A9" },
        { "日本", "dt=%E6%97%A5%E6%9C%AC" },
        { "😀", "dt=%F0%9F%98%80" },
    };

    [Theory]
    [MemberData(nameof(Escapes))]
    public async Task A_partition_value_is_percent_escaped_into_its_directory_name_and_survives_the_round_trip(
        string value, string expectedDirectory)
    {
        var dir = TempDir("pz-delta-escape");
        await WriteAsync(dir, [(1L, value, 1.0)]);
        var location = Path.Combine(dir, "orders");

        Assert.Equal([expectedDirectory], PartitionDirectories(location));

        // The escape is a rendering, not a change to the data: the log carries the value verbatim and
        // both engines hand it back unchanged.
        Assert.Equal(value, PartitionValueInLog(location));
        Assert.Equal(value, await SingleDtAsync(location));
    }

    [Fact]
    public async Task Escaping_alone_can_push_a_short_value_past_the_filesystems_path_component_cap()
    {
        // 90 spaces is 90 bytes of UTF-8 — a third of the 255-byte cap — and 270 bytes once every one
        // of them becomes %20. Nothing but the escaping puts this value over the limit, which is what
        // makes it the proof for "a shorter string can still exceed it".
        var value = new string(' ', 90);
        Assert.Equal(90, Encoding.UTF8.GetByteCount(value));
        Assert.Equal(270, Encoding.UTF8.GetByteCount(Uri.EscapeDataString(value)));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await WriteAsync(TempDir("pz-delta-escape-cap"), [(1L, value, 1.0)]));

        Assert.Contains(DeltaErrors.UnusablePartitionValue, ex.Message);
        Assert.DoesNotContain(value, ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task The_log_path_is_the_uri_encoding_of_the_directory_name_so_an_escape_is_escaped_again()
    {
        // The protocol specifies add.path as a URI that a reader must decode. The directory name is
        // already escaped, so its '%' is escaped a second time on the way into the log: a reader
        // decoding add.path once gets the on-disk name, not the value. Anything walking the log by
        // hand has to know that; this connector never does, which is part of why it does not parse the
        // log at all.
        var dir = TempDir("pz-delta-escape-path");
        await WriteAsync(dir, [(1L, "a/b", 1.0)]);
        var location = Path.Combine(dir, "orders");

        var path = AddActions(location).Single().GetProperty("path").GetString()!;
        Assert.StartsWith("dt=a%252Fb/", path, StringComparison.Ordinal);
        Assert.Equal("dt=a%2Fb", Uri.UnescapeDataString(path).Split('/')[0]);
        Assert.Equal("dt=a%2Fb", Path.GetFileName(
            Directory.EnumerateDirectories(location).Single(d => Path.GetFileName(d) != "_delta_log")));
    }

    [Fact]
    public async Task An_empty_partition_value_is_distinct_from_a_null_on_disk_and_in_the_log_yet_reads_back_null()
    {
        // The reason PZDL0406 refuses an empty partition value, pinned to the mechanism rather than to
        // a plausible story about it. delta-rs writes the two apart — 'dt=' for an empty string,
        // 'dt=__HIVE_DEFAULT_PARTITION__' for a null, and "" against null in the log — so the
        // directory name is NOT ambiguous. The value is lost when the column is reconstructed on the
        // read, where nothing can recover it and no error is raised.
        //
        // Written through delta-rs directly because the connector refuses this write, which is the
        // whole point: this is what it is protecting the user from.
        var dir = TempDir("pz-delta-empty-vs-null");
        var location = Path.Combine(dir, "orders");
        await CreateThroughDeltaRsAsync(location, [(0L, string.Empty, 0.0), (1L, null, 1.0), (2L, "x", 2.0)]);

        Assert.Equal(["dt=", "dt=__HIVE_DEFAULT_PARTITION__", "dt=x"], PartitionDirectories(location));

        var recorded = AddActions(location)
            .Select(a => a.GetProperty("partitionValues").GetProperty("dt"))
            .Select(v => v.ValueKind == JsonValueKind.Null ? "<null>" : v.GetString()!)
            .Order(StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(["", "<null>", "x"], recorded);

        Assert.Equal(["<null>", "<null>", "x"], await DtColumnAsync(location));
    }

    [Fact]
    public async Task An_empty_partition_value_in_a_NOT_NULL_partition_column_is_refused_as_if_it_were_a_null()
    {
        // The other half of the conflation, and the half that fails loudly. The directory name and the
        // log keep an empty value and a null apart, but delta-rs does not: converting the partition
        // values of a NOT NULL column, an empty string arrives as a null and the write is refused for
        // being one. So "an empty value behaves as a null" is right about the OUTCOME on both paths —
        // it is only the usual explanation, that the directory name cannot tell them apart, that is
        // wrong.
        var dir = TempDir("pz-delta-empty-notnull");
        var ex = await Assert.ThrowsAsync<DeltaRuntimeException>(
            async () => await CreateThroughDeltaRsAsync(
                Path.Combine(dir, "orders"), [(1L, string.Empty, 1.0)], NotNullDt));

        Assert.Contains("nulls", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("dt", ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task DuckDb_also_reads_an_empty_partition_value_back_as_null()
    {
        // The claim is that the empty value cannot survive a round trip, not that delta-rs mishandles
        // it. Two independent implementations agreeing is what makes that a property of the format
        // rather than a defect to report upstream.
        Skip.IfNot(await TestNetwork.CanInstallDuckDbExtensionsAsync(),
            "the DuckDB delta extension could not be installed");

        var dir = TempDir("pz-delta-empty-duckdb");
        var location = Path.Combine(dir, "orders");
        await CreateThroughDeltaRsAsync(location, [(0L, string.Empty, 0.0), (1L, null, 1.0), (2L, "x", 2.0)]);

        var dt = (await DuckDbReader.BatchesAsync(location))
            .SelectMany(b => Values((StringArray)b.Column("dt"), b.Length))
            .ToArray();
        Assert.Equal(["<null>", "<null>", "x"], dt);
    }

    /// <summary>The table's partition directories, ordered, with the log directory dropped.</summary>
    private static string[] PartitionDirectories(string location) =>
        [.. Directory.EnumerateDirectories(location)
            .Select(d => Path.GetFileName(d)!)
            .Where(n => n != "_delta_log")
            .Order(StringComparer.Ordinal)];

    private static string TempDir(string name) => Directory.CreateTempSubdirectory(name).FullName;

    private static async Task WriteAsync(string root, IReadOnlyList<(long Id, string? Dt, double Amt)> rows)
    {
        await using var sink = await ((ISinkConnector)new DeltaLakeConnector())
            .OpenAsync(new ConnectorConfig(new Dictionary<string, object?> { ["root"] = root }), default);
        var spec = new OutputSpec("lake", "orders", "append", "fail_on_change",
            new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "dt" } });

        await using var session = await sink.BeginWriteAsync(spec, NullableDt, default);
        await session.WriteBatchAsync(Batch(rows), default);
        await session.CommitAsync(default);
    }

    private static Task CreateThroughDeltaRsAsync(
        string location, IReadOnlyList<(long Id, string? Dt, double Amt)> rows, Schema? schema = null) =>
        DeltaBigStack.RunAsync(async () =>
        {
            var s = schema ?? NullableDt;
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.CreateTableAsync(
                new TableCreateOptions(location, s)
                { PartitionBy = ["dt"], SaveMode = SaveMode.ErrorIfExists }, default);
            await table.InsertAsync([Batch(rows, s)], s, new InsertOptions { SaveMode = SaveMode.Append }, default);
            return 0;
        });

    private static RecordBatch Batch(IReadOnlyList<(long Id, string? Dt, double Amt)> rows, Schema? schema = null)
    {
        var id = new Int64Array.Builder();
        var dt = new StringArray.Builder();
        var amt = new DoubleArray.Builder();
        foreach (var r in rows)
        {
            id.Append(r.Id);
            if (r.Dt is null) { dt.AppendNull(); } else { dt.Append(r.Dt); }
            amt.Append(r.Amt);
        }

        return new RecordBatch(schema ?? NullableDt, [id.Build(), dt.Build(), amt.Build()], rows.Count);
    }

    private static IEnumerable<JsonElement> AddActions(string location) =>
        Directory.EnumerateFiles(Path.Combine(location, "_delta_log"), "*.json")
            .Order(StringComparer.Ordinal)
            .SelectMany(File.ReadAllLines)
            .Select(l => JsonDocument.Parse(l).RootElement)
            .Where(e => e.TryGetProperty("add", out _))
            .Select(e => e.GetProperty("add"));

    private static string PartitionValueInLog(string location) =>
        AddActions(location).Single().GetProperty("partitionValues").GetProperty("dt").GetString()!;

    private static async Task<string> SingleDtAsync(string location) =>
        (await DtColumnAsync(location)).Single();

    private static async Task<IReadOnlyList<string>> DtColumnAsync(string location) =>
        [.. (await DeltaReader.BatchesAsync(location))
            .SelectMany(b => Values((StringArray)b.Column("dt"), b.Length))];

    /// <summary>A column's values with a null rendered as the literal "&lt;null&gt;". A null is one of
    /// the answers under test here, so it has to be comparable rather than filtered out — and no value
    /// this suite writes could collide with the sentinel.</summary>
    private static IEnumerable<string> Values(StringArray column, int length)
    {
        for (var i = 0; i < length; i++)
        {
            yield return column.IsNull(i) ? "<null>" : column.GetString(i);
        }
    }
}
