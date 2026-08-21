using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Table;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>Runs the partition predicate <see cref="DeltaPartitionPredicate"/> derives through real
/// delta-rs, against real partitioned Delta tables on local disk.
///
/// The unit suite next door asserts the derived literal is a particular STRING, which only ever tests
/// this connector's model of the SQL dialect against itself. Two things are invisible to any assertion
/// phrased that way, and both are the whole point of the derivation: whether the literal PARSES as one
/// literal, and whether it MATCHES the partition it came from. A literal that mis-escapes fails the
/// first; a literal whose form the dialect coerces differently fails the second, silently, by turning
/// an update into a duplicate.
///
/// So every assertion here is about consequences. A row whose key is already in the table is UPDATED
/// in place, and a row whose key is absent is INSERTED — with the values it was given. A predicate that
/// hides an existing row's partition from the target scan turns the first into a second copy of the
/// same key, which is exactly the harm the keys rule exists to prevent and exactly what these tests
/// watch for.
///
/// Local disk, no docker, no network. Every delta-rs call goes through <see cref="DeltaBigStack"/>,
/// because delta-kernel-rs can exhaust a default .NET thread stack and take the test host down with
/// it — a risk a test shares with production code.</summary>
public class MergeSafetyExecutionTests
{
    private static DeltaWriteOptions Opts(string[] keys, string[] partitionBy) =>
        new("merge", keys, partitionBy, null, 1024);

    [Fact]
    public async Task Every_derived_literal_parses_as_one_literal_and_matches_its_own_partition()
    {
        // Each value is a fragment of SQL — a quote, a closing paren, a dollar-quote opener, a
        // statement separator, a trailing backslash, an embedded NUL. The trailing backslash is the one
        // that would break silently rather than loudly: if this dialect ever honoured backslash
        // escaping inside '...', 'back\' would escape its own closing quote and swallow the list that
        // follows it. Every one of these rows is UPDATED below, so the assertion covers matching, not
        // just parsing.
        var values = MergeSafetyTests.HostilePartitionValues;
        var target = values.Select((v, i) => ((long)i, v, (double)i)).ToList();
        var source = values.Select((v, i) => ((long)i, v, 1000d + i)).ToList();
        source.Add((500L, "plain", 999d));

        var dir = Directory.CreateTempSubdirectory("pz-delta-partition-exec").FullName;
        try
        {
            var location = await CreatePartitionedAsync(dir, target);
            var batch = DeltaTestTable.RowsWithAmounts(source);
            var options = Opts(["id", "dt"], ["dt"]);

            var outcome = DeltaPartitionPredicate.Derive([batch], options);
            Assert.Null(outcome.SkipReason);
            Assert.Equal(values.Length, outcome.Filters!.Single().Literals.Count);

            await MergeAsync(location, DeltaMergeSql.Build(DeltaTestTable.Schema, options, outcome.Filters), batch);

            var after = await DeltaReader.RowsAsync(location);
            Assert.Equal(values.Length + 1, after.Count);
            foreach (var (id, dt, amt) in source)
            {
                var row = Assert.Single(after, r => r.Id == id);
                Assert.Equal(dt, row.Dt);
                Assert.Equal(amt, row.Amt);
            }
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Theory]
    [InlineData(" padded ")]
    [InlineData("EU")]
    public async Task The_assertion_has_teeth_because_a_filter_missing_one_partition_duplicates_its_rows(
        string missing)
    {
        // The same write with one value struck out of the derived list. Without this, "every row was
        // updated" could be passing because the IN list never reaches the target scan at all — in which
        // case the test above would prove nothing about pruning, and a genuinely unsound predicate
        // would sail through it.
        //
        // The two values struck out are the two a plausible edit would lose: a deriver that trimmed
        // whitespace would stop naming ' padded ', and one that pooled values case-insensitively would
        // stop naming 'EU' beside 'eu'. Both are silent duplicates, and this is what one looks like.
        var values = MergeSafetyTests.HostilePartitionValues;
        var target = values.Select((v, i) => ((long)i, v, (double)i)).ToList();
        var source = values.Select((v, i) => ((long)i, v, 1000d + i)).ToList();

        var dir = Directory.CreateTempSubdirectory("pz-delta-partition-exec").FullName;
        try
        {
            var location = await CreatePartitionedAsync(dir, target);
            var batch = DeltaTestTable.RowsWithAmounts(source);
            var options = Opts(["id", "dt"], ["dt"]);

            var literals = DeltaPartitionPredicate.Derive([batch], options).Filters!.Single().Literals
                .Where(l => l != "'" + missing + "'").ToList();
            Assert.Equal(values.Length - 1, literals.Count);

            await MergeAsync(
                location,
                DeltaMergeSql.Build(DeltaTestTable.Schema, options, [new PartitionFilter("dt", literals)]),
                batch);

            var after = await DeltaReader.RowsAsync(location);
            var orphan = System.Array.IndexOf(values, missing);
            Assert.Equal(values.Length + 1, after.Count);
            Assert.Equal(2, after.Count(r => r.Id == orphan));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Theory]
    [InlineData("O''Brien")]
    [InlineData("''")]
    [InlineData("a''b")]
    public async Task A_value_carrying_a_doubled_quote_is_refused_because_escaping_it_duplicates_the_row(
        string value)
    {
        // Why the refusal is not caution. The literal below is what escaping produces — the value with
        // every quote doubled, which is what this dialect's own rule says an escape is. Merged with no
        // partition predicate the row MATCHES and is updated; merged with that literal in the IN list
        // the same row is invisible to the target scan and a second copy of the same key is inserted,
        // with no error anywhere. Same table, same data, only the filter differs.
        //
        // The statement is built by hand because both ends of this connector now refuse the literal —
        // which is the point: the two sides agree, and this is the measurement they agree about.
        var escaped = "'" + value.Replace("'", "''") + "'";

        Assert.Null(DeltaPartitionPredicate.Derive(
            [DeltaTestTable.RowsWithAmounts([(0L, value, 1d)])], Opts(["id", "dt"], ["dt"])).Filters);

        var dir = Directory.CreateTempSubdirectory("pz-delta-partition-exec").FullName;
        try
        {
            var source = DeltaTestTable.RowsWithAmounts([(0L, value, 42d)]);

            var control = await CreatePartitionedAsync(dir, [(0L, value, 0d)], "control");
            await MergeAsync(control, Merge("target.\"id\" = source.\"id\""), source);
            var matched = Assert.Single(await DeltaReader.RowsAsync(control));
            Assert.Equal(42d, matched.Amt);

            var filtered = await CreatePartitionedAsync(dir, [(0L, value, 0d)], "filtered");
            await MergeAsync(
                filtered,
                Merge($"target.\"id\" = source.\"id\" AND target.\"dt\" IN ({escaped})"),
                DeltaTestTable.RowsWithAmounts([(0L, value, 42d)]));

            var after = await DeltaReader.RowsAsync(filtered);
            Assert.Equal(2, after.Count);
            Assert.Equal(2, after.Count(r => r.Id == 0));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Theory]
    [InlineData("a\\'b")]
    [InlineData("lead\\'x")]
    public async Task A_value_carrying_a_backslash_before_a_quote_is_refused_because_escaping_it_fails_the_write(
        string value)
    {
        // The louder half of the same measurement. Escaping this value produces a literal the parser
        // cannot terminate, and the failure is an uncoded one that takes the whole write with it — in
        // a file whose contract is that a value it cannot handle costs speed, never the write.
        var escaped = "'" + value.Replace("'", "''") + "'";

        Assert.Null(DeltaPartitionPredicate.Derive(
            [DeltaTestTable.RowsWithAmounts([(0L, value, 1d)])], Opts(["id", "dt"], ["dt"])).Filters);

        var dir = Directory.CreateTempSubdirectory("pz-delta-partition-exec").FullName;
        try
        {
            var location = await CreatePartitionedAsync(dir, [(0L, value, 0d)]);
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => MergeAsync(
                location,
                Merge($"target.\"id\" = source.\"id\" AND target.\"dt\" IN ({escaped})"),
                DeltaTestTable.RowsWithAmounts([(0L, value, 42d)])));

            Assert.Contains("Unterminated string literal", failure.Message, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>The statement shape <see cref="DeltaMergeSql.Build"/> produces for this fixture, with
    /// the ON clause supplied by hand — the two tests above need an ON clause the generator now
    /// refuses to build, which is exactly what they are demonstrating.</summary>
    private static string Merge(string on) =>
        $"MERGE INTO target USING source ON {on}\n" +
        "WHEN MATCHED THEN UPDATE SET target.\"dt\" = source.\"dt\", target.\"amt\" = source.\"amt\"\n" +
        "WHEN NOT MATCHED THEN INSERT (\"id\", \"dt\", \"amt\") VALUES " +
        "(source.\"id\", source.\"dt\", source.\"amt\")";

    [Fact]
    public async Task A_partition_column_outside_keys_is_refused_because_deriving_it_would_duplicate_a_row()
    {
        // The keys rule, demonstrated rather than argued. One row moves partition while its key stays
        // the same — a corrected date, a re-bucketed region, an ordinary correction. Merged with no
        // partition predicate it is updated in place; merged with the predicate the derivation WOULD
        // have produced had it not refused, the same write silently leaves two rows with the same key.
        var options = Opts(["id"], ["dt"]);
        var moved = DeltaTestTable.RowsWithAmounts([(0L, "2026-01-02", 42d)]);

        var refusal = DeltaPartitionPredicate.Derive([moved], options);
        Assert.Null(refusal.Filters);
        Assert.Contains("keys", refusal.SkipReason!);

        var dir = Directory.CreateTempSubdirectory("pz-delta-partition-exec").FullName;
        try
        {
            var safe = await CreatePartitionedAsync(dir, [(0L, "2026-01-01", 0d)], "safe");
            await MergeAsync(safe, DeltaMergeSql.Build(DeltaTestTable.Schema, options, null), moved);

            var afterSafe = await DeltaReader.RowsAsync(safe);
            var row = Assert.Single(afterSafe);
            Assert.Equal("2026-01-02", row.Dt);
            Assert.Equal(42d, row.Amt);

            var unsafeLocation = await CreatePartitionedAsync(dir, [(0L, "2026-01-01", 0d)], "unsafe");
            await MergeAsync(
                unsafeLocation,
                DeltaMergeSql.Build(DeltaTestTable.Schema, options, [new PartitionFilter("dt", ["'2026-01-02'"])]),
                moved);

            var afterUnsafe = await DeltaReader.RowsAsync(unsafeLocation);
            Assert.Equal(2, afterUnsafe.Count);
            Assert.Equal(2, afterUnsafe.Count(r => r.Id == 0));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Theory]
    [InlineData("string")]
    [InlineData("stringview")]
    [InlineData("int8")]
    [InlineData("int16")]
    [InlineData("int32")]
    [InlineData("int64")]
    [InlineData("uint8")]
    [InlineData("uint16")]
    [InlineData("uint32")]
    [InlineData("uint64")]
    [InlineData("date32")]
    [InlineData("date64")]
    public async Task Every_partition_column_type_the_deriver_renders_matches_its_own_partition(string kind)
    {
        // One case per branch of the deriver's literal table. A branch that renders a form this dialect
        // coerces differently does not fail here — it MATCHES NOTHING, and the merge inserts a second
        // copy of a key that is already in the table, which is what the row count catches. This is the
        // evidence the table rests on: a type is rendered because it was measured, and a type that
        // cannot be measured is skipped instead.
        var (schema, rows) = Fixture(kind);

        var dir = Directory.CreateTempSubdirectory("pz-delta-partition-exec").FullName;
        try
        {
            var location = Path.Combine(dir, "t");
            await DeltaBigStack.RunAsync(async () =>
            {
                using var engine = new DeltaEngine(EngineOptions.Default);
                var table = await engine.CreateTableAsync(
                    new TableCreateOptions(location, schema) { PartitionBy = ["p"], SaveMode = SaveMode.ErrorIfExists },
                    default);
                await table.InsertAsync(
                    [rows(0, 3, 1d)], schema, new InsertOptions { SaveMode = SaveMode.Append }, default);
                return 0;
            });

            var options = Opts(["id", "p"], ["p"]);
            var batch = rows(0, 2, 42d);
            var outcome = DeltaPartitionPredicate.Derive([batch], options);
            Assert.Null(outcome.SkipReason);
            Assert.Equal(2, outcome.Filters!.Single().Literals.Count);

            await DeltaBigStack.RunAsync(async () =>
            {
                using var engine = new DeltaEngine(EngineOptions.Default);
                var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
                await table.MergeAsync(DeltaMergeSql.Build(schema, options, outcome.Filters), [batch], schema, default);
                return 0;
            });

            // Three rows in, two of them merged by key: still three rows means both matched. Four or
            // five means the derived literal named a partition the scan could not find.
            Assert.Equal(3, await CountAsync(location));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>A table of (id, p, amt) whose partition column p has the Arrow type under test, and a
    /// builder for <c>count</c> consecutive rows of it starting at <c>from</c>. Each row gets its own
    /// partition value, so a fixture of three rows is a table of three partitions.</summary>
    private static (Schema Schema, Func<long, int, double, RecordBatch> Rows) Fixture(string kind)
    {
        IArrowType type = kind switch
        {
            "string" => StringType.Default,
            "stringview" => StringViewType.Default,
            "int8" => Int8Type.Default,
            "int16" => Int16Type.Default,
            "int32" => Int32Type.Default,
            "int64" => Int64Type.Default,
            "uint8" => UInt8Type.Default,
            "uint16" => UInt16Type.Default,
            "uint32" => UInt32Type.Default,
            "uint64" => UInt64Type.Default,
            "date32" => Date32Type.Default,
            _ => Date64Type.Default,
        };

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("p").DataType(type).Nullable(false))
            .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
            .Build();

        IArrowArray Partitions(long from, int count)
        {
            switch (kind)
            {
                case "string":
                    var s = new StringArray.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        s.Append($"p{from + i}");
                    }

                    return s.Build();
                case "stringview":
                    var sv = new StringViewArray.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        sv.Append($"p{from + i}");
                    }

                    return sv.Build();
                case "int8":
                    var i8 = new Int8Array.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        i8.Append((sbyte)(10 + from + i));
                    }

                    return i8.Build();
                case "int16":
                    var i16 = new Int16Array.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        i16.Append((short)(1000 + from + i));
                    }

                    return i16.Build();
                case "int32":
                    var i32 = new Int32Array.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        i32.Append((int)(2020 + from + i));
                    }

                    return i32.Build();
                case "int64":
                    var i64 = new Int64Array.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        i64.Append(9000000000L + from + i);
                    }

                    return i64.Build();
                case "uint8":
                    var u8 = new UInt8Array.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        u8.Append((byte)(10 + from + i));
                    }

                    return u8.Build();
                case "uint16":
                    var u16 = new UInt16Array.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        u16.Append((ushort)(1000 + from + i));
                    }

                    return u16.Build();
                case "uint32":
                    var u32 = new UInt32Array.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        u32.Append((uint)(2020 + from + i));
                    }

                    return u32.Build();
                case "uint64":
                    var u64 = new UInt64Array.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        u64.Append((ulong)(2020 + from + i));
                    }

                    return u64.Build();
                case "date32":
                    var d32 = new Date32Array.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        d32.Append(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(from + i));
                    }

                    return d32.Build();
                default:
                    var d64 = new Date64Array.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        d64.Append(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(from + i));
                    }

                    return d64.Build();
            }
        }

        RecordBatch Rows(long from, int count, double amt)
        {
            var id = new Int64Array.Builder();
            var a = new DoubleArray.Builder();
            for (var i = 0; i < count; i++)
            {
                id.Append(from + i);
                a.Append(amt);
            }

            return new RecordBatch(schema, [id.Build(), Partitions(from, count), a.Build()], count);
        }

        return (schema, Rows);
    }

    private static Task<string> CreatePartitionedAsync(
        string dir, IReadOnlyList<(long Id, string Dt, double Amt)> rows, string name = "orders") =>
        DeltaBigStack.RunAsync(async () =>
        {
            // No ConfigureAwait(false) on this delegate's own awaits: DeltaBigStack pumps plain awaits
            // back onto the big-stack thread, and opting out would run delta-rs on a default-stack pool
            // thread.
            var location = Path.Combine(dir, name);
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.CreateTableAsync(
                new TableCreateOptions(location, DeltaTestTable.Schema)
                {
                    PartitionBy = ["dt"],
                    SaveMode = SaveMode.ErrorIfExists,
                },
                default);
            await table.InsertAsync(
                [DeltaTestTable.RowsWithAmounts(rows)], DeltaTestTable.Schema,
                new InsertOptions { SaveMode = SaveMode.Append }, default);
            return location;
        });

    private static Task MergeAsync(string location, string sql, RecordBatch source) =>
        DeltaBigStack.RunAsync(async () =>
        {
            using var engine = new DeltaEngine(EngineOptions.Default);
            var table = await engine.LoadTableAsync(new TableOptions { TableLocation = location }, default);
            try
            {
                await table.MergeAsync(sql, [source], DeltaTestTable.Schema, default);
            }
            finally
            {
                if (table is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }

            return 0;
        });

    /// <summary>A row count that does not care what the table's columns are — the typed fixtures each
    /// have a different partition column type, so <see cref="DeltaReader"/>'s shaped read cannot serve
    /// them.</summary>
    private static Task<long> CountAsync(string location) =>
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
}
