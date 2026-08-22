using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>What <see cref="DeltaPartitionPredicate"/> derives, and — mostly — what it refuses to
/// derive. Every refusal here costs a slow merge and buys a correct one; the one case that is not
/// merely cautious is the keys rule, which is what stands between an automatic optimization and a
/// silently duplicated row.
///
/// The literal FORMS these tests assert are the same ones MergeSafetyExecutionTests runs through real
/// delta-rs. Asserted here as strings so a change is visible, proved there so the strings are known to
/// be the right ones — neither test is sufficient alone.</summary>
public class MergeSafetyTests
{
    private static DeltaWriteOptions Opts(string[] keys, string[] partitionBy) =>
        new("merge", keys, partitionBy, null, 1024);

    [Fact]
    public void No_partition_by_means_nothing_to_derive()
    {
        var outcome = DeltaPartitionPredicate.Derive([DeltaTestTable.Rows(0, 10)], Opts(["id"], []));
        Assert.Null(outcome.Filters);
        Assert.Null(outcome.SkipReason);
    }

    [Fact]
    public void A_partition_column_contained_in_keys_derives_the_distinct_values()
    {
        var batch = DeltaTestTable.RowsWithAmounts(
            [(1, "2026-01-01", 1), (2, "2026-01-02", 2), (3, "2026-01-01", 3)]);
        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"]));

        var filter = Assert.Single(outcome.Filters!);
        Assert.Equal("dt", filter.Column);
        Assert.Equal(["'2026-01-01'", "'2026-01-02'"], filter.Literals);
        Assert.Null(outcome.SkipReason);
    }

    [Fact]
    public void Distinct_values_are_pooled_across_every_batch_of_the_write()
    {
        // The buffer a merge commits is the WHOLE write, so a value that appears only in the last
        // batch still has to be in the IN list -- a filter derived from one batch would hide the
        // others' partitions from the target scan.
        var outcome = DeltaPartitionPredicate.Derive(
            [
                DeltaTestTable.RowsWithAmounts([(1, "2026-01-01", 1)]),
                DeltaTestTable.RowsWithAmounts([(2, "2026-01-02", 2)]),
            ],
            Opts(["id", "dt"], ["dt"]));

        Assert.Equal(["'2026-01-01'", "'2026-01-02'"], outcome.Filters!.Single().Literals);
    }

    [Fact]
    public void A_partition_column_NOT_in_keys_is_refused_with_a_reason()
    {
        // THE central safety rule. Deriving here could silently duplicate a row whose partition value
        // changed while its key stayed the same. MergeSafetyExecutionTests demonstrates that duplicate
        // against real delta-rs, so this refusal is not a precaution against a hypothetical.
        var outcome = DeltaPartitionPredicate.Derive([DeltaTestTable.Rows(0, 10)], Opts(["id"], ["dt"]));
        Assert.Null(outcome.Filters);
        Assert.NotNull(outcome.SkipReason);
        Assert.Contains("keys", outcome.SkipReason);
    }

    [Fact]
    public void A_partition_column_matching_a_key_only_by_case_is_still_not_in_keys()
    {
        // Delta column names are case-sensitive, so 'DT' does not name the column 'dt' on either side:
        // it is not covered by the key 'dt', and it does not name a column of the data.
        var outcome = DeltaPartitionPredicate.Derive([DeltaTestTable.Rows(0, 4)], Opts(["id", "dt"], ["DT"]));
        Assert.Null(outcome.Filters);
        Assert.Contains("keys", outcome.SkipReason!);
    }

    [Fact]
    public void A_partition_column_absent_from_the_data_blocks_the_derivation()
    {
        var outcome = DeltaPartitionPredicate.Derive([DeltaTestTable.Rows(0, 4)], Opts(["id", "DT"], ["DT"]));
        Assert.Null(outcome.Filters);
        Assert.Contains("not present", outcome.SkipReason!);
    }

    [Fact]
    public void A_null_partition_value_blocks_the_derivation()
    {
        // Delta stores a null partition under a sentinel directory name; an IN list cannot express it,
        // and IN (NULL) matches nothing — which would hide exactly the rows it needs to find.
        var id = new Int64Array.Builder();
        var dt = new StringArray.Builder();
        var amt = new DoubleArray.Builder();
        id.Append(1);
        dt.AppendNull();
        amt.Append(1.0);
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(true))
            .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
            .Build();
        var batch = new RecordBatch(schema, [id.Build(), dt.Build(), amt.Build()], 1);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"]));
        Assert.Null(outcome.Filters);
        Assert.Contains("null", outcome.SkipReason!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Too_many_distinct_values_blocks_the_derivation()
    {
        // A gigantic IN list stops being a pruning aid and becomes a parse cost.
        var outcome = DeltaPartitionPredicate.Derive(
            [DeltaTestTable.RowsWithAmounts(DistinctPartitions(DeltaPartitionPredicate.MaxDistinctValues + 1))],
            Opts(["id", "dt"], ["dt"]));

        Assert.Null(outcome.Filters);
        Assert.Contains("merge_predicate", outcome.SkipReason!);
    }

    [Fact]
    public void Exactly_the_cap_is_still_derived()
    {
        // The cap is a limit, not an off-by-one: the largest list the connector is willing to emit has
        // to actually come out, or the boundary could drift silently in either direction.
        var outcome = DeltaPartitionPredicate.Derive(
            [DeltaTestTable.RowsWithAmounts(DistinctPartitions(DeltaPartitionPredicate.MaxDistinctValues))],
            Opts(["id", "dt"], ["dt"]));

        Assert.Equal(DeltaPartitionPredicate.MaxDistinctValues, outcome.Filters!.Single().Literals.Count);
    }

    [Fact]
    public void A_write_with_no_rows_derives_nothing_rather_than_an_empty_in_list()
    {
        // PartitionFilter(column, []) renders "IN ()", a parser error inside the merge that surfaces as
        // a generic write failure. The value is never constructed rather than caught downstream.
        var outcome = DeltaPartitionPredicate.Derive(
            [DeltaTestTable.Rows(0, 0)], Opts(["id", "dt"], ["dt"]));

        Assert.Null(outcome.Filters);
        Assert.Null(outcome.SkipReason);
    }

    [Fact]
    public void No_batches_at_all_derives_nothing()
    {
        var outcome = DeltaPartitionPredicate.Derive([], Opts(["id", "dt"], ["dt"]));
        Assert.Null(outcome.Filters);
        Assert.Null(outcome.SkipReason);
    }

    [Theory]
    [InlineData("it's")]
    [InlineData("O''Brien")]
    [InlineData("''")]
    [InlineData("back\\")]
    [InlineData("C:\\path\\")]
    [InlineData("a\\'b")]
    [InlineData("'); drop table t --")]
    public void A_value_carrying_a_quote_or_a_backslash_is_refused_not_escaped(string value)
    {
        // Escaping was measured to be the thing that goes wrong, not the thing that was passed over.
        // A value carrying '' merges as though it carried one quote, so the IN list names a partition
        // the table does not have and the row is inserted a second time with no error;  a value
        // carrying \' fails the whole write with an unterminated-literal parser error. Both are
        // demonstrated against real delta-rs in MergeSafetyExecutionTests. The rule refuses any quote
        // or backslash rather than the two shapes one build of one parser mishandles, because the
        // narrow rule readmits the duplicate the moment that parser's unescaping shifts.
        var batch = DeltaTestTable.RowsWithAmounts([(1, value, 1d)]);
        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"]));

        Assert.Null(outcome.Filters);
        Assert.Contains("quote or a backslash", outcome.SkipReason!);
        Assert.DoesNotContain(value, outcome.SkipReason!);
    }

    [Theory]
    [InlineData("string")]
    [InlineData("stringview")]
    [InlineData("largestring")]
    public void The_refusal_applies_to_every_string_encoding(string encoding)
    {
        // Text() handles three encodings because which one arrives is a property of the plan that
        // produced the batch, not of the column's Delta type — so a refusal pinned for only one of them
        // is pinned for none. Measured consequence of getting this wrong: the value reaches Quote
        // unrefused, the statement generator's own guard rejects the literal, and the merge fails with
        // a coded write error — turning "an unhandleable value costs speed, never the write" into its
        // exact opposite for data that used to write fine.
        var options = Opts(["id", "dt"], ["dt"]);

        Assert.Null(DeltaPartitionPredicate.Derive([OneValue(encoding, "it's")], options).Filters);
        Assert.Null(DeltaPartitionPredicate.Derive([OneValue(encoding, "back\\")], options).Filters);

        // And an ordinary value still derives through the same encoding, so a test that stopped
        // deriving anything at all could not pass by accident.
        Assert.Equal(
            ["'plain'"],
            DeltaPartitionPredicate.Derive([OneValue(encoding, "plain")], options).Filters!.Single().Literals);
    }

    /// <summary>One row carrying <paramref name="value"/> in the requested string encoding.</summary>
    private static RecordBatch OneValue(string encoding, string value)
    {
        IArrowType type = encoding switch
        {
            "string" => StringType.Default,
            "stringview" => StringViewType.Default,
            _ => new LargeStringType(),
        };
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(type).Nullable(false))
            .Build();

        IArrowArray values;
        switch (encoding)
        {
            case "string":
                var s = new StringArray.Builder();
                s.Append(value);
                values = s.Build();
                break;
            case "stringview":
                var v = new StringViewArray.Builder();
                v.Append(value);
                values = v.Build();
                break;
            default:
                var l = new LargeStringArray.Builder();
                l.Append(value);
                values = l.Build();
                break;
        }

        return new RecordBatch(schema, [Ids(1), values], 1);
    }

    [Fact]
    public void Every_string_literal_is_two_quotes_with_nothing_quote_shaped_between_them()
    {
        // The whole contract this file owes the statement generator, checked as a shape rather than
        // value by value: after the refusal above there is nothing left for an escape to express, so
        // a literal that needed one is a literal that should never have been built.
        var batch = DeltaTestTable.RowsWithAmounts(
            [.. HostilePartitionValues.Select((v, i) => ((long)i, v, 1d))]);
        var literals = DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"]))
            .Filters!.Single().Literals;

        Assert.Equal(HostilePartitionValues.Length, literals.Count);
        foreach (var literal in literals)
        {
            Assert.StartsWith("'", literal, StringComparison.Ordinal);
            Assert.EndsWith("'", literal, StringComparison.Ordinal);
            Assert.DoesNotContain("'", literal[1..^1], StringComparison.Ordinal);
            Assert.DoesNotContain("\\", literal[1..^1], StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Derived_literals_are_ordered_so_the_generated_sql_is_deterministic()
    {
        var batch = DeltaTestTable.RowsWithAmounts([(1, "c", 1), (2, "a", 2), (3, "b", 3)]);
        var literals = DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"])).Filters!.Single().Literals;
        Assert.Equal(["'a'", "'b'", "'c'"], literals);
    }

    [Fact]
    public void An_integer_partition_column_renders_as_an_unquoted_literal()
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("yr").DataType(Int32Type.Default).Nullable(false))
            .Build();
        var id = new Int64Array.Builder();
        var yr = new Int32Array.Builder();
        id.Append(1);
        yr.Append(2026);
        var batch = new RecordBatch(schema, [id.Build(), yr.Build()], 1);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "yr"], ["yr"]));
        Assert.Equal(["2026"], outcome.Filters!.Single().Literals);
    }

    [Fact]
    public void A_negative_integer_partition_value_keeps_its_sign()
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("yr").DataType(Int64Type.Default).Nullable(false))
            .Build();
        var id = new Int64Array.Builder();
        var yr = new Int64Array.Builder();
        id.Append(1);
        yr.Append(long.MinValue);
        var batch = new RecordBatch(schema, [id.Build(), yr.Build()], 1);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "yr"], ["yr"]));
        Assert.Equal(["-9223372036854775808"], outcome.Filters!.Single().Literals);
    }

    [Fact]
    public void A_date_partition_column_renders_as_a_quoted_calendar_date()
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("d").DataType(Date32Type.Default).Nullable(false))
            .Build();
        var id = new Int64Array.Builder();
        var d = new Date32Array.Builder();
        id.Append(1);
        d.Append(new DateTime(2026, 3, 4, 0, 0, 0, DateTimeKind.Utc));
        var batch = new RecordBatch(schema, [id.Build(), d.Build()], 1);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "d"], ["d"]));
        Assert.Equal(["'2026-03-04'"], outcome.Filters!.Single().Literals);
    }

    [Fact]
    public void A_string_view_column_renders_the_same_literal_as_a_plain_string_column()
    {
        // Which of the three string encodings arrives is a property of the plan that produced the
        // batch, not of the column's Delta type, so all three have to render identically or the
        // optimization would switch itself off for reasons a user cannot see.
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(StringViewType.Default).Nullable(false))
            .Build();
        var id = new Int64Array.Builder();
        var dt = new StringViewArray.Builder();
        id.Append(1);
        dt.Append("a)b");
        var batch = new RecordBatch(schema, [id.Build(), dt.Build()], 1);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"]));
        Assert.Equal(["'a)b'"], outcome.Filters!.Single().Literals);
    }

    [Fact]
    public void An_unrenderable_partition_column_type_blocks_the_derivation_rather_than_guessing()
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("blob").DataType(BinaryType.Default).Nullable(false))
            .Build();
        var id = new Int64Array.Builder();
        var blob = new BinaryArray.Builder();
        id.Append(1);
        blob.Append([1, 2, 3]);
        var batch = new RecordBatch(schema, [id.Build(), blob.Build()], 1);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "blob"], ["blob"]));
        Assert.Null(outcome.Filters);
        Assert.NotNull(outcome.SkipReason);
    }

    [Fact]
    public void A_boolean_partition_column_blocks_the_derivation()
    {
        // Measured against the shipped library: DataFusion accepts IN (true) and refuses IN ('true')
        // ("Cannot infer common argument type for comparison operation Boolean = Utf8"). A bare
        // true/false is outside the literal alphabet DeltaMergeSql accepts, and emitting one would turn
        // a merge into a coded failure — so a boolean partition column skips the derivation instead.
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("live").DataType(BooleanType.Default).Nullable(false))
            .Build();
        var id = new Int64Array.Builder();
        var live = new BooleanArray.Builder();
        id.Append(1);
        live.Append(true);
        var batch = new RecordBatch(schema, [id.Build(), live.Build()], 1);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "live"], ["live"]));
        Assert.Null(outcome.Filters);
        Assert.Contains("merge_predicate", outcome.SkipReason!);

        // The exclusion is not a preference: emitting the one form that works would fail the write
        // outright. If this stops throwing, boolean pruning has become possible and the refusal above
        // is worth revisiting rather than being left as folklore.
        Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaMergeSql.Build(
                schema, DeltaTestTable.ColumnsOf(schema),
                Opts(["id", "live"], ["live"]), [new PartitionFilter("live", ["true"])]));
    }

    [Fact]
    public void Several_partition_columns_each_get_their_own_in_list()
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("yr").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("rg").DataType(StringType.Default).Nullable(false))
            .Build();
        var id = new Int64Array.Builder();
        var yr = new Int32Array.Builder();
        var rg = new StringArray.Builder();
        id.Append(1); yr.Append(2026); rg.Append("eu");
        id.Append(2); yr.Append(2027); rg.Append("us");
        var batch = new RecordBatch(schema, [id.Build(), yr.Build(), rg.Build()], 2);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "yr", "rg"], ["yr", "rg"]));
        Assert.Equal(2, outcome.Filters!.Count);
        Assert.Equal(["yr", "rg"], outcome.Filters.Select(f => f.Column).ToArray());
    }

    [Fact]
    public void Every_derived_literal_is_one_the_statement_generator_accepts()
    {
        // The generator applies its own defensive check to each literal and fails the write on one it
        // cannot recognize. That check is defence in depth; this is the contract. A value shaped like a
        // second list element, a closing paren or a dollar-quote is legal DATA, and has to survive the
        // round trip as one literal rather than being refused at the last moment.
        var batch = DeltaTestTable.RowsWithAmounts(
            [.. HostilePartitionValues.Select((v, i) => ((long)i, v, 1d))]);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"]));

        var sql = DeltaMergeSql.Build(DeltaTestTable.Schema, DeltaTestTable.Columns,
            Opts(["id", "dt"], ["dt"]), outcome.Filters);
        Assert.Contains("target.\"dt\" IN (", sql);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("too-many")]
    [InlineData("unrenderable")]
    public void No_skip_reason_names_a_partition_value(string cause)
    {
        // SkipReason reaches nobody today -- ABI 0.2.2 gives a sink no channel to put it on -- so this
        // holds a property in reserve rather than protecting a live path. It is worth holding: the day
        // the ABI grows a note or a warning, the reason has to be safe to put on it already, and a
        // reason that had learned to name a value in the meantime would leak the user's data on the
        // commit that started surfacing it. Column names are the connector's own vocabulary; partition
        // values are the user's.
        const string Marker = "confidential-tenant";

        var outcome = cause switch
        {
            "null" => DeltaPartitionPredicate.Derive([NullAfter(Marker)], Opts(["id", "dt"], ["dt"])),
            "too-many" => DeltaPartitionPredicate.Derive(
                [
                    DeltaTestTable.RowsWithAmounts(
                        [.. Enumerable.Range(0, DeltaPartitionPredicate.MaxDistinctValues + 1)
                            .Select(i => ((long)i, $"{Marker}-{i}", 1d))]),
                ],
                Opts(["id", "dt"], ["dt"])),
            _ => DeltaPartitionPredicate.Derive([BinaryValued(Marker)], Opts(["id", "dt"], ["dt"])),
        };

        Assert.Null(outcome.Filters);
        Assert.DoesNotContain(Marker, outcome.SkipReason!, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'dt'", outcome.SkipReason!);
    }

    [Fact]
    public void A_key_that_merely_contains_the_partition_columns_name_does_not_cover_it()
    {
        // Membership, not containment. 'dtx' is a different column from 'dt', so a key list holding it
        // leaves 'dt' outside keys and the derivation unsound — the one near-miss shape on the safety
        // rule that a substring test would let through.
        var outcome = DeltaPartitionPredicate.Derive([DeltaTestTable.Rows(0, 4)], Opts(["id", "dtx"], ["dt"]));
        Assert.Null(outcome.Filters);
        Assert.Contains("keys", outcome.SkipReason!);
    }

    [Fact]
    public void Partition_values_differing_only_in_case_stay_two_literals()
    {
        // Pooling them case-insensitively would name one partition and hide the other, and the merge
        // would insert a second copy of every row in the hidden one.
        var batch = DeltaTestTable.RowsWithAmounts([(1, "EU", 1), (2, "eu", 2)]);
        var literals = DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"]))
            .Filters!.Single().Literals;
        Assert.Equal(["'EU'", "'eu'"], literals);
    }

    [Fact]
    public void A_partition_value_keeps_the_whitespace_it_arrived_with()
    {
        // A Delta partition value is the bytes it was written with, so normalising one here would name
        // a partition the table does not have. Very reachable: a CSV or TSV extract pads freely.
        var batch = DeltaTestTable.RowsWithAmounts([(1, " x ", 1)]);
        Assert.Equal(["' x '"], DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"]))
            .Filters!.Single().Literals);
    }

    [Fact]
    public void A_refusal_on_a_later_partition_column_carries_no_filters_at_all()
    {
        // 'yr' renders and 'blob' does not. The outcome must be a refusal and nothing else: an outcome
        // carrying BOTH a filter and a reason is one a consumer can half-read, and the type says one
        // or the other. The filters for 'yr' would even be sound on their own — that is exactly why
        // nothing but a test stops them leaking out beside the reason.
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("yr").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("blob").DataType(BinaryType.Default).Nullable(false))
            .Build();
        var id = new Int64Array.Builder();
        var yr = new Int32Array.Builder();
        var blob = new BinaryArray.Builder();
        id.Append(1);
        yr.Append(2026);
        blob.Append([1, 2, 3]);
        var batch = new RecordBatch(schema, [id.Build(), yr.Build(), blob.Build()], 1);

        var outcome = DeltaPartitionPredicate.Derive(
            [batch], Opts(["id", "yr", "blob"], ["yr", "blob"]));

        Assert.Null(outcome.Filters);
        Assert.NotNull(outcome.SkipReason);
    }

    [Theory]
    [InlineData(3_000_000)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void A_date_beyond_the_calendar_blocks_the_derivation_rather_than_throwing(int days)
    {
        // Arrow DATE is an int32 day count and reaches far past what .NET's DateTime can express;
        // pz's own hub deals in dates past 9999. Derive runs on the buffered batch BEFORE the merge,
        // so an exception escaping it would abort a write that would otherwise have succeeded — this
        // file's contract is that a value it cannot handle costs speed, never the write.
        var days32 = new ArrowBuffer.Builder<int>();
        days32.Append(days);
        var batch = new RecordBatch(
            DateSchema(Date32Type.Default),
            [Ids(1), new Date32Array(new ArrayData(Date32Type.Default, 1, 0, 0, [ArrowBuffer.Empty, days32.Build()]))],
            1);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "d"], ["d"]));
        Assert.Null(outcome.Filters);
        Assert.Contains("date", outcome.SkipReason!);
    }

    [Fact]
    public void A_date64_beyond_the_calendar_blocks_the_derivation_rather_than_throwing()
    {
        var ms = new ArrowBuffer.Builder<long>();
        ms.Append(long.MaxValue);
        var batch = new RecordBatch(
            DateSchema(Date64Type.Default),
            [Ids(1), new Date64Array(new ArrayData(Date64Type.Default, 1, 0, 0, [ArrowBuffer.Empty, ms.Build()]))],
            1);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "d"], ["d"]));
        Assert.Null(outcome.Filters);
        Assert.Contains("date", outcome.SkipReason!);
    }

    [Fact]
    public void A_dictionary_encoded_partition_column_blocks_the_derivation()
    {
        // Which string encoding arrives is a property of the plan that produced the batch, so a
        // dictionary-encoded low-cardinality column is a plausible arrival — but this one is refused
        // on a fact about the write, not about rendering: measured, delta-rs cannot write a
        // dictionary-encoded PARTITION column at all ("Error partitioning record batch: Missing
        // partition column"), so a literal derived from one would narrow a merge that is already
        // doomed. A dictionary-encoded ORDINARY column writes fine and is never read here.
        var dictionary = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(dictionary).Nullable(false))
            .Build();
        var values = new StringArray.Builder();
        values.Append("p0");
        var indices = new Int32Array.Builder();
        indices.Append(0);
        var batch = new RecordBatch(
            schema, [Ids(1), new DictionaryArray(dictionary, indices.Build(), values.Build())], 1);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"]));
        Assert.Null(outcome.Filters);
        Assert.Contains("dictionary", outcome.SkipReason!);
    }

    private static Schema DateSchema(IArrowType type) =>
        new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("d").DataType(type).Nullable(false))
            .Build();

    private static IArrowArray Ids(int count)
    {
        var id = new Int64Array.Builder();
        for (var i = 0; i < count; i++)
        {
            id.Append(i);
        }

        return id.Build();
    }

    /// <summary>Partition values whose text is a fragment of SQL, and which this connector still
    /// renders. None of them needs a hostile author: a tenant name, a product code or a path can
    /// contain any of these, and the value reaches the generated statement from DATA rather than from
    /// configuration. Two of them look redundant and are not: 'EU'/'eu' differ only in case, and a
    /// deriver that pooled them case-insensitively would name one partition and hide the other, and
    /// ' padded ' would name a partition that does not exist if the value were ever trimmed. Both are
    /// silent duplicates, so both are here and both are merged for real next door.</summary>
    internal static readonly string[] HostilePartitionValues =
    [
        "a)b", "((", "))", "$$x$$", "$tag$y$tag$", "a`b", "/*c*/", "semi;--x", "a, b",
        "line\nbreak", "cr\rreturn", "tab\there", "nul\0byte", "caf\u00e9", " padded ",
        "EU", "eu", "plain",
    ];

    private static List<(long, string, double)> DistinctPartitions(int count) =>
        [.. Enumerable.Range(0, count).Select(i => ((long)i, $"2026-{(i % 12) + 1:D2}-{(i % 28) + 1:D2}-{i}", 1d))];

    /// <summary>One non-null row carrying <paramref name="marker"/>, then a null — so a skip reason that
    /// echoed the values it had already seen would carry the marker with it.</summary>
    private static RecordBatch NullAfter(string marker)
    {
        var id = new Int64Array.Builder();
        var dt = new StringArray.Builder();
        var amt = new DoubleArray.Builder();
        id.Append(1);
        dt.Append(marker);
        amt.Append(1d);
        id.Append(2);
        dt.AppendNull();
        amt.Append(2d);
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(StringType.Default).Nullable(true))
            .Field(f => f.Name("amt").DataType(DoubleType.Default).Nullable(true))
            .Build();
        return new RecordBatch(schema, [id.Build(), dt.Build(), amt.Build()], 2);
    }

    [Fact]
    public async Task A_row_whose_partition_value_changes_is_updated_not_duplicated()
    {
        // The reason the keys rule exists, run end to end rather than argued: partition_by = [dt],
        // keys = [id] — dt is NOT a key, so the derivation must be skipped. If it were derived, the
        // merge would look only in the NEW partition, find no match, and INSERT a second copy of id=1
        // while the old one survived in the old partition. Both copies would be legitimate-looking
        // rows and nothing would report a problem.
        //
        // This test and the one below go through the SINK, not through DeltaPartitionPredicate — the
        // rest of this file asserts what the deriver returns, and these two assert what a real write
        // session does with it.
        var dir = Directory.CreateTempSubdirectory("pz-delta-moving").FullName;
        await using var sink = await ((ISinkConnector)new DeltaLakeConnector())
            .OpenAsync(new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dir }), default);

        var spec = new OutputSpec("lake", "orders", "merge", "fail_on_change",
            new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "dt" } }) { Keys = ["id"] };

        await using (var seed = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default))
        {
            await seed.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(1, "2026-01-01", 10.0)]), default);
            await seed.CommitAsync(default);
        }

        var move = await sink.BeginWriteAsync(spec, DeltaTestTable.Schema, default);
        await using (move)
        {
            await move.WriteBatchAsync(DeltaTestTable.RowsWithAmounts([(1, "2026-06-30", 20.0)]), default);
            await move.CommitAsync(default);
        }

        var rows = await DeltaReader.RowsAsync(Path.Combine(dir, "orders"));
        Assert.Single(rows);
        Assert.Equal(20.0, rows[0].Amt);
        Assert.Equal("2026-06-30", rows[0].Dt);

        // And the deriver recorded WHY it stood aside, naming the column and never the value. Nothing
        // surfaces it yet — the connector ABI has no channel for a note — so this seam is the only
        // place the reason is observable at all.
        var typed = Assert.IsType<DeltaWriteSession>(move);
        Assert.Contains("'dt'", typed.LastSkipReason!);
        Assert.DoesNotContain("2026-06-30", typed.LastSkipReason!);
    }

    [SkippableFact]
    public async Task Merge_cost_follows_the_partitions_the_write_touches_not_the_table()
    {
        // The property a user depends on: a merge into a large table costs what the partitions it
        // touches cost, not what the table costs. Pinned here so that if it breaks it fails in this
        // repository rather than becoming a support ticket.
        //
        // Which mechanism delivers it was measured rather than assumed, and the answer is not the one
        // the plan assumed. delta-rs builds its OWN early filter from the source's partition values
        // whenever the partition column is part of the join — which is exactly and only the case in
        // which deriving one is sound. So the derived IN list is worth nothing measurable, and the
        // second assertion pins that it at least costs nothing either. It stays as a hedge: delta-rs's
        // early filter is an internal optimization with no stability contract, and the first assertion
        // is what would catch it disappearing.
        Skip.IfNot(TestEnvironment.RunSlowBenchmarks, "set PZDL_SLOW_TESTS=1 to run the merge cost regression");

        var r = await MergeCostBench.RunAsync(tableRows: 2_000_000, sourceRows: 1_000);

        Assert.Equal(5, r.Literals);
        Assert.True(r.PartitionJoined * 4 < r.Unpruned,
            $"a merge joined on the partition column ({r.PartitionJoined} ms over {r.Literals} of 200 " +
            $"partitions) should be far cheaper than one that is not ({r.Unpruned} ms)");
        Assert.True(r.Derived < (r.PartitionJoined * 2) + 50,
            $"the derived IN list ({r.Derived} ms) must not cost more than leaving it out " +
            $"({r.PartitionJoined} ms); deriving it took {r.DeriveMs} ms");
    }

    private static RecordBatch BinaryValued(string marker)
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("dt").DataType(BinaryType.Default).Nullable(false))
            .Build();
        var id = new Int64Array.Builder();
        var dt = new BinaryArray.Builder();
        id.Append(1);
        dt.Append(System.Text.Encoding.UTF8.GetBytes(marker));
        return new RecordBatch(schema, [id.Build(), dt.Build()], 1);
    }
}
