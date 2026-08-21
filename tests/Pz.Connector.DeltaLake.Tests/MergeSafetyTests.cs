using Apache.Arrow;
using Apache.Arrow.Types;
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

    [Fact]
    public void Derived_literals_are_escaped_not_concatenated()
    {
        var batch = DeltaTestTable.RowsWithAmounts([(1, "it's", 1.0)]);
        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"]));
        Assert.Equal(["'it''s'"], outcome.Filters!.Single().Literals);
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
        dt.Append("it's");
        var batch = new RecordBatch(schema, [id.Build(), dt.Build()], 1);

        var outcome = DeltaPartitionPredicate.Derive([batch], Opts(["id", "dt"], ["dt"]));
        Assert.Equal(["'it''s'"], outcome.Filters!.Single().Literals);
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
                schema, Opts(["id", "live"], ["live"]), [new PartitionFilter("live", ["true"])]));
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

        var sql = DeltaMergeSql.Build(DeltaTestTable.Schema, Opts(["id", "dt"], ["dt"]), outcome.Filters);
        Assert.Contains("target.\"dt\" IN (", sql);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("too-many")]
    [InlineData("unrenderable")]
    public void No_skip_reason_names_a_partition_value(string cause)
    {
        // SkipReason reaches run artifacts and logs. Column names are the connector's own vocabulary;
        // partition values are the user's data and must not travel with them.
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

    /// <summary>Partition values whose text is a fragment of SQL. None of them needs a hostile author:
    /// a tenant name, a product code or a path can contain any of these, and the value reaches the
    /// generated statement from DATA rather than from configuration.</summary>
    internal static readonly string[] HostilePartitionValues =
    [
        "it's", "a)b", "$$x$$", "back\\", "trailing\\\\", "semi;--x", "'); drop table t --",
        "a, 'b", "line\nbreak", "nul\0byte", "plain",
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
