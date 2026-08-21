using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class DeltaWindowSqlTests
{
    private static DatasetSpec Spec(string? cursor = null, string? value = null, string? upper = null,
        bool inclusive = false) =>
        new("lake", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = cursor, WatermarkValue = value,
            WatermarkUpperBound = upper, WatermarkLowerInclusive = inclusive,
        };

    private const string Fragment = "delta_scan('/mnt/lake/orders')";

    [Fact]
    public void A_spec_with_no_watermark_is_returned_byte_identical() =>
        Assert.Equal(Fragment, DeltaWindowSql.Wrap(Fragment, Spec()));

    [Fact]
    public void A_cursor_with_a_null_value_is_equivalent_to_no_watermark()
    {
        // The engine stamps the cursor NAME on every incremental spec — including a first run and
        // --full-refresh — while the value stays null until a watermark actually exists.
        Assert.Equal(Fragment, DeltaWindowSql.Wrap(Fragment, Spec(cursor: "updated_at")));
    }

    [Fact]
    public void A_lower_bound_alone_is_pushed_as_a_strict_comparison() =>
        Assert.Equal(
            "(select * from delta_scan('/mnt/lake/orders') where \"updated_at\" > '2026-01-01')",
            DeltaWindowSql.Wrap(Fragment, Spec("updated_at", "2026-01-01")));

    [Fact]
    public void An_inclusive_lower_bound_is_pushed_as_a_non_strict_comparison() =>
        Assert.Equal(
            "(select * from delta_scan('/mnt/lake/orders') where \"updated_at\" >= '2026-01-01')",
            DeltaWindowSql.Wrap(Fragment, Spec("updated_at", "2026-01-01", inclusive: true)));

    [Fact]
    public void A_full_window_pushes_both_bounds_with_an_inclusive_ceiling() =>
        Assert.Equal(
            "(select * from delta_scan('/mnt/lake/orders') where \"updated_at\" > '2026-01-01' " +
            "and \"updated_at\" <= '2026-02-01')",
            DeltaWindowSql.Wrap(Fragment, Spec("updated_at", "2026-01-01", "2026-02-01")));

    [Fact]
    public void A_cursor_name_containing_a_quote_is_escaped_by_doubling() =>
        Assert.Contains("\"we\"\"ird\"", DeltaWindowSql.Wrap(Fragment, Spec("we\"ird", "1")));

    [Fact]
    public void A_cursor_value_containing_a_quote_is_escaped_by_doubling() =>
        Assert.Contains("'it''s'", DeltaWindowSql.Wrap(Fragment, Spec("c", "it's")));

    [Fact]
    public void An_upper_bound_without_a_value_is_a_broken_pairing_and_fails_loudly()
    {
        // The engine always stamps lower and upper together. A null value here means that pairing
        // broke upstream; failing loudly beats a NullReferenceException with no explanation.
        Assert.Throws<InvalidOperationException>(
            () => DeltaWindowSql.Wrap(Fragment, Spec("c", null, "2026-02-01")));
    }
}
