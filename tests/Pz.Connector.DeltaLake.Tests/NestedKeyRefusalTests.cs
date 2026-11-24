using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>A nested column is never a merge key or a partition column: its values must reach the merge
/// predicate and the partition path only as flat values.</summary>
public class NestedKeyRefusalTests
{
    public static TheoryData<string, IArrowType> NestedTypes => new()
    {
        { "list", new ListType(Int64Type.Default) },
        { "struct", new StructType([new Field("a", Int64Type.Default, true)]) },
        { "map", new MapType(StringType.Default, Int64Type.Default) },
        { "fixed_size_list", new FixedSizeListType(Int64Type.Default, 2) },
    };

    [Theory]
    [MemberData(nameof(NestedTypes))]
    public void A_nested_merge_key_is_refused_naming_the_column(string kind, IArrowType type)
    {
        var schema = new Schema([new Field("k", type, nullable: false)], null);

        var ex = Assert.Throws<PzConnectorException>(
            () => DeltaMergeDedup.AssertResolvableKeys(schema, ["k"], "out"));

        Assert.Contains(DeltaErrors.UnresolvableMergeKeyType, ex.Message);
        Assert.Contains("'k'", ex.Message);
        Assert.Contains("a merge key must be a flat value", ex.Message);
        Assert.True(kind.Length > 0);
    }

    [Theory]
    [MemberData(nameof(NestedTypes))]
    public void A_nested_partition_column_is_refused_naming_the_column(string kind, IArrowType type)
    {
        var schema = new Schema([new Field("p", type, nullable: true)], null);

        var ex = Assert.Throws<PzConnectorException>(
            () => DeltaTypeSupport.AssertPartitionable(schema, ["p"], "out"));

        Assert.Contains("'p'", ex.Message);
        Assert.Contains("a partition column must be a flat value", ex.Message);
        Assert.True(kind.Length > 0);
    }
}
