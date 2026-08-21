using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class DeltaLocationTests
{
    [Theory]
    [InlineData("s3://warehouse/delta", DeltaScheme.S3)]
    [InlineData("s3a://warehouse/delta", DeltaScheme.S3)]
    [InlineData("az://fs/delta", DeltaScheme.Azure)]
    [InlineData("abfss://fs@acct.dfs.core.windows.net/delta", DeltaScheme.Azure)]
    [InlineData("/mnt/lake", DeltaScheme.Local)]
    [InlineData("file:///mnt/lake", DeltaScheme.Local)]
    public void ClassifyRoot_recognizes_every_supported_scheme(string root, DeltaScheme expected) =>
        Assert.Equal(expected, DeltaLocation.ClassifyRoot(root));

    [Theory]
    [InlineData("hdfs://nn/delta")]
    [InlineData("gs://bucket/delta")]
    [InlineData("relative/path")]
    public void ClassifyRoot_rejects_unsupported_roots_with_PZDL0101(string root)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => DeltaLocation.ClassifyRoot(root));
        Assert.Contains("PZDL0101", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Resolve_defaults_the_path_to_the_entity_name_under_root() =>
        Assert.Equal("s3://warehouse/delta/orders", DeltaLocation.Resolve("s3://warehouse/delta", "orders", null));

    [Fact]
    public void Resolve_tolerates_a_trailing_slash_on_root() =>
        Assert.Equal("s3://warehouse/delta/orders", DeltaLocation.Resolve("s3://warehouse/delta/", "orders", null));

    [Fact]
    public void Resolve_treats_a_relative_path_as_relative_to_root() =>
        Assert.Equal("s3://warehouse/delta/curated/orders",
            DeltaLocation.Resolve("s3://warehouse/delta", "orders", "curated/orders"));

    [Fact]
    public void Resolve_lets_an_absolute_uri_path_override_root_entirely() =>
        Assert.Equal("s3://other/elsewhere",
            DeltaLocation.Resolve("s3://warehouse/delta", "orders", "s3://other/elsewhere"));

    [Fact]
    public void Resolve_normalizes_a_local_root_to_an_absolute_path_with_no_trailing_slash()
    {
        var resolved = DeltaLocation.Resolve("/mnt/lake/", "orders", null);
        Assert.Equal("/mnt/lake/orders", resolved);
    }

    [Fact]
    public void Resolve_never_emits_a_trailing_slash() =>
        Assert.DoesNotContain("orders/", DeltaLocation.Resolve("s3://w/d", "orders", null));
}
