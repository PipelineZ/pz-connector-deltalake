using YamlDotNet.Serialization;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>Validates a fenced yaml block from the documentation the way pz and this connector would
/// between them: pz lifts its own names out of a <c>read:</c>/<c>write:</c> block and hands the rest
/// to the connector, which refuses an option it does not implement. An example that would be refused
/// is a bug in the documentation, not in the reader's fingers.
///
/// Only a mapping that declares <c>connector: deltalake</c> is checked. A block showing a project.yml
/// fragment, or another connector's connection, is left alone — this checker owns one connector's
/// surface and must not invent rules for the rest of pz.
///
/// Problems are returned, never thrown, so one run reports every bad block instead of the first.</summary>
internal static class DocsYamlChecker
{
    /// <summary>Names pz reads out of an <c>entities: &lt;e&gt;: read:</c> block itself; the connector
    /// never sees them, so they are legal in an example without appearing in ReadOptions.</summary>
    private static readonly string[] PzReadNames = ["columns", "sync", "retry"];

    /// <summary>The same, for <c>write:</c>. pz strips these before an OutputSpec reaches a sink.</summary>
    private static readonly string[] PzWriteNames =
        ["strategy", "keys", "duplicates", "on_delete", "schema_policy", "retry"];

    /// <summary>Top-level names in a connections.yml-shaped document that declare a `connector:`.
    /// A document that is not connections.yml-shaped — a project.yml fragment, a bare list — yields
    /// none rather than throwing: the caller is comparing sets, not validating.</summary>
    public static HashSet<string> ConnectionNames(string yaml)
    {
        object? graph;
        try
        {
            graph = new DeserializerBuilder().Build().Deserialize<object>(new StringReader(yaml));
        }
        catch (Exception)
        {
            return [];
        }

        if (graph is not IDictionary<object, object> map)
        {
            return [];
        }

        return [.. map
            .Where(kv => kv.Value is IDictionary<object, object> body && body.ContainsKey("connector"))
            .Select(kv => kv.Key as string ?? kv.Key.ToString() ?? string.Empty)];
    }

    public static IReadOnlyList<string> Problems(string block, string file)
    {
        object? graph;
        try
        {
            graph = new DeserializerBuilder().Build().Deserialize<object>(new StringReader(block));
        }
        catch (Exception ex)
        {
            return [$"{file}: a yaml example does not parse: {ex.Message}"];
        }

        var problems = new List<string>();
        Walk(graph, file, problems);
        return problems;
    }

    private static void Walk(object? node, string file, List<string> problems)
    {
        switch (node)
        {
            case IDictionary<object, object> map:
                if (map.TryGetValue("connector", out var connector) && connector as string == "deltalake")
                {
                    CheckConnection(map, file, problems);
                }

                foreach (var value in map.Values)
                {
                    Walk(value, file, problems);
                }

                break;

            case IEnumerable<object> list:
                foreach (var item in list)
                {
                    Walk(item, file, problems);
                }

                break;
        }
    }

    private static void CheckConnection(IDictionary<object, object> connection, string file, List<string> problems)
    {
        var allowed = DeltaLakeSchemas.ConnectionOptions.Concat(["connector", "entities"]).ToArray();
        foreach (var key in Keys(connection).Where(k => !allowed.Contains(k, StringComparer.Ordinal)))
        {
            problems.Add($"{file}: connection option '{key}' is not one this connector accepts");
        }

        if (!connection.ContainsKey("root"))
        {
            problems.Add($"{file}: a deltalake connection example omits the required 'root'");
        }

        if (!connection.TryGetValue("entities", out var entities) ||
            entities is not IDictionary<object, object> entityMap)
        {
            return;
        }

        foreach (var (name, body) in entityMap)
        {
            if (body is not IDictionary<object, object> directions)
            {
                problems.Add($"{file}: entity '{name}' declares no 'read:' or 'write:' block");
                continue;
            }

            foreach (var direction in Keys(directions))
            {
                switch (direction)
                {
                    case "read":
                        CheckOptions(directions["read"], DeltaLakeSchemas.ReadOptions, PzReadNames,
                            $"{file}: entity '{name}' read", problems);
                        break;

                    case "write":
                        CheckOptions(directions["write"], DeltaLakeSchemas.WriteOptions, PzWriteNames,
                            $"{file}: entity '{name}' write", problems);

                        // partition_by used to be refused here: pz read it as one column substituting
                        // calendar tokens in the sink's path and rejected it (PZ0219) when the path
                        // carried none, so a documented example declaring it could not be pasted into
                        // a pz project. pz now reads it as the columns an output is partitioned by --
                        // a name or a list -- and lets `path:` decide who lays the partitions out, so
                        // a Delta write declaring it is an ordinary, pasteable example.
                        break;

                    default:
                        problems.Add($"{file}: entity '{name}': unknown key '{direction}'; " +
                                     "an entity holds 'read:' and/or 'write:', nothing else");
                        break;
                }
            }
        }
    }

    private static void CheckOptions(
        object? block, IReadOnlyList<string> connectorNames, string[] pzNames, string where, List<string> problems)
    {
        // A bodyless `read:`/`write:` is legal and means "the defaults"; it parses as null, not a map.
        if (block is null)
        {
            return;
        }

        if (block is not IDictionary<object, object> options)
        {
            problems.Add($"{where}: must be a mapping of options, or empty");
            return;
        }

        foreach (var key in Keys(options)
                     .Where(k => !connectorNames.Contains(k, StringComparer.Ordinal))
                     .Where(k => !pzNames.Contains(k, StringComparer.Ordinal)))
        {
            problems.Add($"{where}: option '{key}' is neither a pz name nor one this connector accepts");
        }
    }

    private static IEnumerable<string> Keys(IDictionary<object, object> map) =>
        map.Keys.Select(k => k as string ?? k.ToString() ?? string.Empty);
}
