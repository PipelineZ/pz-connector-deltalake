using System.Text.RegularExpressions;
using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>A doc that lies is a bug. These tests hold the committed documentation to the code both
/// ways: an option nobody documented fails the build, and so does a documented option that no longer
/// exists.
///
/// They govern <c>README.md</c>, everything under <c>docs/</c>, and the sample READMEs — and
/// deliberately NOT <c>docs/superpowers/</c>. That tree is working material between the user and the
/// assistant, is git-excluded and never committed, and so exists on an author's machine and not in a
/// fresh clone: a walk that included it would answer differently depending on which checkout it ran
/// in, which is worse than not checking at all. Every recursive walk below therefore drops it.</summary>
public class DocsDriftTests
{
    private const string SuperpowersDirectory = "superpowers";

    [Fact]
    public void Every_connection_option_is_documented_and_nothing_extra_is()
    {
        Assert.Equal(
            DeltaLakeSchemas.ConnectionOptions.Order().ToArray(),
            DocumentedOptions("reference/connection.md").Order().ToArray());
    }

    [Fact]
    public void Every_read_option_is_documented_and_nothing_extra_is()
    {
        Assert.Equal(
            DeltaLakeSchemas.ReadOptions.Order().ToArray(),
            DocumentedOptions("reference/read.md").Order().ToArray());
    }

    [Fact]
    public void Every_write_option_is_documented_and_nothing_extra_is()
    {
        Assert.Equal(
            DeltaLakeSchemas.WriteOptions.Order().ToArray(),
            DocumentedOptions("reference/write.md").Order().ToArray());
    }

    [Fact]
    public void Every_error_code_appears_in_troubleshooting()
    {
        var page = Docs("troubleshooting.md");
        Assert.All(DeltaErrors.AllCodes, code => Assert.Contains(code, page, StringComparison.Ordinal));
    }

    [Fact]
    public void Troubleshooting_invents_no_codes_the_connector_cannot_raise()
    {
        var mentioned = Regex.Matches(Docs("troubleshooting.md"), @"PZDL\d{4}").Select(m => m.Value).Distinct();
        Assert.All(mentioned, code => Assert.Contains(code, DeltaErrors.AllCodes));
    }

    /// <summary>pz refuses <c>mode:</c> on a write and <c>outputs:</c> as a block — both are retired
    /// spellings, and a copy-pasted example using either fails for the reader before it reaches this
    /// connector at all.</summary>
    [Fact]
    public void No_yaml_example_uses_a_spelling_pz_has_retired()
    {
        var problems = new List<string>();
        foreach (var (file, block) in GovernedYamlBlocks())
        {
            if (block.Contains("mode:", StringComparison.Ordinal))
            {
                problems.Add($"{file}: a yaml example uses 'mode:'; pz's own name is 'strategy:'");
            }

            if (Regex.IsMatch(block, @"^\s*outputs:", RegexOptions.Multiline))
            {
                problems.Add($"{file}: a yaml example uses the retired 'outputs:' block (PZ0347); " +
                             "declare the write under 'entities: <e>: write:' or as sink() arguments");
            }
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A copy-pasted example that does not validate is worse than no example.</summary>
    [Fact]
    public void Every_yaml_example_parses_and_its_connector_options_are_recognized()
    {
        var problems = GovernedYamlBlocks()
            .SelectMany(b => DocsYamlChecker.Problems(b.Block, b.File))
            .ToList();

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A number of unknown provenance cannot be reproduced or challenged, so the cost table
    /// has to carry the stack and the shape it was measured on. Scoped to the cost section rather than
    /// the page: the same version string appears elsewhere on the page, and a check the rest of the
    /// page can satisfy would pass with the parameters stripped off the table itself.</summary>
    [Fact]
    public void The_merge_cost_table_carries_the_parameters_it_was_measured_with()
    {
        var section = Section("reference/write.md", "## What a merge costs");
        Assert.Contains("DeltaLake.Net 0.33.0", section, StringComparison.Ordinal);
        Assert.Contains("partitions", section, StringComparison.Ordinal);
        Assert.Contains("scattered", section, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void The_readme_states_the_dependency_size_and_the_supported_platforms()
    {
        var readme = File.ReadAllText(Path.Combine(RepoRoot(), "README.md"));
        Assert.Contains("MB", readme, StringComparison.Ordinal);
        Assert.Contains("linux-x64", readme, StringComparison.Ordinal);
        Assert.Contains("musl", readme, StringComparison.Ordinal);
    }

    /// <summary>Every link into this repository resolves. The documentation is only useful as a set —
    /// a page that promises a section on another page which nobody wrote sends a reader to a 404, and
    /// that is the one failure mode a docs tree in a separate repository cannot afford.</summary>
    [Fact]
    public void Every_link_into_this_repository_resolves_to_a_file_that_exists()
    {
        const string blobPrefix = "https://github.com/coccor/pz-connector-deltalake/blob/main/";
        var root = RepoRoot();
        var problems = new List<string>();

        foreach (var file in GovernedMarkdown())
        {
            foreach (Match match in Regex.Matches(File.ReadAllText(file), @"\]\(([^)\s]+)\)"))
            {
                var target = match.Groups[1].Value.Split('#')[0];
                if (target.Length == 0)
                {
                    continue;
                }

                string resolved;
                if (target.StartsWith(blobPrefix, StringComparison.Ordinal))
                {
                    resolved = Path.Combine(root, target[blobPrefix.Length..]);
                }
                else if (target.Contains("://", StringComparison.Ordinal) ||
                         target.StartsWith("mailto:", StringComparison.Ordinal))
                {
                    continue;
                }
                else
                {
                    resolved = Path.Combine(Path.GetDirectoryName(file)!, target);
                }

                if (!File.Exists(resolved) && !Directory.Exists(resolved))
                {
                    problems.Add($"{Path.GetRelativePath(root, file)}: link '{target}' resolves to nothing");
                }
            }
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>One `## `-level section of a page, heading to the next heading of the same level.</summary>
    private static string Section(string page, string heading)
    {
        var lines = Docs(page).Split('\n');
        var start = Array.FindIndex(lines, l => l.TrimEnd() == heading);
        Assert.True(start >= 0, $"docs/{page}: no '{heading}' section");

        var length = Array.FindIndex(lines, start + 1, l => l.StartsWith("## ", StringComparison.Ordinal));
        return string.Join('\n', lines[(start + 1)..(length < 0 ? lines.Length : length)]);
    }

    private static string Docs(string relative) =>
        File.ReadAllText(Path.Combine(RepoRoot(), "docs", relative));

    private static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Pz.Connector.DeltaLake.slnx")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("repository root not found");
    }

    /// <summary>The reference pages' option tables, read out of the <c>## Options</c> section alone.
    /// Scoping the scan to that one section is what lets the rest of a reference page use ordinary
    /// tables — a worked example, a cost table — without a row of one accidentally registering as an
    /// option nobody implemented. A row in that section that is not the documented four-cell shape is
    /// a failure rather than a skip: silently ignoring it would let an option go undocumented while
    /// still looking documented.</summary>
    private static IReadOnlyList<string> DocumentedOptions(string page)
    {
        var text = Docs(page);
        var lines = text.Split('\n');
        var start = Array.FindIndex(lines, l => l.TrimEnd() == "## Options");
        Assert.True(start >= 0, $"docs/{page}: no '## Options' section");

        var options = new List<string>();
        for (var i = start + 1; i < lines.Length; i++)
        {
            var line = lines[i].Trim();
            if (line.StartsWith("## ", StringComparison.Ordinal))
            {
                break;
            }

            if (!line.StartsWith('|') || line.StartsWith("|---", StringComparison.Ordinal))
            {
                continue;
            }

            var cells = line.Trim('|').Split('|').Select(c => c.Trim()).ToArray();
            if (cells[0] == "option")
            {
                continue;
            }

            Assert.True(cells.Length == 4 && Regex.IsMatch(cells[0], "^`[a-z_]+`$"),
                $"docs/{page}: option row is not `| `name` | type | default | notes |`: {line}");
            options.Add(cells[0].Trim('`'));
        }

        return options;
    }

    /// <summary>The committed documentation set: the two READMEs a reader lands on and everything
    /// under <c>docs/</c> except the working tree that is never committed.</summary>
    private static IEnumerable<string> GovernedMarkdown()
    {
        var root = RepoRoot();
        yield return Path.Combine(root, "README.md");

        foreach (var file in Directory
                     .EnumerateFiles(Path.Combine(root, "docs"), "*.md", SearchOption.AllDirectories)
                     .Where(f => !IsWorkingMaterial(root, f))
                     .Order(StringComparer.Ordinal))
        {
            yield return file;
        }

        foreach (var file in Directory
                     .EnumerateFiles(Path.Combine(root, "samples"), "*.md", SearchOption.AllDirectories)
                     .Order(StringComparer.Ordinal))
        {
            yield return file;
        }
    }

    private static bool IsWorkingMaterial(string root, string file) =>
        Path.GetRelativePath(Path.Combine(root, "docs"), file)
            .Split(Path.DirectorySeparatorChar)[0] == SuperpowersDirectory;

    private static IEnumerable<(string File, string Block)> GovernedYamlBlocks() =>
        from file in GovernedMarkdown()
        let text = File.ReadAllText(file)
        from match in Regex.Matches(text, "```yaml\n(.*?)```", RegexOptions.Singleline)
        select (Path.GetRelativePath(RepoRoot(), file), match.Groups[1].Value);
}
