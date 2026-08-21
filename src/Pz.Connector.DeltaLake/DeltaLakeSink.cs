using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Errors;
using DeltaLake.Interfaces;
using DeltaLake.Table;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>Per-output write options, read once at BeginWriteAsync. Every name here equals its YAML
/// key, so an option declared as a sink() keyword argument and the same option declared under
/// <c>write:</c> resolve identically — they are never merged, only ever one or the other.</summary>
internal sealed record DeltaWriteOptions(
    string Mode,
    IReadOnlyList<string> Keys,
    IReadOnlyList<string> PartitionBy,
    string? MergePredicate,
    long TargetFileBytes)
{
    /// <summary>128 MiB: large enough that an ordinary append lands one file per generation, small
    /// enough that a large append does not hold the whole write in memory.</summary>
    private const long DefaultTargetFileBytes = 128L * 1024 * 1024;

    private readonly record struct Problem(string Code, string What, string NextStep);

    public static DeltaWriteOptions From(OutputSpec spec)
    {
        var problems = new List<Problem>();
        CheckNames(spec, problems);

        var partitionBy = StringList(spec, "partition_by", problems);
        var mergePredicate = Text(spec, "merge_predicate", problems);
        var targetFileBytes = PositiveInt64(spec, "target_file_bytes", problems);

        if (spec.Mode == "merge" && spec.Keys.Count == 0)
        {
            problems.Add(new Problem(DeltaErrors.MergeWithoutKeys,
                "strategy 'merge' requires a non-empty 'keys' list",
                "add the columns that identify a row, e.g. keys: [order_id]"));
        }

        if (problems.Count > 0)
        {
            throw Aggregate(spec, problems);
        }

        return new DeltaWriteOptions(
            spec.Mode, spec.Keys, partitionBy, mergePredicate, targetFileBytes ?? DefaultTargetFileBytes);
    }

    /// <summary>The only validation output options ever get: pz schema-validates source dataset
    /// options but not sink output options, so an unrecognized name here would otherwise be a silent
    /// no-op that reads like a working setting. Every problem found is collected and reported in one
    /// exception — a user fixing four typos one run at a time is a user running the pipeline four
    /// times.</summary>
    private static void CheckNames(OutputSpec spec, List<Problem> problems)
    {
        // Ordinal ordering, not the dictionary's: two runs over the same config must produce the same
        // message, and a Dictionary's enumeration order is not a promise.
        foreach (var key in spec.Options.Keys.Order(StringComparer.Ordinal))
        {
            if (DeltaLakeSchemas.WriteOptions.Contains(key, StringComparer.Ordinal))
            {
                continue;
            }

            if (key == "mode")
            {
                problems.Add(new Problem(DeltaErrors.InvalidWriteOption,
                    "'mode' is not a write option",
                    "use pz's own 'strategy' instead — strategy: append | replace | merge"));
                continue;
            }

            if (key == "version")
            {
                problems.Add(new Problem(DeltaErrors.VersionOnWrite,
                    "'version' is a read option (time travel) and cannot appear on a write",
                    "remove 'version' from the write"));
                continue;
            }

            var suggestion = DeltaLakeSchemas.WriteOptions.OrderBy(o => Distance(o, key)).First();
            problems.Add(new Problem(DeltaErrors.InvalidWriteOption,
                $"unknown write option '{key}'",
                $"did you mean '{suggestion}'? Known write options: " +
                string.Join(", ", DeltaLakeSchemas.WriteOptions)));
        }
    }

    private static PzConnectorException Aggregate(OutputSpec spec, List<Problem> problems)
    {
        if (problems.Count == 1)
        {
            return DeltaErrors.Fail(problems[0].Code, $"output '{spec.Output}': {problems[0].What}",
                problems[0].NextStep);
        }

        // One exception carries them all: the first problem's code heads the message so the exception
        // still has a single code, and every problem repeats its own code inline so none is lost.
        return DeltaErrors.Fail(
            problems[0].Code,
            $"output '{spec.Output}': {problems.Count} problems with this write — " +
            string.Join("; ", problems.Select(p => $"[{p.Code}] {p.What}")),
            string.Join(" ", problems.Select(p => p.NextStep).Distinct(StringComparer.Ordinal)));
    }

    /// <summary>Levenshtein distance, used only to name the nearest known option in an error.</summary>
    private static int Distance(string a, string b)
    {
        var d = new int[a.Length + 1, b.Length + 1];
        for (var i = 0; i <= a.Length; i++) { d[i, 0] = i; }
        for (var j = 0; j <= b.Length; j++) { d[0, j] = j; }
        for (var i = 1; i <= a.Length; i++)
        {
            for (var j = 1; j <= b.Length; j++)
            {
                var cost = a[i - 1] == b[j - 1] ? 0 : 1;
                d[i, j] = Math.Min(Math.Min(d[i - 1, j] + 1, d[i, j - 1] + 1), d[i - 1, j - 1] + cost);
            }
        }

        return d[a.Length, b.Length];
    }

    /// <summary>A list-valued option. A bare string is refused rather than read as an empty list: a
    /// <c>partition_by: dt</c> that quietly produced an unpartitioned table would be a silent, and
    /// permanent, layout change.</summary>
    private static IReadOnlyList<string> StringList(OutputSpec spec, string key, List<Problem> problems)
    {
        if (!spec.Options.TryGetValue(key, out var value) || value is null)
        {
            return [];
        }

        if (value is string or not System.Collections.IEnumerable)
        {
            problems.Add(new Problem(DeltaErrors.InvalidWriteOption,
                $"write option '{key}' must be a list of column names (got the single value '{value}')",
                $"write it as a list — {key}: [{value}]"));
            return [];
        }

        var items = ((System.Collections.IEnumerable)value).Cast<object?>()
            .Select(x => x?.ToString() ?? string.Empty).ToList();
        if (items.Any(string.IsNullOrWhiteSpace))
        {
            problems.Add(new Problem(DeltaErrors.InvalidWriteOption,
                $"write option '{key}' contains an empty column name",
                $"remove the empty entry from {key}"));
            return [];
        }

        return items;
    }

    private static string? Text(OutputSpec spec, string key, List<Problem> problems)
    {
        if (!spec.Options.TryGetValue(key, out var value) || value is null)
        {
            return null;
        }

        if (value is not string text)
        {
            problems.Add(new Problem(DeltaErrors.InvalidWriteOption,
                $"write option '{key}' must be a string (got '{value}')",
                $"quote the value of {key}"));
            return null;
        }

        return text;
    }

    /// <summary>A whole-number option. Convert.ToInt64 is deliberately not used: it raises
    /// FormatException/OverflowException, which is not a PzConnectorException and so reaches the user
    /// with no code, no output name and no next step. The value stays a long because a byte count
    /// routinely exceeds int.MaxValue and narrowing would silently truncate it. Zero and negatives are
    /// refused rather than passed on: a non-positive file target would flush a generation after every
    /// single batch.</summary>
    private static long? PositiveInt64(OutputSpec spec, string key, List<Problem> problems)
    {
        if (!spec.Options.TryGetValue(key, out var value) || value is null)
        {
            return null;
        }

        var text = value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture);
        if (!long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            problems.Add(new Problem(DeltaErrors.InvalidWriteOption,
                $"write option '{key}' must be a whole number (got '{value}')",
                $"set {key} to a positive whole number"));
            return null;
        }

        if (parsed <= 0)
        {
            problems.Add(new Problem(DeltaErrors.InvalidWriteOption,
                $"write option '{key}' must be greater than zero (got {parsed})",
                $"set {key} to a positive whole number"));
            return null;
        }

        return parsed;
    }
}

/// <summary>The write half, on the universal Arrow path. There is no native-copy alternative: DuckDB's
/// delta extension is read-only, and the engine's native-COPY branch never opens a write session, so
/// there would be no point at which a Delta commit could happen.</summary>
internal sealed class DeltaLakeSink : ISink
{
    /// <summary>delta-rs's own <c>NotATable</c> error code — the ONE load failure that means "create
    /// it". DeltaLake.Net 0.33.0 keeps its <c>DeltaTableErrorCode</c> enum internal, so the value is
    /// pinned here as a literal and a test asserts a real absent-table load still reports it. Every
    /// other code (a wrong credential and an unreachable endpoint both surface as 30) must be reported
    /// as an open failure, never retried as a create.</summary>
    internal const int TableAbsentErrorCode = 12;

    /// <summary>The one <c>schema_policy</c> value this connector treats as permission to widen the
    /// table. pz passes any <c>schema_policy</c> string through unvalidated — it constrains the value
    /// nowhere and defaults it to "fail_on_change" — so the set of spellings that mean anything is
    /// defined here, not upstream. Under "evolve" the guard in <see cref="Reconcile"/> stands aside and
    /// delta-rs widens the table itself on the next append; every other value, "fail_on_change"
    /// included, refuses.</summary>
    private const string EvolvingSchemaPolicy = "evolve";

    private readonly ConnectorConfig config;

    // Lazy<Task<T>> rather than Lazy<T>: constructing the engine is itself a delta-rs call and so must
    // run on DeltaBigStack's oversized stack, but Lazy<T>'s factory has to be synchronous. Wrapping the
    // Task keeps construction on the big-stack thread while still amortizing it to one attempt under
    // concurrent first access. Same shape DeltaLakeSource uses.
    private readonly Lazy<Task<IEngine>> engine;

    public DeltaLakeSink(ConnectorConfig config)
        : this(config, () => new DeltaEngine(EngineOptions.Default))
    {
    }

    /// <summary>Takes the engine as a factory so the load-then-create decision in
    /// <see cref="OpenOrCreateAsync"/> can be exercised against an engine whose failures are chosen
    /// rather than provoked. Which delta-rs failures do and do not mean "there is no table here" is the
    /// single most consequential branch in this file — a wrong answer turns a wrong credential into a
    /// create attempt — and no arrangement of real files can make a load fail one way while a create at
    /// the same location would have succeeded, so the branch is otherwise only half observable.
    ///
    /// The sink takes OWNERSHIP of whatever the factory returns: it is constructed once, on the
    /// big-stack thread, and disposed by <see cref="DisposeAsync"/>. A caller must not dispose it
    /// itself, nor hand the same instance to two sinks.</summary>
    internal DeltaLakeSink(ConnectorConfig config, Func<IEngine> engineFactory)
    {
        this.config = config;
        this.engine = new Lazy<Task<IEngine>>(
            () => DeltaBigStack.RunAsync(() => Task.FromResult(engineFactory())));
    }

    /// <summary>An append session flushes bounded generations to keep memory proportional to a file
    /// rather than to the whole write, and a flushed generation is already committed — abort cannot
    /// unwind it. BestEffort is the only accurate value the enum offers.</summary>
    public AbortSemantics AbortSemantics => AbortSemantics.BestEffort;

    public bool TryGetNativeCopy(
        OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var options = DeltaWriteOptions.From(spec);
        DeltaTypeSupport.Assert(schema);

        var names = schema.FieldsList.Select(f => f.Name).ToHashSet(StringComparer.Ordinal);
        var missing = options.Keys.Where(k => !names.Contains(k)).ToList();
        if (missing.Count > 0)
        {
            throw DeltaErrors.Fail(DeltaErrors.MergeKeyNotInSchema,
                $"output '{spec.Output}': merge key(s) {string.Join(", ", missing.Select(k => $"'{k}'"))} " +
                "are not columns of the data being written",
                "correct 'keys' to name columns the pipeline actually selects");
        }

        var location = DeltaLocation.Resolve(
            this.config.GetString("root") ?? string.Empty, spec.Output,
            spec.Options.TryGetValue("path", out var p) ? p?.ToString() : null);

        var table = await this.OpenOrCreateAsync(location, schema, options, spec.Output, ct).ConfigureAwait(false);
        try
        {
            // Schema() and Metadata() are the synchronous, deeply-recursive delta-rs calls
            // DeltaBigStack exists for; both are fetched inside one pass through the gate and Reconcile
            // compares the results on the caller's thread.
            var existing = await DeltaBigStack.RunAsync(
                () => Task.FromResult((table.Schema(), table.Metadata().PartitionColumns))).ConfigureAwait(false);
            Reconcile(schema, existing.Item1, existing.Item2, options, spec);
            // The TABLE's column names, not the write's: they are what a target.-qualified name in a
            // merge_predicate has to resolve against, and under schema_policy: evolve the table
            // legitimately carries nullable columns this write does not produce.
            var targetColumns = existing.Item1.FieldsList.Select(f => f.Name).ToList();

            // The table's partition columns, not options.PartitionBy: a run against a table an earlier
            // run partitioned need not declare partition_by at all, so the option is not what decides
            // which columns become directory names. Reconcile above has already refused a declared value
            // that disagrees with this one.
            return new DeltaWriteSession(
                table, schema, targetColumns, existing.Item2 ?? [], options, spec.Output);
        }
        catch (Exception ex)
        {
            await DisposeTableAsync(table).ConfigureAwait(false);
            throw DeltaErrors.Translate(ex, $"open of output '{spec.Output}'", options.Keys);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!this.engine.IsValueCreated)
        {
            return;
        }

        IEngine engine;
        try
        {
            engine = await this.engine.Value.ConfigureAwait(false);
        }
        catch
        {
            // Construction itself never completed (the failure already surfaced to whichever
            // BeginWriteAsync call awaited it first), so there is no engine here to release.
            return;
        }

        await DeltaBigStack.RunAsync(() =>
        {
            engine.Dispose();
            return Task.CompletedTask;
        }).ConfigureAwait(false);
    }

    /// <summary>Fails a write whose shape does not match the table it is about to write into, naming
    /// the column and both types — a Rust-side type error names neither the column nor the output, and
    /// three of the mismatches below produce no error at all.
    ///
    /// Partitioning is compared here and nowhere else. <c>partition_by</c> is honoured only by
    /// CreateTableAsync, so on a table an earlier run already created it is inert: the run succeeds,
    /// writes no partition directories, reports nothing, and every partition-pruned read against that
    /// table quietly full-scans. Delta cannot repartition a table in place, so the only honest
    /// outcome is to refuse.
    ///
    /// A table column the write does NOT produce is compared here for the same reason. delta-rs fills
    /// a missing NULLABLE column with nulls and says nothing — a pipeline that stops selecting a column
    /// silently null-fills it from then on — and fails a missing NON-NULLABLE one only at insert time,
    /// with a row-data preview, after the table exists and after any earlier flushed generation has
    /// already committed. Under the default schema_policy both are refused; "evolve" is the opt-in that
    /// lets the write's own shape govern, and even it cannot wave through a missing non-nullable
    /// column, because delta-rs itself will not.
    ///
    /// The MIRROR of that rule is enforced here too, and it is the worst failure shape this connector
    /// has produced: a column the write ADDS that is declared NOT NULL. Measured against the shipped
    /// library on a table that already has rows — an append COMMITS it and says nothing, and every
    /// later read of that table fails with "Non-nullable column 'x' is missing from the physical
    /// schema". The damage is silent at write time and surfaces afterwards, in another process
    /// belonging to another person, where no error of ours can reach them. Refusing here, for every
    /// strategy, is the only point at which it can be stopped.
    ///
    /// It is refused UNCONDITIONALLY, including on a table that holds no rows, where the same addition
    /// does succeed. That is a deliberate trade, and the alternative was measured rather than assumed:
    /// a row count is cheap (delta-rs answers select count(*) from the log's own statistics — 11 ms on
    /// two million rows, 1 ms on an empty table), so cost is not what rules it out. What rules it out is
    /// that it cannot be made race-free. Delta is a multi-writer format — this connector classifies a
    /// commit conflict as transient precisely because it expects other writers — so a count taken here
    /// and acted on at commit is a check-then-act whose lost race produces exactly the unreadable table
    /// this rule exists to prevent. A false refusal costs one config edit; a wrong "it looked empty"
    /// costs a table nobody can read. Neither file-listing API is available as an alternative: both
    /// ITable.FilesAsync() and ITable.FileUrisAsync() abort the process on a table that has files.</summary>
    private static void Reconcile(
        Schema incoming, Schema table, IReadOnlyList<string>? tablePartitions,
        DeltaWriteOptions options, OutputSpec spec)
    {
        var byName = table.FieldsList.ToDictionary(f => f.Name, StringComparer.Ordinal);
        var incomingNames = incoming.FieldsList.Select(f => f.Name).ToHashSet(StringComparer.Ordinal);
        var problems = new List<string>();
        var evolving = spec.SchemaPolicy == EvolvingSchemaPolicy;

        var declaredPartitions = options.PartitionBy;
        var actualPartitions = tablePartitions ?? [];
        if (declaredPartitions.Count > 0 && !declaredPartitions.SequenceEqual(actualPartitions, StringComparer.Ordinal))
        {
            problems.Add(
                $"partition_by declares [{string.Join(", ", declaredPartitions)}] but the existing table is " +
                (actualPartitions.Count == 0
                    ? "not partitioned"
                    : $"partitioned by [{string.Join(", ", actualPartitions)}]"));
        }

        foreach (var field in incoming.FieldsList)
        {
            if (!byName.TryGetValue(field.Name, out var existing))
            {
                if (!evolving)
                {
                    problems.Add($"column '{field.Name}' is not in the table");
                }
                else if (!field.IsNullable)
                {
                    problems.Add(
                        $"column '{field.Name}' is not in the table and is declared NOT NULL, so it cannot " +
                        "be added to one that already exists — Delta has no value to give it in rows " +
                        "committed before it existed. It is refused even on a table that happens to hold " +
                        "no rows right now, because whether it does cannot be established without racing " +
                        "another writer");
                }

                continue;
            }

            // The FULL type, not just its ArrowTypeId: Decimal128(38,9) and Decimal128(10,2) share an
            // id, as do two timestamps differing only in unit or timezone, and delta-rs rejects all
            // three pairs at insert time. Comparing only the id would let them past a guard whose whole
            // purpose is to fail first and say which column.
            var wanted = Describe(existing.DataType);
            var got = Describe(field.DataType);
            if (!string.Equals(wanted, got, StringComparison.Ordinal))
            {
                problems.Add(
                    $"column '{field.Name}' is {wanted} in the table but {got} in the data being written");
            }
        }

        foreach (var field in table.FieldsList.Where(f => !incomingNames.Contains(f.Name)))
        {
            if (!field.IsNullable)
            {
                problems.Add(
                    $"column '{field.Name}' is NOT NULL in the table but the data being written has no such column");
            }
            else if (!evolving)
            {
                problems.Add(
                    $"column '{field.Name}' is in the table but not in the data being written, so every " +
                    "written row would be null there");
            }
        }

        if (problems.Count > 0)
        {
            throw DeltaErrors.Fail(DeltaErrors.SchemaMismatch,
                $"output '{spec.Output}': {string.Join("; ", problems)}",
                $"cast or select the columns in the pipeline SQL to match the table; set schema_policy: " +
                $"{EvolvingSchemaPolicy} to let the write add a NULLABLE column the table lacks or leave a " +
                "nullable one null; a column the write ADDS must be nullable, so declare it nullable in " +
                "the pipeline SQL or create a new table with the full schema and backfill into it; a " +
                "partitioning change needs a new table, because Delta cannot repartition one in place");
        }
    }

    /// <summary>A fully parameterized name for an Arrow type. Apache.Arrow's own <c>Name</c> drops the
    /// parameters that decide whether two same-id types are actually the same ("decimal128" for any
    /// precision and scale, "timestamp" for any unit and timezone), and no Arrow type implements value
    /// equality, so the comparison and the error message both need this.</summary>
    private static string Describe(IArrowType type) => type switch
    {
        Decimal128Type d => $"decimal128({d.Precision}, {d.Scale})",
        Decimal256Type d => $"decimal256({d.Precision}, {d.Scale})",
        TimestampType t => $"timestamp[{t.Unit}{(t.Timezone is null ? string.Empty : $", tz={t.Timezone}")}]",
        Time32Type t => $"time32[{t.Unit}]",
        Time64Type t => $"time64[{t.Unit}]",
        DurationType d => $"duration[{d.Unit}]",
        IntervalType i => $"interval[{i.Unit}]",
        FixedSizeBinaryType f => $"fixed_size_binary[{f.ByteWidth}]",
        FixedSizeListType f => $"fixed_size_list<{Describe(f.ValueDataType)}>[{f.ListSize}]",
        ListType l => $"list<{Describe(l.ValueDataType)}>",
        LargeListType l => $"large_list<{Describe(l.ValueDataType)}>",
        MapType m => $"map<{Describe(m.KeyField.DataType)}, {Describe(m.ValueField.DataType)}>",
        StructType s => $"struct<{string.Join(", ", s.Fields.Select(f => $"{f.Name}: {Describe(f.DataType)}"))}>",
        _ => type.Name,
    };

    private static async Task DisposeTableAsync(ITable table)
    {
        try
        {
            await DeltaBigStack.RunAsync(() =>
            {
                // ITable is not documented as IDisposable in the 0.33.0 API docs, so the interface is
                // tested rather than assumed.
                if (table is IDisposable disposable)
                {
                    disposable.Dispose();
                }

                return Task.CompletedTask;
            }).ConfigureAwait(false);
        }
        catch
        {
            // A failure releasing the native handle must not replace the failure the caller is already
            // reporting — that one is the failure the user has to act on, and it is rethrown regardless.
        }
    }

    private async Task<ITable> OpenOrCreateAsync(
        string location, Schema schema, DeltaWriteOptions options, string output, CancellationToken ct)
    {
        var engine = await this.engine.Value.ConfigureAwait(false);
        var storage = DeltaStorageOptions.Build(this.config).ToDictionary(kv => kv.Key, kv => kv.Value);

        try
        {
            return await DeltaBigStack.RunAsync(() => engine.LoadTableAsync(
                new TableOptions { TableLocation = location, StorageOptions = storage }, ct)).ConfigureAwait(false);
        }
        catch (DeltaLakeException absent) when (absent.ErrorCode == TableAbsentErrorCode)
        {
            // The one failure that means "there is no table here yet": fall through and create it. A
            // bare catch would swallow a cancellation, a wrong credential and a transient storage error
            // alike and then report whatever the CREATE said instead — a create failure for a table
            // that exists, on a run the user never asked to create anything.
        }
        catch (Exception ex)
        {
            throw DeltaErrors.Translate(ex, $"open of output '{output}'", options.Keys);
        }

        try
        {
            return await DeltaBigStack.RunAsync(() => engine.CreateTableAsync(
                new TableCreateOptions(location, schema)
                {
                    // TableLocation is NOT set here: TableCreateOptions' constructor already takes the
                    // location and assigns it.
                    PartitionBy = [.. options.PartitionBy],
                    SaveMode = SaveMode.ErrorIfExists,
                    StorageOptions = storage,
                },
                ct)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // A concurrent creator makes this throw the permanent "A Delta Lake table already exists at
            // that location." error, which DeltaErrors classifies as permanent on purpose — the next
            // run's load finds the table and succeeds.
            throw DeltaErrors.Translate(ex, $"create of output '{output}'", options.Keys);
        }
    }
}
