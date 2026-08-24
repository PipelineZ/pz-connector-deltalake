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

    /// <summary>The strategies this sink implements. Kept here rather than inferred from the switch in
    /// DeltaWriteSession.CommitAsync, because an unrecognised one has to be refused BEFORE a table is
    /// opened — that switch runs after the create and after the whole write has been buffered.</summary>
    private static readonly string[] Strategies = ["append", "replace", "merge"];

    private readonly record struct Problem(string Code, string What, string NextStep);

    public static DeltaWriteOptions From(OutputSpec spec)
    {
        var problems = new List<Problem>();
        CheckNames(spec, problems);
        CheckStrategy(spec, problems);

        if (!PartitionColumns.TryRead(spec.Options, out var partitionBy, out var partitionProblem))
        {
            problems.Add(new Problem(DeltaErrors.InvalidWriteOption,
                $"write option '{PartitionColumns.OptionName}' is invalid — {partitionProblem}",
                $"name one column, or a list of them — {PartitionColumns.OptionName}: dt, or " +
                $"{PartitionColumns.OptionName}: [dt, region]"));
        }

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

    /// <summary>Refuses a strategy this sink does not implement, here rather than at commit.
    ///
    /// Left to CommitAsync's default arm, an unrecognised strategy passed BeginWriteAsync,
    /// OpenOrCreateAsync CREATED the Delta table, and every batch was buffered — the flush trigger is
    /// gated on "append", so nothing drained — before PZDL0108 was raised over a table the run had
    /// just brought into existence and a whole write held in memory. That contradicts the rule this
    /// file states elsewhere: a configuration error must not open, create or touch a table. Unreachable
    /// through pz, which validates strategy itself; reachable driven directly.</summary>
    private static void CheckStrategy(OutputSpec spec, List<Problem> problems)
    {
        if (Strategies.Contains(spec.Mode, StringComparer.Ordinal))
        {
            return;
        }

        problems.Add(new Problem(DeltaErrors.InvalidWriteOption,
            $"unsupported write strategy '{spec.Mode}'",
            $"use strategy: {string.Join(", ", Strategies)}"));
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

        // ONE spelling from here down. pz spells a UTC timestamp's timezone "+00:00" and delta-rs
        // accepts only "UTC", so the rewrite has to happen before ANYTHING else reads the schema: the
        // refusal below, the create, the reconcile, the session and every insert must all see the same
        // types, or the create writes one shape and the reconcile compares another.
        schema = DeltaArrowTypes.Canonical(schema);
        DeltaTypeSupport.Assert(schema);
        // Pre-flight, against the DECLARED partition columns: a column Delta cannot partition by is a
        // configuration error, and a configuration error must not open, create or touch a table. The
        // table's own partition columns are checked again below, once they are known.
        DeltaTypeSupport.AssertPartitionable(schema, options.PartitionBy, spec.Output);

        var names = schema.FieldsList.Select(f => f.Name).ToHashSet(StringComparer.Ordinal);
        var missing = options.Keys.Where(k => !names.Contains(k)).ToList();
        if (missing.Count > 0)
        {
            throw DeltaErrors.Fail(DeltaErrors.MergeKeyNotInSchema,
                $"output '{spec.Output}': merge key(s) {string.Join(", ", missing.Select(k => $"'{k}'"))} " +
                "are not columns of the data being written",
                "correct 'keys' to name columns the pipeline actually selects");
        }

        if (spec.Mode == "merge")
        {
            // Before the location is even resolved: a key type the duplicate-key resolver cannot
            // compare is a configuration error, and a configuration error must not open, create or
            // touch a table.
            DeltaMergeDedup.AssertResolvableKeys(schema, options.Keys, spec.Output);
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

            // Again, now against the TABLE's own partition columns. A run that declares no
            // partition_by inherits whatever an earlier run chose, and Reconcile compares a
            // dictionary-encoded column against the value type Delta stored it as — so the encoding
            // reaches here unremarked and would fail inside delta-rs's own partitioning instead.
            DeltaTypeSupport.AssertPartitionable(schema, existing.Item2 ?? [], spec.Output);
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
            throw DeltaErrors.Translate(ex, DeltaOperationKind.Write, $"open of output '{spec.Output}'", options.Keys);
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
    /// has produced: a column the write ADDS that is declared NOT NULL, on a strategy that leaves older
    /// rows in place. Measured against the shipped library on a table that already has rows — an append
    /// COMMITS it and says nothing, and every later read of that table fails with "Non-nullable column
    /// 'x' is missing from the physical schema". The damage is silent at write time and surfaces
    /// afterwards, in another process belonging to another person, where no error of ours can reach
    /// them. Refusing here is the only point at which it can be stopped. See
    /// <see cref="RowsCanOutliveTheWrite"/> for which strategies it applies to and why.
    ///
    /// Where it applies it is refused UNCONDITIONALLY, including on a table that holds no rows, where
    /// the same addition does succeed. That is a deliberate trade, and the alternative was measured
    /// rather than assumed: a row count is cheap (delta-rs answers select count(*) from the log's own
    /// statistics — 11 ms on two million rows, 1 ms on an empty table), so cost is not what rules it
    /// out. What rules it out is that it cannot be made race-free. Delta is a multi-writer format — this
    /// connector classifies a commit conflict as transient precisely because it expects other writers —
    /// so a count taken here and acted on at commit is a check-then-act whose lost race produces exactly
    /// the unreadable table this rule exists to prevent. A false refusal costs one config edit; a wrong
    /// "it looked empty" costs a table nobody can read. Neither file-listing API is available as an
    /// alternative: both ITable.FilesAsync() and ITable.FileUrisAsync() abort the process on a table
    /// that has files.</summary>
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
                // A name that collides with an existing column case-insensitively is checked before
                // anything else, because it is not the "new column" it looks like. Delta cannot hold two
                // columns whose names differ only in case: measured, delta-rs refuses the widening
                // insert with "Duplicate field name (case-insensitive)". Left to it, that arrives at
                // commit as an uncoded failure whose next step talks about protocol versions, which have
                // nothing to do with two spellings of one name — and under 'evolve' it arrives only
                // after the table has been opened and the whole pipeline buffered.
                var collision = table.FieldsList.FirstOrDefault(
                    f => string.Equals(f.Name, field.Name, StringComparison.OrdinalIgnoreCase));
                if (collision is not null)
                {
                    problems.Add(
                        $"column '{field.Name}' differs from the table's '{collision.Name}' only in case, " +
                        "and Delta cannot hold both");
                }
                else if (!evolving)
                {
                    problems.Add($"column '{field.Name}' is not in the table");
                }
                else if (!field.IsNullable && RowsCanOutliveTheWrite(options.Mode))
                {
                    problems.Add(
                        $"column '{field.Name}' is not in the table and is declared NOT NULL, so strategy " +
                        $"'{options.Mode}' cannot add it — Delta has no value to give it in rows this " +
                        "write leaves in place, which were committed before the column existed. It is " +
                        "refused even on a table that happens to hold no rows right now, because whether " +
                        "it does cannot be established without racing another writer");
                }

                continue;
            }

            // The FULL type, not just its ArrowTypeId: Decimal128(38,9) and Decimal128(10,2) share an
            // id, as do two timestamps differing only in unit, and delta-rs rejects both pairs at
            // insert time. Comparing only the id would let them past a guard whose whole purpose is to
            // fail first and say which column.
            //
            // The incoming side is compared as delta-rs will STORE it, not as it was offered. A Delta
            // table does not necessarily come back shaped like the Arrow schema that created it — an
            // unsigned integer comes back signed, every string encoding comes back utf8 — so comparing
            // the offered type refuses the very first write to the table this same call has just
            // created, and leaves an empty table behind for every run after it to fail against.
            // DeltaArrowTypes.Stored is that mapping, observed against delta-rs rather than assumed.
            var wanted = DeltaArrowTypes.Describe(existing.DataType);
            var offered = DeltaArrowTypes.Describe(field.DataType);
            var got = DeltaArrowTypes.Describe(DeltaArrowTypes.Stored(field.DataType));
            if (!string.Equals(wanted, got, StringComparison.Ordinal))
            {
                // Both the type the write OFFERS and the type Delta would store it as, whenever they
                // differ. Naming only the stored one describes the write back to its author as a type
                // they never wrote; naming only the offered one hides why it does not match a table
                // that never held that type either.
                problems.Add(
                    $"column '{field.Name}' is {wanted} in the table but {offered} in the data being " +
                    "written" +
                    (string.Equals(offered, got, StringComparison.Ordinal)
                        ? string.Empty
                        : $", which Delta stores as {got}"));
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
                "nullable one null; a column added by an append or a merge must be nullable, so declare " +
                "it nullable in the pipeline SQL, or use strategy: replace — which discards every row " +
                "the table holds and rewrites it from this run's data — or create a new table with the " +
                "full schema and backfill into it; two columns whose names differ only in case cannot " +
                "both exist, so rename one; a partitioning change needs a new table, because Delta " +
                "cannot repartition one in place");
        }
    }

    /// <summary>Whether rows written before this run can still be in the table after it — which is what
    /// decides whether a NOT NULL column may be added. Delta can give such a column no value in a row
    /// that predates it, so adding one is safe exactly when no such row survives.
    ///
    /// 'append' and 'merge' both leave older rows in place, and both were measured to produce the damage:
    /// append commits and the table becomes unreadable, merge fails at the widening insert.
    ///
    /// 'replace' is exempt, and that is measured rather than reasoned. It is one SaveMode.Overwrite
    /// commit, and that overwrite is TOTAL, not per-partition: on a table partitioned by 'dt' with rows
    /// in two partitions, a replace writing only the first leaves the second's row GONE. So no row
    /// survives that predates the column, and the write commits and reads back with the value present.
    /// A replace that writes nothing at all takes the DeleteAsync path, which changes no schema — also
    /// measured. Time travel is unaffected either way, because an older version carries its own narrower
    /// schema.
    ///
    /// WHAT KEEPS THE OVERWRITE TOTAL, because the exemption is only sound while it is: DeltaLake.Net's
    /// InsertOptions exposes a Predicate property — delta-rs's replaceWhere — which scopes an overwrite
    /// to the rows it matches. This connector never sets it; both construction sites in
    /// DeltaWriteSession pass SaveMode alone. Exposing a replace_where write option, or setting Predicate
    /// for any other reason, would make a replace partial, leave rows behind that predate an added
    /// column, and make this exemption WRONG — without anyone editing this method.</summary>
    private static bool RowsCanOutliveTheWrite(string mode) => mode != "replace";

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
            throw DeltaErrors.Translate(ex, DeltaOperationKind.Write, $"open of output '{output}'", options.Keys);
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
            throw DeltaErrors.Translate(ex, DeltaOperationKind.Write, $"create of output '{output}'", options.Keys);
        }
    }
}
