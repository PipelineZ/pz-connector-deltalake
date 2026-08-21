using Apache.Arrow;

namespace Pz.Connector.DeltaLake.Tests;

/// <summary>A throwaway lake directory for ONE acceptance-suite instance. xunit builds a fresh
/// instance of a test class per fact, so every fact gets its own empty root and no fact can see what
/// another one committed.
///
/// Seeding is deferred to the first <see cref="Root"/> read rather than done in the constructor.
/// Creating a real Delta table is a delta-rs call costing tens of milliseconds, and most facts in a
/// suite either skip or never touch the seeded table; paying for it in the constructor would charge
/// every one of them for a table only a few of them read.
///
/// <see cref="ReadBatchesAsync"/> goes through delta-rs, not DuckDB, and that is deliberate: the
/// ownership, commit and abort assertions the acceptance suite carries are the ones that must not be
/// skippable, and DuckDB's delta extension is a real download that an offline run cannot make.
/// <see cref="ReadBatchesThroughDuckDbAsync"/> is the same read through the OTHER engine — a write is
/// only fully correct if a second implementation can see it — and the suite that uses it skips
/// offline.</summary>
internal sealed class LocalLakeFixture : IDisposable
{
    private readonly Lazy<string> root;

    private LocalLakeFixture(Func<string> seed) => this.root = new Lazy<string>(seed);

    /// <summary>The lake directory, seeded on first read. Not <c>this.root.Value</c> inline at every
    /// call site, because <see cref="Dispose"/> must be able to tell a fixture that was never touched
    /// from one that has a directory to remove.</summary>
    public string Root => this.root.Value;

    public static LocalLakeFixture Empty() =>
        new(() => Directory.CreateTempSubdirectory("pz-delta-lake").FullName);

    /// <summary>A lake holding one seeded table. The delta-rs create is blocked on rather than awaited
    /// because <see cref="Root"/> is a property an acceptance base class reads synchronously — the
    /// block is safe here specifically because <c>DeltaBigStack.RunAsync</c> runs the work on a
    /// thread it creates itself and completes the returned task from that thread, so the blocking
    /// caller is never the thread the work needs (see
    /// <c>DeltaBigStackTests.Blocking_on_the_gate_from_a_constructor_still_runs_on_the_big_stack</c>,
    /// which pins exactly this shape). It does NOT bypass the big stack: the delta-rs call still runs
    /// on the 32 MiB thread, which is the whole reason the gate exists.</summary>
    public static LocalLakeFixture WithTable(string entity, int rows) =>
        new(() =>
        {
            var dir = Directory.CreateTempSubdirectory("pz-delta-lake").FullName;
            DeltaTestTable.CreateAtAsync(Path.Combine(dir, entity), rows).GetAwaiter().GetResult();
            return dir;
        });

    /// <summary>Removes an entity's table outright. A merge fact asserts an exact row count, so a
    /// leftover from the previous fact would make its assertion pass or fail for the wrong reason —
    /// even though the per-fact temp root already makes that impossible today, the base suite's
    /// contract is that this hook resets the target and it is cheaper to honour than to reason
    /// about.</summary>
    public Task DropAsync(string entity)
    {
        var dir = Path.Combine(this.Root, entity);
        if (Directory.Exists(dir))
        {
            Directory.Delete(dir, recursive: true);
        }

        return Task.CompletedTask;
    }

    public async Task<IReadOnlyList<RecordBatch>> ReadBatchesAsync(string entity) =>
        this.HasTable(entity) ? await DeltaReader.BatchesAsync(this.Location(entity)) : [];

    public async Task<IReadOnlyList<RecordBatch>> ReadBatchesThroughDuckDbAsync(string entity) =>
        this.HasTable(entity) ? await DuckDbReader.BatchesAsync(this.Location(entity)) : [];

    public void Dispose()
    {
        if (!this.root.IsValueCreated)
        {
            return;
        }

        try
        {
            Directory.Delete(this.Root, recursive: true);
        }
        catch (IOException)
        {
            // A temp directory that outlives the test run is noise, not a failure.
        }
    }

    private string Location(string entity) => Path.Combine(this.Root, entity);

    /// <summary>A location with no transaction log is a table that was never created, which reads as
    /// no batches rather than as a load failure — the shape an abort or a never-written output leaves
    /// behind. The check is for the log specifically, not a blanket catch: a table that DOES exist and
    /// cannot be read must still fail loudly.</summary>
    private bool HasTable(string entity) =>
        Directory.Exists(Path.Combine(this.Location(entity), "_delta_log"));
}
