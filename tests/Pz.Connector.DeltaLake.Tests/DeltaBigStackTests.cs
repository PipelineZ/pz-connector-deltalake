using Xunit;

namespace Pz.Connector.DeltaLake.Tests;

public class DeltaBigStackTests
{
    [Fact]
    public async Task Work_runs_on_a_dedicated_named_background_thread()
    {
        var callerId = Environment.CurrentManagedThreadId;
        var observed = await DeltaBigStack.RunAsync(() => Task.FromResult(
            (Id: Environment.CurrentManagedThreadId, Name: Thread.CurrentThread.Name,
             Background: Thread.CurrentThread.IsBackground)));

        Assert.NotEqual(callerId, observed.Id);
        Assert.Equal("pz-deltalake", observed.Name);
        Assert.True(observed.Background);
    }

    [Fact]
    public async Task The_result_is_returned_to_the_caller()
    {
        Assert.Equal(42, await DeltaBigStack.RunAsync(() => Task.FromResult(42)));
    }

    [Fact]
    public async Task An_exception_propagates_unwrapped_not_as_an_AggregateException()
    {
        // The write session maps delta-rs exceptions by type and message; an AggregateException
        // wrapper would defeat every one of those checks.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => DeltaBigStack.RunAsync<int>(() => throw new InvalidOperationException("boom")));
        Assert.Equal("boom", ex.Message);
    }

    [Fact]
    public async Task An_exception_from_an_awaited_continuation_also_propagates_unwrapped()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => DeltaBigStack.RunAsync<int>(async () =>
            {
                await Task.Yield();
                throw new InvalidOperationException("late boom");
            }));
        Assert.Equal("late boom", ex.Message);
    }

    [Fact]
    public async Task Cancellation_surfaces_as_OperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => DeltaBigStack.RunAsync(() => Task.FromCanceled<int>(cts.Token)));
    }

    [Fact]
    public async Task An_await_inside_the_delegate_resumes_on_the_same_big_stack_thread()
    {
        // The whole point of the gate: a delegate that sequences two delta-rs calls with a plain
        // await between them (open-then-insert, load-then-pin-a-version) must keep BOTH calls on the
        // big-stack thread, not just the one before the first await. Task.Yield() is used rather than
        // Task.Run(() => {}): Task.Yield() is never synchronously complete, so its continuation always
        // takes a real asynchronous resumption path, and with no SynchronizationContext installed it
        // always queues to the thread pool -- exactly the failure this test needs to catch. Task.Run
        // was tried first and rejected: measured over 3000 iterations against a build with the fix
        // reverted, Task.Run(() => {}) let the antecedent task complete synchronously and resume
        // inline about 0.1% of the time, which would make this test pass even when the bug is present.
        // Task.Yield() gave zero such vacuous passes over the same 3000 iterations.
        var (before, after) = await DeltaBigStack.RunAsync(async () =>
        {
            var beforeThread = Thread.CurrentThread;
            await Task.Yield();
            return (Before: beforeThread, After: Thread.CurrentThread);
        });

        Assert.Same(before, after);
        Assert.Equal("pz-deltalake", after.Name);
    }

    [Fact]
    public async Task An_orphaned_continuation_after_the_pump_drains_runs_on_the_pool_instead_of_aborting_the_process()
    {
        // A fire-and-forget operation started inside a delegate (never awaited before the delegate
        // returns) still captures the pump as its SynchronizationContext the moment it awaits -- but
        // by the time it resumes, RunAsync<T> has already returned its result and the pump's queue has
        // stopped accepting new items. Without a guard, that resumption's Post() throws
        // InvalidOperationException on the async machinery's own unhandled-exception path
        // (Task.ThrowAsync), which is NOT catchable by any try/catch here: it aborts the process
        // (SIGABRT). This test cannot assert "the process did not abort" directly -- if it had, this
        // test method would never finish running at all -- so reaching the final Assert.True below,
        // in a suite that otherwise completes normally, IS the proof.
        var releaseOrphan = new TaskCompletionSource();
        var orphanRan = new TaskCompletionSource<bool>();

        var result = await DeltaBigStack.RunAsync(async () =>
        {
            // Not awaited: this task outlives the delegate's own return.
            _ = RunOrphanAsync();
            return 42;

            async Task RunOrphanAsync()
            {
                // Captures SynchronizationContext.Current == the pump right here, on the big-stack
                // thread, before this delegate returns.
                await releaseOrphan.Task;
                orphanRan.SetResult(true);
            }
        });

        // By now RunAsync<T> has returned: Complete() already ran, the pump's queue is already closed
        // to new Post() calls, and the orphan above is still suspended waiting on releaseOrphan.
        Assert.Equal(42, result);

        // Release it now -- strictly after the pump has drained -- so its Post() call is guaranteed to
        // hit the orphaned-continuation path rather than racing the pump's own shutdown.
        releaseOrphan.SetResult();

        Assert.True(await orphanRan.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void The_stack_is_large_enough_that_the_environment_variable_is_unnecessary()
    {
        // 180000 is what DeltaLake.Net's own tests ask users to set via DOTNET_DefaultStackSize;
        // this connector must be comfortably above it so no user ever has to.
        //
        // Both operands are const, so the comparison folds at compile time -- which does NOT make this
        // an assertion that cannot fail, and the distinction is worth writing down because it reads
        // like one. The FOLD happens at compile time; the ASSERT still happens at run time, over
        // whatever the fold produced. Measured: lowering StackBytes to 512 KiB fails this test.
        Assert.True(DeltaBigStack.StackBytes >= 180_000 * 4);
    }

    [Fact]
    public void The_test_process_does_not_set_DOTNET_DefaultStackSize()
    {
        // Setting it in the runner would hide exactly the failure users would hit, by making the
        // suite pass under conditions users do not have. If this ever fails, delete the variable —
        // do not relax the assertion.
        Assert.Null(Environment.GetEnvironmentVariable("DOTNET_DefaultStackSize"));
    }

    [Fact]
    public async Task Many_sequential_calls_each_terminate_their_own_thread()
    {
        // A process-wide Process.GetCurrentProcess().Threads.Count is the wrong observable here:
        // it includes GC, thread-pool, and finalizer threads this code does not control, and their
        // count moves on the runtime's own schedule, not this gate's. It also races the gate itself
        // — TaskCreationOptions.RunContinuationsAsynchronously means the awaited continuation can
        // resume before the worker thread's OS-level teardown has finished, so "after" can observe
        // a thread that is still exiting.
        //
        // What IS deterministically observable and fully controlled by this code: the exact Thread
        // that RunAsync creates for one call. The work delegate always runs ON that thread (that is
        // the whole point of DeltaBigStack), so capturing Thread.CurrentThread from inside it yields
        // the very Thread object RunAsync started — not a proxy for it. Joining that specific thread
        // with a bounded timeout is a real synchronization primitive (not a sleep): it blocks only
        // until that thread's OS-level teardown is complete, or fails loudly if it never is.
        for (var i = 0; i < 200; i++)
        {
            var thread = await DeltaBigStack.RunAsync(() => Task.FromResult(Thread.CurrentThread));

            Assert.True(
                thread.Join(TimeSpan.FromSeconds(5)),
                $"iteration {i}: gate thread '{thread.Name}' did not terminate within 5s of returning its result");
        }
    }

    /// <summary>A FIELD INITIALIZER that blocks on the gate still gets the big stack, and cannot
    /// deadlock. This is the shape <see cref="LocalLakeFixture"/> uses to seed a Delta table from a
    /// property getter an acceptance base class reads synchronously, so it is pinned here rather than
    /// assumed: <c>RunAsync</c> starts a thread it creates itself and completes the returned task from
    /// that thread, so the blocking caller is never the thread the work needs — the classic
    /// sync-over-async deadlock has no way to form, whatever SynchronizationContext the caller is on
    /// (xunit installs its own on the thread running this test, and therefore on the constructor
    /// below).
    ///
    /// Reaching the assertions at all is the no-deadlock half of the proof; the thread name is the
    /// other half, and it is the one that matters, because a block that quietly ran the delegate on the
    /// caller's own default-sized stack would look identical from the outside until delta-kernel-rs
    /// recursed deeply enough to kill the process.</summary>
    [Fact]
    public void Blocking_on_the_gate_from_a_constructor_still_runs_on_the_big_stack()
    {
        var caller = Thread.CurrentThread;
        var probe = new BlocksInItsFieldInitializer();

        Assert.NotSame(caller, probe.Observed);
        Assert.Equal("pz-deltalake", probe.Observed.Name);
    }

    private sealed class BlocksInItsFieldInitializer
    {
        public Thread Observed { get; } =
            DeltaBigStack.RunAsync(() => Task.FromResult(Thread.CurrentThread)).GetAwaiter().GetResult();
    }
}
