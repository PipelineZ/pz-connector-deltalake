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
        // big-stack thread, not just the one before the first await. Task.Run forces the antecedent
        // task to complete on a genuine ThreadPool worker -- exactly how a real async delta-rs call
        // completes -- so this reproduces the actual failure mode a Task.Yield() might not: a
        // reviewer-instrumented run of the pre-fix code observed the continuation resume on a
        // ".NET TP Worker" thread instead of "pz-deltalake" this way.
        var (before, after) = await DeltaBigStack.RunAsync(async () =>
        {
            var beforeThread = Thread.CurrentThread;
            await Task.Run(() => { });
            return (Before: beforeThread, After: Thread.CurrentThread);
        });

        Assert.Same(before, after);
        Assert.Equal("pz-deltalake", after.Name);
    }

    [Fact]
    public void The_stack_is_large_enough_that_the_environment_variable_is_unnecessary()
    {
        // 180000 is what DeltaLake.Net's own tests ask users to set via DOTNET_DefaultStackSize;
        // this connector must be comfortably above it so no user ever has to.
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
}
