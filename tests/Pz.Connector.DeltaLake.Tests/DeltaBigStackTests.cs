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
    public async Task Many_sequential_calls_do_not_leak_threads()
    {
        var before = System.Diagnostics.Process.GetCurrentProcess().Threads.Count;
        for (var i = 0; i < 200; i++)
        {
            await DeltaBigStack.RunAsync(() => Task.FromResult(i));
        }

        var after = System.Diagnostics.Process.GetCurrentProcess().Threads.Count;
        Assert.True(after - before < 50, $"thread count grew from {before} to {after}");
    }
}
