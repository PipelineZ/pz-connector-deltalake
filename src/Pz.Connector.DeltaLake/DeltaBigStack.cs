using System.Collections.Concurrent;

namespace Pz.Connector.DeltaLake;

/// <summary>Runs every delta-rs call on a thread with an explicitly sized stack. delta-kernel-rs can
/// exhaust a default .NET thread stack on Unix, and a stack overflow kills the process rather than
/// raising something the engine could report — so the connector owns the stack size instead of asking
/// the user to set an environment variable.
///
/// One thread per OPERATION, not per batch: a write session issues a handful of these (open/create,
/// one insert per flushed generation, one merge, one commit), so thread-creation cost is noise next to
/// the I/O each call performs. The size below is reserved address space, not committed memory.
///
/// The gate installs a single-threaded pumping <see cref="SynchronizationContext"/> on the big-stack
/// thread before running the delegate. Without it, a delegate that sequences two delta-rs calls with a
/// plain <c>await</c> between them (the shape every multi-call write path needs — open, then insert;
/// load, then pin a version) only guarantees the FIRST call runs on the big-stack thread: once the
/// awaited antecedent task completes asynchronously, the default .NET await behavior resumes the
/// continuation — including the second delta-rs call — on whichever thread-pool thread completed it,
/// silently defeating the whole point of this type for exactly the calls it exists to protect. This
/// was confirmed to happen in practice, not merely in theory: <c>DeltaStorageOptions.LoadAsync</c>
/// once awaited <c>engine.LoadTableAsync(...).ConfigureAwait(false)</c> then
/// <c>table.LoadVersionAsync(...).ConfigureAwait(false)</c> inside one delegate, and the second call
/// measurably ran on a ".NET TP Worker" thread, not "pz-deltalake". The fix is structural, not a
/// caller convention (a convention nobody has to remember and that broke on the very first delegate
/// that sequenced two calls): a delegate run through <see cref="RunAsync{T}"/> must not call
/// <c>ConfigureAwait(false)</c> on its OWN internal awaits (only on the Task <see cref="RunAsync{T}"/>
/// itself returns, which callers await from a different thread and which this concern does not touch)
/// — every plain <c>await</c> inside then captures this context and is pumped back onto the same OS
/// thread, no matter which thread pool worker actually completed the antecedent task.</summary>
internal static class DeltaBigStack
{
    internal const int StackBytes = 32 * 1024 * 1024;

    public static Task<T> RunAsync<T>(Func<Task<T>> work)
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        var thread = new Thread(
            () =>
            {
                var pump = new SingleThreadSynchronizationContext();
                var previous = SynchronizationContext.Current;
                SynchronizationContext.SetSynchronizationContext(pump);
                try
                {
                    // Blocking on the big stack is the point: the synchronous FFI entry — where the
                    // deep native recursion happens — must run on THIS thread, not a pool thread. The
                    // task starts running synchronously right here, up to its first await; anything
                    // after that resumes through the pump below rather than wherever the antecedent
                    // task happened to complete.
                    var task = work();

                    // Signals the pump to stop once the whole delegate is done, however it finishes.
                    // TaskScheduler.Default (not this context) is deliberate: Complete() only flips a
                    // flag on the queue, touches no delta-rs state, and posting IT through the pump
                    // would deadlock the pump waiting on itself.
                    task.ContinueWith(static (_, state) => ((SingleThreadSynchronizationContext)state!).Complete(),
                        pump, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

                    pump.RunOnCurrentThread();

                    tcs.SetResult(task.GetAwaiter().GetResult());
                }
                catch (OperationCanceledException ex)
                {
                    tcs.SetCanceled(ex.CancellationToken);
                }
                catch (Exception ex)
                {
                    // GetAwaiter().GetResult() already unwraps AggregateException, so callers see the
                    // real delta-rs exception type and message their mapping depends on.
                    tcs.SetException(ex);
                }
                finally
                {
                    SynchronizationContext.SetSynchronizationContext(previous);
                }
            },
            StackBytes)
        {
            IsBackground = true,
            Name = "pz-deltalake",
        };

        thread.Start();
        return tcs.Task;
    }

    // No ConfigureAwait(false) here on purpose: work()'s own task must be awaited in a way that
    // preserves this method's usual identity as "just wrap work in the generic overload" without
    // introducing a second continuation hazard — but the correctness this type promises comes from the
    // pump installed inside RunAsync<T>, not from how this wrapper awaits the inner call. This one
    // extra hop (posting `return null;`) never touches delta-rs, so it is not a hazard either way.
    public static async Task RunAsync(Func<Task> work) =>
        await RunAsync<object?>(async () =>
        {
            await work().ConfigureAwait(false);
            return null;
        }).ConfigureAwait(false);

    /// <summary>A minimal single-threaded message pump (the classic "AsyncPump" shape): <see
    /// cref="Post"/> enqueues, <see cref="RunOnCurrentThread"/> drains the queue on whichever thread
    /// calls it — always the big-stack thread here — until <see cref="Complete"/> is called. This is
    /// what lets a plain <c>await</c> inside a <see cref="RunAsync{T}"/> delegate resume on the same OS
    /// thread it started on instead of a thread-pool worker.</summary>
    private sealed class SingleThreadSynchronizationContext : SynchronizationContext
    {
        private readonly BlockingCollection<(SendOrPostCallback Callback, object? State)> queue = new();

        public override void Post(SendOrPostCallback d, object? state) => this.queue.Add((d, state));

        public override void Send(SendOrPostCallback d, object? state) =>
            throw new NotSupportedException(
                "Synchronous SynchronizationContext.Send is not supported on the pz-deltalake pump.");

        public void RunOnCurrentThread()
        {
            // GetConsumingEnumerable blocks until an item is available or CompleteAdding() (via
            // Complete()) has been called AND the queue has drained -- exactly the "pump until done"
            // loop this type exists to provide.
            foreach (var workItem in this.queue.GetConsumingEnumerable())
            {
                workItem.Callback(workItem.State);
            }
        }

        public void Complete() => this.queue.CompleteAdding();
    }
}
