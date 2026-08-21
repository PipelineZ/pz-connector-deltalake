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
/// thread, no matter which thread pool worker actually completed the antecedent task.
///
/// This guarantee has three known edges — read them before writing a delegate against this gate,
/// because none of the three is enforced by anything, and a partial guarantee that reads as total is
/// worse than no guarantee at all:
/// <list type="bullet">
/// <item><description><c>ConfigureAwait(false)</c> inside a delegate still escapes the gate. Nothing
/// in this type detects or prevents it — the rule above is the whole enforcement mechanism. Measured
/// directly: a delegate ending one internal await in <c>.ConfigureAwait(false)</c> observably resumes
/// on a ".NET TP Worker" thread, not "pz-deltalake".</description></item>
/// <item><description>Only a delegate's own <c>await</c> continuations are pumped back — the BODY of
/// something like <c>Task.Run(...)</c> started from inside a delegate still executes on a thread-pool
/// thread; only the code that resumes AFTER awaiting that Task.Run comes back through the pump. Never
/// put a delta-rs call inside the body of a nested <c>Task.Run</c>.</description></item>
/// <item><description>Library-internal <c>ConfigureAwait(false)</c> calls are outside this gate's
/// reach entirely — it can only govern code in THIS assembly. DeltaLake.Net 0.33.0 itself uses
/// <c>ConfigureAwait(false)</c> inside <c>DeltaEngine.LoadTableAsync</c>,
/// <c>Kernel.Core.Runtime.LoadTableAsync</c>, and <c>Kernel.Core.Table.LoadVersionAsync</c>, so the
/// TAIL of any of those async calls runs on a thread-pool thread regardless of this gate. That is
/// harmless today only because every deeply-recursive delta-rs call this connector makes is the
/// SYNCHRONOUS <c>Schema()</c> — called from a synchronous delegate, which never yields at all, so
/// there is no tail for the library's own internal continuation to run on a pool thread with. Moving
/// any deep-recursion work behind an async delta-rs API in a future task means re-examining this
/// edge, not assuming the gate still covers it.</description></item>
/// </list>
/// </summary>
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

    // The two ConfigureAwait(false) calls below are safe specifically because this wrapper touches no
    // delta-rs state after either of them -- NOT because of anything special about how they interact
    // with the pump. ConfigureAwait(false) means the continuation that resumes here (just
    // `return null;`, and then the return back to RunAsync's own caller) does NOT post through the
    // pump installed inside RunAsync<T>; it queues straight to the thread pool instead, because a
    // custom SynchronizationContext being current makes that continuation an invalid location for
    // inlining. That is fine for `return null;`, which is not a delta-rs call. It would NOT be fine
    // for a caller's own work() delegate to reach for ConfigureAwait(false) the same way if work()
    // itself sequenced two delta-rs calls with an await between them -- that is a property of the
    // CALLER's delegate (see the type doc comment's first edge case), and nothing here enforces it.
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

        public override void Post(SendOrPostCallback d, object? state)
        {
            try
            {
                this.queue.Add((d, state));
            }
            catch (InvalidOperationException)
            {
                // Add() throws once Complete() has already called CompleteAdding() -- meaning the
                // delegate run through RunAsync<T> already returned (or threw) and the pump loop has
                // stopped. The only legitimate way to reach this is a fire-and-forget operation the
                // delegate started but never awaited before returning: its own continuation still
                // captured this context and is posting back to it, strictly after the pump is gone.
                //
                // Letting this exception escape would be its own uncatchable process kill: it surfaces
                // on the async machinery's own unhandled-exception path (Task.ThrowAsync), not
                // somewhere any caller's try/catch can intercept -- exactly the failure mode this
                // WHOLE TYPE exists to prevent for delta-kernel-rs stack overflows, so it must not
                // reappear here for an orphaned continuation instead. Falling back to
                // ThreadPool.UnsafeQueueUserWorkItem reproduces the ordinary pre-pump behavior for
                // that one continuation (default-stack pool thread) rather than crashing the process.
                ThreadPool.UnsafeQueueUserWorkItem(s => d(s), state);
            }
        }

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

        // The queue is deliberately never Dispose()d (BlockingCollection<T> allocates no OS handle
        // when its AvailableWaitHandle is never touched, which it never is here -- this is a small
        // managed-heap allocation the GC reclaims normally, not a leak). Dispose() is documented as
        // NOT safe to call concurrently with Add(), and an orphaned fire-and-forget continuation
        // (see Post above) can legitimately call Add() after RunAsync<T> has already returned --
        // disposing here would trade a nonexistent leak for a real, if rare, race.
        public void Complete() => this.queue.CompleteAdding();
    }
}
