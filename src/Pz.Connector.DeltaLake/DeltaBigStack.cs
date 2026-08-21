namespace Pz.Connector.DeltaLake;

/// <summary>Runs every delta-rs call on a thread with an explicitly sized stack. delta-kernel-rs can
/// exhaust a default .NET thread stack on Unix, and a stack overflow kills the process rather than
/// raising something the engine could report — so the connector owns the stack size instead of asking
/// the user to set an environment variable.
///
/// One thread per OPERATION, not per batch: a write session issues a handful of these (open/create,
/// one insert per flushed generation, one merge, one commit), so thread-creation cost is noise next to
/// the I/O each call performs. The size below is reserved address space, not committed memory.</summary>
internal static class DeltaBigStack
{
    internal const int StackBytes = 32 * 1024 * 1024;

    public static Task<T> RunAsync<T>(Func<Task<T>> work)
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        var thread = new Thread(
            () =>
            {
                try
                {
                    // Blocking on the big stack is the point: the synchronous FFI entry — where the
                    // deep native recursion happens — must run on THIS thread, not a pool thread.
                    tcs.SetResult(work().GetAwaiter().GetResult());
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
            },
            StackBytes)
        {
            IsBackground = true,
            Name = "pz-deltalake",
        };

        thread.Start();
        return tcs.Task;
    }

    public static async Task RunAsync(Func<Task> work) =>
        await RunAsync<object?>(async () =>
        {
            await work().ConfigureAwait(false);
            return null;
        }).ConfigureAwait(false);
}
