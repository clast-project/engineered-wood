// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET472
using System.Diagnostics;

namespace EngineeredWood.Lance.Table.Tests;

/// <summary>
/// Polyfills for post-netstandard2.0 BCL APIs used by the pylance-fixture
/// tests. Compiled only on net472 — every other TFM has these in-box.
/// </summary>
internal static class NetCompat
{
    /// <summary>
    /// Replicates <c>Process.WaitForExitAsync</c> (net5+) using
    /// <c>EnableRaisingEvents</c> + <c>Exited</c> + a <see cref="TaskCompletionSource{T}"/>.
    /// </summary>
    public static Task WaitForExitAsync(this Process process, CancellationToken cancellationToken = default)
    {
        if (process.HasExited) return Task.CompletedTask;

        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        process.EnableRaisingEvents = true;
        process.Exited += (_, _) => tcs.TrySetResult(null);

        if (process.HasExited) tcs.TrySetResult(null);

        if (cancellationToken.CanBeCanceled)
            cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));

        return tcs.Task;
    }
}
#endif
