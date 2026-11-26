// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;

namespace EngineeredWood.Avro.Tests;

/// <summary>Runs a Python process for a test with its output captured and its run time bounded.</summary>
internal static class PythonProcess
{
    /// <summary>What a finished process exited with and wrote.</summary>
    public sealed record Result(int ExitCode, string Stdout, string Stderr);

    /// <summary>
    /// Runs <paramref name="python"/> with <paramref name="arguments"/> and waits up to
    /// <paramref name="timeoutMs"/> for it, killing it and throwing <see cref="TimeoutException"/> if it overruns.
    /// </summary>
    /// <remarks>
    /// Both pipes are drained on their own threads. Reading one to its end while the other fills deadlocks, and
    /// reading stdout to its end on this thread blocks until the process exits, however long that takes, so a
    /// timeout passed to a later <see cref="Process.WaitForExit(int)"/> never applies.
    /// </remarks>
    public static Result Run(
        string python, string arguments, int timeoutMs, IReadOnlyDictionary<string, string>? environment = null)
    {
        var psi = new ProcessStartInfo(python, arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        if (environment != null)
        {
            foreach (var kv in environment)
                psi.Environment[kv.Key] = kv.Value;
        }

        using var p = Process.Start(psi)!;
        var stdout = Task.Factory.StartNew(
            () => p.StandardOutput.ReadToEnd(),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var stderr = Task.Factory.StartNew(
            () => p.StandardError.ReadToEnd(),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        if (!p.WaitForExit(timeoutMs))
        {
            try { p.Kill(); } catch { /* already gone */ }
            throw new TimeoutException($"Python '{arguments}' did not finish in {timeoutMs / 1000} s.");
        }

        // The pipes close when the process exits; a grandchild holding them open must not hang us either.
        static string Drain(Task<string> t) => t.Wait(5_000) ? t.Result : "";
        return new Result(p.ExitCode, Drain(stdout), Drain(stderr));
    }
}
