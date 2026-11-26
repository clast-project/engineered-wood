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
    /// How long, after the process exits, its output may take to reach end of file. The pipes close when every
    /// process holding them has closed them, which is normally the moment Python exits.
    /// </summary>
    private const int DrainGraceMs = 5_000;

    /// <summary>
    /// Runs <paramref name="python"/> with <paramref name="arguments"/> and waits up to
    /// <paramref name="timeoutMs"/> for it, killing it and throwing <see cref="TimeoutException"/> if it overruns.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Both pipes are drained on their own threads. Reading one to its end while the other fills deadlocks, and
    /// reading stdout to its end on this thread blocks until the process exits, however long that takes, so a
    /// timeout passed to a later <see cref="Process.WaitForExit(int)"/> never applies.
    /// </para>
    /// <para>
    /// A process Python started can inherit the pipes and hold them open after Python exits. That throws
    /// <see cref="IOException"/> after <see cref="DrainGraceMs"/>, one deadline for both pipes, rather than
    /// returning the output read so far as if it were all of it. The reader threads cannot be cancelled out of
    /// a blocking pipe read; they finish when that process exits, and the failure makes the case visible
    /// instead of letting it repeat.
    /// </para>
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
            KillTree(p);
            throw new TimeoutException($"Python '{arguments}' did not finish in {timeoutMs / 1000} s.");
        }

        if (!Task.WaitAll(new Task[] { stdout, stderr }, DrainGraceMs))
        {
            throw new IOException(
                $"Python '{arguments}' exited with code {p.ExitCode}, but its output was still open " +
                $"{DrainGraceMs / 1000} s later: a process it started holds the pipes.");
        }

        return new Result(p.ExitCode, stdout.Result, stderr.Result);
    }

    /// <summary>Kills the process and, where the runtime can, every process it started.</summary>
    private static void KillTree(Process p)
    {
        try
        {
#if NETCOREAPP3_0_OR_GREATER
            p.Kill(entireProcessTree: true);
#else
            p.Kill(); // .NET Framework cannot enumerate a process's descendants
#endif
        }
        catch
        {
            // already gone
        }
    }
}
