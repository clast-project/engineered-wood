// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// The fastavro helpers read stdout to its end before their timed wait, so a Python that stalled hung the test
/// run with no timeout, and a full stderr pipe could deadlock it. <see cref="PythonProcess"/> bounds both.
/// </summary>
public class PythonProcessTests
{
    private static readonly Lazy<string?> Python = new(() =>
    {
        foreach (string candidate in new[] { "python3", "python" })
        {
            try
            {
                if (PythonProcess.Run(candidate, "--version", 10_000).ExitCode == 0)
                    return candidate;
            }
            catch
            {
                // not on the PATH
            }
        }
        return null;
    });

    /// <summary>
    /// The script reports its own process id before it sleeps, so the test checks that the interpreter itself
    /// is gone, not only that the wait gave up: on Windows a venv's <c>python.exe</c> can be a launcher that runs
    /// the interpreter as a child, and killing the launcher alone leaves the child running.
    /// </summary>
    [SkippableFact]
    public void AProcessThatOverruns_IsKilledAndReported()
    {
        Skip.If(Python.Value == null, "Python is not on the PATH.");
        string pidFile = Path.Combine(Path.GetTempPath(), $"ew_python_pid_{Guid.NewGuid():N}.txt");
        int? pid = null;
        try
        {
            var started = DateTime.UtcNow;
            // Long enough for the interpreter to start and write its id on a busy machine, far short of the sleep.
            Assert.Throws<TimeoutException>(() => PythonProcess.Run(
                Python.Value!,
                $"-c \"import os, time; open(r'{pidFile}', 'w').write(str(os.getpid())); time.sleep(120)\"",
                10_000));
            Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(60));

            Assert.True(File.Exists(pidFile), "The script did not start within the timeout.");
            pid = int.Parse(File.ReadAllText(pidFile));
            Assert.True(HasExited(pid.Value, TimeSpan.FromSeconds(10)), $"Python (pid {pid}) is still running.");
        }
        finally
        {
            if (pid is int alive && !HasExited(alive, TimeSpan.Zero))
            {
                try { System.Diagnostics.Process.GetProcessById(alive).Kill(); } catch { /* raced to exit */ }
            }
            File.Delete(pidFile);
        }
    }

    private static bool HasExited(int pid, TimeSpan wait)
    {
        try
        {
            using var p = System.Diagnostics.Process.GetProcessById(pid);
            return p.WaitForExit((int)wait.TotalMilliseconds);
        }
        catch (ArgumentException)
        {
            return true; // no such process
        }
    }

    /// <summary>More than a pipe buffer on stderr before anything on stdout: reading stdout first deadlocked.</summary>
    [SkippableFact]
    public void ALargeStderr_DoesNotDeadlock()
    {
        Skip.If(Python.Value == null, "Python is not on the PATH.");
        var result = PythonProcess.Run(
            Python.Value!, "-c \"import sys; sys.stderr.write('x' * 1000000); sys.stderr.flush(); print('done')\"", 60_000);
        Assert.Equal(0, result.ExitCode);
        Assert.Equal("done", result.Stdout.Trim());
        Assert.Equal(1_000_000, result.Stderr.Length);
    }
}
