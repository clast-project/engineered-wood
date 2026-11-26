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

    [SkippableFact]
    public void AProcessThatOverruns_IsKilledAndReported()
    {
        Skip.If(Python.Value == null, "Python is not on the PATH.");
        var started = DateTime.UtcNow;
        Assert.Throws<TimeoutException>(() =>
            PythonProcess.Run(Python.Value!, "-c \"import time; time.sleep(120)\"", 1_000));
        // A generous bound: this guards against waiting out the sleep, not against a slow machine.
        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(60));
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
