// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Text.Json;

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// Reads an object container file with fastavro, the reference this project's Avro behaviour is checked
/// against, and reports either its records or the exception it raised. Malformed input is the point here,
/// so a refusal is a result, not a test failure.
/// </summary>
internal static class FastavroOracle
{
    private const int ProbeTimeoutMs = 10_000;
    private const int ReadTimeoutMs = 30_000;

    private static readonly Lazy<string?> Python = new(FindPython);

    /// <summary>Whether a Python with fastavro installed is on the PATH.</summary>
    public static bool IsAvailable => Python.Value != null;

    /// <summary>What fastavro made of a file: its records as JSON, or the name of the exception it raised.</summary>
    public sealed record Outcome(JsonElement? Records, string? Error);

    public static Outcome Read(Stream file)
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew_fastavro_{Guid.NewGuid():N}.avro");
        string script = Path.Combine(Path.GetTempPath(), $"ew_fastavro_{Guid.NewGuid():N}.py");
        try
        {
            using (var f = File.Create(path))
            {
                file.Position = 0;
                file.CopyTo(f);
            }
            file.Position = 0;

            File.WriteAllText(script, """
                import fastavro, json, sys
                def convert(o):
                    if isinstance(o, bytes): return list(o)
                    if isinstance(o, dict): return {k: convert(v) for k, v in o.items()}
                    if isinstance(o, list): return [convert(v) for v in o]
                    return o
                try:
                    with open(sys.argv[1], 'rb') as f:
                        rows = [convert(r) for r in fastavro.reader(f)]
                    print(json.dumps({"rows": rows}))
                except Exception as e:
                    print(json.dumps({"error": type(e).__name__}))
                """);

            var (exitCode, stdout, stderr) = Run(Python.Value!, $"\"{script}\" \"{path}\"", ReadTimeoutMs);
            if (exitCode != 0)
                throw new InvalidOperationException($"fastavro script failed (exit {exitCode}): {stderr}");

            var root = JsonDocument.Parse(stdout).RootElement;
            return root.TryGetProperty("error", out var error)
                ? new Outcome(null, error.GetString())
                : new Outcome(root.GetProperty("rows").Clone(), null);
        }
        finally
        {
            File.Delete(path);
            File.Delete(script);
        }
    }

    private static string? FindPython()
    {
        foreach (string candidate in new[] { "python3", "python" })
        {
            try
            {
                // Probed in the same isolated mode Read runs in: a fastavro only in the user site, or only on
                // PYTHONPATH, would otherwise pass here and then fail to import there.
                if (Run(candidate, "-c \"import fastavro\"", ProbeTimeoutMs).ExitCode == 0)
                    return candidate;
            }
            catch
            {
                // not on the PATH, or the probe timed out
            }
        }

        return null;
    }

    /// <summary>
    /// Runs Python in isolated mode (<c>-I</c>: no user site, no <c>PYTHON*</c> variables, no script directory on
    /// the path) and returns its exit code and output. Both pipes are drained on their own threads, since reading
    /// one to its end while the other fills would deadlock, and reading stdout on this thread would block until the
    /// process exits however long that takes; the wait is bounded instead, and a process that overruns is killed.
    /// </summary>
    private static (int ExitCode, string Stdout, string Stderr) Run(string python, string arguments, int timeoutMs)
    {
        var psi = new ProcessStartInfo(python, "-I " + arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
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
        string Drain(Task<string> t) => t.Wait(5_000) ? t.Result : "";
        return (p.ExitCode, Drain(stdout), Drain(stderr));
    }
}
