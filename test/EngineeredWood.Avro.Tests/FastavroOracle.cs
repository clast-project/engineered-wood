// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

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
    /// the path), bounded by <see cref="PythonProcess.Run"/>.
    /// </summary>
    private static PythonProcess.Result Run(string python, string arguments, int timeoutMs) =>
        PythonProcess.Run(python, "-I " + arguments, timeoutMs);
}
