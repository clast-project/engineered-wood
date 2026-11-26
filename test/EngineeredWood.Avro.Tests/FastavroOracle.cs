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

            var psi = new ProcessStartInfo(Python.Value!, $"-I \"{script}\" \"{path}\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            using var p = Process.Start(psi)!;
            string stdout = p.StandardOutput.ReadToEnd();
            string stderr = p.StandardError.ReadToEnd();
            p.WaitForExit(30_000);
            if (p.ExitCode != 0)
                throw new InvalidOperationException($"fastavro script failed: {stderr}");

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
                var psi = new ProcessStartInfo(candidate, "-c \"import fastavro\"")
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                };
                using var p = Process.Start(psi)!;
                p.WaitForExit(10_000);
                if (p.ExitCode == 0)
                    return candidate;
            }
            catch
            {
                // not on the PATH
            }
        }

        return null;
    }
}
