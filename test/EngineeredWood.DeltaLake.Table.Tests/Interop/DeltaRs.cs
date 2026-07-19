// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para>Tier-1 external validation: drives <c>delta-rs</c> (pip <c>deltalake</c>) over a table
/// EngineeredWood wrote, and vice versa.</para>
///
/// <para><b>Why this exists.</b> Every other Delta test in this suite round-trips through EW's own
/// reader, which proves reader and writer agree — not that either matches the Delta spec. Every
/// interop bug in <c>doc/upstream-landing-notes.md</c> (DV framing, <c>add.path</c> encoding,
/// checkpoint content, physical names) round-tripped perfectly. These tests use an independent
/// implementation as the oracle.</para>
///
/// <para><b>Availability.</b> When <c>deltalake</c> is not installed these tests silently no-op,
/// matching the existing ORC/Lance cross-validation pattern. That is a real hazard at scale — a
/// whole tier can go dark in CI and leave you back at round-trip-only without a red test. Set
/// <c>EW_REQUIRE_DELTA_INTEROP=1</c> (do this in CI) to turn "tool missing" into a hard failure.</para>
/// </summary>
internal static class DeltaRs
{
    /// <summary>Set in CI so a missing/broken toolchain fails loudly instead of skipping.</summary>
    private const string RequireEnvVar = "EW_REQUIRE_DELTA_INTEROP";

    /// <summary>The version this harness's assertions were established against. Recorded rather than
    /// enforced: a delta-rs upgrade that changes behaviour should read as "the oracle moved", not as
    /// an EW regression, and the first thing to check is this number.</summary>
    public const string ValidatedAgainstVersion = "1.6.2";

    private static readonly Lazy<(string? Exe, string? Version, string? Error)> Probe = new(RunProbe);

    public static bool Available => Probe.Value.Exe is not null;

    public static string? Version => Probe.Value.Version;

    /// <summary>
    /// Gate every interop test on this: <c>if (!DeltaRs.EnsureAvailable()) return;</c>.
    /// Returns false to no-op, or throws when <see cref="RequireEnvVar"/> demands the tool be present.
    /// </summary>
    public static bool EnsureAvailable()
    {
        if (Available)
            return true;

        if (Environment.GetEnvironmentVariable(RequireEnvVar) == "1")
        {
            throw new InvalidOperationException(
                $"{RequireEnvVar}=1 but the delta-rs interop toolchain is unavailable: "
                + $"{Probe.Value.Error}. Install with `pip install deltalake`.");
        }

        return false;
    }

    /// <summary>Runs one driver command and returns its parsed JSON result, asserting <c>ok</c>.</summary>
    public static JsonElement Invoke(string command, object? args = null)
    {
        var result = InvokeRaw(command, args);
        if (!result.GetProperty("ok").GetBoolean())
        {
            string error = result.TryGetProperty("error", out var e) ? e.GetString()! : "(no message)";
            throw new InvalidOperationException($"delta-rs driver '{command}' failed: {error}");
        }

        return result;
    }

    /// <summary>As <see cref="Invoke"/> but does not assert success — for tests that expect a rejection.</summary>
    public static JsonElement InvokeRaw(string command, object? args = null)
    {
        string driver = Path.Combine(AppContext.BaseDirectory, "Interop", "delta_rs_driver.py");
        if (!File.Exists(driver))
            throw new FileNotFoundException($"delta-rs driver script not found at {driver}.");

        // Args go through a file so the command line carries only paths and an identifier. net472
        // has no ProcessStartInfo.ArgumentList, and quoting JSON into a Win32 command line by hand
        // is not worth the bugs.
        string argsFile = Path.Combine(Path.GetTempPath(), $"ew_deltars_args_{Guid.NewGuid():N}.json");
        File.WriteAllText(argsFile, JsonSerializer.Serialize(args ?? new { }), new UTF8Encoding(false));

        try
        {
            var psi = new ProcessStartInfo(Probe.Value.Exe!)
            {
                Arguments = $"\"{driver}\" {command} \"{argsFile}\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
                StandardOutputEncoding = Encoding.UTF8,
                StandardErrorEncoding = Encoding.UTF8,
            };
            // The driver emits non-ASCII paths; Windows consoles default to cp1252 and would mangle them.
            psi.EnvironmentVariables["PYTHONIOENCODING"] = "utf-8";

            using var proc = Process.Start(psi)!;
            string stdout = proc.StandardOutput.ReadToEnd();
            string stderr = proc.StandardError.ReadToEnd();
            if (!proc.WaitForExit(120_000))
            {
                try { proc.Kill(); } catch { }
                throw new TimeoutException($"delta-rs driver '{command}' timed out after 120s.");
            }

            if (string.IsNullOrWhiteSpace(stdout))
            {
                throw new InvalidOperationException(
                    $"delta-rs driver '{command}' produced no output (exit {proc.ExitCode}). stderr: {stderr}");
            }

            return JsonDocument.Parse(stdout).RootElement.Clone();
        }
        finally
        {
            try { File.Delete(argsFile); } catch { }
        }
    }

    private static (string?, string?, string?) RunProbe()
    {
        foreach (string candidate in new[] { "python3", "python" })
        {
            try
            {
                var psi = new ProcessStartInfo(candidate,
                    "-c \"import deltalake, json; print(json.dumps({'v': deltalake.__version__}))\"")
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                };
                using var p = Process.Start(psi)!;
                string stdout = p.StandardOutput.ReadToEnd();
                p.WaitForExit(15_000);
                if (p.ExitCode == 0)
                {
                    string version = JsonDocument.Parse(stdout).RootElement.GetProperty("v").GetString()!;
                    return (candidate, version, null);
                }
            }
            catch
            {
                // Try the next interpreter name.
            }
        }

        return (null, null, "no python interpreter with the `deltalake` package on PATH");
    }
}
