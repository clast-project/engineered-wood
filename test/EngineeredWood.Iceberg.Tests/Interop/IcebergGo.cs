// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

using EngineeredWood.Tests.Interop;

namespace EngineeredWood.Iceberg.Tests.Interop;

/// <summary>
/// <para>The iceberg-go tier: an independent Iceberg implementation, both writer and reader, driven
/// through the Go program in <c>Interop/iceberg-go</c>. It is the per-commit Iceberg oracle of the v3 plan
/// (<c>doc/iceberg-v3-plan.md</c>, #953 item 6). It needs no JVM, it writes v3 (DVs, row lineage,
/// defaults, nanosecond timestamps), and the #953 spike found it agreeing row for row with Spark 4.1 +
/// Iceberg 1.12 on every scenario the driver offers.</para>
///
/// <para><b>Pinned to iceberg-go ≥ 0.8.0</b> (today <c>v0.8.0-rc0</c>, in <c>go.mod</c>). 0.7.0 resurrects
/// deleted rows after a v2→v3 upgrade: the first DV did not absorb the file's v2 position deletes
/// (apache/iceberg-go#2669). The <c>v2_upgrade</c> scenario is that case.</para>
///
/// <para><b>Building.</b> With <c>EW_ICEBERG_GO_DRIVER</c> naming a prebuilt binary, that binary is used.
/// Otherwise the first test to need the tier runs <c>go build</c> over the source copied to the test
/// output. That needs Go on PATH (the version <c>go.mod</c> names, or a toolchain download) and, on a
/// cold module cache, network access. Either missing makes the tier unavailable, which skips its tests
/// unless <c>EW_REQUIRE_ICEBERG_GO_INTEROP=1</c>. The Go build cache makes later builds a link step of a
/// few seconds.</para>
///
/// <para><b>Catalog.</b> The driver keeps its tables in a SQL catalog on sqlite under the warehouse,
/// because iceberg-go's Hadoop catalog does not work on Windows. Tests address tables by metadata-file
/// location, as <c>file:///</c> URIs; <see cref="FileUriFileSystem"/> maps those onto a local root.</para>
/// </summary>
internal static class IcebergGo
{
    /// <summary>The iceberg-go version these tests were established against. Recorded, not enforced:
    /// a bump that changes behaviour should read as "the oracle moved".</summary>
    public const string ValidatedAgainstVersion = "v0.8.0-rc0";

    /// <summary>A prebuilt driver binary to use instead of building one.</summary>
    public const string DriverOverrideEnvVar = "EW_ICEBERG_GO_DRIVER";

    private const int BuildTimeoutMs = 15 * 60_000;

    private static readonly InteropDriver Driver = InteropDriver.ForExecutable(
        displayName: "iceberg-go driver",
        resolve: Resolve,
        requireEnvVar: "EW_REQUIRE_ICEBERG_GO_INTEROP",
        timeoutMs: 180_000);

    public static bool Available => Driver.Available;

    public static string? Version => Driver.Version;

    public static void Require() => Driver.Require();

    public static JsonElement Invoke(string command, object? args = null) => Driver.Invoke(command, args);

    public static JsonElement InvokeRaw(string command, object? args = null) => Driver.InvokeRaw(command, args);

    /// <summary>Writes one of the driver's scenario tables into <paramref name="warehouse"/> and returns
    /// its newest metadata file's location.</summary>
    public static string Scenario(string warehouse, string scenario) =>
        Invoke("scenario", new { warehouse, scenario }).GetProperty("metadata_location").GetString()!;

    private static (string? Exe, string? Version, string? Error) Resolve()
    {
        string? exe = System.Environment.GetEnvironmentVariable(DriverOverrideEnvVar) is { Length: > 0 } ov
            ? ov
            : null;

        if (exe is null)
        {
            string source = Path.Combine(AppContext.BaseDirectory, "Interop", "iceberg-go");
            if (!File.Exists(Path.Combine(source, "go.mod")))
                return (null, null, $"driver source not found at {source}");

            exe = Path.Combine(source, "bin",
                RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "iceberg-go-driver.exe" : "iceberg-go-driver");
            var (built, buildOut, buildErr) = Run("go", $"build -mod=readonly -o \"{exe}\" .", source, BuildTimeoutMs);
            if (built != 0)
                return (null, null, $"`go build` of the iceberg-go driver failed (exit {built}): {Tail(buildOut + buildErr)}");
        }
        else if (!File.Exists(exe))
        {
            return (null, null, $"{DriverOverrideEnvVar} names {exe}, which does not exist");
        }

        var (exit, stdout, stderr) = Run(exe, "version", null, 30_000);
        if (exit != 0)
            return (null, null, $"`{exe} version` failed (exit {exit}): {Tail(stdout + stderr)}");
        try
        {
            string version = JsonDocument.Parse(stdout).RootElement.GetProperty("v").GetString()!;
            return (exe, version, null);
        }
        catch (JsonException e)
        {
            return (null, null, $"`{exe} version` printed no version: {e.Message}: {Tail(stdout)}");
        }
    }

    /// <summary>Runs a process to completion, returning its exit code and output; exit -1 when it cannot
    /// start (e.g. no <c>go</c> on PATH) or times out.</summary>
    private static (int Exit, string Stdout, string Stderr) Run(string exe, string arguments, string? workingDirectory, int timeoutMs)
    {
        var psi = new ProcessStartInfo(exe, arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
        };
        if (workingDirectory is not null)
            psi.WorkingDirectory = workingDirectory;

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Exception e)
        {
            return (-1, "", $"cannot start {exe}: {e.Message}");
        }

        using (proc)
        {
            // Both pipes drain on their own threads: go build writes progress to stderr, and a full pipe
            // would wedge it.
            var stdout = Task.Factory.StartNew(() => proc.StandardOutput.ReadToEnd(), TaskCreationOptions.LongRunning);
            var stderr = Task.Factory.StartNew(() => proc.StandardError.ReadToEnd(), TaskCreationOptions.LongRunning);
            if (!proc.WaitForExit(timeoutMs))
            {
                try { proc.Kill(); } catch { }
                return (-1, "", $"{exe} {arguments} timed out after {timeoutMs / 1000}s");
            }

            proc.WaitForExit();
            return (proc.ExitCode, stdout.Result, stderr.Result);
        }
    }

    private static string Tail(string s) => s.Length <= 2000 ? s : s.Substring(s.Length - 2000);
}
