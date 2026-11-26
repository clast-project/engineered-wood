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
/// location, as <c>file:///</c> URIs, which EW reaches through a <see cref="EngineeredWood.IO.StorageResolver"/>.</para>
/// </summary>
internal static class IcebergGo
{
    /// <summary>The iceberg-go version these tests were established against. Recorded, not enforced:
    /// a bump that changes behaviour should read as "the oracle moved".</summary>
    public const string ValidatedAgainstVersion = "v0.8.0-rc0";

    /// <summary>A prebuilt driver binary to use instead of building one.</summary>
    public const string DriverOverrideEnvVar = "EW_ICEBERG_GO_DRIVER";

    private const int BuildTimeoutMs = 15 * 60_000;

    /// <summary>The allowance for one driver command, the <c>version</c> probe included. Generous because the
    /// CI runner is heavily oversubscribed: on #964 its run queue sat around 18 (peak 73) on 4 vCPUs, and a
    /// <c>version</c> call, which only prints a line, failed to finish within the 30 s it was then given.</summary>
    private const int CommandTimeoutMs = 180_000;

    /// <summary>How many times the <c>version</c> probe may time out before the tier counts as unavailable.
    /// The probe's result is cached for the whole test process, so a single stall would otherwise fail
    /// every iceberg-go test in it: on #964, 22 net472 tests under <c>EW_REQUIRE_ICEBERG_GO_INTEROP=1</c>.</summary>
    private const int VersionAttempts = 2;

    private static readonly InteropDriver Driver = InteropDriver.ForExecutable(
        displayName: "iceberg-go driver",
        resolve: Resolve,
        requireEnvVar: "EW_REQUIRE_ICEBERG_GO_INTEROP",
        timeoutMs: CommandTimeoutMs);

    public static bool Available => Driver.Available;

    public static string? Version => Driver.Version;

    public static void Require() => Driver.Require();

    public static JsonElement Invoke(string command, object? args = null) => Driver.Invoke(command, args);

    public static JsonElement InvokeRaw(string command, object? args = null) => Driver.InvokeRaw(command, args);

    /// <summary>Writes one of the driver's scenario tables into <paramref name="warehouse"/> and returns
    /// its newest metadata file's location.</summary>
    public static string Scenario(string warehouse, string scenario) =>
        Invoke("scenario", new { warehouse, scenario }).GetProperty("metadata_location").GetString()!;

    /// <summary>
    /// Locates or builds the driver and reads its version. Never throws: a failure of any kind becomes
    /// an "unavailable" result carrying the reason, as the Python probe's does. The result is cached in
    /// a <see cref="Lazy{T}"/>, which would cache an exception too, so one that escaped would make every
    /// iceberg-go test error out, instead of skipping or reporting the require-env-var failure.
    /// </summary>
    private static (string? Exe, string? Version, string? Error) Resolve()
    {
        try
        {
            return ResolveCore();
        }
        catch (Exception e)
        {
            return (null, null, $"probing the iceberg-go driver failed: {e.GetType().Name}: {e.Message}");
        }
    }

    private static (string? Exe, string? Version, string? Error) ResolveCore()
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
            var (built, buildOut, buildErr, _) = Run("go", $"build -mod=readonly -o \"{exe}\" .", source, BuildTimeoutMs);
            if (built != 0)
                return (null, null, $"`go build` of the iceberg-go driver failed (exit {built}): {Tail(buildOut + buildErr)}");
        }
        else if (!File.Exists(exe))
        {
            return (null, null, $"{DriverOverrideEnvVar} names {exe}, which does not exist");
        }

        // Only a TIMEOUT is retried: a driver that exits with an error, or prints something unparseable,
        // does so deterministically, and retrying it would only delay the verdict.
        var probe = Run(exe, "version", null, CommandTimeoutMs);
        for (int attempt = 2; attempt <= VersionAttempts && probe.TimedOut; attempt++)
            probe = Run(exe, "version", null, CommandTimeoutMs);

        var (exit, stdout, stderr, timedOut) = probe;
        if (timedOut)
            return (null, null, $"`{exe} version` timed out {VersionAttempts} times ({CommandTimeoutMs / 1000}s each)");
        if (exit != 0)
            return (null, null, $"`{exe} version` failed (exit {exit}): {Tail(stdout + stderr)}");
        JsonElement root;
        try
        {
            root = JsonDocument.Parse(stdout).RootElement.Clone();
        }
        catch (JsonException e)
        {
            return (null, null, $"`{exe} version` did not print JSON ({e.Message}): {Tail(stdout)}");
        }

        if (root.ValueKind != JsonValueKind.Object
            || !root.TryGetProperty("v", out var v)
            || v.ValueKind != JsonValueKind.String
            || v.GetString() is not { Length: > 0 } version)
        {
            return (null, null, $"`{exe} version` printed no string \"v\" version: {Tail(stdout)}");
        }

        return (exe, version, null);
    }

    /// <summary>Runs a process to completion, returning its exit code and output; exit -1 when it cannot
    /// start (e.g. no <c>go</c> on PATH) or times out.</summary>
    private static (int Exit, string Stdout, string Stderr, bool TimedOut) Run(string exe, string arguments, string? workingDirectory, int timeoutMs)
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
            return (-1, "", $"cannot start {exe}: {e.Message}", false);
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
                return (-1, "", $"{exe} {arguments} timed out after {timeoutMs / 1000}s", true);
            }

            proc.WaitForExit();
            return (proc.ExitCode, stdout.Result, stderr.Result, false);
        }
    }

    private static string Tail(string s) => s.Length <= 2000 ? s : s.Substring(s.Length - 2000);
}
