// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace EngineeredWood.Tests.Interop;

/// <summary>
/// <para>Shared plumbing for the external-validation tiers: locate a Python driver script, run one
/// command, parse its JSON result. Every tier is a thin configuration over this — delta-rs, DuckDB
/// and PySpark under the Delta suite, and the DuckDB/DataFusion readers under the Parquet suite.
/// It lives under <c>test/Shared</c> and is linked into each test project rather than copied, so a
/// fix to the process handling below reaches every tier at once.</para>
///
/// <para>Every driver obeys the same contract: <c>python &lt;script&gt; &lt;command&gt;
/// &lt;args-file&gt; &lt;result-file&gt;</c>, writing exactly one JSON object to the result file, with
/// failures reported as <c>{"ok": false, "error": ...}</c> and exit code 0 so the C# side can assert
/// on the message rather than on a process crash.</para>
///
/// <para>The result goes to a FILE rather than stdout because stdout is not ours to own: Spark's JVM
/// writes <c>SUCCESS: The process with PID ... has been terminated.</c> straight to the inherited
/// stdout descriptor during shutdown, which no amount of Python-level redirection prevents. Args
/// travel the same way — net472 lacks <c>ProcessStartInfo.ArgumentList</c>, and hand-quoting JSON
/// into a Win32 command line is not worth the bugs.</para>
/// </summary>
internal sealed class InteropDriver
{
    private readonly Lazy<(string? Exe, string? Version, string? Error)> _probe;

    /// <param name="scriptName">Driver file name, resolved under <c>Interop/</c> in the test output.</param>
    /// <param name="probeExpression">Python expression printing a JSON object with a <c>v</c> version
    /// key. Doubles as the availability check: if it fails to run, the tier is unavailable.</param>
    /// <param name="requireEnvVar">Env var that, when <c>1</c>, turns unavailability into a hard
    /// failure instead of a silent no-op.</param>
    /// <param name="timeoutMs">Per-command timeout. Spark needs far longer than delta-rs.</param>
    /// <param name="environment">Extra environment for the driver process (JAVA_HOME, PATH, ...).</param>
    /// <param name="preflight">Optional check for prerequisites the Python import cannot see, returning
    /// an error string when unmet and null when satisfied. Importing <c>pyspark</c> succeeds on a
    /// machine with no JDK, so without this the tier would go RED rather than no-op — the failure mode
    /// this whole availability mechanism exists to avoid.</param>
    /// <param name="persistent">Keep ONE driver process alive and stream commands to it, instead of
    /// launching a process per command. Only worth it when startup dominates: Spark pays 15-20s per
    /// JVM, delta-rs pays nothing.</param>
    /// <param name="interpreterOverrideEnvVar">Optional env var naming a specific Python interpreter to
    /// use instead of the <c>python3</c>/<c>python</c> on PATH. Lets one tier target an isolated venv
    /// (e.g. a Spark 4.1 install) without disturbing the interpreter another tier resolves — pyspark
    /// bundles its JARs, so two Spark versions cannot share one environment.</param>
    public InteropDriver(
        string scriptName,
        string probeExpression,
        string requireEnvVar,
        int timeoutMs,
        Func<IReadOnlyDictionary<string, string>>? environment = null,
        Func<string?>? preflight = null,
        bool persistent = false,
        string? interpreterOverrideEnvVar = null)
    {
        ScriptName = scriptName;
        RequireEnvVar = requireEnvVar;
        TimeoutMs = timeoutMs;
        Environment = environment;
        Persistent = persistent;
        InterpreterOverrideEnvVar = interpreterOverrideEnvVar;
        _probe = new Lazy<(string?, string?, string?)>(() =>
        {
            string? blocked = preflight?.Invoke();
            return blocked is not null ? (null, null, blocked) : RunProbe(probeExpression);
        });
    }

    private string ScriptName { get; }

    private string RequireEnvVar { get; }

    private int TimeoutMs { get; }

    private Func<IReadOnlyDictionary<string, string>>? Environment { get; }

    private bool Persistent { get; }

    private string? InterpreterOverrideEnvVar { get; }

    /// <summary>The interpreter resolved for this driver — the override env var's value if set and
    /// non-empty, otherwise null (so <see cref="RunProbe"/> falls back to python3/python on PATH).</summary>
    private string? InterpreterOverride =>
        InterpreterOverrideEnvVar is not null
            ? System.Environment.GetEnvironmentVariable(InterpreterOverrideEnvVar) is { Length: > 0 } ov
                ? ov : null
            : null;

    /// <summary>Marker the serve loop writes to stdout when a result file is complete.</summary>
    private const string DoneMarker = "__EW_DONE__";

    private readonly object _serverLock = new();

    private Process? _server;

    public bool Available => _probe.Value.Exe is not null;

    public string? Version => _probe.Value.Version;

    public string? UnavailableReason => _probe.Value.Error;

    /// <summary>
    /// Gate every interop test on this: <c>Driver.Require();</c> as the first statement of a test
    /// marked <c>[SkippableFact]</c> or <c>[SkippableTheory]</c>. Returns when the toolchain is
    /// present, SKIPS the test when it is not, and throws when the require-env-var says this job
    /// depended on it.
    ///
    /// <para>Skipping rather than returning early is the whole point. The early return reported the
    /// test as PASSED, so a machine with no JDK ran 54 Spark conformance tests to green in 94 ms — a
    /// result indistinguishable from having validated all of them. A skip states what happened.</para>
    ///
    /// <para>The skip is deliberately NOT the whole mechanism, because an honest green is still a
    /// green: a job can skip an entire tier and pass. That is what <see cref="RequireEnvVar"/> is
    /// for. The two answer different questions — the skip answers "what did this run actually do?",
    /// the env var answers "was this job allowed to tolerate that?" — which is why both exist and
    /// why the env var is per tier: ci.yml requires delta-rs and DuckDB but not Spark, since
    /// windows-latest has no JDK, while the nightly requires all three.</para>
    /// </summary>
    public void Require()
    {
        if (Available)
            return;

        if (System.Environment.GetEnvironmentVariable(RequireEnvVar) == "1")
        {
            throw new InvalidOperationException(
                $"{RequireEnvVar}=1 but the {ScriptName} interop toolchain is unavailable: "
                + $"{_probe.Value.Error}");
        }

        // Always skips by the time control reaches here — Skip exposes only the two conditional
        // forms, so the condition is restated rather than an unconditional Skip.Always being called.
        Skip.IfNot(Available, $"{ScriptName} toolchain unavailable: {_probe.Value.Error}");
    }

    /// <summary>Runs one command and returns its parsed JSON result, asserting <c>ok</c>.</summary>
    public JsonElement Invoke(string command, object? args = null)
    {
        var result = InvokeRaw(command, args);
        if (!result.GetProperty("ok").GetBoolean())
        {
            string error = result.TryGetProperty("error", out var e) ? e.GetString()! : "(no message)";
            throw new InvalidOperationException($"{ScriptName} '{command}' failed: {error}");
        }

        return result;
    }

    /// <summary>As <see cref="Invoke"/> but does not assert success — for tests expecting a rejection.</summary>
    public JsonElement InvokeRaw(string command, object? args = null)
    {
        string driver = Path.Combine(AppContext.BaseDirectory, "Interop", ScriptName);
        if (!File.Exists(driver))
            throw new FileNotFoundException($"Interop driver script not found at {driver}.");

        string id = Guid.NewGuid().ToString("N");
        string argsFile = Path.Combine(Path.GetTempPath(), $"ew_interop_args_{id}.json");
        string resultFile = Path.Combine(Path.GetTempPath(), $"ew_interop_result_{id}.json");
        File.WriteAllText(argsFile, JsonSerializer.Serialize(args ?? new { }), new UTF8Encoding(false));

        try
        {
            if (Persistent)
                return InvokeOnServer(driver, command, argsFile, resultFile);

            var psi = new ProcessStartInfo(_probe.Value.Exe!)
            {
                Arguments = $"\"{driver}\" {command} \"{argsFile}\" \"{resultFile}\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
                StandardOutputEncoding = Encoding.UTF8,
                StandardErrorEncoding = Encoding.UTF8,
            };
            // Drivers emit non-ASCII paths; Windows consoles default to cp1252 and would mangle them.
            psi.EnvironmentVariables["PYTHONIOENCODING"] = "utf-8";
            foreach (var kvp in Environment?.Invoke() ?? new Dictionary<string, string>())
                psi.EnvironmentVariables[kvp.Key] = kvp.Value;

            using var proc = Process.Start(psi)!;

            // Drain both pipes on dedicated threads, then bound the whole wait by TimeoutMs.
            //
            // Draining separately is required because Spark fills the stderr pipe well past its
            // capacity, and reading the two in sequence would deadlock. But reading stdout to EOF on
            // THIS thread is just as dangerous: EOF arrives only when the child exits, so a driver
            // that writes its result and then wedges on the way out blocks here forever and the
            // WaitForExit timeout below is never reached. Measured: that is exactly how the delta-rs
            // driver behaved under Python 3.13 before it learned to os._exit, and it turned a
            // 3-minute nightly into a 60-minute job that printed nothing at all.
            //
            // LongRunning rather than the pool for the same reason InvokeOnServer uses it: xUnit runs
            // test classes in parallel, so several of these can be outstanding at once.
            var stdoutTask = Task.Factory.StartNew(
                () => proc.StandardOutput.ReadToEnd(),
                CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            var stderrTask = Task.Factory.StartNew(
                () => proc.StandardError.ReadToEnd(),
                CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            if (!proc.WaitForExit(TimeoutMs))
            {
                try { proc.Kill(); } catch { }
                throw new TimeoutException(
                    $"{ScriptName} '{command}' timed out after {TimeoutMs / 1000}s. "
                    + $"stderr tail: {Tail(Drain(stderrTask), 2000)}");
            }

            string stdout = Drain(stdoutTask);
            string stderr = Drain(stderrTask);
            if (!File.Exists(resultFile))
            {
                throw new InvalidOperationException(
                    $"{ScriptName} '{command}' wrote no result file (exit {proc.ExitCode}). "
                    + $"stdout: {Tail(stdout, 500)} stderr tail: {Tail(stderr, 2000)}");
            }

            return JsonDocument.Parse(File.ReadAllText(resultFile)).RootElement.Clone();
        }
        finally
        {
            try { File.Delete(argsFile); } catch { }
            try { File.Delete(resultFile); } catch { }
        }
    }

    /// <summary>
    /// Sends one command to the long-lived driver process and waits for its result.
    ///
    /// <para>Serialized on <see cref="_serverLock"/>: one process, one stdin, one stdout, so exactly
    /// one request may be in flight. Test classes can run in parallel under xUnit, and two
    /// simultaneous writers would interleave into nonsense.</para>
    /// </summary>
    private JsonElement InvokeOnServer(string driver, string command, string argsFile, string resultFile)
    {
        lock (_serverLock)
        {
            var proc = EnsureServer(driver);

            // Tab-separated: paths may contain spaces, but never tabs.
            proc.StandardInput.WriteLine($"{command}\t{argsFile}\t{resultFile}");
            proc.StandardInput.Flush();

            string expected = DoneMarker + Path.GetFileName(resultFile);

            // Contains, not equality: Spark may emit a partial line with no trailing newline, which
            // would leave our marker appended to its text rather than alone on a line.
            //
            // Read on a DEDICATED thread, not a pool one. This method blocks the calling thread for up to
            // TimeoutMs, and xUnit runs test classes in parallel — so several of the pool's threads can be
            // sitting in that Wait at once. On .NET Framework the pool injects new threads about one every
            // 500 ms, so a pool-scheduled reader (or the pool-dispatched stderr drain in EnsureServer) can go
            // unscheduled for a long time; the driver's stderr pipe then fills, Spark BLOCKS writing to it
            // mid-command, and this times out after 600 s against a JVM that is perfectly healthy. Measured:
            // that is exactly how the net472 leg failed, reporting the stall against whichever test happened
            // to hold the lock.
            var wait = Task.Factory.StartNew(
                () =>
                {
                    string? line;
                    while ((line = proc.StandardOutput.ReadLine()) is not null)
                    {
                        if (line.Contains(expected, StringComparison.Ordinal))
                            return true;
                    }

                    return false;
                },
                CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            if (!wait.Wait(TimeoutMs))
            {
                StopServer();
                throw new TimeoutException(
                    $"{ScriptName} '{command}' timed out after {TimeoutMs / 1000}s; server restarted.");
            }

            if (!wait.GetAwaiter().GetResult())
            {
                string stderr = DrainedStderr;
                StopServer();
                throw new InvalidOperationException(
                    $"{ScriptName} server exited while running '{command}'. "
                    + $"stderr tail: {Tail(stderr, 2000)}");
            }

            if (!File.Exists(resultFile))
            {
                throw new InvalidOperationException(
                    $"{ScriptName} '{command}' signalled completion but wrote no result file.");
            }

            return JsonDocument.Parse(File.ReadAllText(resultFile)).RootElement.Clone();
        }
    }

    /// <summary>Starts the serve-mode process, or returns the running one. Caller holds the lock.</summary>
    private Process EnsureServer(string driver)
    {
        if (_server is { HasExited: false })
            return _server;

        _server = null;
        _stderr.Clear();

        var psi = new ProcessStartInfo(_probe.Value.Exe!)
        {
            Arguments = $"\"{driver}\" serve",
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
        };
        psi.EnvironmentVariables["PYTHONIOENCODING"] = "utf-8";
        // Python buffers stdout when it is a pipe; the done-marker would sit in that buffer forever.
        psi.EnvironmentVariables["PYTHONUNBUFFERED"] = "1";
        foreach (var kvp in Environment?.Invoke() ?? new Dictionary<string, string>())
            psi.EnvironmentVariables[kvp.Key] = kvp.Value;

        var proc = Process.Start(psi)!;

        // Drain stderr continuously: Spark writes far more than the pipe buffer holds, and a full
        // pipe would block the driver mid-command.
        proc.ErrorDataReceived += (_, e) =>
        {
            if (e.Data is null)
                return;
            lock (_stderr)
            {
                _stderr.AppendLine(e.Data);
                if (_stderr.Length > 64 * 1024)
                    _stderr.Remove(0, _stderr.Length - 32 * 1024);
            }
        };
        proc.BeginErrorReadLine();

        _server = proc;
        AppDomain.CurrentDomain.ProcessExit += (_, _) => StopServer();
        return proc;
    }

    private void StopServer()
    {
        var proc = _server;
        _server = null;
        if (proc is null)
            return;

        try
        {
            if (!proc.HasExited)
            {
                proc.StandardInput.WriteLine("shutdown");
                proc.StandardInput.Flush();
                if (!proc.WaitForExit(15_000))
                    proc.Kill();
            }
        }
        catch
        {
            try { proc.Kill(); } catch { }
        }
        finally
        {
            proc.Dispose();
        }
    }

    private readonly StringBuilder _stderr = new();

    private string DrainedStderr
    {
        get { lock (_stderr) { return _stderr.ToString(); } }
    }

    /// <summary>Collects a pipe-reader's text, giving up rather than blocking if the child was killed
    /// while something else still holds the write end.</summary>
    private static string Drain(Task<string> reader) =>
        reader.Wait(10_000) ? reader.GetAwaiter().GetResult() : "(pipe reader did not finish)";

    private static string Tail(string s, int max) =>
        string.IsNullOrEmpty(s) || s.Length <= max ? s : s.Substring(s.Length - max);

    private (string?, string?, string?) RunProbe(string probeExpression)
    {
        // An explicit interpreter (a venv python) wins; otherwise fall back to what is on PATH.
        var candidates = InterpreterOverride is { } exe
            ? new[] { exe }
            : new[] { "python3", "python" };
        foreach (string candidate in candidates)
        {
            try
            {
                var psi = new ProcessStartInfo(candidate, $"-c \"{probeExpression}\"")
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                };
                foreach (var kvp in Environment?.Invoke() ?? new Dictionary<string, string>())
                    psi.EnvironmentVariables[kvp.Key] = kvp.Value;

                using var p = Process.Start(psi)!;
                string stdout = p.StandardOutput.ReadToEnd();
                p.WaitForExit(30_000);
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

        return (null, null, $"no python interpreter satisfying `{probeExpression}` on PATH");
    }
}
