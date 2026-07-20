// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using System.Text.Json;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3</b> external validation: drives PySpark + delta-spark, the Delta reference
/// implementation. The only tier that exercises writer features, <c>DESCRIBE DETAIL</c>, clustering
/// and <c>OPTIMIZE</c>, and the only one that reads column-mapped tables written with the legacy
/// <c>minReader=2</c>/<c>minWriter=5</c> numbering that <see cref="DeltaRs"/> declines to open.</para>
///
/// <para><b>Cost.</b> Every command starts a JVM and a fresh SparkSession — 15-20 seconds before any
/// work happens. This tier is opt-in / nightly, not a per-commit gate. Driver commands are
/// coarse-grained so one test needs exactly one session.</para>
///
/// <para><b>Setup.</b> Needs a JDK 17+ (<c>JAVA_HOME</c>), <c>pip install pyspark delta-spark</c>, and
/// on Windows a Hadoop <c>winutils.exe</c> + <c>hadoop.dll</c> pointed at by <c>HADOOP_HOME</c>.
/// Setting <c>HADOOP_HOME</c> alone is NOT enough on Windows: <c>hadoop.dll</c> must also be findable
/// on <c>PATH</c> or Hadoop throws <c>UnsatisfiedLinkError: NativeIO$Windows.access0</c> on the first
/// directory listing. This class prepends <c>%HADOOP_HOME%\bin</c> to the child process PATH so
/// callers do not have to.</para>
/// </summary>
internal static class Spark
{
    /// <summary>Versions these assertions were established against — see <see cref="DeltaRs.ValidatedAgainstVersion"/>
    /// for why this is recorded rather than enforced. delta-spark's major version must match
    /// pyspark's: 4.x pairs with Spark 4.0, 3.x with Spark 3.5.</summary>
    public const string ValidatedAgainstVersion = "pyspark 4.0.1 / delta-spark 4.0.0";

    private static readonly InteropDriver Driver = new(
        scriptName: "spark_driver.py",
        // The `delta` package exposes no __version__; its version lives in the delta-spark dist
        // metadata. Importing delta.tables (not just delta) is what actually proves the pairing works.
        probeExpression:
            "import pyspark, delta.tables, json; from importlib.metadata import version; "
            + "print(json.dumps({'v': pyspark.__version__ + '/' + version('delta-spark')}))",
        requireEnvVar: "EW_REQUIRE_SPARK_INTEROP",
        // A cold start pays JVM boot plus Ivy resolution of the Delta jars on first ever run.
        timeoutMs: 600_000,
        environment: BuildEnvironment,
        preflight: Preflight,
        // One JVM for the whole test run instead of one per command. Startup dominates this tier:
        // per-command it was ~15s of session startup around ~1s of actual work.
        persistent: true,
        // Point the tier at a specific interpreter (e.g. an isolated Spark 4.1 venv) without disturbing
        // the base interpreter delta-rs uses. Unset -> python/python3 on PATH, the existing behaviour.
        interpreterOverrideEnvVar: "EW_SPARK_PYTHON");

    public static bool Available => Driver.Available;

    public static string? Version => Driver.Version;

    public static bool EnsureAvailable() => Driver.EnsureAvailable();

    public static JsonElement Invoke(string command, object? args = null) => Driver.Invoke(command, args);

    public static JsonElement InvokeRaw(string command, object? args = null) => Driver.InvokeRaw(command, args);

    /// <summary>
    /// Prerequisites that <c>import pyspark</c> cannot detect. Both are things a developer can easily
    /// have half-configured, and in both cases Spark fails deep inside the JVM with an error that does
    /// not name the real cause — so check them up front and report the fix.
    /// </summary>
    private static string? Preflight()
    {
        string? javaHome = System.Environment.GetEnvironmentVariable("JAVA_HOME");
        bool javaOnPath = (System.Environment.GetEnvironmentVariable("PATH") ?? "")
            .Split(Path.PathSeparator)
            .Any(dir => !string.IsNullOrWhiteSpace(dir) && JavaExistsIn(dir));

        if (string.IsNullOrEmpty(javaHome) && !javaOnPath)
            return "no JDK found (set JAVA_HOME, or put java on PATH); Spark 4.0 needs Java 17+";

        if (!string.IsNullOrEmpty(javaHome) && !JavaExistsIn(Path.Combine(javaHome, "bin")))
            return $"JAVA_HOME is set to '{javaHome}' but no java executable is under its bin directory";

        // Without winutils.exe + hadoop.dll, Hadoop's Windows shim throws
        // UnsatisfiedLinkError: NativeIO$Windows.access0 on the first directory listing.
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            string? hadoopHome = System.Environment.GetEnvironmentVariable("HADOOP_HOME");
            if (string.IsNullOrEmpty(hadoopHome))
                return "HADOOP_HOME is not set; Spark on Windows needs winutils.exe + hadoop.dll";
            if (!File.Exists(Path.Combine(hadoopHome, "bin", "winutils.exe")))
                return $"HADOOP_HOME is set to '{hadoopHome}' but bin\\winutils.exe is missing";
        }

        return null;
    }

    private static bool JavaExistsIn(string dir) =>
        File.Exists(Path.Combine(dir, "java.exe")) || File.Exists(Path.Combine(dir, "java"));

    private static IReadOnlyDictionary<string, string> BuildEnvironment()
    {
        var env = new Dictionary<string, string>();

        // Spark launches the Python worker by name; without this it can pick a different interpreter
        // than the driver, which fails with a version-mismatch error that reads like a Spark bug. When
        // the tier is pointed at a venv (EW_SPARK_PYTHON), the worker must use that SAME interpreter, or
        // pyspark's driver/worker version check fails.
        env["PYSPARK_PYTHON"] =
            System.Environment.GetEnvironmentVariable("EW_SPARK_PYTHON") is { Length: > 0 } venv
                ? venv : "python";

        string? hadoopHome = System.Environment.GetEnvironmentVariable("HADOOP_HOME");
        if (!string.IsNullOrEmpty(hadoopHome))
        {
            // hadoop.dll must be on PATH, not merely under HADOOP_HOME — see the class remarks.
            string bin = Path.Combine(hadoopHome, "bin");
            string path = System.Environment.GetEnvironmentVariable("PATH") ?? "";
            env["PATH"] = $"{bin}{Path.PathSeparator}{path}";
        }

        return env;
    }
}
