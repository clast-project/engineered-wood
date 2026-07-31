// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// External validation against DuckDB, which reads parquet directly and has a NATIVE <c>VARIANT</c>
/// type. That combination is what the other tiers cannot offer: <see cref="DeltaRs"/> surfaces a
/// variant as its physical <c>struct&lt;value, metadata&gt;</c> and so cannot tell a shredded column
/// from a broken one, and <see cref="Spark"/> costs a JVM per run.
/// </summary>
/// <remarks>
/// <para><b>Setup.</b> <c>pip install duckdb</c> (1.4+; VARIANT does not exist before that, so an
/// older DuckDB would read a shredded column as a bare struct and look like a conformance failure
/// rather than a missing feature). Point <c>EW_DUCKDB_PYTHON</c> at an interpreter that has it —
/// on this project's dev boxes an isolated venv, since the base interpreter's DuckDB is pinned older
/// for unrelated reasons. <c>EW_REQUIRE_DUCKDB_INTEROP=1</c> turns unavailability into a hard failure
/// so a green run proves the tier actually ran.</para>
///
/// <para><b>Cost.</b> No JVM, no session: a command is a process start and a query, well under a
/// second. Cheap enough to run per-commit if it ever earns a place there.</para>
/// </remarks>
internal static class DuckDb
{
    /// <summary>The version these assertions were established against — recorded rather than
    /// enforced, for the reason given on <see cref="DeltaRs.ValidatedAgainstVersion"/>: a DuckDB
    /// upgrade that changes behaviour should read as "the oracle moved", and this is the first
    /// number to check.</summary>
    public const string ValidatedAgainstVersion = "1.5.5";

    private static readonly InteropDriver Driver = new(
        scriptName: "duckdb_driver.py",
        probeExpression: "import duckdb, json; print(json.dumps({'v': duckdb.__version__}))",
        requireEnvVar: "EW_REQUIRE_DUCKDB_INTEROP",
        timeoutMs: 120_000,
        interpreterOverrideEnvVar: "EW_DUCKDB_PYTHON");

    public static bool Available => Driver.Available;

    public static string? Version => Driver.Version;

    public static bool EnsureAvailable() => Driver.EnsureAvailable();

    public static JsonElement Invoke(string command, object? args = null) => Driver.Invoke(command, args);

    public static JsonElement InvokeRaw(string command, object? args = null) => Driver.InvokeRaw(command, args);

    /// <summary>
    /// True when the resolved DuckDB predates <c>VARIANT</c> (&lt; 1.4), where a shredded column reads
    /// as a bare struct. Tests that assert variant semantics skip on it rather than fail.
    /// </summary>
    public static bool HasVariantType
    {
        get
        {
            string? version = Version;
            if (string.IsNullOrEmpty(version)) return false;
            var parts = version!.Split('.');
            return parts.Length >= 2
                && int.TryParse(parts[0], out int major)
                && int.TryParse(parts[1], out int minor)
                && (major > 1 || (major == 1 && minor >= 4));
        }
    }
}
