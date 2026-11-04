// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

using EngineeredWood.Tests.Interop;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// External validation against DuckDB, which reads parquet directly and has a NATIVE <c>VARIANT</c>
/// type. That combination is what the other tiers cannot offer: <see cref="DeltaRs"/> surfaces a
/// variant as its physical <c>struct&lt;value, metadata&gt;</c> and so cannot tell a shredded column
/// from a broken one, and <see cref="Spark"/> costs a JVM per run.
/// </summary>
/// <remarks>
/// <para><b>Setup.</b> <c>pip install duckdb</c> (1.5+ to read EW's variant files, 2.0+ for the cases
/// DuckDB writes; see <see cref="ReadsVariantParquet"/> and <see cref="WritesShreddedVariantOracle"/>).
/// Point <c>EW_DUCKDB_PYTHON</c> at an interpreter that has it —
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
    public const string ValidatedAgainstVersion = "2.0.0";

    private static readonly InteropDriver Driver = new(
        scriptName: "duckdb_driver.py",
        probeExpression: "import duckdb, json; print(json.dumps({'v': duckdb.__version__}))",
        requireEnvVar: "EW_REQUIRE_DUCKDB_INTEROP",
        timeoutMs: 120_000,
        interpreterOverrideEnvVar: "EW_DUCKDB_PYTHON");

    public static bool Available => Driver.Available;

    public static string? Version => Driver.Version;

    public static void Require() => Driver.Require();

    public static JsonElement Invoke(string command, object? args = null) => Driver.Invoke(command, args);

    public static JsonElement InvokeRaw(string command, object? args = null) => Driver.InvokeRaw(command, args);

    /// <summary>
    /// True when the resolved DuckDB can read EW's variant parquet: 1.5+, measured on 1.5.5 and 2.0.0.
    /// <c>VARIANT</c> arrived in 1.4, but 1.4.5 cannot decode EW's files ("Corrupted VARIANT 'metadata'
    /// buffer" unshredded, "found version: 8" shredded), and before 1.4 there is no VARIANT at all.
    /// Tests that read a variant through DuckDB skip below it rather than fail.
    /// </summary>
    public static bool ReadsVariantParquet => AtLeast(1, 5);

    /// <summary>
    /// True when a DuckDB-WRITTEN shredded file can be the oracle: 2.0+. DuckDB 1.5 already shreds on
    /// <c>COPY … TO parquet</c>, but it writes a SQL NULL variant as a present group holding a variant
    /// null (<c>value</c> = <c>0x00</c>), and its own reader reports that as SQL NULL. By the spec it is
    /// a variant null, which is what EW reads, so on 1.5 DuckDB and EW disagree about NULLs and DuckDB
    /// is the one that is wrong. 2.0 writes a SQL NULL as a null group.
    /// </summary>
    public static bool WritesShreddedVariantOracle => AtLeast(2, 0);

    private static bool AtLeast(int wantMajor, int wantMinor)
    {
        string? version = Version;
        if (string.IsNullOrEmpty(version)) return false;
        var parts = version!.Split('.');
        return parts.Length >= 2
            && int.TryParse(parts[0], out int major)
            && int.TryParse(parts[1], out int minor)
            && (major > wantMajor || (major == wantMajor && minor >= wantMinor));
    }
}
