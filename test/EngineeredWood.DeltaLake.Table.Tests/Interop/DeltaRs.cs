// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 1</b> external validation: drives <c>delta-rs</c> (pip <c>deltalake</c>) over a table
/// EngineeredWood wrote, and vice versa.</para>
///
/// <para><b>Why this exists.</b> Every other Delta test in this suite round-trips through EW's own
/// reader, which proves reader and writer agree — not that either matches the Delta spec. Every
/// interop bug in <c>doc/upstream-landing-notes.md</c> (DV framing, <c>add.path</c> encoding,
/// checkpoint content, physical names) round-tripped perfectly. These tests use an independent
/// implementation as the oracle.</para>
///
/// <para>Cheapest tier: no JVM, one <c>pip install deltalake</c>. Cannot read column-mapped tables
/// written with the legacy <c>minReader=2</c> numbering, and has no writer-feature/OPTIMIZE/DESCRIBE
/// DETAIL surface — that is <see cref="Spark"/>'s job.</para>
/// </summary>
internal static class DeltaRs
{
    /// <summary>The version this harness's assertions were established against. Recorded rather than
    /// enforced: a delta-rs upgrade that changes behaviour should read as "the oracle moved", not as
    /// an EW regression, and the first thing to check is this number.</summary>
    public const string ValidatedAgainstVersion = "1.6.2";

    private static readonly InteropDriver Driver = new(
        scriptName: "delta_rs_driver.py",
        probeExpression: "import deltalake, json; print(json.dumps({'v': deltalake.__version__}))",
        requireEnvVar: "EW_REQUIRE_DELTA_INTEROP",
        timeoutMs: 120_000);

    public static bool Available => Driver.Available;

    public static string? Version => Driver.Version;

    public static bool EnsureAvailable() => Driver.EnsureAvailable();

    public static JsonElement Invoke(string command, object? args = null) => Driver.Invoke(command, args);

    public static JsonElement InvokeRaw(string command, object? args = null) => Driver.InvokeRaw(command, args);
}
