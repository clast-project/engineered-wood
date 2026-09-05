// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.Tests.Interop;

namespace EngineeredWood.Tests.Parquet.Interop;

/// <summary>
/// External validation against two parquet readers that nothing else in this repository exercises:
/// <b>DuckDB</b>, whose decoder was written from the spec rather than derived from a reference
/// implementation, and <b>DataFusion</b>, which reads through the <b>arrow-rs</b> <c>parquet</c>
/// crate — the decoder behind polars, delta-rs and every Rust consumer.
/// </summary>
/// <remarks>
/// <para><b>Why a tier of its own.</b> ParquetSharp already covers parquet-cpp from inside the test
/// suite, and the batched-runs prototype was cross-checked against pyarrow and fastparquet out of
/// band. All three descend from, or were written against, the same two reference implementations.
/// A writer-side framing change is exactly the kind of thing a genuinely independent decoder is
/// placed differently to catch, so these two are worth their own tier.</para>
///
/// <para><b>Setup.</b> <c>pip install duckdb datafusion pyarrow</c> into any interpreter and
/// point <c>EW_PARQUET_READERS_PYTHON</c> at it — on this project's dev boxes an isolated venv,
/// since the base interpreter carries a DuckDB pinned older for the Delta suite's reasons.
/// <c>EW_REQUIRE_PARQUET_READERS_INTEROP=1</c> turns unavailability into a hard failure, so a green
/// CI run proves the tier actually ran rather than skipped.</para>
///
/// <para><b>Cost.</b> A process start and a query per call, well under a second — no JVM, no
/// session. The tier first probes whether either reader is installed, then each read reports whether
/// its selected reader is available, so one being absent skips only its own tests.</para>
/// </remarks>
internal static class ExternalParquetReaders
{
    /// <summary>The versions these assertions were established against — recorded rather than
    /// enforced, so that a reader upgrade which changes behaviour reads as "the oracle moved" and
    /// this is the first number to check.</summary>
    public const string ValidatedAgainstVersions = "duckdb 1.5.5, datafusion 54.0.0 (arrow-rs)";

    public const string DuckDb = "duckdb";
    public const string DataFusion = "datafusion";

    private static readonly InteropDriver Driver = new(
        scriptName: "parquet_readers_driver.py",
        // find_spec rather than import: it answers the availability question without paying to
        // load either library, and the asserts are what make a missing prerequisite an unavailable
        // tier rather than an available one that fails later for an unrelated reason.
        //
        // pyarrow is checked despite not being a parquet reader here, because it is the transport
        // both readers hand decoded values back through. Without this the tier would report itself
        // available, satisfy EW_REQUIRE_PARQUET_READERS_INTEROP=1, and then fail with an ImportError
        // indistinguishable from a decode failure — the same undeclared-dependency trap ci.yml
        // already records for deltalake and pyarrow.
        probeExpression:
            "import json, importlib.util as u; "
            + "ms=[m for m in ('duckdb','datafusion') if u.find_spec(m)]; "
            + "assert ms, 'neither duckdb nor datafusion is installed'; "
            + "assert u.find_spec('pyarrow'), "
            + "'pyarrow is missing; it is how both readers return decoded values'; "
            + "print(json.dumps({'v': ', '.join(ms)}))",
        requireEnvVar: "EW_REQUIRE_PARQUET_READERS_INTEROP",
        timeoutMs: 180_000,
        interpreterOverrideEnvVar: "EW_PARQUET_READERS_PYTHON");

    public static bool Available => Driver.Available;

    public static string? Version => Driver.Version;

    public static void Require() => Driver.Require();

    /// <summary>
    /// Decodes <paramref name="path"/> with one reader and reports what happened. The three
    /// outcomes are deliberately distinct, because only one of them is a bug: the reader may be
    /// absent (skip — the two are probed independently, so a box with only one still validates
    /// against that one), it may fail to decode (a finding only if it decoded the file's unbatched
    /// twin), or it may succeed.
    /// </summary>
    public static ReaderOutcome Read(string reader, string path)
    {
        var result = Driver.InvokeRaw("read_digest", new { reader, path });

        if (!result.GetProperty("ok").GetBoolean())
        {
            string error = result.TryGetProperty("error", out var e) ? e.GetString()! : "(no message)";
            return new ReaderOutcome(Installed: true, Error: error, Result: null);
        }

        if (!result.TryGetProperty("available", out var available) || !available.GetBoolean())
        {
            return new ReaderOutcome(
                Installed: false,
                Error: result.TryGetProperty("error", out var a) ? a.GetString() : null,
                Result: null);
        }

        var columns = new List<ReaderColumn>();
        foreach (var column in result.GetProperty("columns").EnumerateArray())
        {
            columns.Add(new ReaderColumn(
                column.GetProperty("name").GetString()!,
                column.GetProperty("digest").GetString()!));
        }

        return new ReaderOutcome(
            Installed: true,
            Error: null,
            Result: new ReaderResult(
                result.GetProperty("rows").GetInt64(),
                columns,
                result.TryGetProperty("version", out var v) ? v.GetString() : null));
    }

    internal sealed record ReaderOutcome(bool Installed, string? Error, ReaderResult? Result);

    internal sealed record ReaderColumn(string Name, string Digest);

    internal sealed record ReaderResult(
        long Rows, IReadOnlyList<ReaderColumn> Columns, string? Version);
}
