// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

using EngineeredWood.Tests.Interop;

namespace EngineeredWood.Iceberg.Tests.Interop;

/// <summary>
/// <para>The pyiceberg tier, through <c>Interop/pyiceberg_driver.py</c>, with tables in a SQL catalog on sqlite.
/// It is one of the per-commit oracles of the v3 plan (<c>doc/iceberg-v3-plan.md</c>, #953 item 6), in two roles:
/// a READER of v3 tables, deletion vectors included, and the v2 WRITER oracle, since it writes v2 but not v3.</para>
///
/// <para>MEASURED on 0.12.0, and pinned by the tests so that a change reads as "the oracle moved":</para>
/// <list type="bullet">
/// <item>The default <c>PyArrowFileIO</c> cannot open a <c>file:///C:/...</c> location on Windows (WinError 123).
/// The driver uses <c>FsspecFileIO</c>, which can.</item>
/// <item>A scan cannot select <c>_row_id</c> or <c>_last_updated_sequence_number</c>, so this tier does not check
/// row lineage. DuckDB and iceberg-go do.</item>
/// <item>A scan of a table with equality deletes is refused.</item>
/// </list>
///
/// <para><b>Setup.</b> <c>pip install "pyiceberg[sql-sqlite,pyarrow]==0.12.0"</c>. <c>EW_ICEBERG_PYTHON</c> names
/// an interpreter to use instead of the one on PATH, shared with the DuckDB tier. Without pyiceberg the tests skip,
/// unless <c>EW_REQUIRE_ICEBERG_PYICEBERG_INTEROP=1</c>.</para>
/// </summary>
internal static class PyIceberg
{
    /// <summary>The version these tests were established against. Recorded, not enforced.</summary>
    public const string ValidatedAgainstVersion = "0.12.0";

    private static readonly InteropDriver Driver = new(
        scriptName: "pyiceberg_driver.py",
        probeExpression: "import json, pyiceberg, pyarrow, sqlalchemy; "
            + "from pyiceberg.catalog.sql import SqlCatalog; from pyiceberg.io.fsspec import FsspecFileIO; "
            + "print(json.dumps({'v': pyiceberg.__version__}))",
        requireEnvVar: "EW_REQUIRE_ICEBERG_PYICEBERG_INTEROP",
        timeoutMs: 180_000,
        interpreterOverrideEnvVar: DuckDbIceberg.InterpreterOverrideEnvVar);

    public static bool Available => Driver.Available;

    public static string? Version => Driver.Version;

    public static void Require() => Driver.Require();

    /// <summary>Every row of the table at <paramref name="metadataLocation"/>.</summary>
    public static JsonElement Scan(string metadataLocation) =>
        Driver.Invoke("scan", new { metadata_location = metadataLocation });

    /// <summary>As <see cref="Scan"/>, without asserting success.</summary>
    public static JsonElement ScanRaw(string metadataLocation) =>
        Driver.InvokeRaw("scan", new { metadata_location = metadataLocation });

    /// <summary>pyiceberg's scan plan: each data file it reads, with the delete files it applies.</summary>
    public static JsonElement Plan(string metadataLocation) =>
        Driver.Invoke("plan", new { metadata_location = metadataLocation });

    /// <summary>pyiceberg's view of a metadata file.</summary>
    public static JsonElement Describe(string metadataLocation) =>
        Driver.Invoke("describe", new { metadata_location = metadataLocation });

    /// <summary>Writes one of the driver's v2 scenario tables into <paramref name="warehouse"/> and returns its
    /// newest metadata file's location.</summary>
    public static string Scenario(string warehouse, string scenario) =>
        Driver.Invoke("scenario", new { warehouse, scenario }).GetProperty("metadata_location").GetString()!;
}
