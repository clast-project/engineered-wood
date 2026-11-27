// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

using EngineeredWood.Tests.Interop;

namespace EngineeredWood.Iceberg.Tests.Interop;

/// <summary>
/// <para>The DuckDB tier: <c>iceberg_scan</c> as a reader of Iceberg tables, through
/// <c>Interop/duckdb_iceberg_driver.py</c>. It is one of the per-commit reader oracles of the v3 plan
/// (<c>doc/iceberg-v3-plan.md</c>, #953 item 6). It reads a table from its metadata file with no catalog, and it
/// reads v3 deletion vectors, nanosecond timestamps, initial defaults and the row-lineage columns
/// <c>_row_id</c> and <c>_last_updated_sequence_number</c>. MEASURED on 2.0.0: it agrees with iceberg-go row for
/// row, lineage included, on every iceberg-go scenario.</para>
///
/// <para><b>Setup.</b> <c>pip install duckdb==2.0.0</c>. The <c>iceberg</c> extension is installed on first use,
/// which needs network access once. <c>EW_ICEBERG_PYTHON</c> names an interpreter to use instead of the
/// <c>python3</c>/<c>python</c> on PATH, shared with the pyiceberg tier. The tier is unavailable below DuckDB 2.0
/// or without the extension, which skips its tests unless <c>EW_REQUIRE_ICEBERG_DUCKDB_INTEROP=1</c>.</para>
/// </summary>
internal static class DuckDbIceberg
{
    /// <summary>The version these tests were established against. Recorded, not enforced: a bump that changes
    /// behaviour should read as "the oracle moved".</summary>
    public const string ValidatedAgainstVersion = "2.0.0";

    /// <summary>An interpreter for the Iceberg Python tiers, instead of the one on PATH.</summary>
    public const string InterpreterOverrideEnvVar = "EW_ICEBERG_PYTHON";

    private static readonly InteropDriver Driver = new(
        scriptName: "duckdb_iceberg_driver.py",
        // Loading the extension is part of the probe: a DuckDB that cannot install it cannot run the tier. So is
        // pyarrow, which every scan's result goes through (to_arrow_table).
        probeExpression: "import duckdb, pyarrow, json; "
            + "assert tuple(int(x) for x in duckdb.__version__.split('.')[:2]) >= (2, 0), 'DuckDB 2.0 or later'; "
            + "c = duckdb.connect(); c.execute('INSTALL iceberg'); c.execute('LOAD iceberg'); "
            + "print(json.dumps({'v': duckdb.__version__}))",
        requireEnvVar: "EW_REQUIRE_ICEBERG_DUCKDB_INTEROP",
        timeoutMs: 180_000,
        interpreterOverrideEnvVar: InterpreterOverrideEnvVar);

    public static bool Available => Driver.Available;

    public static string? Version => Driver.Version;

    public static void Require() => Driver.Require();

    /// <summary>Every row of the table at <paramref name="metadataLocation"/>; with
    /// <paramref name="rowLineage"/>, its <c>_row_id</c> and <c>_last_updated_sequence_number</c> too.</summary>
    public static JsonElement Scan(string metadataLocation, bool rowLineage) =>
        Driver.Invoke("scan", new { metadata_location = metadataLocation, row_lineage = rowLineage });
}
