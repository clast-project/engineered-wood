// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> #745: IcebergCompatV3 against delta-spark. The feature exists from delta-spark 4.3, so these
/// skip on the 4.0 lane. They differ in what they prove by lane: 4.3.1 declares the feature but runs none of its
/// checks (IcebergCompatV3 is missing from its IcebergCompat.knownVersions), so on Spark 4.1 they show the tables
/// are readable and writable through. 4.4.1 runs every V3 check on every commit, so on Spark 4.2 each Spark write
/// to an EW-made table is also a check that the table EW made satisfies them, row tracking included.</para>
/// <para>delta-spark 4.4 refuses any write to a V3 table unless the session enables
/// <c>spark.databricks.delta.uniform.iceberg.v3.enabled</c>, so every statement runs with it.</para>
/// </summary>
[Collection("Interop")]
public class IcebergCompatV3InteropTests : IDisposable
{
    private const string V3Conf = "spark.databricks.delta.uniform.iceberg.v3.enabled";

    private readonly string _tempDir;

    public IcebergCompatV3InteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_iceberg_v3_interop_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema IdKValue = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("k", Int32Type.Default, true))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Rows(params (long Id, int K, string Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var ks = new Int32Array.Builder();
        var values = new StringArray.Builder();
        foreach (var (id, k, value) in rows)
        {
            ids.Append(id);
            ks.Append(k);
            values.Append(value);
        }
        return new RecordBatch(IdKValue, [ids.Build(), ks.Build(), values.Build()], rows.Length);
    }

    private static void RequireV3()
    {
        Spark.Require();
        // Spark.Version is "<pyspark>/<delta-spark>", e.g. "4.1.2/4.3.1".
        string version = Spark.Version ?? "";
        int slash = version.IndexOf('/');
        var parts = slash < 0 ? [] : version.Substring(slash + 1).Trim().Split('.');
        bool atLeast43 = parts.Length >= 2 && int.TryParse(parts[0], out int major)
            && int.TryParse(parts[1], out int minor) && (major > 4 || (major == 4 && minor >= 3));
        Skip.IfNot(atLeast43, $"icebergCompatV3 needs delta-spark 4.3+; resolved {version}");
    }

    private JsonElement Sql(params string[] statements) =>
        Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = statements,
            confs = new Dictionary<string, string> { [V3Conf] = "true" },
        });

    // Each row's (value, row id, row commit version) by id, as EW reads them.
    private static async Task<SortedDictionary<long, (string Value, long RowId, long Version)>> EwRowsAsync(
        DeltaTable table)
    {
        var rows = new SortedDictionary<long, (string, long, long)>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var values = (StringArray)batch.Column(batch.Schema.GetFieldIndex("value"));
            var rowIds = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            var versions = (Int64Array)batch.Column(
                batch.Schema.GetFieldIndex(RowTrackingConfig.RowCommitVersionColumnName));
            for (int i = 0; i < batch.Length; i++)
                rows[ids.GetValue(i)!.Value] = (values.GetString(i)!, rowIds.GetValue(i)!.Value, versions.GetValue(i)!.Value);
        }
        return rows;
    }

    // The same, from the rows of a Spark `SELECT id, value, _metadata.row_id AS rid, _metadata.row_commit_version
    // AS rcv` statement.
    private static SortedDictionary<long, (string Value, long RowId, long Version)> SparkRows(JsonElement statement)
    {
        var rows = new SortedDictionary<long, (string, long, long)>();
        foreach (var row in statement.GetProperty("rows").EnumerateArray())
        {
            rows[row.GetProperty("id").GetInt64()] = (
                row.GetProperty("value").GetString()!,
                row.GetProperty("rid").GetInt64(),
                row.GetProperty("rcv").GetInt64());
        }
        return rows;
    }

    private static JsonElement LastStatement(JsonElement result)
    {
        var statements = result.GetProperty("statements");
        return statements[statements.GetArrayLength() - 1];
    }

    private static List<string> Features(JsonElement result) =>
        [.. result.GetProperty("detail").GetProperty("table_features").EnumerateArray().Select(f => f.GetString()!)];

    private const string SelectRowIds =
        "SELECT id, value, _metadata.row_id AS rid, _metadata.row_commit_version AS rcv FROM delta.`{path}`";

    /// <summary>
    /// EW creates a partitioned V3 table with deletion vectors and writes it through every path that touches row
    /// tracking: appends, a deletion-vector DELETE, an UPDATE and a compaction, whose output carries the materialized
    /// row-tracking columns. Spark then inserts, updates and deletes through it, and both read the same rows with
    /// the same row ids and commit versions.
    /// </summary>
    [SkippableFact]
    public async Task EwCreatedV3Table_SparkWritesThroughIt_AndBothSeeTheSameRowIds()
    {
        RequireV3();
        var fs = new LocalTableFileSystem(_tempDir);

        await using (var table = await DeltaTable.CreateAsync(fs, IdKValue, partitionColumns: ["k"],
            columnMappingMode: ColumnMappingMode.Name, enableDeletionVectors: true,
            configuration: new Dictionary<string, string> { [IcebergCompat.EnableV3Key] = "true" }))
        {
            await table.WriteAsync([Rows((1, 1, "a"), (2, 1, "b"), (3, 2, "c"))]);
            await table.WriteAsync([Rows((4, 1, "d"), (5, 2, "e"))]);
            await table.DeleteAsync(Ex.Equal("id", 2L));
            await table.UpdateAsync(Ex.Equal("id", 5L), batch => batch);
            await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        }

        var result = Sql(
            "INSERT INTO delta.`{path}` VALUES (100, 1, 'spark')",
            "UPDATE delta.`{path}` SET value = 'u' WHERE id = 3",
            "DELETE FROM delta.`{path}` WHERE id = 4",
            SelectRowIds);

        var features = Features(result);
        foreach (string feature in new[] { "icebergCompatV3", "rowTracking", "domainMetadata", "columnMapping", "deletionVectors" })
            Assert.Contains(feature, features);
        var sparkRows = SparkRows(LastStatement(result));
        Assert.Equal([1L, 3L, 5L, 100L], sparkRows.Keys);
        Assert.Equal("u", sparkRows[3].Value);

        await using var reopened = await DeltaTable.OpenAsync(fs);
        Assert.Equal(sparkRows, await EwRowsAsync(reopened));
        // Five rows were appended before Spark's, so its row's id comes after theirs.
        Assert.True(sparkRows[100].RowId >= 5);
    }

    /// <summary>
    /// Spark creates an id-mode V3 table and rewrites rows in it (delta-spark 4.4 materializes their row ids with
    /// Iceberg's row-lineage field ids, which no column of the schema has). EW reads the same row ids, writes through
    /// the table, and Spark reads EW's rows and ids back.
    /// <para>The CREATE names <c>delta.enableRowTracking</c>: delta-spark 4.4 sets it by itself, but 4.3.1 does not
    /// (measured), and EW refuses to write a V3 table without it, as the V3 rules require. Deletion vectors are on
    /// because EW's partial DELETE on an IcebergCompat table needs them (its copy-on-write rewrite is refused
    /// there).</para>
    /// </summary>
    [SkippableFact]
    public async Task SparkCreatedV3Table_EwWritesThroughIt_AndBothSeeTheSameRowIds()
    {
        RequireV3();
        var fs = new LocalTableFileSystem(_tempDir);

        var created = Sql(
            "CREATE TABLE delta.`{path}` (id BIGINT NOT NULL, k INT, value STRING) USING delta PARTITIONED BY (k) "
                + "TBLPROPERTIES ('delta.enableIcebergCompatV3' = 'true', 'delta.columnMapping.mode' = 'id', "
                + "'delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')",
            "INSERT INTO delta.`{path}` VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c')",
            "UPDATE delta.`{path}` SET value = 'B' WHERE id = 2",
            SelectRowIds);
        var sparkBefore = SparkRows(LastStatement(created));

        await using (var table = await DeltaTable.OpenAsync(fs))
        {
            Assert.True(table.IsIcebergCompat);
            Assert.Equal(sparkBefore, await EwRowsAsync(table));

            await table.WriteAsync([Rows((10, 1, "x"), (11, 2, "y"))]);
            await table.DeleteAsync(Ex.Equal("id", 1L));
            await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        }

        var after = Sql(SelectRowIds);
        var sparkAfter = SparkRows(LastStatement(after));
        Assert.Equal([2L, 3L, 10L, 11L], sparkAfter.Keys);
        Assert.Equal(sparkBefore[2], sparkAfter[2]);
        Assert.Equal(sparkBefore[3], sparkAfter[3]);

        await using var reopened = await DeltaTable.OpenAsync(fs);
        Assert.Equal(sparkAfter, await EwRowsAsync(reopened));
    }
}
