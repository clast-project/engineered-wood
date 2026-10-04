// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowStructType = Apache.Arrow.Types.StructType;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// Tier 3 for name-mode column mapping when one column's physical name is another's logical name. That is
/// what upgrading an existing table to name mode leaves behind: every existing column keeps its original name
/// as its physical name, so renames and re-adds reuse names. Spark builds the table through exactly that
/// path, and is the reference for what each column holds. EW must read the same rows, and a file EW writes or
/// compacts into the table must read the same in Spark.
/// See <see cref="ColumnMappingNameReuseTests"/> for the EW-only reproductions.
/// </summary>
[Collection("Interop")]
public class ColumnMappingNameReuseInteropTests : IDisposable
{
    private readonly string _tempDir;

    public ColumnMappingNameReuseInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_spark_reuse_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    // After these statements the schema is total, s<x, y, a>, amount, and:
    //   total  is physically "amount"; amount is new (col-<guid>);
    //   s.x    is physically "a" (the original a); s.y is physically "x"; s.a is new.
    // Row 1 predates the upgrade (its file has no field ids); row 2 is written after it.
    private static readonly string[] UpgradeAndReuseNames =
    [
        "CREATE TABLE delta.`{path}` (amount BIGINT, s STRUCT<a: BIGINT, x: STRING>) USING delta",
        "INSERT INTO delta.`{path}` VALUES (1, named_struct('a', 10L, 'x', 'p'))",
        "ALTER TABLE delta.`{path}` SET TBLPROPERTIES ("
            + "'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')",
        "ALTER TABLE delta.`{path}` RENAME COLUMN s.x TO y",
        "ALTER TABLE delta.`{path}` RENAME COLUMN s.a TO x",
        "ALTER TABLE delta.`{path}` ADD COLUMNS (s.a BIGINT)",
        "ALTER TABLE delta.`{path}` RENAME COLUMN amount TO total",
        "ALTER TABLE delta.`{path}` ADD COLUMNS (amount BIGINT)",
        "INSERT INTO delta.`{path}` VALUES (2, named_struct('x', 20L, 'y', 'q', 'a', 200L), 2000)",
    ];

    private static readonly List<string> SparkBuiltRows =
    [
        "total=1 s.x=10 s.y=p s.a= amount=",
        "total=2 s.x=20 s.y=q s.a=200 amount=2000",
    ];

    // A Spark row (asDict(recursive=True)) rendered the way RenderEw renders an EW row.
    private static string RenderSpark(JsonElement row)
    {
        static string Value(JsonElement v) => v.ValueKind switch
        {
            JsonValueKind.Null => "",
            JsonValueKind.String => v.GetString()!,
            _ => v.GetRawText(),
        };

        var parts = new List<string>();
        foreach (var column in row.EnumerateObject())
        {
            if (column.Value.ValueKind == JsonValueKind.Object)
            {
                foreach (var child in column.Value.EnumerateObject())
                    parts.Add($"{column.Name}.{child.Name}={Value(child.Value)}");
            }
            else
            {
                parts.Add($"{column.Name}={Value(column.Value)}");
            }
        }
        return string.Join(" ", parts);
    }

    private static List<string> RenderSparkRows(JsonElement result)
    {
        var rows = result.GetProperty("rows").EnumerateArray().Select(RenderSpark).ToList();
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static List<string> RenderEw(IEnumerable<RecordBatch> batches)
    {
        static string Value(IArrowArray array, int i) => array.IsNull(i) ? "" : array switch
        {
            Int64Array l => l.GetValue(i)!.Value.ToString(),
            StringArray s => s.GetString(i),
            _ => "?",
        };

        var rows = new List<string>();
        foreach (var batch in batches)
        {
            for (int r = 0; r < batch.Length; r++)
            {
                var parts = new List<string>();
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    var field = batch.Schema.FieldsList[c];
                    if (batch.Column(c) is StructArray st)
                    {
                        var type = (ArrowStructType)field.DataType;
                        for (int k = 0; k < type.Fields.Count; k++)
                            parts.Add($"{field.Name}.{type.Fields[k].Name}={Value(st.Fields[k], r)}");
                    }
                    else
                    {
                        parts.Add($"{field.Name}={Value(batch.Column(c), r)}");
                    }
                }
                rows.Add(string.Join(" ", parts));
            }
        }
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static async Task<List<string>> EwRowsAsync(DeltaTable table)
    {
        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);
        return RenderEw(batches);
    }

    private List<string> SparkRows() => RenderSparkRows(Spark.Invoke("read", new { path = _tempDir }));

    [SkippableFact]
    public async Task SparkUpgradedTable_ReusedNames_EwReadsWhatSparkReads()
    {
        Spark.Require();

        var built = Spark.Invoke("sql", new { path = _tempDir, sql = UpgradeAndReuseNames });
        Assert.Equal(SparkBuiltRows, RenderSparkRows(built));

        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.Equal(SparkBuiltRows, await EwRowsAsync(table));
    }

    [SkippableFact]
    public async Task SparkUpgradedTable_ReusedNames_EwWritesAndCompacts_SparkReadsTheSameRows()
    {
        Spark.Require();
        Spark.Invoke("sql", new { path = _tempDir, sql = UpgradeAndReuseNames });

        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)))
        {
            // Every name EW writes here is also some other column's physical name, so a write that binds
            // names the wrong way lands values in the wrong column, and Spark would see it.
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("total", Int64Type.Default, true))
                .Field(new Field("s", new ArrowStructType(
                [
                    new Field("x", Int64Type.Default, true),
                    new Field("y", StringType.Default, true),
                    new Field("a", Int64Type.Default, true),
                ]), true))
                .Field(new Field("amount", Int64Type.Default, true))
                .Build();
            await table.WriteAsync([new RecordBatch(schema,
                [
                    new Int64Array.Builder().Append(3).Build(),
                    new StructArray((ArrowStructType)schema.FieldsList[1].DataType, 1,
                        [
                            new Int64Array.Builder().Append(30).Build(),
                            new StringArray.Builder().Append("r").Build(),
                            new Int64Array.Builder().Append(300).Build(),
                        ],
                        ArrowBuffer.Empty),
                    new Int64Array.Builder().Append(3000).Build(),
                ], 1)]);
        }

        List<string> expected = [.. SparkBuiltRows, "total=3 s.x=30 s.y=r s.a=300 amount=3000"];
        Assert.Equal(expected, SparkRows());

        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)))
        {
            Assert.NotNull(await table.CompactAsync(
                new CompactionOptions { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue }));
            Assert.Single(table.CurrentSnapshot.ActiveFiles);
            Assert.Equal(expected, await EwRowsAsync(table));
        }

        // The compacted file mixes a pre-upgrade file (no field ids, original names), a Spark file and an EW
        // file; Spark resolving it through the mapping on its own is the check that nothing moved.
        Assert.Equal(expected, SparkRows());
    }

    [SkippableFact]
    public async Task EwCompacted_NestedFieldAdded_SparkReadsEveryNestedValue()
    {
        // No name reuse: an ordinary EW name-mode table (GUID physical names). Compaction used to reconcile each
        // file against a target schema physical only at the top level, which matched no struct child on disk
        // and wrote every nested value as NULL.
        Spark.Require();

        var fs = new LocalTableFileSystem(_tempDir);
        var v1 = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("s", new ArrowStructType([new Field("a", Int64Type.Default, true)]), true))
            .Build();
        await using (var table = await DeltaTable.CreateAsync(fs, v1, columnMappingMode: ColumnMappingMode.Name))
        {
            await table.WriteAsync([new RecordBatch(v1,
                [
                    new Int64Array.Builder().Append(1).Build(),
                    new StructArray((ArrowStructType)v1.FieldsList[1].DataType, 1,
                        [new Int64Array.Builder().Append(10).Build()], ArrowBuffer.Empty),
                ], 1)]);
            await table.AddFieldAsync(["s"], new Field("c", Int64Type.Default, true));

            var v2 = new Apache.Arrow.Schema.Builder()
                .Field(v1.FieldsList[0])
                .Field(new Field("s", new ArrowStructType(
                    [new Field("a", Int64Type.Default, true), new Field("c", Int64Type.Default, true)]), true))
                .Build();
            await table.WriteAsync([new RecordBatch(v2,
                [
                    new Int64Array.Builder().Append(2).Build(),
                    new StructArray((ArrowStructType)v2.FieldsList[1].DataType, 1,
                        [new Int64Array.Builder().Append(20).Build(), new Int64Array.Builder().Append(200).Build()],
                        ArrowBuffer.Empty),
                ], 1)]);
        }

        List<string> expected = ["id=1 s.a=10 s.c=", "id=2 s.a=20 s.c=200"];
        Assert.Equal(expected, SparkRows());

        await using (var table = await DeltaTable.OpenAsync(fs))
        {
            Assert.NotNull(await table.CompactAsync(
                new CompactionOptions { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue }));
            Assert.Single(table.CurrentSnapshot.ActiveFiles);
        }

        Assert.Equal(expected, SparkRows());
    }
}
