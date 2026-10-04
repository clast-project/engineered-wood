// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowStructType = Apache.Arrow.Types.StructType;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Id-mode column mapping resolves a data file's columns by parquet field id, so every path that reads a file
/// (scans, DELETE, compaction) must bind by id, not by name. A file's physical names need not be the ones the
/// schema records: a table converted from Iceberg keeps each file's names from when it was written. Here two
/// same-typed columns swap physical names but keep their ids; binding by name then reads each column's values
/// as the other's, with no error. And a field whose id the table no longer has (dropped) must not bind to a
/// re-added column of the same name.
/// </summary>
public class ColumnMappingIdResolutionTests : IDisposable
{
    private readonly string _tempDir;

    public ColumnMappingIdResolutionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cmid_res_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema PqSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("p", Int64Type.Default, true))
        .Field(new Field("q", Int64Type.Default, true))
        .Build();

    private static RecordBatch Pq(params (long P, long Q)[] rows) => new(PqSchema,
        [
            new Int64Array.Builder().AppendRange(rows.Select(r => r.P)).Build(),
            new Int64Array.Builder().AppendRange(rows.Select(r => r.Q)).Build(),
        ], rows.Length);

    // Rewrites a data file with its first two top-level columns' NAMES swapped; types, ids and values stay put.
    private async Task SwapFirstTwoColumnNamesAsync(string addPath)
    {
        string path = Path.Combine(_tempDir, DeltaPath.Decode(addPath));
        RecordBatch original;
        await using (var rf = new LocalRandomAccessFile(path))
        await using (var reader = new ParquetFileReader(rf, ownsFile: false))
            original = await reader.ReadRowGroupAsync(0);

        var fields = original.Schema.FieldsList.ToArray();
        (var f0, var f1) = (fields[0], fields[1]);
        fields[0] = new Field(f1.Name, f0.DataType, f0.IsNullable, f0.Metadata);
        fields[1] = new Field(f0.Name, f1.DataType, f1.IsNullable, f1.Metadata);
        var rewritten = new RecordBatch(new Apache.Arrow.Schema(fields, null), original.Arrays, original.Length);

        File.Delete(path);
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false);
        await writer.WriteRowGroupAsync(rewritten);
        await writer.CloseAsync();
    }

    private static Task<List<string>> ReadAsync(DeltaTable table) => ColumnMappingNameReuseTests.ReadAsync(table);

    [Fact]
    public async Task Delete_FlatTable_FileWithSwappedNames_DeletesByFieldId()
    {
        // With deletion vectors the DELETE evaluates its predicate over the file it reads itself; binding that
        // file by name evaluated "p = 1" against q's values, matched nothing, and deleted nothing.
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, PqSchema, columnMappingMode: ColumnMappingMode.Id, enableDeletionVectors: true);
        await table.WriteAsync([Pq((1, 10), (2, 20))]);
        await SwapFirstTwoColumnNamesAsync(table.CurrentSnapshot.ActiveFiles.Values.Single().Path);
        Assert.Equal(["p=1 q=10", "p=2 q=20"], await ReadAsync(table));

        var (deleted, _) = await table.DeleteAsync(batch =>
        {
            var p = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("p"));
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < p.Length; i++)
                mask.Append(p.GetValue(i) == 1);
            return mask.Build();
        });

        Assert.Equal(1, deleted);
        Assert.Equal(["p=2 q=20"], await ReadAsync(table));
    }

    [Fact]
    public async Task Compaction_FileWithSwappedNames_KeepsEachColumnsValues()
    {
        // Compaction re-stamps ids by NAME after reading; binding the swapped file by name would rewrite its
        // values under each other's ids, for good.
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, PqSchema, columnMappingMode: ColumnMappingMode.Id);
        await table.WriteAsync([Pq((1, 10))]);
        await SwapFirstTwoColumnNamesAsync(table.CurrentSnapshot.ActiveFiles.Values.Single().Path);
        await table.WriteAsync([Pq((2, 20))]);
        Assert.Equal(["p=1 q=10", "p=2 q=20"], await ReadAsync(table));

        Assert.NotNull(await table.CompactAsync(
            new CompactionOptions { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue }));
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        Assert.Equal(["p=1 q=10", "p=2 q=20"], await ReadAsync(table));
    }

    private static StructField Mapped(string name, DeltaDataType type, int id) => new()
    {
        Name = name,
        Type = type,
        Nullable = true,
        Metadata = new Dictionary<string, string>
        {
            [ColumnMapping.FieldIdKey] = id.ToString(),
            [ColumnMapping.PhysicalNameKey] = name,
        },
    };

    [Fact]
    public async Task DroppedAndReAddedColumns_OldFileValuesDoNotReappear()
    {
        // Physical names equal to logical names (as in a converted table). A dropped column's id is gone from the
        // schema; its values in old files must not bind, by name, to the column re-added under the same name.
        var arrow = new Apache.Arrow.Schema.Builder()
            .Field(new Field("amount", Int64Type.Default, true))
            .Field(new Field("s", new ArrowStructType(
                [new Field("a", Int64Type.Default, true), new Field("x", StringType.Default, true)]), true))
            .Build();
        var preAssigned = new DeltaStructType
        {
            Fields =
            [
                Mapped("amount", new PrimitiveType { TypeName = "long" }, 1),
                Mapped("s", new DeltaStructType
                {
                    Fields =
                    [
                        Mapped("a", new PrimitiveType { TypeName = "long" }, 3),
                        Mapped("x", new PrimitiveType { TypeName = "string" }, 4),
                    ],
                }, 2),
            ],
        };
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, arrow, columnMappingMode: ColumnMappingMode.Id, preAssignedSchema: preAssigned);
        await table.WriteAsync([new RecordBatch(arrow,
            [
                new Int64Array.Builder().Append(1).Build(),
                new StructArray((ArrowStructType)arrow.FieldsList[1].DataType, 1,
                    [new Int64Array.Builder().Append(10).Build(), new StringArray.Builder().Append("p").Build()],
                    ArrowBuffer.Empty),
            ], 1)]);

        await table.DropColumnAsync("amount");
        await table.AddColumnAsync(new Field("amount", Int64Type.Default, true));
        await table.DropFieldAsync(["s", "a"]);
        await table.AddFieldAsync(["s"], new Field("a", Int64Type.Default, true));

        var evolved = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", new ArrowStructType(
                [new Field("x", StringType.Default, true), new Field("a", Int64Type.Default, true)]), true))
            .Field(new Field("amount", Int64Type.Default, true))
            .Build();
        await table.WriteAsync([new RecordBatch(evolved,
            [
                new StructArray((ArrowStructType)evolved.FieldsList[0].DataType, 1,
                    [new StringArray.Builder().Append("q").Build(), new Int64Array.Builder().Append(200).Build()],
                    ArrowBuffer.Empty),
                new Int64Array.Builder().Append(2000).Build(),
            ], 1)]);

        List<string> expected = ["s.x=p s.a= amount=", "s.x=q s.a=200 amount=2000"];
        Assert.Equal(expected, await ReadAsync(table));

        Assert.NotNull(await table.CompactAsync(
            new CompactionOptions { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue }));
        Assert.Equal(expected, await ReadAsync(table));
    }
}
