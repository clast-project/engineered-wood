// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// ADD / RENAME / DROP COLUMN as metadata-only commits on a column-mapping table: no data file is rewritten,
/// so files written before the change disagree with the current schema and the read path must reconcile them.
/// </summary>
public class SchemaEvolutionTests : IDisposable
{
    private readonly string _tempDir;

    public SchemaEvolutionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_evolve_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema TwoColumnSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("name", StringType.Default, true))
            .Build();

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params (long Id, string Name)[] rows)
    {
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var (id, name) in rows)
        {
            ids.Append(id);
            names.Append(name);
        }
        return new RecordBatch(schema, [ids.Build(), names.Build()], rows.Length);
    }

    private static async Task<List<RecordBatch>> ReadAllAsync(DeltaTable table)
    {
        var list = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            list.Add(b);
        return list;
    }

    [Fact]
    public async Task AddColumn_IsMetadataOnly_AndOldFilesBackfillNull()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = TwoColumnSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, options, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Rows(schema, (1, "a"), (2, "b"))]);

        long filesBefore = table.CurrentSnapshot.FileCount;
        await table.AddColumnAsync(new Field("score", DoubleType.Default, true));

        // Metadata-only: no data file was added or removed.
        Assert.Equal(filesBefore, table.CurrentSnapshot.FileCount);
        Assert.Equal(3, table.ArrowSchema.FieldsList.Count);
        Assert.Equal("score", table.ArrowSchema.FieldsList[2].Name);

        // The pre-existing file lacks the column — it reads back as all-NULL.
        var batches = await ReadAllAsync(table);
        var read = Assert.Single(batches);
        Assert.Equal(3, read.ColumnCount);
        Assert.Equal("score", read.Schema.FieldsList[2].Name);
        var score = (DoubleArray)read.Column(2);
        Assert.Equal(2, score.Length);
        Assert.True(score.IsNull(0));
        Assert.True(score.IsNull(1));

        // The original columns are untouched.
        Assert.Equal(1L, ((Int64Array)read.Column(0)).GetValue(0));
        Assert.Equal("b", ((StringArray)read.Column(1)).GetString(1));
    }

    [Fact]
    public async Task AddColumn_AssignsFreshMappingId_AndBumpsMaxColumnId()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = TwoColumnSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);

        int before = int.Parse(
            table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);

        await table.AddColumnAsync(new Field("score", DoubleType.Default, true));

        var added = table.CurrentSnapshot.Schema.Fields[2];
        Assert.Equal((before + 1).ToString(), added.Metadata![ColumnMapping.FieldIdKey]);
        Assert.StartsWith("col-", added.Metadata![ColumnMapping.PhysicalNameKey]);

        int after = int.Parse(
            table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);
        Assert.Equal(before + 1, after);
    }

    [Fact]
    public async Task AddColumn_RejectsNonNullableAndDuplicate()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = TwoColumnSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.AddColumnAsync(new Field("score", DoubleType.Default, false)));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.AddColumnAsync(new Field("name", StringType.Default, true)));
    }

    [Fact]
    public async Task RenameColumn_IsMetadataOnly_AndDataReadsUnderNewName()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = TwoColumnSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, options, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Rows(schema, (1, "a"), (2, "b"))]);

        var physicalBefore = ColumnMapping.GetPhysicalName(
            table.CurrentSnapshot.Schema.Fields[1], ColumnMappingMode.Name);
        long filesBefore = table.CurrentSnapshot.FileCount;

        await table.RenameColumnAsync("name", "label");

        Assert.Equal(filesBefore, table.CurrentSnapshot.FileCount);
        // The physical name (and thus the on-disk data) is unchanged — only the logical name moved.
        Assert.Equal("label", table.CurrentSnapshot.Schema.Fields[1].Name);
        Assert.Equal(physicalBefore, ColumnMapping.GetPhysicalName(
            table.CurrentSnapshot.Schema.Fields[1], ColumnMappingMode.Name));

        var read = Assert.Single(await ReadAllAsync(table));
        Assert.Equal("label", read.Schema.FieldsList[1].Name);
        Assert.Equal("a", ((StringArray)read.Column(1)).GetString(0));
    }

    [Fact]
    public async Task DropColumn_IsMetadataOnly_AndOldFilesDropTheColumn()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = TwoColumnSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, options, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Rows(schema, (1, "a"), (2, "b"))]);

        long filesBefore = table.CurrentSnapshot.FileCount;
        await table.DropColumnAsync("name");

        Assert.Equal(filesBefore, table.CurrentSnapshot.FileCount);
        Assert.Single(table.ArrowSchema.FieldsList);

        // The file still physically carries the dropped column — the read path reconciles it away.
        var read = Assert.Single(await ReadAllAsync(table));
        Assert.Equal(1, read.ColumnCount);
        Assert.Equal("id", read.Schema.FieldsList[0].Name);
        Assert.Equal(2L, ((Int64Array)read.Column(0)).GetValue(1));
    }

    [Fact]
    public async Task DropColumn_RetiresTheColumnId()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = TwoColumnSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);

        var droppedId = table.CurrentSnapshot.Schema.Fields[1].Metadata![ColumnMapping.FieldIdKey];
        await table.DropColumnAsync("name");
        await table.AddColumnAsync(new Field("name", StringType.Default, true));

        // A re-added column of the same NAME must not reuse the retired id, or old files would resolve to it.
        var readded = table.CurrentSnapshot.Schema.Fields[1];
        Assert.NotEqual(droppedId, readded.Metadata![ColumnMapping.FieldIdKey]);
    }

    [Fact]
    public async Task RenameAndDrop_RequireColumnMapping()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = TwoColumnSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var rename = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.RenameColumnAsync("name", "label"));
        Assert.Contains("column mapping", rename.Message);

        var drop = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.DropColumnAsync("name"));
        Assert.Contains("column mapping", drop.Message);
    }

    [Fact]
    public async Task DropColumn_RejectsPartitionColumnAndLastColumn()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = TwoColumnSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, partitionColumns: ["name"], columnMappingMode: ColumnMappingMode.Name);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.DropColumnAsync("name"));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.DropColumnAsync("missing"));
    }

    // Files written on BOTH sides of an ADD must read back with one consistent column set.
    [Fact]
    public async Task MixedVintageFiles_ReconcileToTheCurrentSchema()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = TwoColumnSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, options, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Rows(schema, (1, "old"))]);

        await table.AddColumnAsync(new Field("score", DoubleType.Default, true));

        // Write a NEW file that does carry the added column.
        var newSchema = table.ArrowSchema;
        var batch = new RecordBatch(newSchema,
        [
            new Int64Array.Builder().Append(2).Build(),
            new StringArray.Builder().Append("new").Build(),
            new DoubleArray.Builder().Append(9.5).Build(),
        ], 1);
        await table.WriteAsync([batch]);

        var rows = new List<(long Id, double? Score)>();
        foreach (var b in await ReadAllAsync(table))
        {
            Assert.Equal(3, b.ColumnCount);
            var ids = (Int64Array)b.Column(0);
            var scores = (DoubleArray)b.Column(2);
            for (int i = 0; i < b.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, scores.IsNull(i) ? null : scores.GetValue(i)));
        }

        Assert.Equal(2, rows.Count);
        Assert.Null(rows.Single(r => r.Id == 1).Score);   // pre-ADD file
        Assert.Equal(9.5, rows.Single(r => r.Id == 2).Score); // post-ADD file
    }
}
