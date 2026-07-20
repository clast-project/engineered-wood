// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class DeltaTableTests : IDisposable
{
    private readonly string _tempDir;

    public DeltaTableTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task CreateAndOpen_EmptyTable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("name", StringType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        Assert.Equal(0L, table.CurrentSnapshot.Version);
        Assert.Equal(2, table.ArrowSchema.FieldsList.Count);
        Assert.Equal("id", table.ArrowSchema.FieldsList[0].Name);
        Assert.Equal("name", table.ArrowSchema.FieldsList[1].Name);
        Assert.Equal(0, table.CurrentSnapshot.FileCount);

        // Should be able to re-open
        await using var reopened = await DeltaTable.OpenAsync(fs);
        Assert.Equal(0L, reopened.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task WriteAndReadBack_SimpleData()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write data
        var idArray = new Int64Array.Builder()
            .Append(1).Append(2).Append(3).Build();
        var valueArray = new StringArray.Builder()
            .Append("a").Append("b").Append("c").Build();
        var batch = new RecordBatch(schema, [idArray, valueArray], 3);

        long version = await table.WriteAsync([batch]);
        Assert.Equal(1L, version);
        Assert.Equal(1, table.CurrentSnapshot.FileCount);

        // Read back
        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);

        Assert.Single(batches);
        Assert.Equal(3, batches[0].Length);
    }

    [Fact]
    public async Task WriteMultipleBatches()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build()], 2);
        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(3).Append(4).Build()], 2);

        long version = await table.WriteAsync([batch1, batch2]);
        Assert.Equal(1L, version);
        Assert.Equal(2, table.CurrentSnapshot.FileCount);

        // Read back
        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;

        Assert.Equal(4, totalRows);
    }

    [Fact]
    public async Task Overwrite_ReplacesExistingData()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // First write
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build()], 2);
        await table.WriteAsync([batch1]);

        // Overwrite
        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(10).Append(20).Append(30).Build()], 3);
        long version = await table.WriteAsync([batch2], DeltaWriteMode.Overwrite);
        Assert.Equal(2L, version);

        // Should only have the new data
        Assert.Equal(1, table.CurrentSnapshot.FileCount);

        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;

        Assert.Equal(3, totalRows);
    }

    [Fact]
    public async Task TimeTravel_ReadAtVersion()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Version 1: write 2 rows
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build()], 2);
        await table.WriteAsync([batch1]);

        // Version 2: write 3 more rows
        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(3).Append(4).Append(5).Build()], 3);
        await table.WriteAsync([batch2]);

        // Read at version 1 — should only have 2 rows
        int rowsV1 = 0;
        await foreach (var b in table.ReadAtVersionAsync(1))
            rowsV1 += b.Length;
        Assert.Equal(2, rowsV1);

        // Read at version 2 — should have 5 rows
        int rowsV2 = 0;
        await foreach (var b in table.ReadAtVersionAsync(2))
            rowsV2 += b.Length;
        Assert.Equal(5, rowsV2);
    }

    [Fact]
    public async Task OpenOrCreate_CreatesThenOpens()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        // First call creates
        await using var table1 = await DeltaTable.OpenOrCreateAsync(fs, schema);
        Assert.Equal(0L, table1.CurrentSnapshot.Version);

        // Write some data
        var batch = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table1.WriteAsync([batch]);

        // Second call opens existing
        await using var table2 = await DeltaTable.OpenOrCreateAsync(fs, schema);
        Assert.Equal(1L, table2.CurrentSnapshot.Version);
        Assert.Equal(1, table2.CurrentSnapshot.FileCount);
    }

    [Fact]
    public async Task Create_ThrowsIfAlreadyExists()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => DeltaTable.CreateAsync(fs, schema).AsTask());
    }

    [Fact]
    public async Task Open_ThrowsIfNotExists()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        await Assert.ThrowsAsync<DeltaFormatException>(
            () => DeltaTable.OpenAsync(fs).AsTask());
    }
}
