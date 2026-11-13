// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A read of every column doesn't decode a column the table has dropped (#855): a DROP COLUMN leaves it in the files
/// written before, and the read used to decode it whole only to drop it. Measured as the native bytes the read
/// allocates, which must be what a read projecting the table's columns allocates.
/// </summary>
[Collection(NativeMemoryCollection.Name)]
public class DroppedColumnDecodeTests : IDisposable
{
    private const int Rows = 8_000;
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;

    public DroppedColumnDecodeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-dropped-decode-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    [Theory]
    [InlineData("name")]
    [InlineData("id")]
    public async Task AnUnprojectedRead_DecodesNoDroppedColumn(string mapping)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true))
            .Field(new Field("gone", StringType.Default, true))
            .Field(new Field("tag", StringType.Default, true)).Build();
        var ids = new Int32Array.Builder();
        var gone = new StringArray.Builder();
        var tags = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            ids.Append(i);
            gone.Append("a dropped column's value " + i);
            tags.Append("t" + (i % 4));
        }

        await using (var table = await DeltaTable.CreateAsync(
                         _fs, schema, configuration: new Dictionary<string, string> { ["delta.columnMapping.mode"] = mapping }))
        {
            await table.WriteAsync([new RecordBatch(schema, [ids.Build(), gone.Build(), tags.Build()], Rows)]);
            await table.DropColumnAsync("gone");
        }

        var (everyColumn, rows) = await AllocatedAsync(null);
        var (projected, projectedRows) = await AllocatedAsync(["id", "tag"]);

        Assert.Equal(Rows, rows);
        Assert.Equal(Rows, projectedRows);
        Assert.Equal(projected, everyColumn);
    }

    private async Task<(long Bytes, long Rows)> AllocatedAsync(string[]? columns)
    {
        long before = NativeMemoryTracker.TotalAllocated, rows = 0;
        await using var table = await DeltaTable.OpenAsync(_fs, new DeltaTableOptions { MaxConcurrentReads = 1 });
        await foreach (var b in table.ReadAsync(columns is null ? null : new DeltaReadOptions { Columns = columns }))
        {
            Assert.Equal(["id", "tag"], b.Schema.FieldsList.Select(f => f.Name));
            rows += b.Length;
            b.Dispose();
        }

        return (NativeMemoryTracker.TotalAllocated - before, rows);
    }
}
