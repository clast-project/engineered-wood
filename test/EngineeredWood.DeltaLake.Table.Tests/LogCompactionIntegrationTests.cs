// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class LogCompactionIntegrationTests : IDisposable
{
    private readonly string _tempDir;

    public LogCompactionIntegrationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_lci_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task CompactLog_ThenRead_SameResult()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write 5 commits
        for (int i = 0; i < 5; i++)
        {
            var batch = new RecordBatch(schema,
                [new Int64Array.Builder().Append(i + 1).Build()], 1);
            await table.WriteAsync([batch]);
        }

        Assert.Equal(5, table.CurrentSnapshot.FileCount);

        // Compact log versions 1-3
        await table.CompactLogAsync(1, 3);

        // Re-open table — should use compacted file and still have all data
        await using var reopened = await DeltaTable.OpenAsync(fs);
        Assert.Equal(5L, reopened.CurrentSnapshot.Version);
        Assert.Equal(5, reopened.CurrentSnapshot.FileCount);

        // Verify all data readable
        int totalRows = 0;
        await foreach (var b in reopened.ReadAllAsync())
            totalRows += b.Length;
        Assert.Equal(5, totalRows);
    }

    [Fact]
    public async Task CompactLog_WithDeletedFiles_Reconciles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write, then overwrite (removes old files)
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build()], 2);
        await table.WriteAsync([batch1]);

        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(10).Append(20).Append(30).Build()], 3);
        await table.WriteAsync([batch2], DeltaWriteMode.Overwrite);

        // Compact all data commits
        await table.CompactLogAsync(1, 2);

        // Re-open and verify
        await using var reopened = await DeltaTable.OpenAsync(fs);
        Assert.Equal(1, reopened.CurrentSnapshot.FileCount);

        var readIds = new List<long>();
        await foreach (var b in reopened.ReadAllAsync())
        {
            var ids = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                readIds.Add(ids.GetValue(i)!.Value);
        }
        Assert.Equal(new long[] { 10, 20, 30 }, readIds.OrderBy(x => x).ToArray());
    }
}
