// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class CompactionTests : IDisposable
{
    private readonly string _tempDir;

    public CompactionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_compact_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task Compact_MergesSmallFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        // Use very small target to force compaction eligibility
        var options = new DeltaTableOptions
        {
            CheckpointInterval = 0, // Disable auto-checkpoint
        };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options);

        // Write 5 small files (one row each)
        for (int i = 0; i < 5; i++)
        {
            var batch = new RecordBatch(schema,
                [new Int64Array.Builder().Append(i).Build()], 1);
            await table.WriteAsync([batch]);
        }

        Assert.Equal(5, table.CurrentSnapshot.FileCount);

        // Compact with a very large MinFileSize so all files qualify
        var compactResult = await table.CompactAsync(new CompactionOptions
        {
            MinFileSize = long.MaxValue, // All files are "small"
            TargetFileSize = long.MaxValue,
        });

        Assert.NotNull(compactResult);
        // After compaction, should have fewer files
        Assert.True(table.CurrentSnapshot.FileCount < 5);

        // Data integrity check: should still have 5 rows
        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;
        Assert.Equal(5, totalRows);
    }

    [Fact]
    public async Task Compact_NoOpWhenNotNeeded()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write one file
        var batch = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch]);

        // Compact with default settings — single file won't be compacted
        var result = await table.CompactAsync(new CompactionOptions
        {
            MinFileSize = long.MaxValue,
        });

        // Single file → no compaction needed
        Assert.Null(result);
    }
}
