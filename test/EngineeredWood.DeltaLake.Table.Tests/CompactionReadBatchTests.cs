// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Compaction reads its sources in batches of the output row group (#875). The 262,144-row scan default made the
/// rolling writer join four or more slices into each row group it wrote.
/// </summary>
public class CompactionReadBatchTests : IDisposable
{
    private readonly string _tempDir;

    public CompactionReadBatchTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_compact_batch_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema Schema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private async Task<DeltaTable> CreateAsync(DeltaTableOptions options) =>
        await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), Schema, options);

    [Fact]
    public async Task NoHostLimit_ReadsTheOutputRowGroup()
    {
        await using var table = await CreateAsync(new DeltaTableOptions());
        var write = ParquetWriteOptions.Default with { RowGroupMaxRows = 2_000_000 };

        var read = table.CompactionReadOptions(write);

        Assert.Equal(2_000_000, read.BatchSize);
        Assert.Null(read.MaxBatchByteSize);
    }

    [Fact]
    public async Task SmallOutputRowGroup_KeepsTheScanDefault()
    {
        await using var table = await CreateAsync(new DeltaTableOptions());
        var write = ParquetWriteOptions.Default with { RowGroupMaxRows = 10_000 };

        Assert.Equal(DeltaTable.DefaultReadBatchSize, table.CompactionReadOptions(write).BatchSize);
    }

    [Fact]
    public async Task HostBatchSize_IsKept()
    {
        await using var table = await CreateAsync(new DeltaTableOptions
        {
            ParquetReadOptions = ParquetReadOptions.Default with { BatchSize = 50_000 },
        });

        Assert.Equal(50_000, table.CompactionReadOptions(ParquetWriteOptions.Default).BatchSize);
    }

    [Fact]
    public async Task HostByteLimit_IsKept()
    {
        await using var table = await CreateAsync(new DeltaTableOptions
        {
            ParquetReadOptions = ParquetReadOptions.Default with { MaxBatchByteSize = 1 << 20 },
        });

        var read = table.CompactionReadOptions(ParquetWriteOptions.Default);

        Assert.Null(read.BatchSize);
        Assert.Equal(1 << 20, read.MaxBatchByteSize);
    }

    [Fact]
    public async Task Compaction_StillJoinsRowGroupsAndKeepsEveryRow()
    {
        var write = ParquetWriteOptions.Default with { RowGroupMaxRows = 300_000 };
        await using var table = await CreateAsync(new DeltaTableOptions { ParquetWriteOptions = write });
        for (int f = 0; f < 4; f++)
        {
            var ids = new Int64Array.Builder();
            for (int i = 0; i < 100_000; i++)
                ids.Append(f * 100_000L + i);
            await table.WriteAsync([new RecordBatch(Schema, [ids.Build()], 100_000)]);
        }

        await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue });

        var add = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        await using var file = await new LocalTableFileSystem(_tempDir).OpenReadAsync(DeltaPath.Decode(add.Path));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var groups = (await reader.ReadMetadataAsync()).RowGroups.Select(rg => rg.NumRows).ToArray();
        Assert.Equal([300_000L, 100_000L], groups);

        long sum = 0, rows = 0;
        await foreach (var batch in table.ReadAsync())
        {
            using (batch)
            {
                var ids = (Int64Array)batch.Column(0);
                for (int i = 0; i < ids.Length; i++)
                    sum += ids.GetValue(i)!.Value;
                rows += batch.Length;
            }
        }
        Assert.Equal(400_000, rows);
        Assert.Equal(400_000L * 399_999 / 2, sum);
    }
}
