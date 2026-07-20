// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

public class LogCompactionTests : IDisposable
{
    private readonly string _tempDir;

    public LogCompactionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_lc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task CompactRange_CreatesCompactedFile()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Create 4 commits
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "lc-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await log.WriteCommitAsync(1, new List<DeltaAction>
        {
            new AddFile
            {
                Path = "f1.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            },
        });

        await log.WriteCommitAsync(2, new List<DeltaAction>
        {
            new AddFile
            {
                Path = "f2.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 200, ModificationTime = 2000, DataChange = true,
            },
            new RemoveFile
            {
                Path = "f1.parquet",
                DeletionTimestamp = 2000,
                DataChange = true,
            },
        });

        await log.WriteCommitAsync(3, new List<DeltaAction>
        {
            new AddFile
            {
                Path = "f3.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 300, ModificationTime = 3000, DataChange = true,
            },
        });

        // Compact commits 1-3
        var compaction = new LogCompaction(fs, log);
        await compaction.CompactRangeAsync(1, 3);

        // Verify compacted file exists
        var compactedFiles = await compaction.ListCompactedFilesAsync();
        Assert.Single(compactedFiles);
        Assert.Equal(1L, compactedFiles[0].Start);
        Assert.Equal(3L, compactedFiles[0].End);

        // Read compacted file and verify reconciliation
        var actions = await compaction.ReadCompactedAsync(compactedFiles[0].Path);

        // f1 was added then removed → should appear as remove tombstone
        // f2 and f3 should be adds
        var adds = actions.OfType<AddFile>().ToList();
        var removes = actions.OfType<RemoveFile>().ToList();

        Assert.Equal(2, adds.Count); // f2 + f3
        Assert.Single(removes); // f1

        Assert.Contains(adds, a => a.Path == "f2.parquet");
        Assert.Contains(adds, a => a.Path == "f3.parquet");
        Assert.Contains(removes, r => r.Path == "f1.parquet");
    }

    [Fact]
    public async Task CompactRange_ReconcilesTxn()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "lc-txn",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[]}""",
                PartitionColumns = [],
            },
        });

        await log.WriteCommitAsync(1, new List<DeltaAction>
        {
            new TransactionId { AppId = "app1", Version = 1 },
        });

        await log.WriteCommitAsync(2, new List<DeltaAction>
        {
            new TransactionId { AppId = "app1", Version = 5 },
            new TransactionId { AppId = "app2", Version = 1 },
        });

        var compaction = new LogCompaction(fs, log);
        await compaction.CompactRangeAsync(1, 2);

        var actions = await compaction.ReadCompactedAsync(
            DeltaVersion.CompactedPath(1, 2));

        var txns = actions.OfType<TransactionId>().ToList();
        Assert.Equal(2, txns.Count);
        Assert.Equal(5L, txns.First(t => t.AppId == "app1").Version); // Latest wins
        Assert.Equal(1L, txns.First(t => t.AppId == "app2").Version);
    }

    [Fact]
    public async Task SnapshotBuilder_UsesCompactedFile()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "lc-snap",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        for (int v = 1; v <= 5; v++)
        {
            await log.WriteCommitAsync(v, new List<DeltaAction>
            {
                new AddFile
                {
                    Path = $"v{v}.parquet",
                    PartitionValues = new Dictionary<string, string>(),
                    Size = 100, ModificationTime = 1000 + v, DataChange = true,
                },
            });
        }

        // Compact commits 1-3
        var compaction = new LogCompaction(fs, log);
        await compaction.CompactRangeAsync(1, 3);

        // Build snapshot — should use compacted file for versions 1-3,
        // then replay 4-5 individually
        var snapshot = await SnapshotBuilder.BuildAsync(log);

        Assert.Equal(5L, snapshot.Version);
        Assert.Equal(5, snapshot.FileCount);
        for (int v = 1; v <= 5; v++)
            Assert.Contains($"v{v}.parquet", snapshot.ActiveFiles.Keys);
    }

    [Fact]
    public async Task CompactRange_ExcludesCommitInfo()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "lc-noci",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[]}""",
                PartitionColumns = [],
            },
        });

        await log.WriteCommitAsync(1, new List<DeltaAction>
        {
            new CommitInfo
            {
                Values = new Dictionary<string, System.Text.Json.JsonElement>
                {
                    { "operation", System.Text.Json.JsonDocument.Parse("\"WRITE\"").RootElement.Clone() },
                },
            },
            new AddFile
            {
                Path = "data.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            },
        });

        var compaction = new LogCompaction(fs, log);
        await compaction.CompactRangeAsync(0, 1);

        var actions = await compaction.ReadCompactedAsync(DeltaVersion.CompactedPath(0, 1));

        // Should not contain CommitInfo, Protocol, or Metadata
        Assert.Empty(actions.OfType<CommitInfo>());
        // Should have the add file
        Assert.Single(actions.OfType<AddFile>());
    }

    [Fact]
    public void TryParseCompactedRange_Valid()
    {
        Assert.True(DeltaVersion.TryParseCompactedRange(
            "00000000000000000004.00000000000000000006.compacted.json",
            out long start, out long end));
        Assert.Equal(4L, start);
        Assert.Equal(6L, end);
    }

    [Fact]
    public void TryParseCompactedRange_Invalid()
    {
        Assert.False(DeltaVersion.TryParseCompactedRange(
            "00000000000000000004.json", out _, out _));
        Assert.False(DeltaVersion.TryParseCompactedRange(
            "00000000000000000004.00000000000000000004.compacted.json", out _, out _)); // start == end
    }
}
