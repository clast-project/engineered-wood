// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

public class CheckpointRoundTripTests : IDisposable
{
    private readonly string _tempDir;

    public CheckpointRoundTripTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_ckpt_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task WriteAndReadCheckpoint_BasicTable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Create initial commit
        var actions0 = new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "test-table-id",
                Name = "test_table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
                CreatedTime = 1700000000000,
            },
        };
        await log.WriteCommitAsync(0, actions0);

        // Add some files in commit 1
        var actions1 = new List<DeltaAction>
        {
            new AddFile
            {
                Path = "part-00000.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1000,
                ModificationTime = 1700000001000,
                DataChange = true,
                Stats = """{"numRecords":100}""",
            },
            new AddFile
            {
                Path = "part-00001.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 2000,
                ModificationTime = 1700000002000,
                DataChange = true,
                Stats = """{"numRecords":200}""",
            },
        };
        await log.WriteCommitAsync(1, actions1);

        // Build snapshot at version 1
        var snapshot = await SnapshotBuilder.BuildAsync(log);

        Assert.Equal(1L, snapshot.Version);
        Assert.Equal(2, snapshot.FileCount);

        // Write checkpoint
        var writer = new CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot);

        // Verify _last_checkpoint exists
        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();
        Assert.NotNull(lastCkpt);
        Assert.Equal(1L, lastCkpt!.Version);

        // Read checkpoint back
        var checkpointActions = await reader.ReadCheckpointAsync(lastCkpt);
        Assert.NotEmpty(checkpointActions);

        // Build a new snapshot from checkpoint only (no log replay)
        var builder = new SnapshotBuilder();
        builder.ApplyCommit(lastCkpt.Version, checkpointActions);
        var restoredSnapshot = builder.Build();

        // Verify the restored snapshot matches the original
        Assert.Equal(snapshot.Version, restoredSnapshot.Version);
        Assert.Equal(snapshot.Protocol.MinReaderVersion, restoredSnapshot.Protocol.MinReaderVersion);
        Assert.Equal(snapshot.Protocol.MinWriterVersion, restoredSnapshot.Protocol.MinWriterVersion);
        Assert.Equal(snapshot.Metadata.Id, restoredSnapshot.Metadata.Id);
        Assert.Equal(snapshot.Metadata.Name, restoredSnapshot.Metadata.Name);
        Assert.Equal(snapshot.FileCount, restoredSnapshot.FileCount);

        // Verify file details
        foreach (var kvp in snapshot.ActiveFiles)
        {
            Assert.True(restoredSnapshot.ActiveFiles.ContainsKey(kvp.Key),
                $"Missing file: {kvp.Key}");
            var original = kvp.Value;
            var restored = restoredSnapshot.ActiveFiles[kvp.Key];
            Assert.Equal(original.Path, restored.Path);
            Assert.Equal(original.Size, restored.Size);
            Assert.Equal(original.ModificationTime, restored.ModificationTime);
            Assert.Equal(original.DataChange, restored.DataChange);
        }
    }

    [Fact]
    public async Task WriteAndReadCheckpoint_PreservesRemoveTombstoneDeletionVector()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 3, MinWriterVersion = 7 },
            new MetadataAction
            {
                Id = "dv-tombstone-table",
                Name = "dv_tombstone",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
                CreatedTime = 1700000000000,
            },
        });

        var dv = new DeletionVector
        {
            StorageType = "u",
            PathOrInlineDv = "ab^cdefghijklmnopqrs",
            Offset = 1,
            SizeInBytes = 40,
            Cardinality = 5,
        };

        // Add a file carrying a DV, then fully remove it — the remove tombstone carries the same DV.
        await log.WriteCommitAsync(1, new List<DeltaAction>
        {
            new AddFile
            {
                Path = "part-00000.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1000,
                ModificationTime = 1700000001000,
                DataChange = true,
                DeletionVector = dv,
            },
        });
        await log.WriteCommitAsync(2, new List<DeltaAction>
        {
            new RemoveFile
            {
                Path = "part-00000.parquet",
                DataChange = true,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), // unexpired tombstone
                DeletionVector = dv,
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        Assert.Single(snapshot.Tombstones);

        var writer = new CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot);

        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();
        Assert.NotNull(lastCkpt);
        var checkpointActions = await reader.ReadCheckpointAsync(lastCkpt!);

        var restored = new SnapshotBuilder();
        restored.ApplyCommit(lastCkpt!.Version, checkpointActions);
        var restoredSnapshot = restored.Build();

        // The tombstone survived the checkpoint AND kept its deletion-vector reference (VACUUM retention
        // safety: a spec VACUUM protects the DV file while the tombstone is unexpired).
        var tombstone = Assert.Single(restoredSnapshot.Tombstones).Value;
        Assert.Equal("part-00000.parquet", tombstone.Path);
        Assert.NotNull(tombstone.DeletionVector);
        Assert.Equal(dv.StorageType, tombstone.DeletionVector!.StorageType);
        Assert.Equal(dv.PathOrInlineDv, tombstone.DeletionVector.PathOrInlineDv);
        Assert.Equal(dv.Offset, tombstone.DeletionVector.Offset);
        Assert.Equal(dv.SizeInBytes, tombstone.DeletionVector.SizeInBytes);
        Assert.Equal(dv.Cardinality, tombstone.DeletionVector.Cardinality);
    }

    [Fact]
    public async Task WriteAndReadCheckpoint_WithTransactions()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        var actions = new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "txn-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new TransactionId { AppId = "app1", Version = 42, LastUpdated = 1700000000000 },
            new TransactionId { AppId = "app2", Version = 7 },
            new AddFile
            {
                Path = "data.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 500,
                ModificationTime = 1700000000000,
                DataChange = true,
            },
        };
        await log.WriteCommitAsync(0, actions);

        var snapshot = await SnapshotBuilder.BuildAsync(log);

        var writer = new CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot);

        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();
        var checkpointActions = await reader.ReadCheckpointAsync(lastCkpt!);

        var builder = new SnapshotBuilder();
        builder.ApplyCommit(lastCkpt!.Version, checkpointActions);
        var restored = builder.Build();

        Assert.Equal(2, restored.AppTransactions.Count);
        Assert.Equal(42L, restored.AppTransactions["app1"].Version);
        Assert.Equal(7L, restored.AppTransactions["app2"].Version);
    }

    [Fact]
    public async Task SnapshotBuilder_BootstrapsFromCheckpoint()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Version 0: create table
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "boot-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        // Version 1: add file
        await log.WriteCommitAsync(1, new List<DeltaAction>
        {
            new AddFile
            {
                Path = "file1.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            },
        });

        // Build snapshot and write checkpoint at version 1
        var snapshot1 = await SnapshotBuilder.BuildAsync(log);
        var writer = new CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot1);

        // Version 2: add another file
        await log.WriteCommitAsync(2, new List<DeltaAction>
        {
            new AddFile
            {
                Path = "file2.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 200, ModificationTime = 2000, DataChange = true,
            },
        });

        // Version 3: add another file
        await log.WriteCommitAsync(3, new List<DeltaAction>
        {
            new AddFile
            {
                Path = "file3.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 300, ModificationTime = 3000, DataChange = true,
            },
        });

        // Build snapshot with checkpoint bootstrapping
        // Should read checkpoint at v1, then replay v2 and v3
        var checkpointReader = new CheckpointReader(fs);
        var snapshot3 = await SnapshotBuilder.BuildAsync(log, checkpointReader);

        Assert.Equal(3L, snapshot3.Version);
        Assert.Equal(3, snapshot3.FileCount);
        Assert.Contains("file1.parquet", snapshot3.ActiveFiles.Keys);
        Assert.Contains("file2.parquet", snapshot3.ActiveFiles.Keys);
        Assert.Contains("file3.parquet", snapshot3.ActiveFiles.Keys);
    }

    [Fact]
    public async Task SnapshotBuilder_BootstrapWithTimeTravel()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "tt-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new AddFile
            {
                Path = "v0.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            },
        });

        // Write checkpoint at version 5
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

        var snapshot5 = await SnapshotBuilder.BuildAsync(log);
        var writer = new CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot5);

        // Add more commits
        for (int v = 6; v <= 8; v++)
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

        // Time travel to version 7 — should use checkpoint at v5 then replay v6-v7
        var checkpointReader = new CheckpointReader(fs);
        var snapshot7 = await SnapshotBuilder.BuildAsync(
            log, checkpointReader, atVersion: 7);

        Assert.Equal(7L, snapshot7.Version);
        Assert.Equal(8, snapshot7.FileCount); // v0 through v7
        Assert.DoesNotContain("v8.parquet", snapshot7.ActiveFiles.Keys);
    }

    [Fact]
    public async Task WriteAndReadCheckpoint_WithPartitionValues()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "part-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"date","type":"date","nullable":false,"metadata":{}}]}""",
                PartitionColumns = ["date"],
                Configuration = new Dictionary<string, string>
                {
                    { "delta.appendOnly", "true" },
                },
            },
            new AddFile
            {
                Path = "date=2024-01-01/part-00000.parquet",
                PartitionValues = new Dictionary<string, string> { { "date", "2024-01-01" } },
                Size = 1000,
                ModificationTime = 1700000000000,
                DataChange = true,
            },
            new AddFile
            {
                Path = "date=2024-01-02/part-00000.parquet",
                PartitionValues = new Dictionary<string, string> { { "date", "2024-01-02" } },
                Size = 2000,
                ModificationTime = 1700000000000,
                DataChange = true,
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);

        var writer = new CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot);

        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();
        var checkpointActions = await reader.ReadCheckpointAsync(lastCkpt!);

        var builder = new SnapshotBuilder();
        builder.ApplyCommit(lastCkpt!.Version, checkpointActions);
        var restored = builder.Build();

        Assert.Equal(2, restored.FileCount);

        // Verify partition values survived the round-trip
        var file1 = restored.ActiveFiles.Values
            .First(f => f.Path.Contains("2024-01-01"));
        Assert.Equal("2024-01-01", file1.PartitionValues["date"]);

        var file2 = restored.ActiveFiles.Values
            .First(f => f.Path.Contains("2024-01-02"));
        Assert.Equal("2024-01-02", file2.PartitionValues["date"]);

        // Verify metadata round-tripped
        Assert.Equal("part-table", restored.Metadata.Id);
        Assert.Single(restored.Metadata.PartitionColumns);
        Assert.Equal("date", restored.Metadata.PartitionColumns[0]);
    }
}
