// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

public class V2CheckpointTests : IDisposable
{
    private readonly string _tempDir;

    public V2CheckpointTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_v2ckpt_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task WriteAndRead_V2Checkpoint_Inline()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "v2-inline",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new AddFile
            {
                Path = "file1.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            },
            new AddFile
            {
                Path = "file2.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 200, ModificationTime = 2000, DataChange = true,
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);

        // Write V2 checkpoint (inline — few files, under sidecar threshold)
        var writer = new V2CheckpointWriter(fs) { SidecarThreshold = 1000 };
        await writer.WriteCheckpointAsync(snapshot);

        // Read _last_checkpoint
        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();
        Assert.NotNull(lastCkpt);
        Assert.True(lastCkpt!.IsV2);
        Assert.Contains(".checkpoint.", lastCkpt.V2CheckpointPath!);
        Assert.EndsWith(".json", lastCkpt.V2CheckpointPath!);

        // Read checkpoint actions
        var actions = await reader.ReadCheckpointAsync(lastCkpt);
        Assert.NotEmpty(actions);

        // Build snapshot from checkpoint
        var builder = new SnapshotBuilder();
        builder.ApplyCommit(lastCkpt.Version, actions);
        var restored = builder.Build();

        Assert.Equal(snapshot.Version, restored.Version);
        Assert.Equal(snapshot.Metadata.Id, restored.Metadata.Id);
        Assert.Equal(snapshot.FileCount, restored.FileCount);
        Assert.Contains("file1.parquet", restored.ActiveFiles.Keys);
        Assert.Contains("file2.parquet", restored.ActiveFiles.Keys);
    }

    [Fact]
    public async Task WriteAndRead_V2Checkpoint_WithSidecars()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        var initActions = new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "v2-sidecar",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        };

        // Add enough files to exceed sidecar threshold
        for (int i = 0; i < 5; i++)
        {
            initActions.Add(new AddFile
            {
                Path = $"part-{i:D5}.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1000 + i, ModificationTime = 1000 + i, DataChange = true,
            });
        }

        await log.WriteCommitAsync(0, initActions);
        var snapshot = await SnapshotBuilder.BuildAsync(log);

        // Write V2 checkpoint with low sidecar threshold
        var writer = new V2CheckpointWriter(fs) { SidecarThreshold = 2 };
        await writer.WriteCheckpointAsync(snapshot);

        // Verify sidecar file exists
        bool sidecarExists = false;
        await foreach (var file in fs.ListAsync("_delta_log/_sidecars/"))
        {
            sidecarExists = true;
            Assert.EndsWith(".parquet", file.Path);
        }
        Assert.True(sidecarExists, "Sidecar file should exist in _delta_log/_sidecars/");

        // Read checkpoint
        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();
        Assert.True(lastCkpt!.IsV2);

        var actions = await reader.ReadCheckpointAsync(lastCkpt);

        var builder = new SnapshotBuilder();
        builder.ApplyCommit(lastCkpt.Version, actions);
        var restored = builder.Build();

        Assert.Equal(5, restored.FileCount);
        Assert.Equal("v2-sidecar", restored.Metadata.Id);
    }

    [Fact]
    public async Task V2Checkpoint_PreservesTransactions()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "v2-txn",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new TransactionId { AppId = "app1", Version = 42 },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);

        var writer = new V2CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot);

        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();
        var actions = await reader.ReadCheckpointAsync(lastCkpt!);

        var builder = new SnapshotBuilder();
        builder.ApplyCommit(lastCkpt!.Version, actions);
        var restored = builder.Build();

        Assert.Equal(42L, restored.AppTransactions["app1"].Version);
    }

    [Fact]
    public async Task V2Checkpoint_PreservesDomainMetadata()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "v2-dm",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new DomainMetadata
            {
                Domain = "myApp",
                Configuration = """{"key":"value"}""",
                Removed = false,
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);

        var writer = new V2CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot);

        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();
        var actions = await reader.ReadCheckpointAsync(lastCkpt!);

        var builder = new SnapshotBuilder();
        builder.ApplyCommit(lastCkpt!.Version, actions);
        var restored = builder.Build();

        Assert.Equal("""{"key":"value"}""", restored.DomainMetadata["myApp"].Configuration);
    }

    [Fact]
    public async Task ActionSerializer_CheckpointMetadata_RoundTrip()
    {
        var actions = new List<DeltaAction>
        {
            new CheckpointMetadata { Version = 42 },
        };

        byte[] serialized = ActionSerializer.Serialize(actions);
        var deserialized = ActionSerializer.Deserialize(serialized);

        var cm = Assert.IsType<CheckpointMetadata>(Assert.Single(deserialized));
        Assert.Equal(42L, cm.Version);
    }

    [Fact]
    public async Task ActionSerializer_Sidecar_RoundTrip()
    {
        var actions = new List<DeltaAction>
        {
            new SidecarFile
            {
                Path = "abc123.parquet",
                SizeInBytes = 12345,
                ModificationTime = 1700000000000,
            },
        };

        byte[] serialized = ActionSerializer.Serialize(actions);
        var deserialized = ActionSerializer.Deserialize(serialized);

        var sc = Assert.IsType<SidecarFile>(Assert.Single(deserialized));
        Assert.Equal("abc123.parquet", sc.Path);
        Assert.Equal(12345L, sc.SizeInBytes);
        Assert.Equal(1700000000000L, sc.ModificationTime);
    }

    [Fact]
    public async Task SnapshotBuilder_BootstrapsFromV2Checkpoint()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Version 0: create table
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "v2-boot",
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

        // Write V2 checkpoint at version 0
        var snapshot0 = await SnapshotBuilder.BuildAsync(log);
        var writer = new V2CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot0);

        // Version 1: add file
        await log.WriteCommitAsync(1, new List<DeltaAction>
        {
            new AddFile
            {
                Path = "v1.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 200, ModificationTime = 2000, DataChange = true,
            },
        });

        // Build snapshot with V2 checkpoint bootstrapping
        var checkpointReader = new CheckpointReader(fs);
        var snapshot1 = await SnapshotBuilder.BuildAsync(log, checkpointReader);

        Assert.Equal(1L, snapshot1.Version);
        Assert.Equal(2, snapshot1.FileCount);
        Assert.Contains("v0.parquet", snapshot1.ActiveFiles.Keys);
        Assert.Contains("v1.parquet", snapshot1.ActiveFiles.Keys);
    }
}
