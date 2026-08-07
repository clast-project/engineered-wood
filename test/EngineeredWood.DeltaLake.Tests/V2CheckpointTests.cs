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

    /// <summary>
    /// Writes a table with one active file and one unexpired remove tombstone, and returns its snapshot.
    /// </summary>
    private async Task<(LocalTableFileSystem Fs, Snapshot.Snapshot Snapshot)> BuildTombstoneTableAsync(
        string tableId)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = tableId,
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new AddFile
            {
                Path = "kept.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            },
            new AddFile
            {
                Path = "doomed.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 200, ModificationTime = 2000, DataChange = true,
            },
        });

        await log.WriteCommitAsync(1, new List<DeltaAction>
        {
            new RemoveFile
            {
                Path = "doomed.parquet",
                DataChange = true,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), // unexpired
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        Assert.Single(snapshot.Tombstones);
        return (fs, snapshot);
    }

    // A checkpoint that drops unexpired tombstones is not safely replayable: a reader bootstrapping from
    // it alone cannot tell the file was deleted, and VACUUM loses the retention record protecting it.
    // The V1 writer has always preserved them; the V2 writer used to emit snapshot.ActiveFiles only.
    [Theory]
    [InlineData(1000, false)] // threshold above the file count — actions inline
    [InlineData(1, true)]     // threshold below the file count — actions in a sidecar
    public async Task V2Checkpoint_PreservesUnexpiredTombstones(int sidecarThreshold, bool expectSidecar)
    {
        var (fs, snapshot) = await BuildTombstoneTableAsync($"v2-tomb-{sidecarThreshold}");

        var writer = new V2CheckpointWriter(fs) { SidecarThreshold = sidecarThreshold };
        await writer.WriteCheckpointAsync(snapshot);

        bool sawSidecar = false;
        await foreach (var _ in fs.ListAsync("_delta_log/_sidecars/"))
            sawSidecar = true;
        Assert.Equal(expectSidecar, sawSidecar);

        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();
        var actions = await reader.ReadCheckpointAsync(lastCkpt!);

        var builder = new SnapshotBuilder();
        builder.ApplyCommit(lastCkpt!.Version, actions);
        var restored = builder.Build();

        Assert.Equal("doomed.parquet", Assert.Single(restored.Tombstones).Value.Path);
        Assert.Equal(1, restored.FileCount);
        Assert.Contains("kept.parquet", restored.ActiveFiles.Keys);
    }

    // PROTOCOL.md: a sidecar "can have only add file and remove file entries", and the non-file actions
    // "must be part of the v2 spec checkpoint itself". Building the sidecar body from a whole snapshot
    // emitted a protocol and a metaData row into it as well, duplicating the checkpoint's own.
    [Fact]
    public async Task V2Checkpoint_Sidecar_CarriesFileActionsOnly()
    {
        var (fs, snapshot) = await BuildTombstoneTableAsync("v2-sidecar-contents");

        var writer = new V2CheckpointWriter(fs) { SidecarThreshold = 1 };
        await writer.WriteCheckpointAsync(snapshot);

        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();

        // ReadCheckpointAsync concatenates the checkpoint file and its sidecars, so a duplicated
        // protocol/metaData shows up as a second copy in the combined action list.
        var actions = await reader.ReadCheckpointAsync(lastCkpt!);

        Assert.Single(actions.OfType<ProtocolAction>());
        Assert.Single(actions.OfType<MetadataAction>());
        Assert.Equal("kept.parquet", Assert.Single(actions.OfType<AddFile>()).Path);
        Assert.Equal("doomed.parquet", Assert.Single(actions.OfType<RemoveFile>()).Path);
    }

    // sidecar.sizeInBytes is required by the spec. It used to be measured by reading the whole sidecar
    // back off storage — correct, but it doubled every sidecar's I/O and pulled the file into memory to
    // take .Length. This pins the cheap measurement (the writer's own end position) to the same answer.
    [Fact]
    public async Task V2Checkpoint_Sidecar_ReportsTrueSizeInBytes()
    {
        var (fs, snapshot) = await BuildTombstoneTableAsync("v2-sidecar-size");

        var writer = new V2CheckpointWriter(fs) { SidecarThreshold = 1 };
        await writer.WriteCheckpointAsync(snapshot);

        long onDisk = 0;
        await foreach (var file in fs.ListAsync("_delta_log/_sidecars/"))
            onDisk = file.Size;
        Assert.True(onDisk > 0);

        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();
        byte[] body = await fs.ReadAllBytesAsync(lastCkpt!.V2CheckpointPath!);
        var sidecar = Assert.Single(ActionSerializer.Deserialize(body).OfType<SidecarFile>());

        Assert.Equal(onDisk, sidecar.SizeInBytes);
    }

    /// <summary>Overwrites <c>_last_checkpoint</c> with a hint naming <paramref name="path"/>.</summary>
    private async Task WriteLastCheckpointHintAsync(
        LocalTableFileSystem fs, long version, string path)
    {
        // Built by concatenation rather than a raw literal: the JSON's own braces collide with
        // interpolation delimiters and the escaping obscures what is being written.
        string json =
            "{\"version\":" + version + ",\"size\":1,\"v2Checkpoint\":{\"path\":\"" + path + "\"}}";
        await fs.WriteAllBytesAsync(
            DeltaVersion.LastCheckpointPath, System.Text.Encoding.UTF8.GetBytes(json));
    }

    /// <summary>
    /// delta-spark writes <c>v2Checkpoint.path</c> in <c>_last_checkpoint</c> as a BARE FILE NAME,
    /// not a table-relative one. Taking it verbatim looked for the checkpoint beside the data
    /// directories rather than inside <c>_delta_log</c>, so every V2 checkpoint Spark wrote failed to
    /// load — and the table failed to open at all once its commits had been cleaned away.
    /// </summary>
    /// <remarks>
    /// Covered here as well as in <c>SparkInteropTests</c> because the interop tier only runs where a
    /// Spark install is present, which is not where CI runs.
    /// </remarks>
    [Theory]
    [InlineData(false)] // "_delta_log/<n>.checkpoint.<uuid>.json" — what EW itself writes
    [InlineData(true)]  // "<n>.checkpoint.<uuid>.json"            — what delta-spark writes
    public async Task V2Checkpoint_LastCheckpointHint_IsResolvedRelativeToTheLogDirectory(
        bool bareFileName)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("v2-hint"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        await new V2CheckpointWriter(fs).WriteCheckpointAsync(snapshot);

        var reader = new CheckpointReader(fs);
        string written = (await reader.ReadLastCheckpointAsync())!.V2CheckpointPath!;

        if (bareFileName)
        {
            await WriteLastCheckpointHintAsync(fs, 1, Path.GetFileName(written));

            // Pinned directly, not only through the snapshot: the hint must be RESOLVED into the log
            // directory. Without this the theory would still pass via the listing fallback, which
            // costs a failed read and would mask a regression in the resolution itself.
            var reread = await new CheckpointReader(fs).ReadLastCheckpointAsync();
            Assert.StartsWith(DeltaVersion.LogPrefix, reread!.V2CheckpointPath!, StringComparison.Ordinal);
        }

        // Delete the commits the checkpoint subsumes, so the hint is the only way in.
        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{0:D20}.json"));
        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{1:D20}.json"));

        var rebuilt = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs));

        Assert.Equal(1, rebuilt.Version);
        Assert.Equal("v2-hint", rebuilt.Metadata.Id);
        Assert.Contains("a.parquet", rebuilt.ActiveFiles.Keys);
    }

    /// <summary>
    /// A hint that names a checkpoint file which does not exist must fall through to the log listing,
    /// which is the truth the hint only summarises.
    /// </summary>
    /// <remarks>
    /// The fallback existed but was unreachable here: it was skipped whenever the listing's candidate
    /// had the same VERSION as the failed hint, and a hint with a wrong path still names the right
    /// version. Two candidates for one version are the same candidate only if they name the same
    /// file.
    /// </remarks>
    [Fact]
    public async Task V2Checkpoint_HintNamingAMissingFile_FallsBackToTheListing()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("v2-stale-hint"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        await new V2CheckpointWriter(fs).WriteCheckpointAsync(snapshot);

        // Same version, different (nonexistent) file — exactly the shape the version-only skip
        // mistook for "the listing already found this one".
        await WriteLastCheckpointHintAsync(
            fs, 1, $"_delta_log/{1:D20}.checkpoint.{Guid.NewGuid()}.json");

        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{0:D20}.json"));
        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{1:D20}.json"));

        var rebuilt = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs));

        Assert.Equal(1, rebuilt.Version);
        Assert.Contains("a.parquet", rebuilt.ActiveFiles.Keys);
    }

    /// <summary>
    /// Writes a Parquet-bodied V2 checkpoint name at <paramref name="version"/>. The contents do not
    /// matter: the listing decides what it can decode from the NAME, and this reader never opens it.
    /// </summary>
    private void PlaceParquetBodiedV2Checkpoint(long version) =>
        File.WriteAllText(
            Path.Combine(_tempDir, "_delta_log", $"{version:D20}.checkpoint.{Guid.NewGuid()}.parquet"),
            "not really parquet");

    /// <summary>
    /// A table whose only route to the requested version runs through a Parquet-bodied V2 checkpoint
    /// must say so, rather than blaming the log.
    /// </summary>
    /// <remarks>
    /// Both failures leave the same hole in the replay, so both used to surface as "Delta log is
    /// incomplete: version N is missing" — which points a user at retention settings for what is
    /// really a limitation of this reader. That confusion was not hypothetical: the same message was
    /// what a perfectly readable Spark table produced for an unrelated reason (#74), and the two were
    /// indistinguishable.
    /// </remarks>
    [Fact]
    public async Task ParquetBodiedV2Checkpoint_IsNamedAsTheCause_NotTheLog()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("v2-parquet-body"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);

        PlaceParquetBodiedV2Checkpoint(1);

        // Cleanup removed what that checkpoint subsumes, so it is the only way to version 1.
        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{0:D20}.json"));
        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{1:D20}.json"));

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs)));

        Assert.Equal(DeltaErrorCodes.UnsupportedCheckpointFormat, ex.ErrorCode);
        Assert.NotEqual(DeltaErrorCodes.TruncatedTransactionLog, ex.ErrorCode);

        // The message must not send anyone to look at log retention.
        Assert.Contains("Parquet-bodied", ex.Message);
        Assert.DoesNotContain("incomplete", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The case that must NOT change: a table carrying a Parquet-bodied V2 checkpoint is still read
    /// normally when something else covers the range. Recognising the form must not become a reason
    /// to refuse a table we can read.
    /// </summary>
    [Fact]
    public async Task ParquetBodiedV2Checkpoint_DoesNotRefuseAnOtherwiseReadableTable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("v2-parquet-tolerated"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);

        // Present, unreadable, and entirely beside the point: the commits still cover [0..1].
        PlaceParquetBodiedV2Checkpoint(1);

        var snapshot = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs));

        Assert.Equal(1, snapshot.Version);
        Assert.Contains("a.parquet", snapshot.ActiveFiles.Keys);
    }

    /// <summary>
    /// And when an older readable checkpoint covers the gap, that is used — the unreadable one is not
    /// allowed to poison checkpoint selection.
    /// </summary>
    [Fact]
    public async Task ParquetBodiedV2Checkpoint_DoesNotDisplaceAReadableOlderCheckpoint()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("v2-parquet-fallback"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);

        // A readable checkpoint at 1 …
        var atOne = await SnapshotBuilder.BuildAsync(log);
        await new CheckpointWriter(fs).WriteCheckpointAsync(atOne);

        await log.WriteCommitAsync(2, [Add("b.parquet")]);

        // … and an unreadable one at 2, which is newer and would otherwise be preferred.
        PlaceParquetBodiedV2Checkpoint(2);

        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{0:D20}.json"));
        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{1:D20}.json"));

        var snapshot = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs));

        Assert.Equal(2, snapshot.Version);
        Assert.Contains("a.parquet", snapshot.ActiveFiles.Keys);
        Assert.Contains("b.parquet", snapshot.ActiveFiles.Keys);
    }

    private static List<DeltaAction> CreateCommit(string id) =>
    [
        new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
        new MetadataAction
        {
            Id = id,
            Format = Format.Parquet,
            SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
            PartitionColumns = [],
        },
    ];

    private static AddFile Add(string path) => new()
    {
        Path = path,
        PartitionValues = new Dictionary<string, string>(),
        Size = 100,
        ModificationTime = 1000,
        DataChange = true,
    };
}
