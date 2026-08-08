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

    /// <summary>Deletes the commit files in <c>[0..through]</c>, as log cleanup eventually does.</summary>
    /// <remarks>
    /// The point of every test that calls this: with the commits gone, a successful read PROVES the
    /// checkpoint carried the state. Without it the replay would supply the same answer from the log and
    /// the checkpoint path would never be exercised at all.
    /// </remarks>
    private void DeleteCommitsThrough(long through)
    {
        for (long v = 0; v <= through; v++)
            File.Delete(Path.Combine(_tempDir, "_delta_log", $"{v:D20}.json"));
    }

    /// <summary>
    /// PROTOCOL.md defines TWO bodies for a UUID-named V2 checkpoint — <c>n.checkpoint.u.{json/parquet}</c>
    /// — and delta-spark picks between them with a session config rather than deriving it from the table,
    /// so a reader cannot predict which one it will meet. Only the NDJSON body used to be decoded; a
    /// table whose newest checkpoint took the other form failed to open once its commits were cleaned.
    /// </summary>
    [Theory]
    [InlineData(V2CheckpointBody.Json, 1000)]    // inline: threshold above the file count
    [InlineData(V2CheckpointBody.Json, 1)]       // sidecar
    [InlineData(V2CheckpointBody.Parquet, 1000)] // inline
    [InlineData(V2CheckpointBody.Parquet, 1)]    // sidecar
    public async Task V2Checkpoint_IsReadBackFromTheCheckpointAlone_InEitherBody(
        V2CheckpointBody body, int sidecarThreshold)
    {
        var (fs, snapshot) = await BuildTombstoneTableAsync($"v2-body-{body}-{sidecarThreshold}");
        var log = new TransactionLog(fs);

        await new V2CheckpointWriter(fs)
        {
            Body = body,
            SidecarThreshold = sidecarThreshold,
        }.WriteCheckpointAsync(snapshot);

        // The name records the body, and the listing keys off exactly that.
        var reader = new CheckpointReader(fs);
        string written = (await reader.ReadLastCheckpointAsync())!.V2CheckpointPath!;
        Assert.EndsWith(body == V2CheckpointBody.Parquet ? ".parquet" : ".json", written);

        DeleteCommitsThrough(snapshot.Version);

        var rebuilt = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs));

        Assert.Equal(snapshot.Version, rebuilt.Version);
        Assert.Equal($"v2-body-{body}-{sidecarThreshold}", rebuilt.Metadata.Id);
        Assert.Equal(1, rebuilt.FileCount);
        Assert.Contains("kept.parquet", rebuilt.ActiveFiles.Keys);

        // NOT just the active file set. A checkpoint that dropped its unexpired tombstones would still
        // produce the right ActiveFiles and break only VACUUM retention safety.
        Assert.Equal("doomed.parquet", Assert.Single(rebuilt.Tombstones).Value.Path);
    }

    /// <summary>
    /// The same, reached through the <c>_last_checkpoint</c> hint rather than the listing — the hint
    /// carries the path verbatim, so it is the other way a Parquet body arrives at the reader.
    /// </summary>
    [Fact]
    public async Task ParquetBodiedV2Checkpoint_IsReadFromTheLastCheckpointHint()
    {
        var (fs, snapshot) = await BuildTombstoneTableAsync("v2-parquet-hint");

        await new V2CheckpointWriter(fs) { Body = V2CheckpointBody.Parquet }
            .WriteCheckpointAsync(snapshot);

        var hinted = await new CheckpointReader(fs).ReadLastCheckpointAsync();
        Assert.True(hinted!.IsV2);

        // Read straight off the hint, with no listing involved at all.
        var actions = await new CheckpointReader(fs).ReadCheckpointAsync(hinted);

        var builder = new SnapshotBuilder();
        builder.ApplyCommit(hinted.Version, actions);
        var restored = builder.Build();

        Assert.Equal("v2-parquet-hint", restored.Metadata.Id);
        Assert.Contains("kept.parquet", restored.ActiveFiles.Keys);
        Assert.Equal("doomed.parquet", Assert.Single(restored.Tombstones).Value.Path);

        // And the checkpointMetadata row, which describes the checkpoint rather than the table, must not
        // have been mistaken for one of the table's own actions.
        Assert.Empty(actions.OfType<CheckpointMetadata>());
    }

    /// <summary>
    /// A CLASSIC-named <c>n.checkpoint.parquet</c> is allowed to follow the V2 spec —
    /// PROTOCOL.md: "Could follow V2 spec … may or may not have sidecar files". Its sidecar rows used to
    /// be dropped on the floor along with every file action they pointed at, and the resulting snapshot
    /// had a protocol, a metaData and NO FILES, reported as a success.
    /// </summary>
    /// <remarks>
    /// Produced by writing a V2 checkpoint with a Parquet body and renaming it to the classic name, which
    /// is exactly the artefact a writer choosing that combination emits — and, unlike hand-building the
    /// Arrow batch, keeps the test honest about the schema a real one carries.
    /// </remarks>
    [Fact]
    public async Task ClassicNamedCheckpointFollowingV2Spec_ResolvesItsSidecars()
    {
        var (fs, snapshot) = await BuildTombstoneTableAsync("v2-classic-named");
        var log = new TransactionLog(fs);

        await new V2CheckpointWriter(fs)
        {
            Body = V2CheckpointBody.Parquet,
            SidecarThreshold = 1, // force the file actions out into a sidecar
        }.WriteCheckpointAsync(snapshot);

        string logDir = Path.Combine(_tempDir, "_delta_log");
        string uuidNamed = Directory.GetFiles(logDir, "*.checkpoint.*.parquet").Single();
        File.Move(uuidNamed, Path.Combine(logDir, $"{snapshot.Version:D20}.checkpoint.parquet"));

        // The hint still names the UUID file, which no longer exists; the listing is the fallback and
        // finds the classic name. Deleting it is simpler and makes the listing the only route.
        File.Delete(Path.Combine(logDir, "_last_checkpoint"));

        DeleteCommitsThrough(snapshot.Version);

        var rebuilt = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs));

        Assert.Equal("v2-classic-named", rebuilt.Metadata.Id);
        Assert.Equal(1, rebuilt.FileCount);
        Assert.Contains("kept.parquet", rebuilt.ActiveFiles.Keys);
        Assert.Equal("doomed.parquet", Assert.Single(rebuilt.Tombstones).Value.Path);
    }

    /// <summary>
    /// A sidecar may carry add and remove entries only. One that references another sidecar is refused
    /// rather than followed: the reader has no bound on where that ends, and a cycle would not terminate.
    /// </summary>
    [Fact]
    public async Task Sidecar_ThatReferencesASidecar_IsRefused()
    {
        var (fs, snapshot) = await BuildTombstoneTableAsync("v2-sidecar-cycle");

        await new V2CheckpointWriter(fs)
        {
            Body = V2CheckpointBody.Parquet,
            SidecarThreshold = 1,
        }.WriteCheckpointAsync(snapshot);

        string logDir = Path.Combine(_tempDir, "_delta_log");
        string checkpoint = Directory.GetFiles(logDir, "*.checkpoint.*.parquet").Single();
        string sidecar = Directory.GetFiles(Path.Combine(logDir, "_sidecars"), "*.parquet").Single();

        // The checkpoint body IS a valid sidecar body plus a sidecar row, so putting it in the sidecars
        // directory under the sidecar's own name makes that sidecar point at itself.
        File.Copy(checkpoint, sidecar, overwrite: true);

        var hint = await new CheckpointReader(fs).ReadLastCheckpointAsync();
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await new CheckpointReader(fs).ReadCheckpointAsync(hint!));

        Assert.Equal(DeltaErrorCodes.UnsupportedCheckpointFormat, ex.ErrorCode);
        Assert.Contains("sidecar", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// A UUID-named checkpoint whose body is neither of the two the spec defines is recognised as a
    /// checkpoint and reported as the cause when it is the only route to the requested version — rather
    /// than surfacing as "Delta log is incomplete", which sends a user to look at retention settings for
    /// what is a limitation of this decoder.
    /// </summary>
    [Fact]
    public async Task UndecodableV2CheckpointBody_IsNamedAsTheCause_NotTheLog()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("v2-unknown-body"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);

        File.WriteAllText(
            Path.Combine(_tempDir, "_delta_log", $"{1:D20}.checkpoint.{Guid.NewGuid()}.avro"),
            "not a body this reader knows");

        DeleteCommitsThrough(1);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs)));

        Assert.Equal(DeltaErrorCodes.UnsupportedCheckpointFormat, ex.ErrorCode);
        Assert.DoesNotContain("incomplete", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The case that must NOT change: a checkpoint form this reader passes over is still no reason to
    /// refuse a table something else covers.
    /// </summary>
    [Fact]
    public async Task UndecodableV2CheckpointBody_DoesNotRefuseAnOtherwiseReadableTable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("v2-unknown-tolerated"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);

        File.WriteAllText(
            Path.Combine(_tempDir, "_delta_log", $"{1:D20}.checkpoint.{Guid.NewGuid()}.avro"),
            "not a body this reader knows");

        var snapshot = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs));

        Assert.Equal(1, snapshot.Version);
        Assert.Contains("a.parquet", snapshot.ActiveFiles.Keys);
    }

    /// <summary>
    /// The other way a replay ends up with a hole nothing accounts for: a multi-part checkpoint whose
    /// parts did not all land. The history really is gone — so this stays a truncated log — but the torn
    /// checkpoint is the cause, and "your retention is too aggressive" is the wrong thing to conclude
    /// from a checkpoint write that did not finish.
    /// </summary>
    [Fact]
    public async Task TornMultiPartCheckpoint_IsNamedAsTheCause()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("torn-multipart"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);

        // Two of a declared three parts — the prefix a writer that died midway leaves behind.
        string logDir = Path.Combine(_tempDir, "_delta_log");
        for (int part = 1; part <= 2; part++)
        {
            File.WriteAllText(
                Path.Combine(logDir, $"{1:D20}.checkpoint.{part:D10}.{3:D10}.parquet"),
                "a part that landed");
        }

        DeleteCommitsThrough(1);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs)));

        Assert.Equal(DeltaErrorCodes.TruncatedTransactionLog, ex.ErrorCode);
        Assert.Contains("2 of its 3 parts", ex.Message);
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
