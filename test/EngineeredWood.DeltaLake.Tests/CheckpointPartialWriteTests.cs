// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// A streamed checkpoint file used to be published by disposing it, and <c>await using</c> disposes on the
/// failure path too, so a write that threw after its first bytes left a truncated checkpoint at its final,
/// listable name. A streamed file is now published only by <c>CompleteAsync</c>, which the Parquet writer calls
/// from <c>CloseAsync</c> and never from dispose, so a checkpoint write that fails part-way is discarded. The
/// classic writer streams too: its completion is the create-if-absent step for its fixed name. The streaming faults here land
/// AFTER bytes reach storage: one before <c>CreateAsync</c> never creates the file and proves nothing.
/// </summary>
public class CheckpointPartialWriteTests : IDisposable
{
    private readonly string _tempDir;

    public CheckpointPartialWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_ckpt_partial_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string LogDir => Path.Combine(_tempDir, "_delta_log");

    private async Task<(FailMidStreamFileSystem Fs, Snapshot.Snapshot Snapshot)> BuildTableAsync()
    {
        var fs = new FailMidStreamFileSystem(new LocalTableFileSystem(_tempDir));
        var log = new TransactionLog(fs);
        var actions = new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["v2Checkpoint"],
                WriterFeatures = ["v2Checkpoint"],
            },
            new MetadataAction
            {
                Id = "partial",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        };
        for (int i = 0; i < 4; i++)
        {
            actions.Add(new AddFile
            {
                Path = $"part-{i}.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            });
        }
        await log.WriteCommitAsync(0, actions);
        return (fs, await SnapshotBuilder.BuildAsync(log));
    }

    private string ClassicPath(Snapshot.Snapshot snapshot) =>
        Path.Combine(_tempDir, DeltaVersion.CheckpointPath(snapshot.Version));

    /// <summary>
    /// A classic checkpoint streams to its fixed name and is published only when its writer closes, so a
    /// write that breaks part-way leaves nothing behind, and there is nothing to clean up: nothing is
    /// deleted, which matters because nothing about a failed write could prove which file at that name is
    /// its own.
    /// </summary>
    [Fact]
    public async Task Classic_FailingMidStream_LeavesNoCheckpointAndDeletesNothing()
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailMidStream = path => path.Contains(".checkpoint");

        await Assert.ThrowsAsync<IOException>(async () =>
            await new CheckpointWriter(fs) { Format = CheckpointFormat.Classic }.WriteCheckpointAsync(snapshot));

        Assert.True(fs.BytesReachedStorage);
        Assert.Contains(DeltaVersion.CheckpointPath(snapshot.Version), fs.Streamed);
        Assert.Empty(Directory.GetFileSystemEntries(LogDir, "*.checkpoint*"));
        Assert.Empty(Directory.GetFiles(LogDir, ".*"));
        Assert.False(File.Exists(Path.Combine(LogDir, "_last_checkpoint")));
        Assert.Empty(fs.Deleted);
    }

    /// <summary>
    /// Another writer's checkpoint of the same version (or this writer's earlier one) is kept, not
    /// replaced: nothing about a second write can prove which object at the fixed name is its own.
    /// </summary>
    [Fact]
    public async Task Classic_AnExistingCheckpoint_IsKeptNotReplaced()
    {
        var (fs, snapshot) = await BuildTableAsync();
        var writer = new CheckpointWriter(fs) { Format = CheckpointFormat.Classic };
        await writer.WriteCheckpointAsync(snapshot);
        byte[] first = File.ReadAllBytes(ClassicPath(snapshot));
        File.WriteAllBytes(ClassicPath(snapshot), [.. first, 0x00]);    // tell the two apart

        await writer.WriteCheckpointAsync(snapshot);

        Assert.Equal(first.Length + 1, File.ReadAllBytes(ClassicPath(snapshot)).Length);
        Assert.Empty(fs.Deleted);
        var hint = await new CheckpointReader(fs).ReadLastCheckpointAsync();
        Assert.Equal(snapshot.Version, hint!.Version);
    }

    /// <summary>
    /// Two writers checkpointing the same version both stream to the same name. The one that completes
    /// second loses the create-if-absent step: the winner's checkpoint is kept byte for byte, and the loser
    /// still writes the hint, since the checkpoint it names is there and covers the same state.
    /// </summary>
    [Fact]
    public async Task Classic_LosingTheRaceAtCompletion_KeepsTheWinnersCheckpoint()
    {
        var (fs, snapshot) = await BuildTableAsync();
        // The bytes only have to be recognisable: nothing here reads the checkpoint back.
        byte[] winners = "the other writer's checkpoint"u8.ToArray();
        fs.BeforeComplete = async path =>
        {
            if (path.Contains(".checkpoint"))
                Assert.True(await fs.TryWriteAllBytesAsync(path, winners));
        };

        await new CheckpointWriter(fs) { Format = CheckpointFormat.Classic }.WriteCheckpointAsync(snapshot);

        Assert.Equal(winners, File.ReadAllBytes(ClassicPath(snapshot)));
        Assert.Empty(Directory.GetFiles(LogDir, ".*"));
        Assert.Empty(fs.Deleted);
        var hint = await new CheckpointReader(fs).ReadLastCheckpointAsync();
        Assert.Equal(snapshot.Version, hint!.Version);
    }

    /// <summary>
    /// A completion that landed but reported failure (a lost response, say) leaves a whole checkpoint at the
    /// name. It is kept, not cleaned up, and the hint is written: the checkpoint it names is there.
    /// </summary>
    [Fact]
    public async Task Classic_CompletionThatLandsThenFails_KeepsTheCheckpointAndWritesTheHint()
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailAfterComplete = path => path.Contains(".checkpoint");

        await new CheckpointWriter(fs) { Format = CheckpointFormat.Classic }.WriteCheckpointAsync(snapshot);

        Assert.True(fs.BytesReachedStorage);
        Assert.True(File.Exists(ClassicPath(snapshot)));
        Assert.Empty(fs.Deleted);
        var hint = await new CheckpointReader(fs).ReadLastCheckpointAsync();
        Assert.Equal(snapshot.Version, hint!.Version);
    }

    [Fact]
    public async Task V2ParquetBody_FailingMidStream_LeavesNoCheckpoint()
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailMidStream = path => path.Contains(".checkpoint.");

        await Assert.ThrowsAsync<IOException>(async () =>
            await new V2CheckpointWriter(fs) { Body = V2CheckpointBody.Parquet }.WriteCheckpointAsync(snapshot));

        Assert.True(fs.BytesReachedStorage);
        Assert.Empty(Directory.GetFiles(LogDir, "*.checkpoint*"));
    }

    [Fact]
    public async Task V2Sidecar_FailingMidStream_LeavesNoSidecar()
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailMidStream = path => path.StartsWith(DeltaVersion.SidecarPrefix, StringComparison.Ordinal);

        await Assert.ThrowsAsync<IOException>(async () =>
            await new V2CheckpointWriter(fs) { SidecarThreshold = 1 }.WriteCheckpointAsync(snapshot));

        Assert.True(fs.BytesReachedStorage);
        string sidecars = Path.Combine(LogDir, "_sidecars");
        Assert.True(!Directory.Exists(sidecars) || Directory.GetFiles(sidecars).Length == 0);
        Assert.Empty(Directory.GetFiles(LogDir, "*.checkpoint*"));
    }

    /// <summary>
    /// An NDJSON body cut at a line break still parses, and nothing in it says how many lines it should
    /// have, so a partial one at its final name loads with file actions silently missing (#539). It is
    /// streamed like the Parquet body, never written whole, since the local whole-file write is in place.
    /// </summary>
    [Fact]
    public async Task V2JsonBody_FailingMidStream_LeavesNoCheckpoint()
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailMidStream = path => path.Contains(".checkpoint.");

        await Assert.ThrowsAsync<IOException>(async () =>
            await new V2CheckpointWriter(fs) { Body = V2CheckpointBody.Json }.WriteCheckpointAsync(snapshot));

        Assert.True(fs.BytesReachedStorage);
        Assert.Contains(fs.Streamed, p => p.Contains(".checkpoint.") && p.EndsWith(".json", StringComparison.Ordinal));
        Assert.DoesNotContain(fs.WrittenWhole, p => p.Contains(".checkpoint."));
        Assert.Empty(Directory.GetFiles(LogDir, "*.checkpoint*"));
        Assert.Empty(Directory.GetFiles(LogDir, ".*"));
    }

    /// <summary>A compacted log file has the same NDJSON shape, and the same exposure, as a JSON body.</summary>
    [Fact]
    public async Task Compaction_FailingMidStream_LeavesNoCompactedFile()
    {
        var (fs, _) = await BuildTableAsync();
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(1, new List<DeltaAction>
        {
            new AddFile
            {
                Path = "part-4.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 2000, DataChange = true,
            },
        });
        fs.FailMidStream = path => path.EndsWith(".compacted.json", StringComparison.Ordinal);

        await Assert.ThrowsAsync<IOException>(async () =>
            await new LogCompaction(fs, log).CompactRangeAsync(0, 1));

        Assert.True(fs.BytesReachedStorage);
        Assert.DoesNotContain(fs.WrittenWhole, p => p.EndsWith(".compacted.json", StringComparison.Ordinal));
        Assert.Empty(Directory.GetFiles(LogDir, "*.compacted.json"));
        Assert.Empty(Directory.GetFiles(LogDir, ".*"));
        Assert.Equal(5, (await SnapshotBuilder.BuildAsync(log)).FileCount);
    }

    private string[] Sidecars()
    {
        string dir = Path.Combine(LogDir, "_sidecars");
        return Directory.Exists(dir) ? Directory.GetFiles(dir) : [];
    }

    /// <summary>
    /// Sidecars are written before the body. When the body then fails, nothing references them, and a
    /// checkpoint that fails on every interval would otherwise pile them up for the sweep's age guard.
    /// </summary>
    [Fact]
    public async Task V2_FailedBody_DeletesTheSidecarsItsAttemptWrote()
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailMidStream = path => path.Contains(".checkpoint.");

        await Assert.ThrowsAsync<IOException>(async () =>
            await new V2CheckpointWriter(fs) { SidecarThreshold = 1, MaxActionsPerSidecar = 2 }
                .WriteCheckpointAsync(snapshot));

        Assert.True(fs.BytesReachedStorage);
        Assert.Equal(2, fs.Deleted.Count(p => p.StartsWith(DeltaVersion.SidecarPrefix, StringComparison.Ordinal)));
        Assert.Empty(Sidecars());
        Assert.Empty(Directory.GetFiles(LogDir, "*.checkpoint*"));
    }

    /// <summary>
    /// A body whose completion landed but reported failure is whole, and it names its sidecars. Deleting
    /// them would turn it into a checkpoint that points at missing files.
    /// </summary>
    [Theory]
    [InlineData(V2CheckpointBody.Json)]
    [InlineData(V2CheckpointBody.Parquet)]
    public async Task V2_FailedBodyThatLanded_KeepsItsSidecars(V2CheckpointBody body)
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailAfterComplete = path => path.Contains(".checkpoint.");

        await Assert.ThrowsAsync<IOException>(async () =>
            await new V2CheckpointWriter(fs) { Body = body, SidecarThreshold = 1, MaxActionsPerSidecar = 2 }
                .WriteCheckpointAsync(snapshot));

        Assert.True(fs.BytesReachedStorage);
        Assert.Single(Directory.GetFiles(LogDir, "*.checkpoint*"));
        Assert.Equal(2, Sidecars().Length);
        Assert.Empty(fs.Deleted);
    }

    [Fact]
    public async Task WriteNewFile_ThatFailsBeforeClosing_PublishesNothing()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        const string path = "_delta_log/_sidecars/failed-before-closing.parquet";

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await CheckpointWriter.WriteNewFileAsync(fs, path, async file =>
            {
                await using var writer = new EngineeredWood.Parquet.ParquetFileWriter(file, ownsFile: false);
                await file.WriteAsync(new byte[] { 1, 2, 3 });
                throw new InvalidOperationException("the write failed before closing (injected)");
            }, CancellationToken.None));

        Assert.False(await fs.ExistsAsync(path));
        Assert.Empty(Directory.GetFiles(Path.Combine(_tempDir, "_delta_log", "_sidecars")));
    }

    /// <summary>
    /// A file this call did not publish survives it: here another writer's, which made this call's own
    /// completion lose the create-if-absent race.
    /// </summary>
    [Fact]
    public async Task WriteNewFile_LeavesAFileItDidNotPublish()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        const string path = "_delta_log/_sidecars/lost-the-race.parquet";
        byte[] theirs = [9, 9, 9];

        await Assert.ThrowsAsync<IOException>(async () =>
            await CheckpointWriter.WriteNewFileAsync(fs, path, async file =>
            {
                await file.WriteAsync(new byte[] { 1, 2, 3 });
                Assert.True(await fs.TryWriteAllBytesAsync(path, theirs));
            }, CancellationToken.None));

        Assert.Equal(theirs, await fs.ReadAllBytesAsync(path));
    }

    /// <summary>Lets a write's bytes reach storage, then throws. Records every delete.</summary>
    private sealed class FailMidStreamFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        public Func<string, bool>? FailMidStream { get; set; }

        /// <summary>Streamed files these match are completed, then report failure.</summary>
        public Func<string, bool>? FailAfterComplete { get; set; }

        /// <summary>Runs just before a streamed file is completed.</summary>
        public Func<string, Task>? BeforeComplete { get; set; }

        /// <summary>Every path opened for streaming.</summary>
        public List<string> Streamed { get; } = [];

        /// <summary>Every path written whole, by either kind of whole-file write.</summary>
        public List<string> WrittenWhole { get; } = [];

        public bool BytesReachedStorage { get; private set; }

        public List<string> Deleted { get; } = [];

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public IAsyncEnumerable<TableFileInfo> ListAsync(
            string prefix, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(
            string path, CancellationToken cancellationToken = default) =>
            inner.OpenReadAsync(path, cancellationToken);

        public async ValueTask<ISequentialFile> CreateAsync(
            string path, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            Streamed.Add(path);
            var file = await inner.CreateAsync(path, overwrite, cancellationToken);
            return new FailingFile(this, file, path);
        }

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
        {
            Deleted.Add(path);
            return inner.DeleteAsync(path, cancellationToken);
        }

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
            inner.ExistsAsync(path, cancellationToken);

        public ValueTask<byte[]> ReadAllBytesAsync(
            string path, CancellationToken cancellationToken = default) =>
            inner.ReadAllBytesAsync(path, cancellationToken);

        public async ValueTask<bool> TryWriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            WrittenWhole.Add(path);
            return await inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        }

        public async ValueTask WriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            WrittenWhole.Add(path);
            await inner.WriteAllBytesAsync(path, data, cancellationToken);
        }

        private sealed class FailingFile(FailMidStreamFileSystem owner, ISequentialFile inner, string path)
            : ISequentialFile
        {
            public long Position => inner.Position;

            public async ValueTask WriteAsync(
                ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            {
                await inner.WriteAsync(data, cancellationToken);
                if (owner.FailMidStream?.Invoke(path) != true)
                    return;
                await inner.FlushAsync(cancellationToken);
                owner.BytesReachedStorage = true;
                throw new IOException($"stream broke after its first write (injected): {path}");
            }

            public ValueTask FlushAsync(CancellationToken cancellationToken = default) =>
                inner.FlushAsync(cancellationToken);

            public async ValueTask CompleteAsync(CancellationToken cancellationToken = default)
            {
                if (owner.BeforeComplete is { } before)
                    await before(path);
                await inner.CompleteAsync(cancellationToken);
                if (owner.FailAfterComplete?.Invoke(path) == true)
                {
                    owner.BytesReachedStorage = true;
                    throw new IOException($"completion landed, then reported failure (injected): {path}");
                }
            }

            public ValueTask DisposeAsync() => inner.DisposeAsync();

            public void Dispose() => inner.Dispose();
        }
    }
}
