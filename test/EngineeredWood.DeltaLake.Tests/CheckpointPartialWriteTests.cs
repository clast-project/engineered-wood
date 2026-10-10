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
/// A streamed checkpoint file is published by disposing it, and <c>await using</c> disposes on the failure
/// path too, so a write that threw after its first bytes used to leave a truncated checkpoint at its final,
/// listable name. The V2 writers, whose files have unique names, now delete what they created when the write
/// fails; the classic writer, whose name is fixed, no longer streams at all. The streaming faults here land
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
    /// A classic checkpoint has a fixed name, so it is encoded in memory and published in one
    /// create-if-absent request: a failure then leaves nothing to clean up, and nothing is deleted.
    /// </summary>
    [Fact]
    public async Task Classic_FailedPublish_LeavesNoCheckpointAndDeletesNothing()
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailTryWriteAllBytes = path => path.Contains(".checkpoint");

        await Assert.ThrowsAsync<IOException>(async () =>
            await new CheckpointWriter(fs) { Format = CheckpointFormat.Classic }.WriteCheckpointAsync(snapshot));

        Assert.Empty(Directory.GetFiles(LogDir, "*.checkpoint*"));
        Assert.False(File.Exists(Path.Combine(LogDir, "_last_checkpoint")));
        Assert.Empty(fs.Deleted);
        Assert.Empty(fs.Streamed);
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
    /// A publish that landed but reported failure (a lost response, say) must not be cleaned up: the
    /// object at the fixed name may be this writer's, or a concurrent writer's good one.
    /// </summary>
    [Fact]
    public async Task Classic_PublishThatLandsThenFails_KeepsTheCheckpoint()
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailAfterWriteAllBytes = path => path.Contains(".checkpoint");

        await Assert.ThrowsAsync<IOException>(async () =>
            await new CheckpointWriter(fs) { Format = CheckpointFormat.Classic }.WriteCheckpointAsync(snapshot));

        Assert.True(fs.BytesReachedStorage);
        Assert.True(File.Exists(ClassicPath(snapshot)));
        Assert.Empty(fs.Deleted);
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

    [Fact]
    public async Task V2JsonBody_FailingAfterItsBytesLand_LeavesNoCheckpoint()
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailAfterWriteAllBytes = path => path.Contains(".checkpoint.");

        await Assert.ThrowsAsync<IOException>(async () =>
            await new V2CheckpointWriter(fs) { Body = V2CheckpointBody.Json }.WriteCheckpointAsync(snapshot));

        Assert.True(fs.BytesReachedStorage);
        Assert.Empty(Directory.GetFiles(LogDir, "*.checkpoint*"));
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
        fs.FailAfterWriteAllBytes = path => path.Contains(".checkpoint.");

        await Assert.ThrowsAsync<IOException>(async () =>
            await new V2CheckpointWriter(fs) { SidecarThreshold = 1, MaxActionsPerSidecar = 2 }
                .WriteCheckpointAsync(snapshot));

        Assert.True(fs.BytesReachedStorage);
        Assert.Equal(2, fs.Deleted.Count(p => p.StartsWith(DeltaVersion.SidecarPrefix, StringComparison.Ordinal)));
        Assert.Empty(Sidecars());
        Assert.Empty(Directory.GetFiles(LogDir, "*.checkpoint*"));
    }

    /// <summary>
    /// If the failed body could not be deleted either, it still names its sidecars. Deleting them would
    /// turn a checkpoint that was only partial into one that points at missing files.
    /// </summary>
    [Fact]
    public async Task V2_FailedBodyThatCouldNotBeDeleted_KeepsItsSidecars()
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailAfterWriteAllBytes = path => path.Contains(".checkpoint.");
        fs.FailDelete = path => path.Contains(".checkpoint.");

        await Assert.ThrowsAsync<IOException>(async () =>
            await new V2CheckpointWriter(fs) { SidecarThreshold = 1, MaxActionsPerSidecar = 2 }
                .WriteCheckpointAsync(snapshot));

        Assert.Single(Directory.GetFiles(LogDir, "*.checkpoint*"));
        Assert.Equal(2, Sidecars().Length);
    }

    /// <summary>Lets a write's bytes reach storage, then throws. Records every delete.</summary>
    private sealed class FailMidStreamFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        public Func<string, bool>? FailMidStream { get; set; }

        /// <summary>Create-if-absent writes these match throw before writing anything.</summary>
        public Func<string, bool>? FailTryWriteAllBytes { get; set; }

        /// <summary>Every path opened for streaming.</summary>
        public List<string> Streamed { get; } = [];

        /// <summary>Whole-file writes (either kind) these match land, then throw.</summary>
        public Func<string, bool>? FailAfterWriteAllBytes { get; set; }

        public bool BytesReachedStorage { get; private set; }

        public List<string> Deleted { get; } = [];

        /// <summary>Deletes of paths these match throw instead.</summary>
        public Func<string, bool>? FailDelete { get; set; }

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
            return FailMidStream?.Invoke(path) == true ? new FailingFile(this, file, path) : file;
        }

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
        {
            if (FailDelete?.Invoke(path) == true)
                throw new IOException($"delete refused (injected): {path}");
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
            if (FailTryWriteAllBytes?.Invoke(path) == true)
                throw new IOException($"write refused (injected): {path}");
            bool written = await inner.TryWriteAllBytesAsync(path, data, cancellationToken);
            if (FailAfterWriteAllBytes?.Invoke(path) == true)
            {
                BytesReachedStorage = true;
                throw new IOException($"write failed after its bytes landed (injected): {path}");
            }
            return written;
        }

        public async ValueTask WriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            await inner.WriteAllBytesAsync(path, data, cancellationToken);
            if (FailAfterWriteAllBytes?.Invoke(path) == true)
            {
                BytesReachedStorage = true;
                throw new IOException($"write failed after its bytes landed (injected): {path}");
            }
        }

        private sealed class FailingFile(FailMidStreamFileSystem owner, ISequentialFile inner, string path)
            : ISequentialFile
        {
            public long Position => inner.Position;

            public async ValueTask WriteAsync(
                ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            {
                await inner.WriteAsync(data, cancellationToken);
                await inner.FlushAsync(cancellationToken);
                owner.BytesReachedStorage = true;
                throw new IOException($"stream broke after its first write (injected): {path}");
            }

            public ValueTask FlushAsync(CancellationToken cancellationToken = default) =>
                inner.FlushAsync(cancellationToken);

            public ValueTask DisposeAsync() => inner.DisposeAsync();

            public void Dispose() => inner.Dispose();
        }
    }
}
