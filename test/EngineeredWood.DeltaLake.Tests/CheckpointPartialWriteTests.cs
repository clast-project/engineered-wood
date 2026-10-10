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
/// listable name. Each writer now deletes what it created when the write fails. The fault here lands AFTER
/// bytes reach storage: one before <c>CreateAsync</c> never creates the file and proves nothing.
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

    [Fact]
    public async Task Classic_FailingMidStream_LeavesNoCheckpoint()
    {
        var (fs, snapshot) = await BuildTableAsync();
        fs.FailMidStream = path => path.Contains(".checkpoint");

        await Assert.ThrowsAsync<IOException>(async () =>
            await new CheckpointWriter(fs) { Format = CheckpointFormat.Classic }.WriteCheckpointAsync(snapshot));

        Assert.True(fs.BytesReachedStorage);
        Assert.Empty(Directory.GetFiles(LogDir, "*.checkpoint*"));
        Assert.False(File.Exists(Path.Combine(LogDir, "_last_checkpoint")));
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
    /// A classic checkpoint is rewritten at its fixed name when the same version is checkpointed again. An
    /// object store keeps the old object until the new upload completes, so a failed retry must not delete
    /// the path: that would remove a good checkpoint the hint still names.
    /// </summary>
    [Fact]
    public async Task Classic_FailedRetryOverAnExistingCheckpoint_DoesNotDeleteIt()
    {
        var (fs, snapshot) = await BuildTableAsync();
        var writer = new CheckpointWriter(fs) { Format = CheckpointFormat.Classic };
        await writer.WriteCheckpointAsync(snapshot);
        fs.FailMidStream = path => path.Contains(".checkpoint");

        await Assert.ThrowsAsync<IOException>(async () => await writer.WriteCheckpointAsync(snapshot));

        Assert.True(fs.BytesReachedStorage);
        Assert.DoesNotContain(DeltaVersion.CheckpointPath(snapshot.Version), fs.Deleted);
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

    /// <summary>Lets a write's bytes reach storage, then throws. Records every delete.</summary>
    private sealed class FailMidStreamFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        public Func<string, bool>? FailMidStream { get; set; }

        /// <summary>Whole-file writes these match land, then throw.</summary>
        public Func<string, bool>? FailAfterWriteAllBytes { get; set; }

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
            var file = await inner.CreateAsync(path, overwrite, cancellationToken);
            return FailMidStream?.Invoke(path) == true ? new FailingFile(this, file, path) : file;
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

        public ValueTask<bool> TryWriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            inner.TryWriteAllBytesAsync(path, data, cancellationToken);

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
