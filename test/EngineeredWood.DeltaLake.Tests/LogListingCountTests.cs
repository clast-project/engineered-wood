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
/// Building a snapshot needs four views of <c>_delta_log</c> — the latest version, the newest checkpoint,
/// the compaction files, the commits to replay — and it used to walk the directory once per view. On an
/// object store each walk is a round-trip, so the count is the whole point of reading it once; asserting
/// it here is what keeps a later change from quietly reintroducing a second walk.
/// </summary>
public class LogListingCountTests : IDisposable
{
    private readonly string _tempDir;
    private readonly CountingFileSystem _fs;
    private readonly TransactionLog _log;

    public LogListingCountTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_listcount_{Guid.NewGuid():N}");
        Directory.CreateDirectory(Path.Combine(_tempDir, "_delta_log"));
        _fs = new CountingFileSystem(new LocalTableFileSystem(_tempDir));
        _log = new TransactionLog(_fs);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    private async Task WriteTableAsync(int addCommits)
    {
        await _log.WriteCommitAsync(0, [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "listing-count",
                Format = Format.Parquet,
                SchemaString =
                    """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        ]);

        for (int i = 1; i <= addCommits; i++)
        {
            await _log.WriteCommitAsync(i, [
                new AddFile
                {
                    Path = $"part-{i}.parquet",
                    PartitionValues = new Dictionary<string, string>(),
                    Size = 100,
                    ModificationTime = 1700000000000 + i,
                    DataChange = true,
                },
            ]);
        }
    }

    /// <summary>The plain path: latest version, compaction files and commits from one walk.</summary>
    [Fact]
    public async Task BuildAsync_ListsTheLogOnce()
    {
        await WriteTableAsync(addCommits: 3);
        _fs.Reset();

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(1, _fs.ListCalls);
        Assert.Equal(3L, snapshot.Version);
    }

    /// <summary>
    /// The expensive path, and the one that used to cost the most: an unusable hint sends the reader
    /// looking for the checkpoint itself, which was a walk of its own on top of the other three.
    /// </summary>
    [Fact]
    public async Task BuildAsync_WithAnUnusableHint_StillListsOnce()
    {
        await WriteTableAsync(addCommits: 3);
        await new CheckpointWriter(_fs).WriteCheckpointAsync(
            await SnapshotBuilder.BuildAsync(_log, atVersion: 3));
        File.WriteAllBytes(Path.Combine(_tempDir, DeltaVersion.LastCheckpointPath), []);
        _fs.Reset();

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(1, _fs.ListCalls);
        Assert.Equal(3L, snapshot.Version);
        Assert.Equal(3, snapshot.FileCount);
    }

    [Fact]
    public async Task UpdateAsync_ListsTheLogOnce()
    {
        await WriteTableAsync(addCommits: 3);
        var atV1 = await SnapshotBuilder.BuildAsync(_log, atVersion: 1);
        _fs.Reset();

        var updated = await SnapshotBuilder.UpdateAsync(atV1, _log);

        Assert.Equal(1, _fs.ListCalls);
        Assert.Equal(3L, updated.Version);
    }

    [Fact]
    public async Task GetLatestVersionAsync_ListsTheLogOnce()
    {
        await WriteTableAsync(addCommits: 2);
        _fs.Reset();

        Assert.Equal(2L, await _log.GetLatestVersionAsync());
        Assert.Equal(1, _fs.ListCalls);
    }

    /// <summary>Counts <see cref="ListAsync"/>; everything else passes straight through.</summary>
    private sealed class CountingFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        private int _listCalls;

        public int ListCalls => _listCalls;

        public void Reset() => _listCalls = 0;

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _listCalls);
            return inner.ListAsync(prefix, cancellationToken);
        }

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
            => inner.ReadAllBytesAsync(path, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
            => inner.OpenReadAsync(path, cancellationToken);

        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);

        public ValueTask<bool> RenameAsync(string sourcePath, string targetPath, CancellationToken cancellationToken = default)
            => inner.RenameAsync(sourcePath, targetPath, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);

        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);

        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);
    }
}
