// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Issue #226: post-commit maintenance — the version checksum, the interval checkpoint and log cleanup — runs
/// after the commit is durable, so its failure must not fail the write. Each one goes to
/// <see cref="DeltaTableOptions.OnPostCommitMaintenanceFailure"/> instead. The interval checkpoint used to be
/// the one that threw.
///
/// <para>Both commit routes are covered, because the guard lives at each route's own trigger rather than in
/// the seam they share with the explicit <see cref="DeltaTable.CheckpointAsync"/>: a blind append goes
/// through <c>LogCommitter</c>, an overwrite through <c>AfterCommitAsync</c>.</para>
/// </summary>
public class PostCommitMaintenanceFailureTests : IDisposable
{
    private readonly string _tempDir;

    public PostCommitMaintenanceFailureTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_pcmf_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema IdSchema =
        new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, false)).Build();

    private static RecordBatch Row(long id) => new(IdSchema, [new Int64Array.Builder().Append(id).Build()], 1);

    private static bool IsCheckpoint(string path) =>
        path.Contains(".checkpoint") || path.EndsWith("_last_checkpoint", StringComparison.Ordinal);

    private static bool IsChecksum(string path) =>
        path.StartsWith("_delta_log/", StringComparison.Ordinal) && path.EndsWith(".crc", StringComparison.Ordinal);

    private async Task<(DeltaTable Table, FaultingFileSystem Fs, List<PostCommitMaintenanceFailure> Reports)>
        CreateAsync(int checkpointInterval = 2, bool listen = true)
    {
        var fs = new FaultingFileSystem(new LocalTableFileSystem(_tempDir));
        var reports = new List<PostCommitMaintenanceFailure>();
        var table = await DeltaTable.CreateAsync(fs, IdSchema, new DeltaTableOptions
        {
            CheckpointInterval = checkpointInterval,
            OnPostCommitMaintenanceFailure = listen ? reports.Add : null,
        });
        return (table, fs, reports);
    }

    private async Task<long> CountRowsAsync()
    {
        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        long rows = 0;
        await foreach (var b in reopened.ReadAllAsync())
            rows += b.Length;
        return rows;
    }

    [Fact]
    public async Task Append_FailedIntervalCheckpoint_WriteSucceedsAndIsReported()
    {
        var (table, fs, reports) = await CreateAsync();
        await using var _ = table;
        await table.WriteAsync([Row(1)]);                              // v1
        fs.FailWrite = IsCheckpoint;

        long v2 = await table.WriteAsync([Row(2)]);                    // v2: on the interval

        Assert.Equal(2, v2);
        var report = Assert.Single(reports);
        Assert.Equal(PostCommitMaintenanceStep.Checkpoint, report.Step);
        Assert.Equal(2, report.Version);
        Assert.IsType<IOException>(report.Exception);
        Assert.False(File.Exists(Path.Combine(_tempDir, DeltaVersion.CheckpointPath(2))));
        Assert.Equal(2, table.CurrentSnapshot.Version);
        Assert.Equal(2, await CountRowsAsync());
    }

    [Fact]
    public async Task Overwrite_FailedIntervalCheckpoint_WriteSucceedsAndIsReported()
    {
        var (table, fs, reports) = await CreateAsync();
        await using var _ = table;
        await table.WriteAsync([Row(1)]);                              // v1
        fs.FailWrite = IsCheckpoint;

        long v2 = await table.WriteAsync([Row(2)], DeltaWriteMode.Overwrite);

        Assert.Equal(2, v2);
        var report = Assert.Single(reports);
        Assert.Equal(PostCommitMaintenanceStep.Checkpoint, report.Step);
        Assert.Equal(2, report.Version);
        Assert.Equal(1, await CountRowsAsync());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NoListener_FailedIntervalCheckpoint_StillDoesNotFailTheWrite(bool overwrite)
    {
        var (table, fs, _) = await CreateAsync(listen: false);
        await using var t = table;
        await table.WriteAsync([Row(1)]);
        fs.FailWrite = IsCheckpoint;

        long v2 = overwrite
            ? await table.WriteAsync([Row(2)], DeltaWriteMode.Overwrite)
            : await table.WriteAsync([Row(2)]);

        Assert.Equal(2, v2);
    }

    /// <summary>
    /// The guard must not fall through to cleanup: with every commit past retention, a cleanup that ran
    /// anyway would delete v0 and v1 with no checkpoint covering them, and the table would not open.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedCheckpoint_SkipsCleanup(bool overwrite)
    {
        var (table, fs, _) = await CreateAsync();
        await using var t = table;
        await table.WriteAsync([Row(1)]);                              // v1
        var old = DateTime.UtcNow.AddDays(-60);
        foreach (string file in Directory.GetFiles(Path.Combine(_tempDir, "_delta_log")))
            File.SetLastWriteTimeUtc(file, old);
        fs.FailWrite = IsCheckpoint;

        if (overwrite)
            await table.WriteAsync([Row(2)], DeltaWriteMode.Overwrite);
        else
            await table.WriteAsync([Row(2)]);

        Assert.True(File.Exists(Path.Combine(_tempDir, "_delta_log", $"{DeltaVersion.Format(0)}.json")));
        Assert.True(File.Exists(Path.Combine(_tempDir, "_delta_log", $"{DeltaVersion.Format(1)}.json")));
        Assert.Equal(overwrite ? 1 : 2, await CountRowsAsync());
    }

    [Fact]
    public async Task FailedCheckpoint_IsRetriedOnTheNextInterval()
    {
        var (table, fs, reports) = await CreateAsync();
        await using var _ = table;
        await table.WriteAsync([Row(1)]);
        fs.FailWrite = IsCheckpoint;
        await table.WriteAsync([Row(2)]);                              // v2: checkpoint fails
        fs.FailWrite = null;
        await table.WriteAsync([Row(3)]);                              // v3
        await table.WriteAsync([Row(4)]);                              // v4: checkpoint lands

        Assert.Single(reports);
        Assert.True(File.Exists(Path.Combine(_tempDir, DeltaVersion.CheckpointPath(4))));
    }

    [Fact]
    public async Task ExplicitCheckpoint_StillThrows()
    {
        var (table, fs, reports) = await CreateAsync(checkpointInterval: 0);
        await using var _ = table;
        await table.WriteAsync([Row(1)]);
        fs.FailWrite = IsCheckpoint;

        await Assert.ThrowsAsync<IOException>(async () => await table.CheckpointAsync());
        Assert.Empty(reports);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedChecksum_IsReported(bool overwrite)
    {
        var (table, fs, reports) = await CreateAsync(checkpointInterval: 0);
        await using var _ = table;
        fs.FailWrite = IsChecksum;

        long v1 = overwrite
            ? await table.WriteAsync([Row(1)], DeltaWriteMode.Overwrite)
            : await table.WriteAsync([Row(1)]);

        var report = Assert.Single(reports);
        Assert.Equal(PostCommitMaintenanceStep.VersionChecksum, report.Step);
        Assert.Equal(v1, report.Version);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedCleanupDelete_IsReported(bool overwrite)
    {
        var (table, fs, reports) = await CreateAsync();
        await using var _ = table;
        await table.WriteAsync([Row(1)]);                              // v1
        var old = DateTime.UtcNow.AddDays(-60);
        foreach (string file in Directory.GetFiles(Path.Combine(_tempDir, "_delta_log")))
            File.SetLastWriteTimeUtc(file, old);
        fs.FailDelete = true;

        long v2 = overwrite
            ? await table.WriteAsync([Row(2)], DeltaWriteMode.Overwrite)
            : await table.WriteAsync([Row(2)]);

        Assert.Equal(2, v2);
        Assert.True(File.Exists(Path.Combine(_tempDir, DeltaVersion.CheckpointPath(2))));
        Assert.NotEmpty(reports);
        Assert.All(reports, r =>
        {
            Assert.Equal(PostCommitMaintenanceStep.LogCleanup, r.Step);
            Assert.Equal(2, r.Version);
        });
    }

    [Fact]
    public async Task ThrowingListener_DoesNotFailTheWrite()
    {
        var fs = new FaultingFileSystem(new LocalTableFileSystem(_tempDir));
        int calls = 0;
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, new DeltaTableOptions
        {
            CheckpointInterval = 2,
            OnPostCommitMaintenanceFailure = _ =>
            {
                calls++;
                throw new InvalidOperationException("listener is broken");
            },
        });
        await table.WriteAsync([Row(1)]);
        fs.FailWrite = IsCheckpoint;

        long v2 = await table.WriteAsync([Row(2)]);

        Assert.Equal(2, v2);
        Assert.Equal(1, calls);
    }

    /// <summary>Fails the writes <see cref="FailWrite"/> matches, and every delete while
    /// <see cref="FailDelete"/> is set.</summary>
    private sealed class FaultingFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        public Func<string, bool>? FailWrite { get; set; }

        public bool FailDelete { get; set; }

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        private void ThrowIfWriteFails(string path)
        {
            if (FailWrite?.Invoke(path) == true)
                throw new IOException($"write refused (injected): {path}");
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(
            string prefix, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(
            string path, CancellationToken cancellationToken = default) =>
            inner.OpenReadAsync(path, cancellationToken);

        public ValueTask<ISequentialFile> CreateAsync(
            string path, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            ThrowIfWriteFails(path);
            return inner.CreateAsync(path, overwrite, cancellationToken);
        }

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
        {
            if (FailDelete)
                throw new IOException($"delete refused (injected): {path}");
            return inner.DeleteAsync(path, cancellationToken);
        }

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
            inner.ExistsAsync(path, cancellationToken);

        public ValueTask<byte[]> ReadAllBytesAsync(
            string path, CancellationToken cancellationToken = default) =>
            inner.ReadAllBytesAsync(path, cancellationToken);

        public ValueTask<bool> TryWriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            ThrowIfWriteFails(path);
            return inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        }

        public ValueTask WriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            ThrowIfWriteFails(path);
            return inner.WriteAllBytesAsync(path, data, cancellationToken);
        }
    }
}
