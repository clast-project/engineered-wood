// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #740: a snapshot held across a drop and re-create of the table at the same path. The new log can reach the
/// snapshot's version again, or pass it, so version numbers alone let a refresh replay the new table's commits onto
/// the old table's state, or keep the old state outright. The refresh now recognises the replaced log by the
/// identity of the file that named the snapshot's version, and rebuilds; an unchanged log still refreshes
/// incrementally.
/// </summary>
public class ReplacedLogTests : IDisposable
{
    private static readonly string Commit0 = $"_delta_log/{0L:D20}.json";

    private readonly string _tempDir = Directory.CreateDirectory(
        Path.Combine(Path.GetTempPath(), $"delta_replaced_log_{Guid.NewGuid():N}")).FullName;

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Local => new(_tempDir);

    private static DeltaTableOptions Options => new() { CheckpointInterval = 0 };

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Build();

    private static RecordBatch Rows(params long[] ids)
    {
        var id = new Int64Array.Builder();
        foreach (long i in ids)
            id.Append(i);
        return new RecordBatch(TableSchema, [id.Build()], ids.Length);
    }

    private static async Task<List<long>> IdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync())
        {
            var column = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    /// <summary>Creates the table (version 0) and appends each id as its own commit.</summary>
    private async Task CreateAsync(ITableFileSystem fs, params long[] ids)
    {
        await using var writer = await DeltaTable.CreateAsync(fs, TableSchema, Options);
        foreach (long id in ids)
            await writer.WriteAsync([Rows(id)]);
    }

    /// <summary>Drops the table: everything at the path goes, as a DROP of an external location leaves it.</summary>
    private void Drop()
    {
        foreach (string entry in Directory.EnumerateFileSystemEntries(_tempDir))
        {
            if (Directory.Exists(entry))
                Directory.Delete(entry, recursive: true);
            else
                File.Delete(entry);
        }
    }

    [Fact]
    public async Task Unchanged_log_refreshes_without_a_rebuild()
    {
        var fs = new ListingRewriteFileSystem(Local, f => f);
        await CreateAsync(fs, 1, 2);
        await using var table = await DeltaTable.OpenAsync(fs, Options);
        var before = table.CurrentSnapshot;
        int reads = fs.ReadsOf(Commit0);

        await table.RefreshAsync();

        Assert.Same(before.ActiveFiles, table.CurrentSnapshot.ActiveFiles);
        Assert.Equal(reads, fs.ReadsOf(Commit0));
    }

    [Fact]
    public async Task New_commit_on_the_same_log_replays_incrementally()
    {
        var fs = new ListingRewriteFileSystem(Local, f => f);
        await CreateAsync(fs, 1, 2);
        await using var table = await DeltaTable.OpenAsync(fs, Options);
        await using (var writer = await DeltaTable.OpenAsync(fs, Options))
            await writer.WriteAsync([Rows(3)]);
        int reads = fs.ReadsOf(Commit0);

        await table.RefreshAsync();

        Assert.Equal(3, table.CurrentSnapshot.Version);
        Assert.Equal([1L, 2, 3], await IdsAsync(table));
        Assert.Equal(reads, fs.ReadsOf(Commit0));
    }

    [Fact]
    public async Task A_handles_own_commits_keep_its_refresh_incremental()
    {
        // The creation commit builds its snapshot in memory, with no listing behind it; the commits after it refresh
        // through a listing, which records the log file again.
        var fs = new ListingRewriteFileSystem(Local, f => f);
        await using var table = await DeltaTable.CreateAsync(fs, TableSchema, Options);
        await table.WriteAsync([Rows(1)]);
        await table.WriteAsync([Rows(2)]);
        int reads = fs.ReadsOf(Commit0);

        await table.RefreshAsync();

        Assert.Equal(reads, fs.ReadsOf(Commit0));
        Assert.Equal([1L, 2], await IdsAsync(table));
    }

    /// <summary>
    /// The three shapes the new log can take: shorter than the snapshot, at the same version, and past it with
    /// commits that carry no metadata (so no table-id change shows up in the replay either).
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(5)]
    public async Task Recreated_table_is_rebuilt_not_mixed(int newAppends)
    {
        await CreateAsync(Local, 1, 2, 3);                                   // v3: ids 1..3
        await using var table = await DeltaTable.OpenAsync(Local, Options);
        string oldId = table.CurrentSnapshot.Metadata.Id;

        Drop();
        long[] newIds = Enumerable.Range(0, newAppends).Select(i => 100L + i).ToArray();
        await CreateAsync(Local, newIds);                                    // v<newAppends>: ids 100..

        await table.RefreshAsync();

        Assert.Equal(newAppends, table.CurrentSnapshot.Version);
        Assert.NotEqual(oldId, table.CurrentSnapshot.Metadata.Id);
        Assert.Equal(newIds, await IdsAsync(table));
    }

    [Fact]
    public async Task Rebuild_of_a_recreated_table_lists_the_log_once()
    {
        var fs = new ListingRewriteFileSystem(Local, f => f);
        await CreateAsync(fs, 1, 2, 3);
        await using var table = await DeltaTable.OpenAsync(fs, Options);
        Drop();
        await CreateAsync(fs, 100);
        int listings = fs.Listings;

        await table.RefreshAsync();

        Assert.Equal(listings + 1, fs.Listings);
        Assert.Equal([100L], await IdsAsync(table));
    }

    [Fact]
    public async Task Dropped_table_is_not_found_on_refresh()
    {
        await CreateAsync(Local, 1, 2);
        await using var table = await DeltaTable.OpenAsync(Local, Options);

        Drop();

        await Assert.ThrowsAsync<DeltaTableNotFoundException>(async () => await table.RefreshAsync());
    }

    /// <summary>
    /// A listing whose ETag is the MD5 of the file's bytes, as S3's is for a single-part upload. Recreated commits
    /// differ in content (fresh table id, file names, timestamps), so they get different tags.
    /// </summary>
    private ListingRewriteFileSystem ContentTagged() => new(Local, f =>
    {
        using var md5 = System.Security.Cryptography.MD5.Create();
        byte[] hash = md5.ComputeHash(File.ReadAllBytes(Path.Combine(_tempDir, f.Path)));
        return f with { ETag = $"\"{BitConverter.ToString(hash).Replace("-", "")}\"" };
    });

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public async Task Strict_update_refuses_a_replaced_log(int newAppends)
    {
        var fs = ContentTagged();
        await CreateAsync(fs, 1, 2);                                         // v2
        await using var table = await DeltaTable.OpenAsync(fs, Options);
        var held = table.CurrentSnapshot;

        Drop();
        await CreateAsync(fs, Enumerable.Range(0, newAppends).Select(i => 100L + i).ToArray());

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.UpdateAsync(held, new TransactionLog(fs)));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
    }

    [Fact]
    public async Task Touched_log_file_is_rebuilt_by_a_reader_and_updated_by_a_committer()
    {
        // Without ETags, a different modification time is all a touched or copied-in-place file shows. That cannot
        // prove a replacement, so a committer refreshing after a durable commit must not fail on it; a reader
        // rebuilds, which is always safe.
        await CreateAsync(Local, 1, 2);
        await using var table = await DeltaTable.OpenAsync(Local, Options);
        var held = table.CurrentSnapshot;
        string commit = Path.Combine(_tempDir, "_delta_log", $"{held.Version:D20}.json");
        File.SetLastWriteTimeUtc(commit, File.GetLastWriteTimeUtc(commit).AddDays(-1));

        var updated = await SnapshotBuilder.UpdateAsync(held, new TransactionLog(Local));
        Assert.Same(held.ActiveFiles, updated.ActiveFiles);

        await table.RefreshAsync();
        Assert.NotSame(held.ActiveFiles, table.CurrentSnapshot.ActiveFiles);
        Assert.Equal([1L, 2], await IdsAsync(table));
    }

    [Fact]
    public async Task Unidentifiable_log_file_is_rebuilt_by_a_reader_and_updated_by_a_committer()
    {
        // A backend that reports neither an ETag nor a modification time: nothing can show the log is unchanged.
        var fs = new ListingRewriteFileSystem(Local, f => f with { LastModified = default, ETag = null });
        await CreateAsync(fs, 1, 2);
        await using var table = await DeltaTable.OpenAsync(fs, Options);
        var held = table.CurrentSnapshot;
        int reads = fs.ReadsOf(Commit0);
        int listings = fs.Listings;

        await table.RefreshAsync();

        Assert.True(fs.ReadsOf(Commit0) > reads, "an unverifiable snapshot is rebuilt, never continued");
        Assert.Equal(listings + 1, fs.Listings);                             // the rebuild reuses the listing
        Assert.Equal([1L, 2], await IdsAsync(table));

        // The strict update has no rebuild to fall back on, and refusing every such snapshot would make the
        // backend unwritable; it continues, as it did before the check.
        await using (var writer = await DeltaTable.OpenAsync(fs, Options))
            await writer.WriteAsync([Rows(3)]);
        var updated = await SnapshotBuilder.UpdateAsync(held, new TransactionLog(fs));
        Assert.Equal(3, updated.Version);
    }

    [Fact]
    public async Task ETag_decides_over_size_and_time()
    {
        // Every file reports one fixed time, and an ETag the test controls, so a replaced object can be simulated
        // whose size and time match the original. The commit's bytes do not even change.
        var generation = new Dictionary<string, int>(StringComparer.Ordinal);
        var fixedTime = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var fs = new ListingRewriteFileSystem(Local, f => f with
        {
            LastModified = fixedTime,
            ETag = $"\"{f.Path}#{(generation.TryGetValue(f.Path, out int g) ? g : 0)}\"",
        });
        await CreateAsync(fs, 1, 2);
        await using var table = await DeltaTable.OpenAsync(fs, Options);
        string held = $"_delta_log/{table.CurrentSnapshot.Version:D20}.json";

        int reads = fs.ReadsOf(Commit0);
        await table.RefreshAsync();
        Assert.Equal(reads, fs.ReadsOf(Commit0));                           // same tag: incremental

        generation[held] = 1;
        await table.RefreshAsync();
        Assert.True(fs.ReadsOf(Commit0) > reads, "a different ETag at the same path is a different object");
    }

    [Fact]
    public async Task Snapshot_on_a_checkpoint_whose_commit_was_removed_is_still_recognised()
    {
        // Log cleanup can leave a checkpoint with no commit beside it. The checkpoint then stands for the version.
        var fs = new ListingRewriteFileSystem(Local, f => f);
        await CreateAsync(fs, 1, 2);
        await using (var writer = await DeltaTable.OpenAsync(fs, Options))
            await writer.CheckpointAsync();                                  // checkpoint at v2
        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{2L:D20}.json"));
        string checkpoint = $"_delta_log/{2L:D20}.checkpoint.parquet";
        Assert.True(File.Exists(Path.Combine(_tempDir, checkpoint)));

        await using var table = await DeltaTable.OpenAsync(fs, Options);
        var before = table.CurrentSnapshot;
        int reads = fs.ReadsOf(checkpoint);

        await table.RefreshAsync();

        Assert.Same(before.ActiveFiles, table.CurrentSnapshot.ActiveFiles);
        Assert.Equal(reads, fs.ReadsOf(checkpoint));
    }
}
