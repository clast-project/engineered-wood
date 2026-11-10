// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #750: a handle held while its table is dropped and re-created at the same path must not commit into the new
/// table's log. Before the fix, the create of base + 1 was the only check: against a shorter new log it succeeded and
/// left a hole every reader then failed on, and against one as long or longer the commit became durable before the
/// refresh refused the log. Now the base's commit file is checked before anything is written.
/// </summary>
public class StaleHandleOnReplacedLogTests : IDisposable
{
    private readonly string _tempDir = Directory.CreateDirectory(
        Path.Combine(Path.GetTempPath(), $"delta_stale_replaced_{Guid.NewGuid():N}")).FullName;

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

    private static async Task CreateAsync(ITableFileSystem fs, params long[] ids)
    {
        await using var writer = await DeltaTable.CreateAsync(fs, TableSchema, Options);
        foreach (long id in ids)
            await writer.WriteAsync([Rows(id)]);
    }

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

    private List<long> CommitVersions() =>
        Directory.GetFiles(Path.Combine(_tempDir, "_delta_log"), "*.json")
            .Select(f => DeltaVersion.TryParseCommitVersion(Path.GetFileName(f), out long v) ? v : -1)
            .Where(v => v >= 0)
            .OrderBy(v => v)
            .ToList();

    /// <summary>A listing whose ETag is the MD5 of the file's bytes, as S3's is for a single-part upload.</summary>
    private ListingRewriteFileSystem ContentTagged() => new(Local, f =>
    {
        using var md5 = System.Security.Cryptography.MD5.Create();
        byte[] hash = md5.ComputeHash(File.ReadAllBytes(Path.Combine(_tempDir, f.Path)));
        return f with { ETag = $"\"{BitConverter.ToString(hash).Replace("-", "")}\"" };
    });

    /// <summary>
    /// The issue's three shapes: the new log shorter than the handle's base (v3), at it, and past it. Each is refused
    /// with nothing written, and the new table is left as its own writer made it.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(5)]
    public async Task Stale_handle_cannot_commit_into_a_recreated_table(int newAppends)
    {
        var fs = ContentTagged();
        await CreateAsync(fs, 1, 2, 3);                                      // v3
        await using var stale = await DeltaTable.OpenAsync(fs, Options);

        Drop();
        long[] newIds = Enumerable.Range(0, newAppends).Select(i => 100L + i).ToArray();
        await CreateAsync(fs, newIds);                                       // v<newAppends>

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await stale.WriteAsync([Rows(9)]));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
        Assert.Contains("Nothing was written", ex.Message, StringComparison.Ordinal);

        Assert.Equal(Enumerable.Range(0, newAppends + 1).Select(v => (long)v), CommitVersions());
        await using var fresh = await DeltaTable.OpenAsync(fs, Options);
        Assert.Equal(newIds, await IdsAsync(fresh));
    }

    private string CommitFile(long version) => Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json");

    private string TxnIdIn(long version)
    {
        foreach (string line in File.ReadAllLines(CommitFile(version)))
        {
            using var doc = System.Text.Json.JsonDocument.Parse(line);
            if (doc.RootElement.TryGetProperty("commitInfo", out var ci))
                return ci.GetProperty(InCommitTimestamp.TxnIdKey).GetString()!;
        }
        throw new InvalidOperationException($"no commitInfo in v{version}");
    }

    /// <summary>
    /// #755 follow-up, the case measured: a table re-created with the same data writes a commit at the base's version
    /// of exactly the old size (478 bytes both times), so without ETags only the time differs. The base commit's
    /// txnId decides: it names another transaction, and the write is refused before anything lands.
    /// </summary>
    [Fact]
    public async Task Table_recreated_with_the_same_data_is_refused_without_ETags()
    {
        await CreateAsync(Local, 1, 2, 3);                                   // v3
        await using var stale = await DeltaTable.OpenAsync(Local, Options);
        long oldSize = new FileInfo(CommitFile(3)).Length;

        Drop();
        await CreateAsync(Local, 1, 2, 3);                                   // the same table again
        Assert.Equal(oldSize, new FileInfo(CommitFile(3)).Length);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await stale.WriteAsync([Rows(9)]));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
        Assert.Contains("names another transaction", ex.Message, StringComparison.Ordinal);
        Assert.Equal([0L, 1, 2, 3], CommitVersions());
    }

    /// <summary>
    /// The same, made deterministic: the base commit's bytes with only its txnId changed, or removed and the line padded
    /// back to its length (another engine's commit, which records none).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Same_size_commit_naming_another_transaction_is_refused(bool removeId)
    {
        await CreateAsync(Local, 1, 2);
        await using var stale = await DeltaTable.OpenAsync(Local, Options);
        string original = TxnIdIn(2);
        string text = File.ReadAllText(CommitFile(2));
        string field = $",\"{InCommitTimestamp.TxnIdKey}\":\"{original}\"";
        Assert.Contains(field, text, StringComparison.Ordinal);
        // Trailing spaces after the commitInfo object are valid JSON, and keep the line, and so the file, its length.
        string replaced = removeId
            ? string.Join("\n", text.Split('\n').Select(line => line.Contains(field)
                ? line.Replace(field, "") + new string(' ', field.Length)
                : line))
            : text.Replace(original, Guid.NewGuid().ToString("D"));
        // Without ETags, an equal size and an equal time prove the same object, and the txnId is never read: a
        // rewrite within the clock's tick would pass for the original. A re-created log's commit is written later.
        var written = File.GetLastWriteTimeUtc(CommitFile(2));
        File.WriteAllText(CommitFile(2), replaced);
        File.SetLastWriteTimeUtc(CommitFile(2), written.AddSeconds(2));
        Assert.Equal(text.Length, File.ReadAllText(CommitFile(2)).Length);
        Assert.Equal(removeId, !File.ReadAllText(CommitFile(2)).Contains(InCommitTimestamp.TxnIdKey));

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await stale.WriteAsync([Rows(9)]));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
        Assert.Equal([0L, 1, 2], CommitVersions());
    }

    [Fact]
    public async Task The_anchor_records_the_base_commits_txnId_after_an_open_and_after_a_commit()
    {
        await CreateAsync(Local, 1, 2);
        await using var table = await DeltaTable.OpenAsync(Local, Options);
        Assert.Equal(TxnIdIn(2), table.CurrentSnapshot.LogAnchor!.CommitTxnId);

        await table.WriteAsync([Rows(3)]);
        Assert.Equal(3, table.CurrentSnapshot.Version);
        Assert.Equal(TxnIdIn(3), table.CurrentSnapshot.LogAnchor!.CommitTxnId);
    }

    /// <summary>
    /// A refresh continues from the held snapshot, so its builder starts with that snapshot's id. A commit it applies
    /// with no commitInfo at all (some engines write none) must not inherit it: the guard would then read that commit,
    /// find no id, and refuse a write to an unchanged table.
    /// </summary>
    [Fact]
    public async Task A_commit_without_a_commitInfo_does_not_inherit_the_previous_id()
    {
        await CreateAsync(Local, 1, 2);
        await using var table = await DeltaTable.OpenAsync(Local, Options);
        Assert.NotNull(table.CurrentSnapshot.LogAnchor!.CommitTxnId);
        await using (var other = await DeltaTable.OpenAsync(Local, Options))
            await other.WriteAsync([Rows(3)]);
        File.WriteAllLines(
            CommitFile(3), File.ReadAllLines(CommitFile(3)).Where(l => !l.Contains("\"commitInfo\"")));

        await table.RefreshAsync();
        Assert.Equal(3, table.CurrentSnapshot.Version);
        Assert.Null(table.CurrentSnapshot.LogAnchor!.CommitTxnId);

        File.SetLastWriteTimeUtc(CommitFile(3), File.GetLastWriteTimeUtc(CommitFile(3)).AddDays(-1));
        Assert.Equal(4, await table.WriteAsync([Rows(4)]));
    }

    [Fact]
    public async Task A_base_commit_without_a_txnId_commits_as_before()
    {
        // Written by an engine that records none: nothing to compare, so a time-only difference is no proof.
        await CreateAsync(Local, 1, 2);
        string original = TxnIdIn(2);
        File.WriteAllText(
            CommitFile(2), File.ReadAllText(CommitFile(2)).Replace($",\"{InCommitTimestamp.TxnIdKey}\":\"{original}\"", ""));
        await using var table = await DeltaTable.OpenAsync(Local, Options);
        Assert.Null(table.CurrentSnapshot.LogAnchor!.CommitTxnId);
        File.SetLastWriteTimeUtc(CommitFile(2), File.GetLastWriteTimeUtc(CommitFile(2)).AddDays(-1));

        Assert.Equal(3, await table.WriteAsync([Rows(3)]));
    }

    /// <summary>
    /// Without ETags (a local file system) the shorter log is still refused: it no longer reaches the base. A
    /// re-created commit at the base's version of a different size is refused too.
    /// </summary>
    [Fact]
    public async Task Shorter_recreated_log_is_refused_without_ETags()
    {
        await CreateAsync(Local, 1, 2, 3);
        await using var stale = await DeltaTable.OpenAsync(Local, Options);

        Drop();
        await CreateAsync(Local, 100);                                       // v1

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await stale.WriteAsync([Rows(9)]));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
        Assert.Equal([0L, 1], CommitVersions());                             // no hole at 2..3, no commit 4
    }

    [Fact]
    public async Task Overwrite_through_a_stale_handle_is_refused()
    {
        var fs = ContentTagged();
        await CreateAsync(fs, 1, 2);
        await using var stale = await DeltaTable.OpenAsync(fs, Options);

        Drop();
        await CreateAsync(fs, 100, 101, 102);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await stale.WriteAsync([Rows(9)], DeltaWriteMode.Overwrite));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
        Assert.Equal([0L, 1, 2, 3], CommitVersions());
    }

    [Fact]
    public async Task Touched_base_commit_does_not_fail_a_write()
    {
        // A different modification time is all a touched or copied-in-place file shows; not proof.
        await CreateAsync(Local, 1, 2);
        await using var table = await DeltaTable.OpenAsync(Local, Options);
        string commit = Path.Combine(_tempDir, "_delta_log", $"{table.CurrentSnapshot.Version:D20}.json");
        File.SetLastWriteTimeUtc(commit, File.GetLastWriteTimeUtc(commit).AddDays(-1));

        Assert.Equal(3, await table.WriteAsync([Rows(3)]));
        Assert.Equal([1L, 2, 3], await IdsAsync(table));
    }

    [Fact]
    public async Task Concurrent_commit_on_the_same_log_still_rebases()
    {
        var fs = ContentTagged();
        await CreateAsync(fs, 1);
        await using var a = await DeltaTable.OpenAsync(fs, Options);
        await using var b = await DeltaTable.OpenAsync(fs, Options);
        await a.WriteAsync([Rows(2)]);

        Assert.Equal(3, await b.WriteAsync([Rows(3)]));
        Assert.Equal([1L, 2, 3], await IdsAsync(b));
    }

    [Fact]
    public async Task Commit_appearing_beside_a_checkpoint_only_base_is_refused()
    {
        // The handle's base stands on a checkpoint alone (cleanup removed its commit). A new table that reaches that
        // version publishes a commit there, which the old log could never have done.
        await CreateAsync(Local, 1, 2);
        await using (var writer = await DeltaTable.OpenAsync(Local, Options))
            await writer.CheckpointAsync();                                  // checkpoint at v2
        string logDir = Path.Combine(_tempDir, "_delta_log");
        File.Delete(Path.Combine(logDir, $"{2L:D20}.json"));
        await using var stale = await DeltaTable.OpenAsync(Local, Options);
        Assert.False(stale.CurrentSnapshot.LogAnchor!.HasCommit);

        string other = Directory.CreateDirectory(Path.Combine(_tempDir, "other")).FullName;
        await CreateAsync(new LocalTableFileSystem(other), 100, 101, 102);
        foreach (string file in Directory.GetFiles(logDir))
        {
            if (!file.EndsWith(".checkpoint.parquet", StringComparison.Ordinal))
                File.Delete(file);
        }
        foreach (string file in Directory.GetFiles(Path.Combine(other, "_delta_log"), "*.json"))
            File.Move(file, Path.Combine(logDir, Path.GetFileName(file)));
        Directory.Delete(other, recursive: true);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await stale.WriteAsync([Rows(9)]));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
        Assert.Equal([0L, 1, 2, 3], CommitVersions());
    }

    [Fact]
    public async Task Same_named_file_under_the_log_is_not_the_base_commit()
    {
        // The local file system lists a prefix recursively, so the base commit's own listing also returns a file of the
        // same name in a directory under the log (#755 review). It is not the commit, and must not refuse the write.
        await CreateAsync(Local, 1, 2);
        await using var table = await DeltaTable.OpenAsync(Local, Options);
        string backup = Directory.CreateDirectory(Path.Combine(_tempDir, "_delta_log", "backup")).FullName;
        File.WriteAllText(Path.Combine(backup, $"{2L:D20}.json"), "{}");

        Assert.Equal(3, await table.WriteAsync([Rows(3)]));
        Assert.Equal([1L, 2, 3], await IdsAsync(table));
    }

    /// <summary>
    /// The table is re-created between the base commit's own listing, which found nothing, and the full listing
    /// that follows (#755 review). The full listing's commit at the base is checked too. The wrapper hides the base
    /// commit from the single-file listing, as a swap just after it would.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Table_recreated_between_the_two_listings_is_refused(bool baseHadCommit)
    {
        var tagged = ContentTagged();
        await CreateAsync(tagged, 1, 2);                                     // v2
        string logDir = Path.Combine(_tempDir, "_delta_log");
        if (!baseHadCommit)
        {
            await using (var writer = await DeltaTable.OpenAsync(tagged, Options))
                await writer.CheckpointAsync();
            File.Delete(Path.Combine(logDir, $"{2L:D20}.json"));
        }
        await using var stale = await DeltaTable.OpenAsync(new SingleFileListingHider(tagged), Options);
        Assert.Equal(baseHadCommit, stale.CurrentSnapshot.LogAnchor!.HasCommit);

        Drop();
        await CreateAsync(Local, 100, 101, 102);                             // v3, a new commit at v2

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await stale.WriteAsync([Rows(9)]));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
        Assert.Equal([0L, 1, 2, 3], CommitVersions());
    }

    /// <summary>Drops the table and re-creates it with five appends: a new log at v5.</summary>
    private async Task SwapInANewTableAsync()
    {
        Drop();
        await CreateAsync(Local, 100, 101, 102, 103, 104);
    }

    /// <summary>
    /// The table is re-created after the check, just before the first create (#755 review). The new log is longer, so
    /// the create collides, and the retry must check again rather than rebase into the new log.
    /// </summary>
    [Fact]
    public async Task Retry_after_a_collision_checks_the_log_again()
    {
        var swapper = new SwapAtFileSystem(ContentTagged());
        await CreateAsync(swapper, 1, 2);                                    // v2
        await using var stale = await DeltaTable.OpenAsync(swapper, Options);
        swapper.BeforeFirstCommitCreate = SwapInANewTableAsync;

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await stale.WriteAsync([Rows(9)]));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
        Assert.Equal([0L, 1, 2, 3, 4, 5], CommitVersions());
    }

    [Fact]
    public async Task Vacuum_start_retry_checks_the_log_again()
    {
        var swapper = new SwapAtFileSystem(ContentTagged());
        await CreateAsync(swapper, 1, 2);
        await using var stale = await DeltaTable.OpenAsync(swapper, Options);
        swapper.BeforeFirstCommitCreate = SwapInANewTableAsync;

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await stale.VacuumAsync(TimeSpan.FromDays(7)));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
        Assert.Equal([0L, 1, 2, 3, 4, 5], CommitVersions());
    }

    [Fact]
    public async Task Vacuum_end_checks_the_log_after_the_deletes()
    {
        // The table is re-created while VACUUM deletes, shorter than START's version, so END's first create would
        // find its version free and leave a hole in the new log. END is checked before its first attempt for this.
        var swapper = new SwapAtFileSystem(ContentTagged());
        await CreateAsync(swapper, 1, 2);
        string orphan = Path.Combine(_tempDir, "orphan.parquet");
        File.WriteAllBytes(orphan, [1, 2, 3]);
        File.SetLastWriteTimeUtc(orphan, DateTime.UtcNow.AddDays(-30));
        await using var stale = await DeltaTable.OpenAsync(swapper, Options);
        swapper.BeforeFirstDelete = async () =>
        {
            Drop();
            await CreateAsync(Local, 100);                                   // v1
        };

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await stale.VacuumAsync(TimeSpan.FromDays(7)));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
        Assert.Equal([0L, 1], CommitVersions());
    }

    [Fact]
    public async Task Commit_named_file_in_a_directory_under_the_log_is_not_a_version()
    {
        // A listing is recursive on the local file system (and S3). A file named like commit 5 in a directory under
        // the log is not one: the table is at v2, and opens, reads and commits there (#755 review).
        await CreateAsync(Local, 1, 2);
        string logDir = Path.Combine(_tempDir, "_delta_log");
        string backup = Directory.CreateDirectory(Path.Combine(logDir, "backup")).FullName;
        File.Copy(Path.Combine(logDir, $"{1L:D20}.json"), Path.Combine(backup, $"{5L:D20}.json"));

        await using var table = await DeltaTable.OpenAsync(Local, Options);
        Assert.Equal(2, table.CurrentSnapshot.Version);
        Assert.Equal(3, await table.WriteAsync([Rows(3)]));
        Assert.Equal([1L, 2, 3], await IdsAsync(table));
    }

    /// <summary>
    /// #755 review: VACUUM START collides with another writer's commit, and before its retry that writer checkpoints
    /// past it and log cleanup removes the commit that collided. The retry's next version is free again, below the
    /// checkpoint, where START would be invisible. It moves past the latest version instead, and END follows it.
    /// </summary>
    [Fact]
    public async Task Vacuum_start_retry_moves_past_a_checkpoint_that_cleaned_below_it()
    {
        var swapper = new SwapAtFileSystem(Local);
        await CreateAsync(swapper, 1, 2);                                    // v2
        await using var vacuuming = await DeltaTable.OpenAsync(swapper, Options);
        swapper.BeforeFirstCommitCreate = async () =>                        // START at v3 then collides
        {
            await using var other = await DeltaTable.OpenAsync(Local, Options);
            await other.WriteAsync([Rows(3)]);                               // v3
        };
        swapper.AfterFirstCommitCreate = async () =>
        {
            await using (var other = await DeltaTable.OpenAsync(Local, Options))
            {
                await other.WriteAsync([Rows(4)]);                           // v4
                await other.WriteAsync([Rows(5)]);                           // v5
                await other.CheckpointAsync();                               // checkpoint at v5
            }
            for (long v = 0; v <= 4; v++)                                    // log cleanup, below the checkpoint
                File.Delete(Path.Combine(_tempDir, "_delta_log", $"{v:D20}.json"));
        };

        await vacuuming.VacuumAsync(TimeSpan.FromDays(7));

        Assert.Equal([5L, 6, 7], CommitVersions());                          // START at v6, END at v7
        Assert.Contains("VACUUM START", File.ReadAllText(Path.Combine(_tempDir, "_delta_log", $"{6L:D20}.json")));
        await using var fresh = await DeltaTable.OpenAsync(Local, Options);
        Assert.Equal(7, fresh.CurrentSnapshot.Version);
        Assert.Equal([1L, 2, 3, 4, 5], await IdsAsync(fresh));
    }

    /// <summary>Runs a one-shot action just before the first commit create, or the first delete, once armed.</summary>
    private sealed class SwapAtFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        public Func<Task>? BeforeFirstCommitCreate { get; set; }

        public Func<Task>? BeforeFirstDelete { get; set; }

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        /// <summary>Runs once just after the first commit create returns, whatever it returned.</summary>
        public Func<Task>? AfterFirstCommitCreate { get; set; }

        public async ValueTask<bool> TryWriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            bool isCommit = DeltaVersion.TryParseCommitVersion(Path.GetFileName(path), out _);
            if (BeforeFirstCommitCreate is { } swap && isCommit)
            {
                BeforeFirstCommitCreate = null;
                await swap();
            }
            bool created = await inner.TryWriteAllBytesAsync(path, data, cancellationToken);
            if (AfterFirstCommitCreate is { } after && isCommit)
            {
                AfterFirstCommitCreate = null;
                await after();
            }
            return created;
        }

        public async ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
        {
            if (BeforeFirstDelete is { } swap)
            {
                BeforeFirstDelete = null;
                await swap();
            }
            await inner.DeleteAsync(path, cancellationToken);
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
            => inner.OpenReadAsync(path, cancellationToken);

        public ValueTask<ISequentialFile> CreateAsync(
            string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
            => inner.ReadAllBytesAsync(path, cancellationToken);

        public ValueTask WriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);
    }

    /// <summary>Lists nothing for a prefix that is one commit file's path; everything else passes through.</summary>
    private sealed class SingleFileListingHider(ITableFileSystem inner) : ITableFileSystem
    {
        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async IAsyncEnumerable<TableFileInfo> ListAsync(
            string prefix,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (DeltaVersion.TryParseCommitVersion(Path.GetFileName(prefix), out _))
                yield break;
            await foreach (var file in inner.ListAsync(prefix, cancellationToken).ConfigureAwait(false))
                yield return file;
        }

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
            => inner.OpenReadAsync(path, cancellationToken);

        public ValueTask<ISequentialFile> CreateAsync(
            string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
            => inner.ReadAllBytesAsync(path, cancellationToken);

        public ValueTask<bool> TryWriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);

        public ValueTask WriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);
    }

    [Fact]
    public async Task Checkpoint_only_base_still_commits()
    {
        // The same base with nothing replaced: no commit at v2, and nothing after it.
        await CreateAsync(Local, 1, 2);
        await using (var writer = await DeltaTable.OpenAsync(Local, Options))
            await writer.CheckpointAsync();
        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{2L:D20}.json"));
        await using var table = await DeltaTable.OpenAsync(Local, Options);

        Assert.Equal(3, await table.WriteAsync([Rows(3)]));
        Assert.Equal([1L, 2, 3], await IdsAsync(table));
    }
}
