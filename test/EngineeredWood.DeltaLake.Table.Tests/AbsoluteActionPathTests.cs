// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <c>add</c>, <c>remove</c> and <c>cdc</c> paths that are absolute locations (PROTOCOL.md: "a relative path ... or an
/// absolute path"), as a shallow clone writes them: read through <see cref="DeltaTableOptions.StorageResolver"/>
/// outside the table and through the table's own file system inside it, written only under the table, and never
/// deleted by VACUUM outside it (#963).
/// </summary>
public sealed class AbsoluteActionPathTests : IDisposable
{
    private static readonly Apache.Arrow.Schema IdSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, nullable: false))
        .Build();

    private readonly string _root = Path.Combine(Path.GetTempPath(), $"ew_abs_paths_{Guid.NewGuid():N}");
    private readonly string _sourceDir;
    private readonly string _cloneDir;

    public AbsoluteActionPathTests()
    {
        _sourceDir = Path.Combine(_root, "source");
        _cloneDir = Path.Combine(_root, "clone");
        Directory.CreateDirectory(_sourceDir);
        Directory.CreateDirectory(_cloneDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }

    [Fact]
    public async Task Read_AbsolutePathsOutsideTheTable_GoThroughTheResolver()
    {
        await WriteSourceAsync([1, 2, 3], [4, 5, 6]);
        CloneLog(_sourceDir, _cloneDir);

        await using (var unregistered = await DeltaTable.OpenAsync(new LocalTableFileSystem(_cloneDir)))
            await Assert.ThrowsAsync<UnregisteredStorageLocationException>(async () => await ReadIdsAsync(unregistered));

        await using var clone = await OpenCloneAsync();
        Assert.Equal([1, 2, 3, 4, 5, 6], await ReadIdsAsync(clone));
        Assert.All(clone.CurrentSnapshot.ActiveFiles.Keys, p => Assert.StartsWith("file:", p));
    }

    [Fact]
    public async Task Read_AbsolutePathInsideTheTable_NeedsNoRegistration_AndVacuumKeepsIt()
    {
        await WriteSourceAsync([1, 2, 3], [4, 5, 6]);
        // The table's own files named by absolute location: no resolver reaches them, the table's file system does.
        RewriteLogInPlace(_sourceDir, rel => FileUri(_sourceDir) + "/" + rel);

        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_sourceDir));
        Assert.Equal([1, 2, 3, 4, 5, 6], await ReadIdsAsync(table));

        // The keep-set holds them by their path under the root, so a zero-retention vacuum deletes neither.
        var result = await table.VacuumAsync(TimeSpan.Zero);
        Assert.Empty(result.FilesToDelete);
        Assert.Empty(result.FilesOutsideTable);
        Assert.Equal([1, 2, 3, 4, 5, 6], await ReadIdsAsync(table));
    }

    [Fact]
    public async Task Read_AbsoluteUri_IsPercentDecodedOnce()
    {
        // A partition value with a space and a percent: its directory name is Hive-escaped on disk, and add.path is the
        // URL-encoded form of that name. The absolute URI carries the same encoding, so decoding it once finds the file,
        // and decoding it twice (or not at all) would not.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, nullable: false))
            .Field(new Field("p", StringType.Default, nullable: true))
            .Build();
        await using (var source = await DeltaTable.CreateAsync(
                         new LocalTableFileSystem(_sourceDir), schema, partitionColumns: ["p"]))
        {
            await source.WriteAsync([new RecordBatch(schema,
            [
                new Int64Array.Builder().Append(1).Append(2).Build(),
                new StringArray.Builder().Append("a b%c").Append("a b%c").Build(),
            ], 2)]);
        }
        CloneLog(_sourceDir, _cloneDir);

        await using var clone = await OpenCloneAsync();
        Assert.Contains(clone.CurrentSnapshot.ActiveFiles.Keys, p => p.Contains("%25"));
        Assert.Equal([1, 2], await ReadIdsAsync(clone));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Read_RootedPathWithoutScheme_IsRefused(bool encodedSlash)
    {
        await WriteSourceAsync([1, 2, 3]);
        // A bare rooted path (what Path.GetFullPath gives, or its encoded spelling): Spark would open it on its default
        // file system, delta-kernel-rs under the table's bucket. Not read here.
        string bare = Path.GetFullPath(_sourceDir).Replace('\\', '/');
        CloneLog(_sourceDir, _cloneDir, rel => (encodedSlash && bare.StartsWith("/", StringComparison.Ordinal)
            ? "%2F" + bare.Substring(1)
            : bare) + "/" + rel);

        await using var clone = await DeltaTable.OpenAsync(new LocalTableFileSystem(_cloneDir));
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await ReadIdsAsync(clone));
        Assert.Equal(DeltaErrorCodes.InvalidFilePath, ex.ErrorCode);
    }

    [Fact]
    public async Task Delete_RewritesUnderTheClone_AndVacuumLeavesTheSourceAlone()
    {
        await WriteSourceAsync([1, 2, 3], [4, 5, 6]);
        CloneLog(_sourceDir, _cloneDir);
        var sourceFiles = DataFiles(_sourceDir);

        await using var clone = await OpenCloneAsync();
        var (deleted, _) = await clone.DeleteAsync(IdEquals(2), RowDeleteMode.CopyOnWrite);
        Assert.Equal(1, deleted);

        // The rewrite of a source file is the clone's: written under the clone, named relative to it.
        var added = Assert.Single(clone.CurrentSnapshot.ActiveFiles.Keys, p => !p.StartsWith("file:", StringComparison.Ordinal));
        Assert.True(File.Exists(Path.Combine(_cloneDir, added)), added);
        Assert.Equal([1, 3, 4, 5, 6], await ReadIdsAsync(clone));

        await Task.Delay(20);
        var result = await clone.VacuumAsync(TimeSpan.Zero);

        // Nothing outside the clone is touched, and the source file the clone stopped referencing is reported.
        Assert.Equal(sourceFiles, DataFiles(_sourceDir));
        string removed = Assert.Single(result.FilesOutsideTable);
        Assert.StartsWith(FileUri(_sourceDir), removed);
        Assert.DoesNotContain(result.FilesToDelete, p => p.Contains("source"));
        Assert.Equal([1, 3, 4, 5, 6], await ReadIdsAsync(clone));

        // The source table is unaffected.
        await using var source = await DeltaTable.OpenAsync(new LocalTableFileSystem(_sourceDir));
        Assert.Equal([1, 2, 3, 4, 5, 6], await ReadIdsAsync(source));
    }

    /// <summary>
    /// A deletion-vector DELETE on a source file removes it and adds it back with a vector, under one path: the file is
    /// still live, so it is not reported as left behind (the snapshot keys files by path AND vector).
    /// </summary>
    [Fact]
    public async Task Vacuum_DoesNotReportAFileAVectorDeleteKeptLive()
    {
        await using (var source = await DeltaTable.CreateAsync(
                         new LocalTableFileSystem(_sourceDir), IdSchema, enableDeletionVectors: true))
        {
            await source.WriteAsync([Ids(1, 2, 3)]);
        }
        CloneLog(_sourceDir, _cloneDir);

        await using var clone = await OpenCloneAsync();
        await clone.DeleteAsync(IdEquals(2), RowDeleteMode.DeletionVector);
        Assert.Contains(clone.CurrentSnapshot.Tombstones.Values, t => t.Path.StartsWith("file:", StringComparison.Ordinal));

        await Task.Delay(20);
        var result = await clone.VacuumAsync(TimeSpan.Zero);
        Assert.Empty(result.FilesOutsideTable);
        Assert.Equal([1, 3], await ReadIdsAsync(clone));
    }

    /// <summary>
    /// A <c>remove</c> without a <c>deletionTimestamp</c> (the field is optional) is not known to be past retention, so
    /// the file it names is not reported, as checkpoint expiry keeps such a tombstone too.
    /// </summary>
    [Fact]
    public async Task Vacuum_DoesNotReportARemoveWithoutADeletionTime()
    {
        await WriteSourceAsync([1, 2, 3], [4, 5, 6]);
        CloneLog(_sourceDir, _cloneDir);

        var cloneFs = new LocalTableFileSystem(_cloneDir);
        await using var clone = await OpenCloneAsync();
        var gone = clone.CurrentSnapshot.ActiveFiles.Values.First();
        await new TransactionLog(cloneFs).WriteCommitAsync(clone.CurrentSnapshot.Version + 1, new DeltaAction[]
        {
            new RemoveFile { Path = gone.Path, DataChange = true },
        });
        await clone.RefreshAsync();
        Assert.Contains(clone.CurrentSnapshot.Tombstones.Values, t => t.Path == gone.Path && t.DeletionTimestamp is null);

        var result = await clone.VacuumAsync(TimeSpan.Zero);
        Assert.Empty(result.FilesOutsideTable);
    }

    /// <summary>
    /// On Windows a URI naming <c>PART.parquet</c> opens the file listed as <c>part.parquet</c>: VACUUM's keep-set and the
    /// discard guard compare as that file system does, or they would delete a live file.
    /// </summary>
    [Fact]
    public async Task Windows_AbsolutePathInADifferentCase_IsStillTheLiveFile()
    {
        if (Path.DirectorySeparatorChar != '\\')
            return; // only a case-insensitive file system has two spellings of one file

        await WriteSourceAsync([1, 2, 3]);
        RewriteLogInPlace(_sourceDir, rel => FileUri(_sourceDir) + "/" + rel.ToUpperInvariant());

        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_sourceDir));
        Assert.Equal([1, 2, 3], await ReadIdsAsync(table));
        string onDisk = Assert.Single(DataFiles(_sourceDir));
        Assert.NotEqual(onDisk, onDisk.ToUpperInvariant());

        await Assert.ThrowsAsync<ArgumentException>(async () => await table.DiscardDataFilesAsync(
            [new WrittenDataFile(onDisk, 1, 3, null, null)]));

        var result = await table.VacuumAsync(TimeSpan.Zero);
        Assert.Empty(result.FilesToDelete);
        Assert.Equal([1, 2, 3], await ReadIdsAsync(table));
    }

    [Fact]
    public async Task Vacuum_RefusesWhenTheTableLocationIsUnknown()
    {
        await WriteSourceAsync([1, 2, 3]);
        CloneLog(_sourceDir, _cloneDir);

        // A wrapper that does not say where it is rooted: an absolute path cannot be placed inside or outside it.
        await using var clone = await DeltaTable.OpenAsync(
            new UnlocatedFileSystem(new LocalTableFileSystem(_cloneDir)),
            new DeltaTableOptions { StorageResolver = new StorageResolver().Register(new LocalTableFileSystem(_sourceDir)) });
        Assert.Equal([1, 2, 3], await ReadIdsAsync(clone));
        await Assert.ThrowsAsync<NotSupportedException>(async () => await clone.VacuumAsync(TimeSpan.Zero, dryRun: true));
    }

    [Fact]
    public async Task Compact_WritesUnderTheClone()
    {
        await WriteSourceAsync([1, 2], [3, 4], [5, 6]);
        CloneLog(_sourceDir, _cloneDir);
        var sourceFiles = DataFiles(_sourceDir);

        await using var clone = await OpenCloneAsync();
        Assert.NotNull(await clone.CompactAsync());

        var active = Assert.Single(clone.CurrentSnapshot.ActiveFiles.Keys);
        Assert.False(DeltaFileLocator.IsAbsolute(active), active);
        Assert.True(File.Exists(Path.Combine(_cloneDir, active)), active);
        Assert.Equal([1, 2, 3, 4, 5, 6], await ReadIdsAsync(clone));
        Assert.Equal(sourceFiles, DataFiles(_sourceDir));
    }

    [Fact]
    public async Task ChangeFeed_ReadsAbsoluteAddsAndChangeFiles()
    {
        var cdf = new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" };
        await using (var source = await DeltaTable.CreateAsync(
                         new LocalTableFileSystem(_sourceDir), IdSchema, configuration: cdf))
        {
            await source.WriteAsync([Ids(1, 2, 3)]);                                       // v1: inserts from the add
            await source.DeleteAsync(IdEquals(2), RowDeleteMode.CopyOnWrite);             // v2: a cdc file
        }
        CloneLog(_sourceDir, _cloneDir);

        await using var clone = await OpenCloneAsync();
        var changes = new List<(long Id, string Type)>();
        await foreach (var batch in clone.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 1, EndVersion = 2 }))
        {
            var ids = (Int64Array)batch.Column("id");
            var types = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                changes.Add((ids.GetValue(i)!.Value, types.GetString(i)));
        }

        Assert.Contains((1L, "insert"), changes);
        Assert.Contains((2L, "delete"), changes);
    }

    [Fact]
    public async Task Restore_ChecksAbsoluteFilesWhereTheyAreRead()
    {
        await WriteSourceAsync([1, 2, 3]);
        CloneLog(_sourceDir, _cloneDir);

        await using var clone = await OpenCloneAsync();
        await clone.DeleteAsync(IdEquals(1), RowDeleteMode.CopyOnWrite);
        await clone.RestoreAsync(1);
        Assert.Equal([1, 2, 3], await ReadIdsAsync(clone));

        // Gone from the source: the restore says so rather than re-add a file that cannot be read.
        await clone.DeleteAsync(IdEquals(1), RowDeleteMode.CopyOnWrite);
        foreach (var f in DataFiles(_sourceDir))
            File.Delete(Path.Combine(_sourceDir, f));
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await clone.RestoreAsync(1));
        Assert.Contains(FileUri(_sourceDir), ex.Message);
    }

    // ── helpers ──

    private async Task WriteSourceAsync(params long[][] commits)
    {
        await using var source = await DeltaTable.CreateAsync(new LocalTableFileSystem(_sourceDir), IdSchema);
        foreach (var ids in commits)
            await source.WriteAsync([Ids(ids)]);
    }

    private ValueTask<DeltaTable> OpenCloneAsync() =>
        DeltaTable.OpenAsync(
            new LocalTableFileSystem(_cloneDir),
            new DeltaTableOptions { StorageResolver = new StorageResolver().Register(new LocalTableFileSystem(_sourceDir)) });

    private static RecordBatch Ids(params long[] ids)
    {
        var b = new Int64Array.Builder();
        foreach (long id in ids)
            b.Append(id);
        return new RecordBatch(IdSchema, [b.Build()], ids.Length);
    }

    private static Func<RecordBatch, BooleanArray> IdEquals(long id) => batch =>
    {
        var ids = (Int64Array)batch.Column("id");
        var b = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            b.Append(ids.GetValue(i) == id);
        return b.Build();
    };

    private static async Task<List<long>> ReadIdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var column = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    /// <summary>The table's data files, relative, sorted: everything outside <c>_delta_log</c>.</summary>
    private static List<string> DataFiles(string dir) =>
        Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories)
            .Select(f => Path.GetFullPath(f).Substring(Path.GetFullPath(dir).TrimEnd('\\', '/').Length + 1).Replace('\\', '/'))
            .Where(f => !f.StartsWith("_delta_log/", StringComparison.Ordinal))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();

    /// <summary>The directory as an RFC 8089 file URI (<c>file:///C:/x</c>, <c>file:///tmp/x</c>).</summary>
    private static string FileUri(string dir) =>
        "file:///" + Path.GetFullPath(dir).Replace('\\', '/').TrimStart('/');

    /// <summary>
    /// A shallow clone of <paramref name="sourceDir"/> at <paramref name="cloneDir"/>: its commits, with every relative
    /// <c>add</c>, <c>remove</c> and <c>cdc</c> path replaced by <paramref name="rewrite"/> (by default the source
    /// file's absolute URI), as delta-spark's SHALLOW CLONE writes them. Version checksums are left behind: they are
    /// optional, and would describe the source.
    /// </summary>
    private static void CloneLog(string sourceDir, string cloneDir, Func<string, string>? rewrite = null)
    {
        rewrite ??= rel => FileUri(sourceDir) + "/" + rel;
        string target = Path.Combine(cloneDir, "_delta_log");
        Directory.CreateDirectory(target);
        foreach (string commit in Directory.EnumerateFiles(Path.Combine(sourceDir, "_delta_log"), "*.json"))
            File.WriteAllLines(Path.Combine(target, Path.GetFileName(commit)), RewritePaths(File.ReadAllLines(commit), rewrite));
    }

    private static void RewriteLogInPlace(string tableDir, Func<string, string> rewrite)
    {
        string log = Path.Combine(tableDir, "_delta_log");
        foreach (string crc in Directory.EnumerateFiles(log, "*.crc"))
            File.Delete(crc);
        foreach (string commit in Directory.EnumerateFiles(log, "*.json"))
            File.WriteAllLines(commit, RewritePaths(File.ReadAllLines(commit), rewrite));
    }

    private static IEnumerable<string> RewritePaths(IEnumerable<string> lines, Func<string, string> rewrite)
    {
        foreach (string line in lines)
        {
            if (line.Length == 0)
                continue;
            var action = JsonNode.Parse(line)!.AsObject();
            foreach (string kind in new[] { "add", "remove", "cdc" })
            {
                if (action[kind] is JsonObject file && file["path"]?.GetValue<string>() is { } path
                    && !DeltaFileLocator.IsAbsolute(path))
                {
                    file["path"] = rewrite(path);
                }
            }
            yield return action.ToJsonString();
        }
    }

    /// <summary>Forwards to another file system without saying where it is rooted.</summary>
    private sealed class UnlocatedFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default) =>
            inner.OpenReadAsync(path, cancellationToken);

        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default) =>
            inner.CreateAsync(path, overwrite, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
            inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
            inner.ExistsAsync(path, cancellationToken);

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default) =>
            inner.ReadAllBytesAsync(path, cancellationToken);

        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            inner.TryWriteAllBytesAsync(path, data, cancellationToken);

        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            inner.WriteAllBytesAsync(path, data, cancellationToken);
    }
}
