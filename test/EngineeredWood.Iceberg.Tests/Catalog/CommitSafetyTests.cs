// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Iceberg.Expressions;
using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Iceberg.Tests.Catalog;

/// <summary>
/// Optimistic concurrency for catalog commits (#953 item 1): a commit based on a stale version fails rather
/// than overwriting the winner, retries re-apply the change to the newer metadata, and a table whose metadata
/// names a missing file is refused rather than read as smaller.
/// </summary>
public class CommitSafetyTests : IDisposable
{
    private readonly string _warehouse;
    private readonly LocalTableFileSystem _fs;
    private readonly Namespace _ns = Namespace.Of("db");
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "data", IcebergType.String, false),
    ]);

    // Retry fast and often enough that a burst of concurrent writers all get in.
    private static readonly CommitRetryOptions EagerRetry =
        new(100, TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(20), TimeSpan.FromMinutes(2));

    public CommitSafetyTests()
    {
        _warehouse = Path.Combine(Path.GetTempPath(), $"iceberg-commit-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_warehouse);
        _fs = new LocalTableFileSystem(_warehouse);
    }

    public void Dispose()
    {
        if (Directory.Exists(_warehouse))
            Directory.Delete(_warehouse, recursive: true);
    }

    private static DataFile File(string name, long rows = 10) => new()
    {
        FilePath = $"db/t/data/{name}.parquet",
        RecordCount = rows,
        FileSizeInBytes = 1000,
    };

    private static TableMetadata WithProperty(TableMetadata m, string key, string value)
    {
        var props = m.Properties.ToDictionary(kv => kv.Key, kv => kv.Value);
        props[key] = value;
        return m with { Properties = props };
    }

    private async Task<(FileSystemCatalog Catalog, TableIdentifier Id, Table Table)> CreateTableAsync()
    {
        var catalog = new FileSystemCatalog(_fs);
        await catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "t");
        var table = await catalog.CreateTableAsync(id, _schema);
        return (catalog, id, table);
    }

    private string MetadataDir => Path.Combine(_warehouse, "db", "t", "metadata");

    // --- FileSystemCatalog ---

    [Fact]
    public async Task StaleCommit_FailsAndLeavesTheWinnersVersion()
    {
        var (writerA, id, _) = await CreateTableAsync();
        var writerB = new FileSystemCatalog(new LocalTableFileSystem(_warehouse));

        var baseA = await writerA.LoadTableAsync(id);
        var baseB = await writerB.LoadTableAsync(id);

        await writerA.CommitTableAsync(baseA, WithProperty(baseA.Metadata, "writer", "a"));
        var ex = await Assert.ThrowsAsync<CommitFailedException>(
            () => writerB.CommitTableAsync(baseB, WithProperty(baseB.Metadata, "writer", "b")));
        Assert.Equal(id, ex.Identifier);

        var loaded = await writerB.LoadTableAsync(id);
        Assert.Equal("a", loaded.Metadata.Properties["writer"]);
        Assert.EndsWith("/v2.metadata.json", loaded.MetadataLocation);
        Assert.False(System.IO.File.Exists(Path.Combine(MetadataDir, "v3.metadata.json")));
    }

    [Fact]
    public async Task SimultaneousCommits_ExactlyOneWins()
    {
        var (catalog, id, _) = await CreateTableAsync();
        const int writers = 8;
        var bases = new List<Table>();
        for (var i = 0; i < writers; i++)
            bases.Add(await new FileSystemCatalog(new LocalTableFileSystem(_warehouse)).LoadTableAsync(id));

        using var start = new ManualResetEventSlim(false);
        var attempts = bases.Select((b, i) => Task.Run(async () =>
        {
            start.Wait();
            try
            {
                await catalog.CommitTableAsync(b, WithProperty(b.Metadata, "writer", i.ToString()));
                return true;
            }
            catch (CommitFailedException)
            {
                return false;
            }
        })).ToList();
        start.Set();
        var results = await Task.WhenAll(attempts);

        Assert.Equal(1, results.Count(r => r));
        var loaded = await catalog.LoadTableAsync(id);
        Assert.EndsWith("/v2.metadata.json", loaded.MetadataLocation);
        Assert.Equal(results.ToList().IndexOf(true).ToString(), loaded.Metadata.Properties["writer"]);
    }

    [Fact]
    public async Task ConcurrentAppendsWithRetry_KeepEveryFile()
    {
        var (catalog, id, _) = await CreateTableAsync();
        const int writers = 8;

        var appends = Enumerable.Range(0, writers).Select(i => Task.Run(async () =>
        {
            var fs = new LocalTableFileSystem(_warehouse);
            var ops = new TableOperations(fs);
            await new FileSystemCatalog(fs).CommitWithRetryAsync(
                id,
                (metadata, ct) => ops.AppendFilesAsync(metadata, [File($"f{i}")], ct),
                EagerRetry);
        })).ToList();
        await Task.WhenAll(appends);

        var table = await catalog.LoadTableAsync(id);
        Assert.Equal(writers, table.Metadata.Snapshots.Count);
        Assert.Equal(writers, table.Metadata.LastSequenceNumber);

        // One linear history: every snapshot's parent is the one before it.
        var bySequence = table.Metadata.Snapshots.OrderBy(s => s.SequenceNumber).ToList();
        Assert.Null(bySequence[0].ParentSnapshotId);
        for (var i = 1; i < bySequence.Count; i++)
            Assert.Equal(bySequence[i - 1].SnapshotId, bySequence[i].ParentSnapshotId);
        Assert.Equal(bySequence[^1].SnapshotId, table.Metadata.CurrentSnapshotId);

        var files = await new TableOperations(_fs).ListDataFilesAsync(table.Metadata);
        Assert.Equal(
            Enumerable.Range(0, writers).Select(i => $"db/t/data/f{i}.parquet").OrderBy(p => p),
            files.Select(f => f.FilePath).OrderBy(p => p));
        Assert.Equal((10L * writers).ToString(), bySequence[^1].Summary["total-records"]);
    }

    [Fact]
    public async Task CommitWithRetry_GivesUpAfterTheRetryLimit()
    {
        var (catalog, id, _) = await CreateTableAsync();
        var interloper = new FileSystemCatalog(new LocalTableFileSystem(_warehouse));
        var calls = 0;

        // Every attempt loses: another writer commits between our load and our commit.
        await Assert.ThrowsAsync<CommitFailedException>(() => catalog.CommitWithRetryAsync(
            id,
            async (metadata, ct) =>
            {
                calls++;
                var current = await interloper.LoadTableAsync(id, ct);
                await interloper.CommitTableAsync(current, WithProperty(current.Metadata, "n", calls.ToString()), ct);
                return WithProperty(metadata, "mine", "yes");
            },
            new CommitRetryOptions(2, TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(1), TimeSpan.FromMinutes(1))));

        Assert.Equal(3, calls);
        Assert.False((await catalog.LoadTableAsync(id)).Metadata.Properties.ContainsKey("mine"));
    }

    [Fact]
    public async Task CommitWithRetry_DoesNotRetryAValidationFailure()
    {
        var (catalog, id, _) = await CreateTableAsync();
        var ops = new TableOperations(_fs);
        var calls = 0;

        await Assert.ThrowsAsync<CommitValidationException>(() => catalog.CommitWithRetryAsync(
            id,
            (metadata, ct) =>
            {
                calls++;
                return ops.OverwriteFilesAsync(metadata, [File("never-added")], [File("new")], ct);
            },
            EagerRetry));

        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task CommitWithRetry_ReadsRetryLimitsFromTableProperties()
    {
        var options = CommitRetryOptions.FromProperties(new Dictionary<string, string>
        {
            ["commit.retry.num-retries"] = "7",
            ["commit.retry.min-wait-ms"] = "5",
            ["commit.retry.max-wait-ms"] = "not a number",
        });

        Assert.Equal(7, options.NumRetries);
        Assert.Equal(TimeSpan.FromMilliseconds(5), options.MinWait);
        Assert.Equal(CommitRetryOptions.Default.MaxWait, options.MaxWait);
        Assert.Equal(CommitRetryOptions.Default.TotalTimeout, options.TotalTimeout);
    }

    [Theory]
    [InlineData("1")]        // stale: two writers' hint updates landed out of order
    [InlineData(null)]       // missing: the writer failed between the commit and the hint
    [InlineData("garbage")]  // unreadable
    [InlineData("9")]        // ahead of any version file
    public async Task VersionHint_IsOnlyAHint(string? hint)
    {
        var (catalog, id, table) = await CreateTableAsync();
        var v2 = await catalog.CommitTableAsync(table, WithProperty(table.Metadata, "v", "2"));
        var v3 = await catalog.CommitTableAsync(v2, WithProperty(v2.Metadata, "v", "3"));

        var hintPath = Path.Combine(MetadataDir, "version-hint.text");
        if (hint is null)
            System.IO.File.Delete(hintPath);
        else
            System.IO.File.WriteAllText(hintPath, hint);

        var fresh = new FileSystemCatalog(new LocalTableFileSystem(_warehouse));
        Assert.True(await fresh.TableExistsAsync(id));
        Assert.Contains(id, await fresh.ListTablesAsync(_ns));

        var loaded = await fresh.LoadTableAsync(id);
        Assert.Equal("3", loaded.Metadata.Properties["v"]);
        Assert.Equal(v3.MetadataLocation, loaded.MetadataLocation);

        // A commit based on a stale view still fails, whatever the hint says...
        await Assert.ThrowsAsync<CommitFailedException>(
            () => fresh.CommitTableAsync(v2, WithProperty(v2.Metadata, "v", "stale")));

        // ...and one based on the current version lands as v4.
        var v4 = await fresh.CommitTableAsync(loaded, WithProperty(loaded.Metadata, "v", "4"));
        Assert.EndsWith("/v4.metadata.json", v4.MetadataLocation);
        Assert.Equal("4", System.IO.File.ReadAllText(hintPath));
    }

    [Fact]
    public async Task Commit_RecordsThePreviousVersionInTheMetadataLog()
    {
        var (catalog, id, table) = await CreateTableAsync();
        var v2 = await catalog.CommitTableAsync(table, WithProperty(table.Metadata, "write.metadata.previous-versions-max", "2"));
        var v3 = await catalog.CommitTableAsync(v2, v2.Metadata);
        var v4 = await catalog.CommitTableAsync(v3, v3.Metadata);

        var log = (await catalog.LoadTableAsync(id)).Metadata.MetadataLog;
        Assert.Equal([v2.MetadataLocation, v3.MetadataLocation], log.Select(e => e.MetadataFile));
        Assert.Equal(v3.Metadata.LastUpdatedMs, log[^1].TimestampMs);
        Assert.EndsWith("/v4.metadata.json", v4.MetadataLocation);
    }

    [Fact]
    public async Task Commit_RefusesABaseFromElsewhereOrAnotherTablesMetadata()
    {
        var (catalog, id, table) = await CreateTableAsync();

        await Assert.ThrowsAsync<ArgumentException>(
            () => catalog.CommitTableAsync(new Table(id, table.Metadata), table.Metadata));
        await Assert.ThrowsAsync<ArgumentException>(
            () => catalog.CommitTableAsync(table, table.Metadata with { TableUuid = Guid.NewGuid().ToString() }));
    }

    [Fact]
    public async Task Commit_ToADroppedTable_Throws()
    {
        var (catalog, id, table) = await CreateTableAsync();
        await catalog.DropTableAsync(id);

        await Assert.ThrowsAsync<NoSuchTableException>(() => catalog.CommitTableAsync(table, table.Metadata));
    }

    [Fact]
    public async Task SimultaneousCreates_ExactlyOneWins()
    {
        var catalog = new FileSystemCatalog(_fs);
        await catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "t");

        using var start = new ManualResetEventSlim(false);
        var creates = Enumerable.Range(0, 8).Select(i => Task.Run(async () =>
        {
            start.Wait();
            try
            {
                await new FileSystemCatalog(new LocalTableFileSystem(_warehouse)).CreateTableAsync(
                    id, _schema, properties: new Dictionary<string, string> { ["creator"] = i.ToString() });
                return true;
            }
            catch (TableAlreadyExistsException)
            {
                return false;
            }
        })).ToList();
        start.Set();
        var results = await Task.WhenAll(creates);

        Assert.Equal(1, results.Count(r => r));
        var loaded = await catalog.LoadTableAsync(id);
        Assert.Equal(results.ToList().IndexOf(true).ToString(), loaded.Metadata.Properties["creator"]);
    }

    [Fact]
    public async Task Drop_RemovesTheMetadataSoTheTableCanBeRecreated()
    {
        var (catalog, id, table) = await CreateTableAsync();
        await catalog.CommitTableAsync(table, WithProperty(table.Metadata, "old", "yes"));
        var dataFile = Path.Combine(_warehouse, "db", "t", "data", "keep.parquet");
        Directory.CreateDirectory(Path.GetDirectoryName(dataFile)!);
        System.IO.File.WriteAllText(dataFile, "data");

        await catalog.DropTableAsync(id);

        Assert.False(await catalog.TableExistsAsync(id));
        Assert.Empty(Directory.EnumerateFiles(MetadataDir));
        Assert.True(System.IO.File.Exists(dataFile)); // without purge, data stays

        var recreated = await catalog.CreateTableAsync(id, _schema);
        Assert.EndsWith("/v1.metadata.json", recreated.MetadataLocation);
        Assert.False((await catalog.LoadTableAsync(id)).Metadata.Properties.ContainsKey("old"));

        await catalog.DropTableAsync(id, purge: true);
        Assert.False(System.IO.File.Exists(dataFile));
    }

    [Fact]
    public async Task ListTables_IgnoresChildNamespacesAndPartialWrites()
    {
        var (catalog, id, _) = await CreateTableAsync();
        var child = Namespace.Of("db", "sub");
        await catalog.CreateNamespaceAsync(child);
        await catalog.CreateTableAsync(new TableIdentifier(child, "inner"), _schema);

        // An abandoned temporary file from a create-if-absent must not make a table appear.
        var orphanDir = Path.Combine(_warehouse, "db", "ghost", "metadata");
        Directory.CreateDirectory(orphanDir);
        System.IO.File.WriteAllText(Path.Combine(orphanDir, "v1.metadata.json.tmp.0123"), "{}");

        Assert.Equal([id], await catalog.ListTablesAsync(_ns));
        Assert.False(await catalog.TableExistsAsync(new TableIdentifier(_ns, "ghost")));
        await Assert.ThrowsAsync<NamespaceNotEmptyException>(() => catalog.DropNamespaceAsync(child));
    }

    [Fact]
    public async Task Rename_MovesTheMetadataAndFreesTheOldName()
    {
        var (catalog, id, table) = await CreateTableAsync();
        await catalog.CommitTableAsync(table, WithProperty(table.Metadata, "k", "v"));
        var to = new TableIdentifier(_ns, "renamed");

        await catalog.RenameTableAsync(id, to);

        Assert.False(await catalog.TableExistsAsync(id));
        var moved = await catalog.LoadTableAsync(to);
        Assert.Equal("v", moved.Metadata.Properties["k"]);
        Assert.Equal("db/renamed", moved.Location);
        Assert.Empty(moved.Metadata.MetadataLog);
        Assert.EndsWith("/v1.metadata.json", moved.MetadataLocation);
    }

    // --- InMemoryCatalog ---

    [Fact]
    public async Task InMemory_StaleCommitFails()
    {
        var catalog = new InMemoryCatalog();
        await catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "t");
        var created = await catalog.CreateTableAsync(id, _schema);

        var a = await catalog.LoadTableAsync(id);
        var b = await catalog.LoadTableAsync(id);
        var committed = await catalog.CommitTableAsync(a, WithProperty(a.Metadata, "w", "a"));
        await Assert.ThrowsAsync<CommitFailedException>(
            () => catalog.CommitTableAsync(b, WithProperty(b.Metadata, "w", "b")));
        await Assert.ThrowsAsync<CommitFailedException>(() => catalog.CommitTableAsync(created, created.Metadata));

        // The committed table is the base for the next commit.
        await catalog.CommitTableAsync(committed, WithProperty(committed.Metadata, "w", "a2"));
        Assert.Equal("a2", (await catalog.LoadTableAsync(id)).Metadata.Properties["w"]);
    }

    [Fact]
    public async Task InMemory_RecreatedTableRejectsACommitBasedOnTheOldOne()
    {
        var catalog = new InMemoryCatalog();
        await catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "t");
        var old = await catalog.CreateTableAsync(id, _schema);
        await catalog.DropTableAsync(id);
        await catalog.CreateTableAsync(id, _schema);

        await Assert.ThrowsAsync<CommitFailedException>(() => catalog.CommitTableAsync(old, old.Metadata));
    }

    [Fact]
    public async Task RecreatedTable_RejectsACommitBasedOnTheOldOne()
    {
        var (catalog, id, old) = await CreateTableAsync();
        await catalog.DropTableAsync(id);
        var recreated = await catalog.CreateTableAsync(id, _schema);
        Assert.Equal(old.MetadataLocation, recreated.MetadataLocation); // both v1

        await Assert.ThrowsAsync<CommitFailedException>(
            () => catalog.CommitTableAsync(old, WithProperty(old.Metadata, "stale", "yes")));
        Assert.False((await catalog.LoadTableAsync(id)).Metadata.Properties.ContainsKey("stale"));
    }

    // --- Missing files ---

    [Fact]
    public async Task MissingManifestList_IsAnErrorNotAnEmptyTable()
    {
        var (catalog, id, table) = await CreateTableAsync();
        var ops = new TableOperations(_fs);
        var committed = await catalog.CommitTableAsync(table, await ops.AppendFilesAsync(table.Metadata, [File("a")]));

        var snapshot = committed.Metadata.Snapshots.Single();
        System.IO.File.Delete(Path.Combine(_warehouse, snapshot.ManifestList));

        await Assert.ThrowsAsync<FileNotFoundException>(() => ops.ListDataFilesAsync(committed.Metadata).AsTask());
        await Assert.ThrowsAsync<FileNotFoundException>(
            () => new TableScan(committed.Metadata, _fs).PlanFilesAsync().AsTask());
        // The next commit carries the current state forward, so it must not proceed without it either.
        await Assert.ThrowsAsync<FileNotFoundException>(
            () => ops.AppendFilesAsync(committed.Metadata, [File("b")]).AsTask());
        await Assert.ThrowsAsync<FileNotFoundException>(
            () => ops.OverwriteFilesAsync(committed.Metadata, [File("a")], [File("b")]).AsTask());
    }

    [Fact]
    public async Task MissingManifest_IsAnErrorNotAnEmptyTable()
    {
        var (catalog, id, table) = await CreateTableAsync();
        var ops = new TableOperations(_fs);
        var committed = await catalog.CommitTableAsync(table, await ops.AppendFilesAsync(table.Metadata, [File("a")]));

        var manifest = (await ManifestIO.ReadManifestListAsync(_fs, committed.Metadata.Snapshots.Single().ManifestList)).Single();
        System.IO.File.Delete(Path.Combine(_warehouse, manifest.ManifestPath));

        var ex = await Assert.ThrowsAsync<FileNotFoundException>(() => ops.ListDataFilesAsync(committed.Metadata).AsTask());
        Assert.Equal(manifest.ManifestPath, ex.FileName);
        await Assert.ThrowsAsync<FileNotFoundException>(
            () => new TableScan(committed.Metadata, _fs).PlanFilesAsync().AsTask());
        await Assert.ThrowsAsync<FileNotFoundException>(
            () => ops.OverwriteFilesAsync(committed.Metadata, [File("a")], [File("b")]).AsTask());
        // An append carries the manifest list forward without opening the manifests (as a fast append
        // does), so it does not notice; the next read does.
    }

    [Fact]
    public async Task Overwrite_OfAFileTheTableDoesNotHold_WritesNothing()
    {
        var (catalog, id, table) = await CreateTableAsync();
        var ops = new TableOperations(_fs);
        var committed = await catalog.CommitTableAsync(table, await ops.AppendFilesAsync(table.Metadata, [File("a")]));
        var before = Directory.GetFiles(MetadataDir).Length;

        var ex = await Assert.ThrowsAsync<CommitValidationException>(
            () => ops.OverwriteFilesAsync(committed.Metadata, [File("a"), File("gone")], [File("b")]).AsTask());
        Assert.Contains("db/t/data/gone.parquet", ex.Message);
        Assert.DoesNotContain("db/t/data/a.parquet", ex.Message);
        Assert.Equal(before, Directory.GetFiles(MetadataDir).Length);
    }
}
