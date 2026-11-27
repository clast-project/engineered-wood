// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Iceberg.Expressions;
using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.Iceberg.Tests.Corpus;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Iceberg.Tests.Catalog;

/// <summary>
/// Every location EW writes into Iceberg metadata is absolute, as the spec and every other engine expect, and every
/// location it reads is reached through a <see cref="StorageResolver"/> (#963).
/// </summary>
public class AbsoluteLocationTests : IDisposable
{
    private readonly string _root;
    private readonly Schema _schema = new(0, [new NestedField(1, "id", IcebergType.Long, true)]);

    public AbsoluteLocationTests()
    {
        _root = Path.Combine(Path.GetTempPath(), $"iceberg-abs-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_root);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }

    private static DataFile File(string location) => new()
    {
        FilePath = location,
        FileFormat = FileFormat.Parquet,
        RecordCount = 10,
        FileSizeInBytes = 100,
    };

    private async Task<(FileSystemCatalog Catalog, Table Table)> CreateAsync(FileSystemCatalog catalog)
    {
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var table = await catalog.CreateTableAsync(new TableIdentifier(Namespace.Of("db"), "t"), _schema);
        return (catalog, table);
    }

    [Fact]
    public async Task ACatalogWritesFileUrisUnderItsWarehouse()
    {
        var (catalog, table) = await CreateAsync(new FileSystemCatalog(new LocalTableFileSystem(_root)));
        var warehouse = StorageLocation.Parse(_root);

        Assert.StartsWith("file:///", table.Location, StringComparison.Ordinal);
        Assert.Equal(warehouse.Combine("db/t"), StorageLocation.Parse(table.Location));
        Assert.StartsWith(table.Location + "/metadata/v1.metadata.json", table.MetadataLocation!, StringComparison.Ordinal);

        var ops = new TableOperations(catalog.Resolver);
        var appended = await ops.AppendFilesAsync(table.Metadata, [File(table.Location + "/data/a.parquet")]);
        var committed = await catalog.CommitTableAsync(table, appended);

        var snapshot = committed.Metadata.Snapshots.Single();
        Assert.StartsWith(table.Location + "/metadata/snap-", snapshot.ManifestList, StringComparison.Ordinal);
        var list = await ManifestIO.ReadManifestListAsync(new LocationFileSystem(catalog.Resolver), snapshot.ManifestList);
        Assert.All(list, m => Assert.StartsWith(table.Location + "/metadata/", m.ManifestPath, StringComparison.Ordinal));
        Assert.True(System.IO.File.Exists(StorageLocation.Parse(snapshot.ManifestList).LocalPath));
        Assert.StartsWith(table.Location + "/metadata/", committed.Metadata.MetadataLog.Single().MetadataFile, StringComparison.Ordinal);

        var reloaded = await new FileSystemCatalog(new LocalTableFileSystem(_root)).LoadTableAsync(table.Identifier);
        var scan = await new TableScan(reloaded.Metadata, catalog.Resolver).PlanFilesAsync();
        Assert.Equal(table.Location + "/data/a.parquet", scan.DataFiles.Single().FilePath);
    }

    /// <summary>A warehouse under a broader registration (one client for a whole bucket, say) works as one of its own.</summary>
    [Fact]
    public async Task AWarehouseUnderARegisteredPrefixIsReachedThroughIt()
    {
        string warehouse = Path.Combine(_root, "lake", "wh");
        var resolver = new StorageResolver().Register(new LocalTableFileSystem(_root));
        var (catalog, table) = await CreateAsync(new FileSystemCatalog(resolver, StorageLocation.Parse(warehouse)));

        Assert.True(System.IO.File.Exists(Path.Combine(warehouse, "db", "t", "metadata", "v1.metadata.json")));
        Assert.Equal(StorageLocation.Parse(warehouse).Combine("db/t"), StorageLocation.Parse(table.Location));
        Assert.Equal([table.Identifier], await catalog.ListTablesAsync(Namespace.Of("db")));
        Assert.Equal([Namespace.Of("db")], await catalog.ListNamespacesAsync());
    }

    [Fact]
    public async Task ATableLocationOutsideEveryRegistrationIsRefused()
    {
        var catalog = new FileSystemCatalog(new LocalTableFileSystem(_root));
        var elsewhere = TableMetadata.Create(_schema, location: "s3://bucket/elsewhere/t");

        await Assert.ThrowsAsync<UnregisteredStorageLocationException>(async () =>
            await new TableOperations(catalog.Resolver).AppendFilesAsync(elsewhere, [File("s3://bucket/elsewhere/t/data/a.parquet")]));
    }

    /// <summary>
    /// A file-system catalog keeps each table at its path under the warehouse, as Iceberg Java's HadoopCatalog does
    /// (READ FROM 1.12.0's bytecode: <c>HadoopCatalogTableBuilder.withLocation</c> checks that the location is the
    /// default one): the catalog finds, and a purge deletes, a table by that path, so data anywhere else would
    /// outlive a purge. Another spelling of the default location is the same location.
    /// </summary>
    [Fact]
    public async Task ATableIsCreatedOnlyAtItsPathUnderTheWarehouse()
    {
        var catalog = new FileSystemCatalog(new LocalTableFileSystem(_root));
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await catalog.CreateTableAsync(new TableIdentifier(Namespace.Of("db"), "t"), _schema, location: "s3://bucket/elsewhere/t"));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await catalog.CreateTableAsync(new TableIdentifier(Namespace.Of("db"), "t"), _schema,
                location: StorageLocation.Parse(_root).Combine("db/other").ToString()));
        Assert.False(await catalog.TableExistsAsync(new TableIdentifier(Namespace.Of("db"), "t")));

        string respelled = "file:/" + StorageLocation.Parse(_root).Combine("db/u").ToString().Substring("file:///".Length);
        var table = await catalog.CreateTableAsync(new TableIdentifier(Namespace.Of("db"), "u"), _schema, location: respelled);
        Assert.Equal(respelled, table.Location);

        // A bare path naming the same place is accepted, and written as the URI other engines resolve.
        var bare = await catalog.CreateTableAsync(new TableIdentifier(Namespace.Of("db"), "v"), _schema,
            location: Path.Combine(_root, "db", "v"));
        Assert.StartsWith("file:///", bare.Location, StringComparison.Ordinal);
        Assert.Equal(StorageLocation.Parse(_root).Combine("db/v"), StorageLocation.Parse(bare.Location));
    }

    /// <summary>A commit can no more move a table, or log a relative metadata file, than a create can: the
    /// location stays the table's path under the warehouse, and nothing is written for a refused commit.</summary>
    [Fact]
    public async Task ACommitCannotMoveTheTableOrLogARelativeLocation()
    {
        var (catalog, table) = await CreateAsync(new FileSystemCatalog(new LocalTableFileSystem(_root)));

        foreach (string location in new[] { "db/t", "s3://bucket/elsewhere/t", StorageLocation.Parse(_root).Combine("db/other").ToString() })
        {
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await catalog.CommitTableAsync(table, table.Metadata with { Location = location }));
        }

        foreach (string logged in new[]
                 {
                     "db/t/metadata/v0.metadata.json",
                     StorageLocation.Parse(_root).Combine("other/metadata/v1.metadata.json").ToString(),
                     "s3://bucket/wh/db/t/metadata/v1.metadata.json",
                     table.Location + "/metadata/sub/v1.metadata.json",
                 })
        {
            await Assert.ThrowsAsync<ArgumentException>(async () => await catalog.CommitTableAsync(table,
                table.Metadata with { MetadataLog = [new MetadataLogEntry(0, logged)] }));
        }

        var appended = await new TableOperations(catalog.Resolver).AppendFilesAsync(table.Metadata, [File(table.Location + "/data/a.parquet")]);
        var snapshot = appended.Snapshots.Single();
        await Assert.ThrowsAsync<ArgumentException>(async () => await catalog.CommitTableAsync(table,
            appended with { Snapshots = [snapshot with { ManifestList = "db/t/metadata/snap.avro" }] }));
        await Assert.ThrowsAsync<ArgumentException>(async () => await catalog.CommitTableAsync(table,
            appended with { Statistics = [new StatisticsFile(snapshot.SnapshotId, "db/t/metadata/s.stats", 1, 1, [])] }));
        await Assert.ThrowsAsync<ArgumentException>(async () => await catalog.CommitTableAsync(table,
            appended with { PartitionStatistics = [new PartitionStatisticsFile(snapshot.SnapshotId, "db/t/metadata/p.parquet", 1)] }));

        Assert.False(System.IO.File.Exists(Path.Combine(_root, "db", "t", "metadata", "v2.metadata.json")));
        var respelled = "file:/" + table.Location.Substring("file:///".Length);
        var committed = await catalog.CommitTableAsync(table, table.Metadata with { Location = respelled });
        Assert.Equal(respelled, committed.Metadata.Location);

        // A bare path naming the same place is accepted, and written as the URI other engines resolve.
        var bare = await catalog.CommitTableAsync(committed, committed.Metadata with { Location = Path.Combine(_root, "db", "t") });
        Assert.StartsWith("file:///", bare.Metadata.Location, StringComparison.Ordinal);
        Assert.StartsWith("file:///", (await catalog.LoadTableAsync(table.Identifier)).Metadata.Location, StringComparison.Ordinal);
    }

    /// <summary>A table whose metadata gives a relative location, as every table EngineeredWood wrote before it
    /// wrote absolute locations does, is refused when it is loaded, by name, rather than returned for every operation
    /// to refuse later.</summary>
    [Fact]
    public async Task ATableWrittenWithARelativeLocationIsRefusedOnLoad()
    {
        var (catalog, table) = await CreateAsync(new FileSystemCatalog(new LocalTableFileSystem(_root)));
        string v1 = StorageLocation.Parse(table.MetadataLocation!).LocalPath!;
        string json = System.IO.File.ReadAllText(v1).Replace(table.Location, "db/t");
        System.IO.File.WriteAllText(v1, json);

        var ex = await Assert.ThrowsAsync<FormatException>(async () => await catalog.LoadTableAsync(table.Identifier));
        Assert.Contains("db/t", ex.Message, StringComparison.Ordinal);
        Assert.True(await catalog.TableExistsAsync(table.Identifier));
    }

    /// <summary>A load holds a table to what create and commit do: its location is its path under the warehouse
    /// (which is what a purge deletes), and the files it names are absolute. A version that breaks either, from an
    /// older EngineeredWood or an edit by hand, is refused on load, and so cannot be renamed.</summary>
    [Fact]
    public async Task ATableThatLivesElsewhereOrNamesARelativeFileIsRefusedOnLoad()
    {
        var (catalog, table) = await CreateAsync(new FileSystemCatalog(new LocalTableFileSystem(_root)));
        var appended = await new TableOperations(catalog.Resolver).AppendFilesAsync(table.Metadata, [File(table.Location + "/data/a.parquet")]);
        table = await catalog.CommitTableAsync(table, appended);
        string current = StorageLocation.Parse(table.MetadataLocation!).LocalPath!;
        string original = System.IO.File.ReadAllText(current);
        var renameTo = new TableIdentifier(Namespace.Of("db"), "renamed");

        string elsewhere = original.Replace($"\"location\":\"{table.Location}\"", "\"location\":\"s3://other/t\"");
        Assert.NotEqual(original, elsewhere);
        System.IO.File.WriteAllText(current, elsewhere);
        await Assert.ThrowsAsync<FormatException>(async () => await catalog.LoadTableAsync(table.Identifier));
        await Assert.ThrowsAsync<FormatException>(async () => await catalog.RenameTableAsync(table.Identifier, renameTo));

        string manifestList = table.Metadata.Snapshots.Single().ManifestList;
        string relative = original.Replace(manifestList, "db/t/metadata/snap.avro");
        Assert.NotEqual(original, relative);
        System.IO.File.WriteAllText(current, relative);
        await Assert.ThrowsAsync<FormatException>(async () => await catalog.LoadTableAsync(table.Identifier));
        await Assert.ThrowsAsync<FormatException>(async () => await catalog.RenameTableAsync(table.Identifier, renameTo));
        Assert.False(await catalog.TableExistsAsync(renameTo));
    }

    /// <summary>A rename publishes the destination's location like a create does, so a name whose location would
    /// not read back is refused before anything changes, and the source keeps its name.</summary>
    [Theory]
    [InlineData("a#b")]
    [InlineData("a?b")]
    public async Task ARenameWhoseLocationWouldNotReadBackIsRefused(string name)
    {
        var (catalog, table) = await CreateAsync(new FileSystemCatalog(new LocalTableFileSystem(_root)));
        var to = new TableIdentifier(Namespace.Of("db"), name);

        await Assert.ThrowsAsync<ArgumentException>(async () => await catalog.RenameTableAsync(table.Identifier, to));

        Assert.True(await catalog.TableExistsAsync(table.Identifier));
        Assert.Equal([table.Identifier], await catalog.ListTablesAsync(Namespace.Of("db")));
    }

    [Fact]
    public async Task ARelativeTableLocationIsRefused()
    {
        var catalog = new FileSystemCatalog(new LocalTableFileSystem(_root));
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        await Assert.ThrowsAsync<FormatException>(async () =>
            await catalog.CreateTableAsync(new TableIdentifier(Namespace.Of("db"), "t"), _schema, location: "db/t"));

        var relative = TableMetadata.Create(_schema, location: "db/t");
        await Assert.ThrowsAsync<FormatException>(async () =>
            await new TableOperations(catalog.Resolver).AppendFilesAsync(relative, [File("s3://bucket/db/t/data/a.parquet")]));
    }

    /// <summary>The spec (v1 to v3) makes every file location absolute; a relative one would be read against
    /// whatever base each engine chose. It is refused before anything is written.</summary>
    [Fact]
    public async Task ARelativeFileLocationIsRefused()
    {
        var (catalog, table) = await CreateAsync(new FileSystemCatalog(new LocalTableFileSystem(_root)));
        var ops = new TableOperations(catalog.Resolver);
        string metadataDir = Path.Combine(_root, "db", "t", "metadata");

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await ops.AppendFilesAsync(table.Metadata, [File("data/a.parquet")]));

        var dv = File(table.Location + "/data/dv.puffin") with
        {
            Content = FileContent.PositionDeletes, FileFormat = FileFormat.Puffin,
            ReferencedDataFile = "data/a.parquet", ContentOffset = 4, ContentSizeInBytes = 10,
        };
        await Assert.ThrowsAsync<ArgumentException>(async () => await ops.AppendDeleteFilesAsync(table.Metadata, [dv]));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await ops.OverwriteFilesAsync(table.Metadata, [], [File("data/a.parquet")]));

        Assert.Empty(Directory.GetFiles(metadataDir, "*.avro"));
    }

    /// <summary>A table's location is written into its metadata as text and parsed by every later read, so a name
    /// that would not read back (a <c>#</c> or <c>?</c> under a <c>file:</c> warehouse starts a fragment or query) is
    /// refused before anything is written, rather than creating a table no operation can then use.</summary>
    [Theory]
    [InlineData("a#b")]
    [InlineData("a?b")]
    public async Task ATableWhoseLocationWouldNotReadBackIsRefused(string name)
    {
        var catalog = new FileSystemCatalog(new LocalTableFileSystem(_root));
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var id = new TableIdentifier(Namespace.Of("db"), name);

        await Assert.ThrowsAsync<ArgumentException>(async () => await catalog.CreateTableAsync(id, _schema));
        Assert.Empty(Directory.GetDirectories(Path.Combine(_root, "db")));
    }

    /// <summary>Each namespace level and table name is a directory under the warehouse, so one that is not a plain
    /// segment would put files beside it: still inside the backend's root, where no root check would catch it.</summary>
    [Theory]
    [InlineData("..")]
    [InlineData(".")]
    [InlineData("a/b")]
    [InlineData("a\\b")]
    [InlineData("a\u0001b")]
    public async Task NamesThatAreNotOneSegmentAreRefused(string name)
    {
        string warehouse = Path.Combine(_root, "lake", "wh");
        var resolver = new StorageResolver().Register(new LocalTableFileSystem(_root));
        var catalog = new FileSystemCatalog(resolver, StorageLocation.Parse(warehouse));
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));

        await Assert.ThrowsAsync<ArgumentException>(async () => await catalog.CreateNamespaceAsync(Namespace.Of(name)));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await catalog.UpdateNamespacePropertiesAsync(Namespace.Of(name, "sibling"), updates: new Dictionary<string, string> { ["k"] = "v" }));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await catalog.CreateTableAsync(new TableIdentifier(Namespace.Of("db"), name), _schema));

        Assert.Empty(Directory.GetFiles(Path.Combine(_root, "lake"), "*", SearchOption.TopDirectoryOnly));
        Assert.Equal([Path.Combine(_root, "lake", "wh")], Directory.GetDirectories(Path.Combine(_root, "lake")));
    }

    /// <summary>Two spellings of one warehouse (<c>file:///</c> and <c>file:/</c>) are one warehouse: a table loaded
    /// through one catalog commits through the other.</summary>
    [Fact]
    public async Task ATableCommitsThroughAnotherSpellingOfItsWarehouse()
    {
        var resolver = new StorageResolver().Register(new LocalTableFileSystem(_root));
        var warehouse = StorageLocation.Parse(_root);
        var (catalog, table) = await CreateAsync(new FileSystemCatalog(resolver, warehouse));
        string other = "file:/" + warehouse.ToString().Substring("file:///".Length);
        Assert.NotEqual(warehouse.ToString(), other);
        var otherCatalog = new FileSystemCatalog(resolver, StorageLocation.Parse(other));

        var appended = await new TableOperations(resolver).AppendFilesAsync(table.Metadata, [File(table.Location + "/data/a.parquet")]);
        var committed = await otherCatalog.CommitTableAsync(table, appended);

        Assert.Single(committed.Metadata.Snapshots);
        Assert.Single((await catalog.LoadTableAsync(table.Identifier)).Metadata.Snapshots);
    }

    [Fact]
    public async Task AManifestListNamedByARelativePathIsRefused()
    {
        var resolver = TestResolvers.For(new LocalTableFileSystem(_root));
        var md = TableMetadata.Create(_schema, location: _root);
        var snapshot = new Snapshot(1, null, 1, 0, "metadata/snap-1.avro", new Dictionary<string, string> { ["operation"] = "append" }, 0, null, null, null);
        md = md with { Snapshots = [snapshot], CurrentSnapshotId = 1 };

        await Assert.ThrowsAsync<ArgumentException>(async () => await new TableScan(md, resolver).PlanFilesAsync());
    }

    /// <summary>
    /// Expiry deletes only under the table's own location: a manifest the metadata names elsewhere may be another
    /// table's (after a migration or a copy), as Delta's VACUUM touches only its own table's files.
    /// </summary>
    [Fact]
    public async Task ExpiryLeavesFilesOutsideTheTableInPlace()
    {
        var fs = new LocalTableFileSystem(_root);
        var resolver = TestResolvers.For(fs);
        var ops = new TableOperations(resolver);
        var other = TableMetadata.Create(_schema, location: Path.Combine(_root, "other"));
        other = await ops.AppendFilesAsync(other, [File(StorageLocation.Parse(_root).Combine("other/data/a.parquet").ToString())]);
        var foreignList = other.Snapshots.Single().ManifestList;

        // A table whose first snapshot is the other table's.
        var md = TableMetadata.Create(_schema, location: Path.Combine(_root, "t")) with
        {
            Snapshots = [other.Snapshots.Single() with { }],
            CurrentSnapshotId = other.CurrentSnapshotId,
            LastSequenceNumber = other.LastSequenceNumber,
        };
        var expiredId = md.CurrentSnapshotId!.Value;
        md = await ops.AppendFilesAsync(md, [File(StorageLocation.Parse(_root).Combine("t/data/b.parquet").ToString())]);

        var result = await new ExpireSnapshots().ExpireSnapshotId(expiredId).ApplyAsync(md, resolver);

        Assert.Contains(foreignList, result.FilesOutsideTable);
        Assert.DoesNotContain(foreignList, result.DeletedFiles);
        Assert.True(System.IO.File.Exists(StorageLocation.Parse(foreignList).LocalPath));
    }

    /// <summary>
    /// A kept snapshot can name a manifest list or manifest in another spelling than an expired one does (one engine
    /// writes <c>file:/</c>, another <c>file:///</c>): it is one file, and expiry keeps it.
    /// </summary>
    [Fact]
    public async Task ExpiryKeepsAFileAKeptSnapshotNamesInAnotherSpelling()
    {
        var fs = new LocalTableFileSystem(_root);
        var resolver = TestResolvers.For(fs);
        var ops = new TableOperations(resolver);
        var md = TableMetadata.Create(_schema, location: Path.Combine(_root, "t"));
        md = await ops.AppendFilesAsync(md, [File(StorageLocation.Parse(_root).Combine("t/data/a.parquet").ToString())]);
        var first = md.Snapshots.Single();
        Assert.StartsWith("file:///", first.ManifestList, StringComparison.Ordinal);

        // The same snapshot again, its manifest list spelled file:/ rather than file:///.
        var respelled = first with
        {
            SnapshotId = first.SnapshotId + 1,
            ParentSnapshotId = first.SnapshotId,
            ManifestList = "file:/" + first.ManifestList.Substring("file:///".Length),
        };
        md = md with { Snapshots = [first, respelled], CurrentSnapshotId = respelled.SnapshotId };

        var result = await new ExpireSnapshots().ExpireSnapshotId(first.SnapshotId).ApplyAsync(md, resolver);

        Assert.Empty(result.DeletedFiles);
        Assert.True(System.IO.File.Exists(StorageLocation.Parse(first.ManifestList).LocalPath));
        var manifests = await ManifestIO.ReadManifestListAsync(new LocationFileSystem(resolver), respelled.ManifestList);
        Assert.All(manifests, m => Assert.True(System.IO.File.Exists(StorageLocation.Parse(m.ManifestPath).LocalPath)));
    }

    /// <summary>
    /// MEASURED on the committed corpora: every data and delete file a manifest names exists at the location read
    /// verbatim. Spark's partition directories hold literal escapes (<c>fx=fwAAAQ%3D%3D</c>) both on disk and in
    /// the manifests, so a reader that percent-decoded locations would look for files that are not there.
    /// </summary>
    [Theory]
    [InlineData("spark")]
    [InlineData("iceberg-go")]
    public async Task EveryCorpusFileExistsAtItsLocationReadVerbatim(string engine)
    {
        var corpus = engine == "spark" ? IcebergCorpus.Spark : IcebergCorpus.IcebergGo;
        int checkedFiles = 0, escaped = 0;
        foreach (string table in corpus.Tables)
        {
            var md = corpus.Metadata(table);
            foreach (var snapshot in md.Snapshots)
            {
                var (data, deletes) = await new TableOperations(corpus.Resolver).ScanFilesAsync(md with { CurrentSnapshotId = snapshot.SnapshotId });
                foreach (var file in data.Concat(deletes))
                {
                    var resolved = corpus.Resolver.Resolve(file.FilePath);
                    Assert.True(await resolved.FileSystem.ExistsAsync(resolved.Path), $"{table}: {file.FilePath}");
                    checkedFiles++;
                    if (file.FilePath.Contains('%'))
                        escaped++;
                }
            }
        }

        Assert.True(checkedFiles > 0);
        if (engine == "spark")
            Assert.True(escaped > 0, "the Spark corpus has percent-escaped partition directories");
    }
}
