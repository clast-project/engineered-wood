// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;

namespace EngineeredWood.Iceberg.Tests.Catalog;

public class FileSystemCatalogTests : IDisposable
{
    private readonly string _warehouse;
    private readonly FileSystemCatalog _catalog;
    private readonly Namespace _ns = Namespace.Of("db");
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "data", IcebergType.String, false),
    ]);

    public FileSystemCatalogTests()
    {
        _warehouse = Path.Combine(Path.GetTempPath(), $"iceberg-warehouse-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_warehouse);
        _catalog = new FileSystemCatalog(new LocalTableFileSystem(_warehouse));
    }

    public void Dispose()
    {
        if (Directory.Exists(_warehouse))
            Directory.Delete(_warehouse, recursive: true);
    }

    // --- Namespace tests ---

    [Fact]
    public async Task CreateAndLoadNamespace()
    {
        var props = new Dictionary<string, string> { ["owner"] = "test" };
        await _catalog.CreateNamespaceAsync(_ns, props);

        Assert.True(await _catalog.NamespaceExistsAsync(_ns));
        var loaded = await _catalog.GetNamespacePropertiesAsync(_ns);
        Assert.Equal("test", loaded["owner"]);
    }

    [Fact]
    public async Task CreateNamespace_Duplicate_Throws()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        await Assert.ThrowsAsync<NamespaceAlreadyExistsException>(
            () => _catalog.CreateNamespaceAsync(_ns));
    }

    [Fact]
    public async Task ListNamespaces_ReturnsCreated()
    {
        await _catalog.CreateNamespaceAsync(Namespace.Of("a"));
        await _catalog.CreateNamespaceAsync(Namespace.Of("b"));

        var result = await _catalog.ListNamespacesAsync();
        Assert.Equal(2, result.Count);
    }

    [Fact]
    public async Task DropNamespace_Succeeds()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        await _catalog.DropNamespaceAsync(_ns);
        Assert.False(await _catalog.NamespaceExistsAsync(_ns));
    }

    [Fact]
    public async Task UpdateNamespaceProperties()
    {
        await _catalog.CreateNamespaceAsync(_ns, new Dictionary<string, string> { ["a"] = "1" });
        await _catalog.UpdateNamespacePropertiesAsync(_ns,
            removals: ["a"],
            updates: new Dictionary<string, string> { ["b"] = "2" });

        var props = await _catalog.GetNamespacePropertiesAsync(_ns);
        Assert.False(props.ContainsKey("a"));
        Assert.Equal("2", props["b"]);
    }

    // --- Table tests ---

    [Fact]
    public async Task CreateTable_PersistsMetadata()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "events");

        var table = await _catalog.CreateTableAsync(id, _schema);
        Assert.Equal(2, table.Metadata.FormatVersion);

        // Load from disk
        var loaded = await _catalog.LoadTableAsync(id);
        Assert.Equal(table.Metadata.TableUuid, loaded.Metadata.TableUuid);
        Assert.Equal(2, loaded.CurrentSchema.Fields.Count);
    }

    [Fact]
    public async Task CreateTable_Duplicate_Throws()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "t");
        await _catalog.CreateTableAsync(id, _schema);

        await Assert.ThrowsAsync<TableAlreadyExistsException>(
            () => _catalog.CreateTableAsync(id, _schema));
    }

    [Fact]
    public async Task ListTables_ReturnsCreated()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        await _catalog.CreateTableAsync(new TableIdentifier(_ns, "a"), _schema);
        await _catalog.CreateTableAsync(new TableIdentifier(_ns, "b"), _schema);

        var tables = await _catalog.ListTablesAsync(_ns);
        Assert.Equal(2, tables.Count);
    }

    [Fact]
    public async Task DropTable_Succeeds()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "t");
        await _catalog.CreateTableAsync(id, _schema);

        await _catalog.DropTableAsync(id);
        Assert.False(await _catalog.TableExistsAsync(id));
    }

    [Fact]
    public async Task DropTable_Purge_DeletesDirectory()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "t");
        var table = await _catalog.CreateTableAsync(id, _schema);

        await _catalog.DropTableAsync(id, purge: true);

        Assert.False(Directory.Exists(table.Location));
    }

    [Fact]
    public async Task RenameTable_Succeeds()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var from = new TableIdentifier(_ns, "old");
        var to = new TableIdentifier(_ns, "new_name");
        await _catalog.CreateTableAsync(from, _schema);

        await _catalog.RenameTableAsync(from, to);

        Assert.False(await _catalog.TableExistsAsync(from));
        Assert.True(await _catalog.TableExistsAsync(to));
    }

    [Fact]
    public async Task UpdateTable_IncrementsVersion()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "t");
        var table = await _catalog.CreateTableAsync(id, _schema);

        var updated = table.Metadata with
        {
            Properties = new Dictionary<string, string> { ["updated"] = "true" }
        };

        await _catalog.UpdateTableAsync(id, updated);

        var loaded = await _catalog.LoadTableAsync(id);
        Assert.Equal("true", loaded.Metadata.Properties["updated"]);

        // Verify version file says "2"
        var versionPath = Path.Combine(_warehouse, "db", "t", "metadata", "version-hint.text");
        Assert.Equal("2", File.ReadAllText(versionPath));
    }

    [Fact]
    public async Task Metadata_SurvivesNewCatalogInstance()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "durable");
        await _catalog.CreateTableAsync(id, _schema);

        // Create a fresh catalog pointing to same warehouse
        var catalog2 = new FileSystemCatalog(new LocalTableFileSystem(_warehouse));

        Assert.True(await catalog2.NamespaceExistsAsync(_ns));
        var table = await catalog2.LoadTableAsync(id);
        Assert.Equal(2, table.CurrentSchema.Fields.Count);
    }
}
