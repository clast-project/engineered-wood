// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg.Tests.Catalog;

public class InMemoryCatalogTests
{
    private readonly InMemoryCatalog _catalog = new();
    private readonly Namespace _ns = Namespace.Of("db");
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "data", IcebergType.String, false),
    ]);

    // --- Namespace tests ---

    [Fact]
    public async Task CreateNamespace_CreatesSuccessfully()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        Assert.True(await _catalog.NamespaceExistsAsync(_ns));
    }

    [Fact]
    public async Task CreateNamespace_Duplicate_Throws()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        await Assert.ThrowsAsync<NamespaceAlreadyExistsException>(
            () => _catalog.CreateNamespaceAsync(_ns));
    }

    [Fact]
    public async Task CreateNamespace_WithProperties()
    {
        var props = new Dictionary<string, string> { ["owner"] = "test" };
        await _catalog.CreateNamespaceAsync(_ns, props);

        var loaded = await _catalog.GetNamespacePropertiesAsync(_ns);
        Assert.Equal("test", loaded["owner"]);
    }

    [Fact]
    public async Task ListNamespaces_TopLevel()
    {
        await _catalog.CreateNamespaceAsync(Namespace.Of("a"));
        await _catalog.CreateNamespaceAsync(Namespace.Of("b"));
        await _catalog.CreateNamespaceAsync(Namespace.Of("a", "child"));

        var result = await _catalog.ListNamespacesAsync();
        Assert.Equal(2, result.Count);
        Assert.Contains(result, n => n == Namespace.Of("a"));
        Assert.Contains(result, n => n == Namespace.Of("b"));
    }

    [Fact]
    public async Task ListNamespaces_UnderParent()
    {
        var parent = Namespace.Of("db");
        await _catalog.CreateNamespaceAsync(parent);
        await _catalog.CreateNamespaceAsync(Namespace.Of("db", "schema1"));
        await _catalog.CreateNamespaceAsync(Namespace.Of("db", "schema2"));
        await _catalog.CreateNamespaceAsync(Namespace.Of("db", "schema1", "deep"));

        var result = await _catalog.ListNamespacesAsync(parent);
        Assert.Equal(2, result.Count);
    }

    [Fact]
    public async Task GetNamespaceProperties_NotFound_Throws()
    {
        await Assert.ThrowsAsync<NoSuchNamespaceException>(
            () => _catalog.GetNamespacePropertiesAsync(Namespace.Of("missing")));
    }

    [Fact]
    public async Task UpdateNamespaceProperties_AddsAndRemoves()
    {
        var props = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" };
        await _catalog.CreateNamespaceAsync(_ns, props);

        await _catalog.UpdateNamespacePropertiesAsync(_ns,
            removals: ["a"],
            updates: new Dictionary<string, string> { ["c"] = "3" });

        var updated = await _catalog.GetNamespacePropertiesAsync(_ns);
        Assert.False(updated.ContainsKey("a"));
        Assert.Equal("2", updated["b"]);
        Assert.Equal("3", updated["c"]);
    }

    [Fact]
    public async Task DropNamespace_Succeeds()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        await _catalog.DropNamespaceAsync(_ns);
        Assert.False(await _catalog.NamespaceExistsAsync(_ns));
    }

    [Fact]
    public async Task DropNamespace_NotFound_Throws()
    {
        await Assert.ThrowsAsync<NoSuchNamespaceException>(
            () => _catalog.DropNamespaceAsync(Namespace.Of("missing")));
    }

    [Fact]
    public async Task DropNamespace_WithTables_Throws()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "t");
        await _catalog.CreateTableAsync(id, _schema);

        await Assert.ThrowsAsync<NamespaceNotEmptyException>(
            () => _catalog.DropNamespaceAsync(_ns));
    }

    [Fact]
    public async Task DropNamespace_WithChildNamespaces_Throws()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        await _catalog.CreateNamespaceAsync(Namespace.Of("db", "child"));

        await Assert.ThrowsAsync<NamespaceNotEmptyException>(
            () => _catalog.DropNamespaceAsync(_ns));
    }

    // --- Table tests ---

    [Fact]
    public async Task CreateTable_CreatesSuccessfully()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "events");

        var table = await _catalog.CreateTableAsync(id, _schema);

        Assert.Equal(id, table.Identifier);
        Assert.Equal(2, table.Metadata.FormatVersion);
        Assert.Equal(0, table.Metadata.CurrentSchemaId);
        Assert.Equal(2, table.CurrentSchema.Fields.Count);
        Assert.Null(table.Metadata.CurrentSnapshotId);
    }

    [Fact]
    public async Task CreateTable_WithPartitionSpec()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "events");
        var spec = new PartitionSpec(0, [
            new PartitionField(1, 1000, "id_bucket", Transform.Bucket(16)),
        ]);

        var table = await _catalog.CreateTableAsync(id, _schema, spec: spec);

        Assert.Single(table.CurrentSpec.Fields);
        Assert.IsType<BucketTransform>(table.CurrentSpec.Fields[0].Transform);
    }

    [Fact]
    public async Task CreateTable_WithSortOrder()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "events");
        var order = new SortOrder(1, [
            new SortField(1, Transform.Identity, SortDirection.Asc, NullOrder.NullsFirst),
        ]);

        var table = await _catalog.CreateTableAsync(id, _schema, sortOrder: order);

        Assert.Single(table.CurrentSortOrder.Fields);
    }

    [Fact]
    public async Task CreateTable_WithProperties()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "events");
        var props = new Dictionary<string, string> { ["write.format.default"] = "parquet" };

        var table = await _catalog.CreateTableAsync(id, _schema, properties: props);

        Assert.Equal("parquet", table.Metadata.Properties["write.format.default"]);
    }

    [Fact]
    public async Task CreateTable_SetsLocationFromWarehouse()
    {
        var catalog = new InMemoryCatalog("s3://bucket/warehouse");
        await catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "events");

        var table = await catalog.CreateTableAsync(id, _schema);

        Assert.StartsWith("s3://bucket/warehouse/", table.Location);
    }

    [Fact]
    public async Task CreateTable_NoNamespace_Throws()
    {
        var id = new TableIdentifier(_ns, "events");

        await Assert.ThrowsAsync<NoSuchNamespaceException>(
            () => _catalog.CreateTableAsync(id, _schema));
    }

    [Fact]
    public async Task CreateTable_Duplicate_Throws()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "events");
        await _catalog.CreateTableAsync(id, _schema);

        await Assert.ThrowsAsync<TableAlreadyExistsException>(
            () => _catalog.CreateTableAsync(id, _schema));
    }

    [Fact]
    public async Task CreateTable_SetsMaxColumnId()
    {
        var schema = new Schema(0, [
            new NestedField(1, "id", IcebergType.Long, true),
            new NestedField(5, "name", IcebergType.String, false),
            new NestedField(3, "ts", IcebergType.Timestamp, true),
        ]);

        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "t");
        var table = await _catalog.CreateTableAsync(id, schema);

        Assert.Equal(5, table.Metadata.LastColumnId);
    }

    [Fact]
    public async Task LoadTable_Succeeds()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "events");
        await _catalog.CreateTableAsync(id, _schema);

        var table = await _catalog.LoadTableAsync(id);

        Assert.Equal(id, table.Identifier);
        Assert.Equal(2, table.CurrentSchema.Fields.Count);
    }

    [Fact]
    public async Task LoadTable_NotFound_Throws()
    {
        await Assert.ThrowsAsync<NoSuchTableException>(
            () => _catalog.LoadTableAsync(new TableIdentifier(_ns, "missing")));
    }

    [Fact]
    public async Task ListTables_ReturnsTablesInNamespace()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        await _catalog.CreateTableAsync(new TableIdentifier(_ns, "a"), _schema);
        await _catalog.CreateTableAsync(new TableIdentifier(_ns, "b"), _schema);

        var other = Namespace.Of("other");
        await _catalog.CreateNamespaceAsync(other);
        await _catalog.CreateTableAsync(new TableIdentifier(other, "c"), _schema);

        var tables = await _catalog.ListTablesAsync(_ns);
        Assert.Equal(2, tables.Count);
    }

    [Fact]
    public async Task ListTables_NoNamespace_Throws()
    {
        await Assert.ThrowsAsync<NoSuchNamespaceException>(
            () => _catalog.ListTablesAsync(Namespace.Of("missing")));
    }

    [Fact]
    public async Task TableExists_ReturnsTrueAndFalse()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "events");

        Assert.False(await _catalog.TableExistsAsync(id));
        await _catalog.CreateTableAsync(id, _schema);
        Assert.True(await _catalog.TableExistsAsync(id));
    }

    [Fact]
    public async Task DropTable_Succeeds()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var id = new TableIdentifier(_ns, "events");
        await _catalog.CreateTableAsync(id, _schema);

        await _catalog.DropTableAsync(id);

        Assert.False(await _catalog.TableExistsAsync(id));
    }

    [Fact]
    public async Task DropTable_NotFound_Throws()
    {
        await Assert.ThrowsAsync<NoSuchTableException>(
            () => _catalog.DropTableAsync(new TableIdentifier(_ns, "missing")));
    }

    [Fact]
    public async Task RenameTable_Succeeds()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        var from = new TableIdentifier(_ns, "old");
        var to = new TableIdentifier(_ns, "new");
        await _catalog.CreateTableAsync(from, _schema);

        await _catalog.RenameTableAsync(from, to);

        Assert.False(await _catalog.TableExistsAsync(from));
        Assert.True(await _catalog.TableExistsAsync(to));
    }

    [Fact]
    public async Task RenameTable_AcrossNamespaces()
    {
        var ns2 = Namespace.Of("other");
        await _catalog.CreateNamespaceAsync(_ns);
        await _catalog.CreateNamespaceAsync(ns2);

        var from = new TableIdentifier(_ns, "t");
        var to = new TableIdentifier(ns2, "t");
        await _catalog.CreateTableAsync(from, _schema);

        await _catalog.RenameTableAsync(from, to);

        Assert.False(await _catalog.TableExistsAsync(from));
        Assert.True(await _catalog.TableExistsAsync(to));
    }

    [Fact]
    public async Task RenameTable_SourceNotFound_Throws()
    {
        await _catalog.CreateNamespaceAsync(_ns);

        await Assert.ThrowsAsync<NoSuchTableException>(
            () => _catalog.RenameTableAsync(
                new TableIdentifier(_ns, "missing"),
                new TableIdentifier(_ns, "new")));
    }

    [Fact]
    public async Task RenameTable_TargetExists_Throws()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        await _catalog.CreateTableAsync(new TableIdentifier(_ns, "a"), _schema);
        await _catalog.CreateTableAsync(new TableIdentifier(_ns, "b"), _schema);

        await Assert.ThrowsAsync<TableAlreadyExistsException>(
            () => _catalog.RenameTableAsync(
                new TableIdentifier(_ns, "a"),
                new TableIdentifier(_ns, "b")));
    }

    [Fact]
    public async Task RenameTable_TargetNamespaceMissing_Throws()
    {
        await _catalog.CreateNamespaceAsync(_ns);
        await _catalog.CreateTableAsync(new TableIdentifier(_ns, "t"), _schema);

        await Assert.ThrowsAsync<NoSuchNamespaceException>(
            () => _catalog.RenameTableAsync(
                new TableIdentifier(_ns, "t"),
                new TableIdentifier(Namespace.Of("missing"), "t")));
    }
}
