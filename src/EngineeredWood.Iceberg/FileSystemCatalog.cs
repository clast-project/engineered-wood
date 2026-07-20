// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.Iceberg.Serialization;
using EngineeredWood.IO;

namespace EngineeredWood.Iceberg;

/// <summary>
/// An <see cref="ICatalog"/> implementation that persists namespace and table metadata
/// as JSON files on an <see cref="ITableFileSystem"/>, using the Hadoop-style layout
/// with version-hint files and numbered metadata versions.
/// </summary>
public sealed class FileSystemCatalog : ICatalog
{
    private readonly ITableFileSystem _fs;

    /// <summary>Initializes a new <see cref="FileSystemCatalog"/> backed by the given file system.</summary>
    /// <param name="fs">The file system used to store catalog metadata.</param>
    public FileSystemCatalog(ITableFileSystem fs)
    {
        _fs = fs;
    }

    // --- Namespace operations ---

    private static string NamespacePath(Namespace ns) =>
        string.Join("/", ns.Levels);

    private static string NamespacePropertiesPath(Namespace ns) =>
        $"{NamespacePath(ns)}/.namespace-properties.json";

    public async Task<IReadOnlyList<Namespace>> ListNamespacesAsync(
        Namespace? parent = null, CancellationToken ct = default)
    {
        var prefix = parent is null || parent.IsEmpty
            ? ""
            : NamespacePath(parent) + "/";

        var depth = parent is null || parent.IsEmpty ? 1 : parent.Length + 1;
        var result = new List<Namespace>();
        var seen = new HashSet<string>();

        await foreach (var file in _fs.ListAsync(prefix, ct).ConfigureAwait(false))
        {
            if (!file.Path.EndsWith("/.namespace-properties.json", StringComparison.Ordinal))
                continue;

            // Extract the namespace from the path by taking the expected depth of levels
            var parts = file.Path.Split('/');
            if (parts.Length < depth + 1) // need at least depth levels + the filename
                continue;

            var levels = parts.Take(depth).ToArray();
            var key = string.Join("/", levels);
            if (seen.Add(key))
                result.Add(new Namespace(levels));
        }

        return result;
    }

    public async Task CreateNamespaceAsync(
        Namespace ns,
        IReadOnlyDictionary<string, string>? properties = null,
        CancellationToken ct = default)
    {
        if (ns.IsEmpty)
            throw new ArgumentException("Cannot create empty namespace");

        var propsPath = NamespacePropertiesPath(ns);

        if (await _fs.ExistsAsync(propsPath, ct).ConfigureAwait(false))
            throw new NamespaceAlreadyExistsException(ns);

        var props = properties ?? new Dictionary<string, string>();
        var json = IcebergJsonSerializer.Serialize(props);
        await _fs.WriteAllBytesAsync(propsPath, Encoding.UTF8.GetBytes(json), ct)
            .ConfigureAwait(false);
    }

    public async Task<IReadOnlyDictionary<string, string>> GetNamespacePropertiesAsync(
        Namespace ns, CancellationToken ct = default)
    {
        var propsPath = NamespacePropertiesPath(ns);

        if (!await _fs.ExistsAsync(propsPath, ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(ns);

        var data = await _fs.ReadAllBytesAsync(propsPath, ct).ConfigureAwait(false);
        var json = Encoding.UTF8.GetString(data);
        return IcebergJsonSerializer.Deserialize<Dictionary<string, string>>(json);
    }

    public async Task UpdateNamespacePropertiesAsync(
        Namespace ns,
        IReadOnlyList<string>? removals = null,
        IReadOnlyDictionary<string, string>? updates = null,
        CancellationToken ct = default)
    {
        var propsPath = NamespacePropertiesPath(ns);

        if (!await _fs.ExistsAsync(propsPath, ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(ns);

        var data = await _fs.ReadAllBytesAsync(propsPath, ct).ConfigureAwait(false);
        var props = IcebergJsonSerializer.Deserialize<Dictionary<string, string>>(
            Encoding.UTF8.GetString(data));

        if (removals is not null)
            foreach (var key in removals)
                props.Remove(key);

        if (updates is not null)
            foreach (var (key, value) in updates)
                props[key] = value;

        var updatedJson = IcebergJsonSerializer.Serialize(props);
        await _fs.WriteAllBytesAsync(propsPath, Encoding.UTF8.GetBytes(updatedJson), ct)
            .ConfigureAwait(false);
    }

    public async Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default)
    {
        var propsPath = NamespacePropertiesPath(ns);

        if (!await _fs.ExistsAsync(propsPath, ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(ns);

        var nsPrefix = NamespacePath(ns) + "/";

        // Check for tables and child namespaces by scanning files
        await foreach (var file in _fs.ListAsync(nsPrefix, ct).ConfigureAwait(false))
        {
            // Skip the namespace's own properties file
            if (file.Path == propsPath.Replace('\\', '/'))
                continue;

            if (file.Path.EndsWith("/version-hint.text", StringComparison.Ordinal))
                throw new NamespaceNotEmptyException(ns);

            if (file.Path.EndsWith("/.namespace-properties.json", StringComparison.Ordinal))
                throw new NamespaceNotEmptyException(ns);
        }

        await _fs.DeleteAsync(propsPath, ct).ConfigureAwait(false);
    }

    public async Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default)
    {
        return await _fs.ExistsAsync(NamespacePropertiesPath(ns), ct).ConfigureAwait(false);
    }

    // --- Table operations ---

    private static string TablePath(TableIdentifier id) =>
        $"{NamespacePath(id.Namespace)}/{id.Name}";

    private static string MetadataDir(TableIdentifier id) =>
        $"{TablePath(id)}/metadata";

    private static string VersionHintPath(TableIdentifier id) =>
        $"{MetadataDir(id)}/version-hint.text";

    private static string MetadataFilePath(TableIdentifier id, int version) =>
        $"{MetadataDir(id)}/v{version}.metadata.json";

    public async Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(
        Namespace ns, CancellationToken ct = default)
    {
        if (!await _fs.ExistsAsync(NamespacePropertiesPath(ns), ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(ns);

        var nsPrefix = NamespacePath(ns) + "/";
        var result = new List<TableIdentifier>();
        var seen = new HashSet<string>();

        await foreach (var file in _fs.ListAsync(nsPrefix, ct).ConfigureAwait(false))
        {
            if (!file.Path.EndsWith("/metadata/version-hint.text", StringComparison.Ordinal))
                continue;

            // Extract table name: nsPrefix + tableName + /metadata/version-hint.text
            var relativePath = file.Path[nsPrefix.Length..];
            var slashIdx = relativePath.IndexOf('/');
            if (slashIdx <= 0)
                continue;

            var tableName = relativePath[..slashIdx];
            if (seen.Add(tableName))
                result.Add(new TableIdentifier(ns, tableName));
        }

        return result;
    }

    public async Task<Table> CreateTableAsync(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec? spec = null,
        SortOrder? sortOrder = null,
        string? location = null,
        IReadOnlyDictionary<string, string>? properties = null,
        CancellationToken ct = default)
    {
        if (!await _fs.ExistsAsync(NamespacePropertiesPath(identifier.Namespace), ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(identifier.Namespace);

        var versionHint = VersionHintPath(identifier);
        if (await _fs.ExistsAsync(versionHint, ct).ConfigureAwait(false))
            throw new TableAlreadyExistsException(identifier);

        var tableLocation = location ?? TablePath(identifier);
        var metadata = TableMetadata.Create(schema, spec, sortOrder, tableLocation, properties);

        await WriteMetadataAsync(identifier, metadata, 1, ct).ConfigureAwait(false);

        return new Table(identifier, metadata);
    }

    public async Task<Table> LoadTableAsync(
        TableIdentifier identifier, CancellationToken ct = default)
    {
        var metadata = await ReadCurrentMetadataAsync(identifier, ct).ConfigureAwait(false);
        return new Table(identifier, metadata);
    }

    public async Task DropTableAsync(
        TableIdentifier identifier, bool purge = false, CancellationToken ct = default)
    {
        var versionHint = VersionHintPath(identifier);
        if (!await _fs.ExistsAsync(versionHint, ct).ConfigureAwait(false))
            throw new NoSuchTableException(identifier);

        if (purge)
        {
            var tablePrefix = TablePath(identifier) + "/";
            await foreach (var file in _fs.ListAsync(tablePrefix, ct).ConfigureAwait(false))
                await _fs.DeleteAsync(file.Path, ct).ConfigureAwait(false);
        }
        else
        {
            await _fs.DeleteAsync(versionHint, ct).ConfigureAwait(false);
        }
    }

    public async Task RenameTableAsync(
        TableIdentifier from, TableIdentifier to, CancellationToken ct = default)
    {
        var currentMetadata = await ReadCurrentMetadataAsync(from, ct).ConfigureAwait(false);

        if (!await _fs.ExistsAsync(NamespacePropertiesPath(to.Namespace), ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(to.Namespace);

        if (await _fs.ExistsAsync(VersionHintPath(to), ct).ConfigureAwait(false))
            throw new TableAlreadyExistsException(to);

        // Write metadata to new location, then drop old
        var newLocation = TablePath(to);
        var updatedMetadata = currentMetadata with { Location = newLocation };
        await WriteMetadataAsync(to, updatedMetadata, 1, ct).ConfigureAwait(false);

        // Remove old version hint (soft drop)
        await _fs.DeleteAsync(VersionHintPath(from), ct).ConfigureAwait(false);
    }

    public async Task<bool> TableExistsAsync(
        TableIdentifier identifier, CancellationToken ct = default)
    {
        return await _fs.ExistsAsync(VersionHintPath(identifier), ct).ConfigureAwait(false);
    }

    public async Task<Table> UpdateTableAsync(
        TableIdentifier identifier, TableMetadata newMetadata, CancellationToken ct = default)
    {
        var version = await ReadVersionAsync(identifier, ct).ConfigureAwait(false);
        await WriteMetadataAsync(identifier, newMetadata, version + 1, ct).ConfigureAwait(false);
        return new Table(identifier, newMetadata);
    }

    // --- Internal helpers ---

    private async ValueTask<int> ReadVersionAsync(TableIdentifier identifier, CancellationToken ct)
    {
        var versionHint = VersionHintPath(identifier);
        if (!await _fs.ExistsAsync(versionHint, ct).ConfigureAwait(false))
            throw new NoSuchTableException(identifier);

        var data = await _fs.ReadAllBytesAsync(versionHint, ct).ConfigureAwait(false);
        return int.Parse(Encoding.UTF8.GetString(data).Trim());
    }

    private async ValueTask<TableMetadata> ReadCurrentMetadataAsync(
        TableIdentifier identifier, CancellationToken ct)
    {
        var version = await ReadVersionAsync(identifier, ct).ConfigureAwait(false);
        var metadataPath = MetadataFilePath(identifier, version);

        var data = await _fs.ReadAllBytesAsync(metadataPath, ct).ConfigureAwait(false);
        var json = Encoding.UTF8.GetString(data);
        return IcebergJsonSerializer.Deserialize<TableMetadata>(json);
    }

    private async ValueTask WriteMetadataAsync(
        TableIdentifier identifier, TableMetadata metadata, int version, CancellationToken ct)
    {
        var metadataPath = MetadataFilePath(identifier, version);
        var json = IcebergJsonSerializer.Serialize(metadata);
        await _fs.WriteAllBytesAsync(metadataPath, Encoding.UTF8.GetBytes(json), ct)
            .ConfigureAwait(false);

        // Update version hint
        var versionHintPath = VersionHintPath(identifier);
        await _fs.WriteAllBytesAsync(versionHintPath, Encoding.UTF8.GetBytes(version.ToString()), ct)
            .ConfigureAwait(false);
    }
}
