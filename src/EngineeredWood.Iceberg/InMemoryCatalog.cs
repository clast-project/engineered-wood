// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// An <see cref="ICatalog"/> implementation that stores all namespace and table metadata
/// in memory. Useful for testing and short-lived scenarios where persistence is not required.
/// </summary>
public sealed class InMemoryCatalog : ICatalog
{
    private readonly string _warehouseLocation;
    private readonly object _lock = new();
    private readonly Dictionary<Namespace, Dictionary<string, string>> _namespaces = new();
    // Each stored version carries a catalog-unique token, so a commit can tell whether the table is still at
    // the version it was based on (and a dropped and re-created table never reuses one).
    private readonly Dictionary<TableIdentifier, (TableMetadata Metadata, string Location)> _tables = new();
    private long _nextVersion;

    // Unique to this instance, so a table loaded from another InMemoryCatalog never matches a token here.
    private readonly string _instanceId = Guid.NewGuid().ToString("N");

    private string NextLocation(TableIdentifier identifier) =>
        $"{_warehouseLocation}/{identifier.Namespace}/{identifier.Name}/metadata/{_instanceId}-{++_nextVersion}";

    /// <summary>Initializes a new <see cref="InMemoryCatalog"/> with the given warehouse location URI.</summary>
    /// <param name="warehouseLocation">The base URI used to construct table locations.</param>
    public InMemoryCatalog(string warehouseLocation = "memory://warehouse")
    {
        _warehouseLocation = warehouseLocation;
    }

    public Task<IReadOnlyList<Namespace>> ListNamespacesAsync(
        Namespace? parent = null, CancellationToken ct = default)
    {
        lock (_lock)
        {
            var result = _namespaces.Keys
                .Where(ns => parent is null || parent.IsEmpty
                    ? ns.Length == 1
                    : parent.IsAncestorOf(ns) && ns.Length == parent.Length + 1)
                .ToList();

            return Task.FromResult<IReadOnlyList<Namespace>>(result);
        }
    }

    public Task CreateNamespaceAsync(
        Namespace ns,
        IReadOnlyDictionary<string, string>? properties = null,
        CancellationToken ct = default)
    {
        if (ns is null) throw new ArgumentNullException(nameof(ns));

        if (ns.IsEmpty)
            throw new ArgumentException("Cannot create empty namespace");

        lock (_lock)
        {
            if (_namespaces.ContainsKey(ns))
                throw new NamespaceAlreadyExistsException(ns);

            _namespaces[ns] = properties is not null
                ? properties.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
                : new Dictionary<string, string>();
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyDictionary<string, string>> GetNamespacePropertiesAsync(
        Namespace ns, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_namespaces.TryGetValue(ns, out var properties))
                throw new NoSuchNamespaceException(ns);

            return Task.FromResult<IReadOnlyDictionary<string, string>>(
                new Dictionary<string, string>(properties));
        }
    }

    public Task UpdateNamespacePropertiesAsync(
        Namespace ns,
        IReadOnlyList<string>? removals = null,
        IReadOnlyDictionary<string, string>? updates = null,
        CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_namespaces.TryGetValue(ns, out var properties))
                throw new NoSuchNamespaceException(ns);

            if (removals is not null)
            {
                foreach (var key in removals)
                    properties.Remove(key);
            }

            if (updates is not null)
            {
                foreach (var (key, value) in updates)
                    properties[key] = value;
            }
        }

        return Task.CompletedTask;
    }

    public Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_namespaces.ContainsKey(ns))
                throw new NoSuchNamespaceException(ns);

            // Check if namespace has tables
            if (_tables.Keys.Any(t => t.Namespace.Equals(ns)))
                throw new NamespaceNotEmptyException(ns);

            // Check if namespace has child namespaces
            if (_namespaces.Keys.Any(n => ns.IsAncestorOf(n)))
                throw new NamespaceNotEmptyException(ns);

            _namespaces.Remove(ns);
        }

        return Task.CompletedTask;
    }

    public Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default)
    {
        lock (_lock)
        {
            return Task.FromResult(_namespaces.ContainsKey(ns));
        }
    }

    public Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(
        Namespace ns, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_namespaces.ContainsKey(ns))
                throw new NoSuchNamespaceException(ns);

            var result = _tables.Keys
                .Where(t => t.Namespace.Equals(ns))
                .ToList();

            return Task.FromResult<IReadOnlyList<TableIdentifier>>(result);
        }
    }

    public Task<Table> CreateTableAsync(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec? spec = null,
        SortOrder? sortOrder = null,
        string? location = null,
        IReadOnlyDictionary<string, string>? properties = null,
        CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_namespaces.ContainsKey(identifier.Namespace))
                throw new NoSuchNamespaceException(identifier.Namespace);

            if (_tables.ContainsKey(identifier))
                throw new TableAlreadyExistsException(identifier);

            var tableLocation = location ?? $"{_warehouseLocation}/{identifier.Namespace}/{identifier.Name}";

            var metadata = TableMetadata.Create(
                schema, spec, sortOrder, tableLocation, properties);

            var stored = (Metadata: metadata, Location: NextLocation(identifier));
            _tables[identifier] = stored;

            return Task.FromResult(new Table(identifier, stored.Metadata, stored.Location));
        }
    }

    public Task<Table> LoadTableAsync(
        TableIdentifier identifier, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_tables.TryGetValue(identifier, out var stored))
                throw new NoSuchTableException(identifier);

            return Task.FromResult(new Table(identifier, stored.Metadata, stored.Location));
        }
    }

    public Task DropTableAsync(
        TableIdentifier identifier, bool purge = false, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_tables.Remove(identifier))
                throw new NoSuchTableException(identifier);
        }

        return Task.CompletedTask;
    }

    public Task RenameTableAsync(
        TableIdentifier from, TableIdentifier to, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_tables.TryGetValue(from, out var stored))
                throw new NoSuchTableException(from);

            if (!_namespaces.ContainsKey(to.Namespace))
                throw new NoSuchNamespaceException(to.Namespace);

            if (_tables.ContainsKey(to))
                throw new TableAlreadyExistsException(to);

            // The renamed table reports, and builds new files under, its new location, as FileSystemCatalog's does.
            _tables.Remove(from);
            _tables[to] = (
                stored.Metadata with { Location = $"{_warehouseLocation}/{to.Namespace}/{to.Name}" },
                NextLocation(to));
        }

        return Task.CompletedTask;
    }

    public Task<bool> TableExistsAsync(
        TableIdentifier identifier, CancellationToken ct = default)
    {
        lock (_lock)
        {
            return Task.FromResult(_tables.ContainsKey(identifier));
        }
    }

    public Task<Table> CommitTableAsync(
        Table baseTable, TableMetadata newMetadata, CancellationToken ct = default)
    {
        var identifier = baseTable.Identifier;
        if (newMetadata.TableUuid != baseTable.Metadata.TableUuid)
            throw new ArgumentException(
                $"The new metadata belongs to table {newMetadata.TableUuid}, not {baseTable.Metadata.TableUuid}.",
                nameof(newMetadata));

        // A base this catalog never issued (constructed by hand, or loaded from another catalog) is a misuse,
        // not a lost race, as in FileSystemCatalog.
        if (baseTable.MetadataLocation is not { } location ||
            location.IndexOf($"/metadata/{_instanceId}-", StringComparison.Ordinal) < 0)
            throw new ArgumentException(
                $"The base table's metadata location '{baseTable.MetadataLocation}' was not issued by this catalog; " +
                "load the table from this catalog before committing to it.",
                nameof(baseTable));

        lock (_lock)
        {
            if (!_tables.TryGetValue(identifier, out var current))
                throw new NoSuchTableException(identifier);

            if (current.Metadata.TableUuid != baseTable.Metadata.TableUuid)
                throw new CommitFailedException(identifier,
                    $"the commit is based on table {baseTable.Metadata.TableUuid}, but {identifier} is table {current.Metadata.TableUuid}.");

            if (baseTable.MetadataLocation != current.Location)
                throw new CommitFailedException(identifier,
                    $"the commit is based on version '{baseTable.MetadataLocation}', but the table is at '{current.Location}'.");

            var stored = (Metadata: newMetadata, Location: NextLocation(identifier));
            _tables[identifier] = stored;
            return Task.FromResult(new Table(identifier, stored.Metadata, stored.Location));
        }
    }
}
