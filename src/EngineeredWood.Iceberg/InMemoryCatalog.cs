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
    private readonly Dictionary<TableIdentifier, TableMetadata> _tables = new();

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

            _tables[identifier] = metadata;

            return Task.FromResult(new Table(identifier, metadata));
        }
    }

    public Task<Table> LoadTableAsync(
        TableIdentifier identifier, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_tables.TryGetValue(identifier, out var metadata))
                throw new NoSuchTableException(identifier);

            return Task.FromResult(new Table(identifier, metadata));
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
            if (!_tables.TryGetValue(from, out var metadata))
                throw new NoSuchTableException(from);

            if (!_namespaces.ContainsKey(to.Namespace))
                throw new NoSuchNamespaceException(to.Namespace);

            if (_tables.ContainsKey(to))
                throw new TableAlreadyExistsException(to);

            _tables.Remove(from);
            _tables[to] = metadata;
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

    public Task<Table> UpdateTableAsync(
        TableIdentifier identifier, TableMetadata newMetadata, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_tables.ContainsKey(identifier))
                throw new NoSuchTableException(identifier);

            _tables[identifier] = newMetadata;
            return Task.FromResult(new Table(identifier, newMetadata));
        }
    }
}
