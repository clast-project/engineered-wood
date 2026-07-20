// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// Defines the operations for managing Iceberg namespaces and tables.
/// Implementations provide the storage and coordination layer for catalog metadata.
/// </summary>
public interface ICatalog
{
    // Namespace operations

    /// <summary>Lists child namespaces under the given parent, or top-level namespaces if <paramref name="parent"/> is <see langword="null"/>.</summary>
    /// <param name="parent">The parent namespace, or <see langword="null"/> for top-level.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The list of matching namespaces.</returns>
    Task<IReadOnlyList<Namespace>> ListNamespacesAsync(
        Namespace? parent = null, CancellationToken ct = default);

    /// <summary>Creates a new namespace with optional properties.</summary>
    /// <param name="ns">The namespace to create.</param>
    /// <param name="properties">Optional key-value properties to associate with the namespace.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="NamespaceAlreadyExistsException">The namespace already exists.</exception>
    Task CreateNamespaceAsync(
        Namespace ns,
        IReadOnlyDictionary<string, string>? properties = null,
        CancellationToken ct = default);

    /// <summary>Returns the properties associated with a namespace.</summary>
    /// <param name="ns">The namespace to query.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The namespace properties.</returns>
    /// <exception cref="NoSuchNamespaceException">The namespace does not exist.</exception>
    Task<IReadOnlyDictionary<string, string>> GetNamespacePropertiesAsync(
        Namespace ns, CancellationToken ct = default);

    /// <summary>Removes and/or sets properties on an existing namespace.</summary>
    /// <param name="ns">The namespace to update.</param>
    /// <param name="removals">Property keys to remove.</param>
    /// <param name="updates">Property key-value pairs to set or overwrite.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="NoSuchNamespaceException">The namespace does not exist.</exception>
    Task UpdateNamespacePropertiesAsync(
        Namespace ns,
        IReadOnlyList<string>? removals = null,
        IReadOnlyDictionary<string, string>? updates = null,
        CancellationToken ct = default);

    /// <summary>Drops an existing namespace.</summary>
    /// <param name="ns">The namespace to drop.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="NoSuchNamespaceException">The namespace does not exist.</exception>
    /// <exception cref="NamespaceNotEmptyException">The namespace contains tables or child namespaces.</exception>
    Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default);

    /// <summary>Returns whether the given namespace exists in the catalog.</summary>
    /// <param name="ns">The namespace to check.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns><see langword="true"/> if the namespace exists; otherwise <see langword="false"/>.</returns>
    Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default);

    // Table operations

    /// <summary>Lists all tables in the given namespace.</summary>
    /// <param name="ns">The namespace to list tables from.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The list of table identifiers in the namespace.</returns>
    /// <exception cref="NoSuchNamespaceException">The namespace does not exist.</exception>
    Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(
        Namespace ns, CancellationToken ct = default);

    /// <summary>Creates a new Iceberg table with the given schema and optional configuration.</summary>
    /// <param name="identifier">The fully-qualified table identifier.</param>
    /// <param name="schema">The table schema.</param>
    /// <param name="spec">The partition spec, or <see langword="null"/> for unpartitioned.</param>
    /// <param name="sortOrder">The sort order, or <see langword="null"/> for unsorted.</param>
    /// <param name="location">An explicit table location, or <see langword="null"/> to use the catalog default.</param>
    /// <param name="properties">Optional table properties.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The newly created <see cref="Table"/>.</returns>
    /// <exception cref="TableAlreadyExistsException">A table with the same identifier already exists.</exception>
    Task<Table> CreateTableAsync(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec? spec = null,
        SortOrder? sortOrder = null,
        string? location = null,
        IReadOnlyDictionary<string, string>? properties = null,
        CancellationToken ct = default);

    /// <summary>Loads an existing table by its identifier.</summary>
    /// <param name="identifier">The table to load.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The loaded <see cref="Table"/>.</returns>
    /// <exception cref="NoSuchTableException">The table does not exist.</exception>
    Task<Table> LoadTableAsync(
        TableIdentifier identifier, CancellationToken ct = default);

    /// <summary>Drops a table from the catalog.</summary>
    /// <param name="identifier">The table to drop.</param>
    /// <param name="purge">If <see langword="true"/>, delete the underlying data files as well.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="NoSuchTableException">The table does not exist.</exception>
    Task DropTableAsync(
        TableIdentifier identifier, bool purge = false, CancellationToken ct = default);

    /// <summary>Renames a table, optionally moving it to a different namespace.</summary>
    /// <param name="from">The current table identifier.</param>
    /// <param name="to">The new table identifier.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="NoSuchTableException">The source table does not exist.</exception>
    /// <exception cref="TableAlreadyExistsException">A table with the target identifier already exists.</exception>
    Task RenameTableAsync(
        TableIdentifier from, TableIdentifier to, CancellationToken ct = default);

    /// <summary>Returns whether a table with the given identifier exists in the catalog.</summary>
    /// <param name="identifier">The table to check.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns><see langword="true"/> if the table exists; otherwise <see langword="false"/>.</returns>
    Task<bool> TableExistsAsync(
        TableIdentifier identifier, CancellationToken ct = default);

    /// <summary>Commits new metadata for an existing table, creating a new metadata version.</summary>
    /// <param name="identifier">The table to update.</param>
    /// <param name="newMetadata">The replacement <see cref="TableMetadata"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The updated <see cref="Table"/>.</returns>
    /// <exception cref="NoSuchTableException">The table does not exist.</exception>
    Task<Table> UpdateTableAsync(
        TableIdentifier identifier, TableMetadata newMetadata, CancellationToken ct = default);
}
