// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance.Table.Indices;

/// <summary>
/// Metadata about one segment of a Lance secondary index that's active on a
/// dataset version: one entry of the manifest's index section (#991). An
/// index can have several segments, which share its <see cref="Name"/> and
/// each cover their own <see cref="FragmentIds"/>; <c>optimize_indices</c>
/// without merging adds one for the fragments appended since.
/// </summary>
public sealed class IndexInfo
{
    /// <summary>
    /// Index name, which every segment of the index shares.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Unique 16-byte UUID of this segment, assigned when it was written.
    /// Doubles as the name of its directory under <c>_indices/</c>.
    /// </summary>
    public required Guid Uuid { get; init; }

    /// <summary>
    /// The type URL from the index's <c>IndexDetails</c> Any payload,
    /// e.g. <c>"/lance.table.BTreeIndexDetails"</c>. Empty when the
    /// writer didn't set it.
    /// </summary>
    public required string TypeUrl { get; init; }

    /// <summary>
    /// Names of the columns this index covers, in declaration order: each
    /// one's dotted path (<c>s.x</c> for field <c>x</c> of struct <c>s</c>).
    /// Resolved from the <c>field_id</c> list in the IndexMetadata
    /// against the dataset's current schema. See <see cref="ColumnPaths"/>
    /// for the same paths as parts.
    /// </summary>
    public required IReadOnlyList<string> ColumnNames { get; init; }

    /// <summary>
    /// The paths of the columns this index covers, in declaration order: a
    /// top-level field, then a field of each struct below it. A field whose
    /// id the schema lacks has a one-part placeholder path,
    /// <c>&lt;field#id&gt;</c>.
    /// </summary>
    public required IReadOnlyList<IReadOnlyList<string>> ColumnPaths { get; init; }

    /// <summary>
    /// Fragment ids covered by this segment, decoded from its stored
    /// Roaring bitmap (CRoaring portable variant — same flavour Lance uses
    /// for deletion vectors). Sorted ascending. The segment says nothing
    /// about any other fragment, such as one appended after it was built.
    /// </summary>
    public required IReadOnlyList<uint> FragmentIds { get; init; }

    /// <summary>
    /// Path of the index directory, e.g. <c>"_indices/{uuid}"</c>: relative to
    /// the dataset root when <see cref="BaseId"/> is null, and otherwise to
    /// that base (a shallow clone's inherited index lies in its source). For
    /// an index under the root, pass it to
    /// <see cref="BTreeIndex.OpenAsync(IO.ITableFileSystem, string, CancellationToken)"/>;
    /// <see cref="LanceTable.OpenBTreeIndexAsync(IndexInfo, CancellationToken)"/>
    /// and <see cref="LanceTable.OpenBitmapIndexAsync(IndexInfo, CancellationToken)"/>
    /// open either kind, resolving a base through the dataset's storage resolver.
    /// </summary>
    public required string DirectoryPath { get; init; }

    /// <summary>
    /// The id of the manifest's <c>base_paths</c> entry the index lies under,
    /// or null when it lies under the dataset root.
    /// </summary>
    public uint? BaseId { get; init; }

    /// <summary>The manifest's entry for this segment; null for one this library did not read.</summary>
    internal Proto.IndexMetadata? Metadata { get; init; }

    /// <summary>The locator of the table that listed this segment.</summary>
    internal LanceFileLocator? Locator { get; init; }

    /// <summary>
    /// The dataset version this segment was built from (the manifest's
    /// <c>dataset_version</c>). Useful for diagnostics; which fragments it
    /// covers is <see cref="FragmentIds"/>.
    /// </summary>
    public required ulong BuiltFromVersion { get; init; }

    /// <summary>
    /// The file system and path of this segment's directory, resolving its base now: throws
    /// <see cref="IO.UnregisteredStorageLocationException"/> when the base is outside the dataset and not registered.
    /// </summary>
    internal ResolvedLanceFile ResolveDirectory(IO.ITableFileSystem tableFileSystem) =>
        Metadata is not null && Locator is not null
            ? Locator.ResolveIndexDirectory(Metadata, Uuid)
            : new ResolvedLanceFile(tableFileSystem, DirectoryPath);
}
