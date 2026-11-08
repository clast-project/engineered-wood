// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance.Table.Indices;

/// <summary>
/// Discoverable metadata about a Lance secondary index that's currently
/// active on a dataset (i.e. created by some past transaction and not
/// later dropped).
/// </summary>
public sealed class IndexInfo
{
    /// <summary>
    /// Index name. Unique within the dataset.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Unique 16-byte UUID assigned when the index was created. Doubles as
    /// the directory name under <c>_indices/</c>.
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
    /// Fragment ids covered by the index, decoded from the index's
    /// stored Roaring bitmap (CRoaring portable variant — same flavour
    /// Lance uses for deletion vectors). Sorted ascending.
    /// </summary>
    public required IReadOnlyList<uint> FragmentIds { get; init; }

    /// <summary>
    /// Path of the index directory relative to the dataset root, e.g.
    /// <c>"_indices/{uuid}"</c>. Pass to <see cref="BTreeIndex.OpenAsync(IO.ITableFileSystem, string, CancellationToken)"/>
    /// to read the index files.
    /// </summary>
    public required string DirectoryPath { get; init; }

    /// <summary>
    /// The dataset version at which this index was built. Useful for
    /// diagnostics; the index may still cover newer fragments via the
    /// fragment bitmap above.
    /// </summary>
    public required ulong BuiltFromVersion { get; init; }
}
