// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Which per-row metadata columns a read appends, beyond the table's own. Combinable — asking for two kinds
/// costs ONE pass, which is the point: the resolution behind all three already happens on the same read, and
/// a host wanting a mutation key AND a stable identity used to have to read the table twice to get both.
///
/// <para>Metadata columns are appended after the user columns, in the order the flags are declared below.
/// <see cref="DeltaTable.GetReadSchema"/> reports the exact result without reading anything.</para>
/// </summary>
[Flags]
public enum DeltaRowMetadata
{
    /// <summary>No metadata columns — the table's own schema, unchanged.</summary>
    None = 0,

    /// <summary>
    /// Appends <see cref="TransientRowAddress.ColumnName"/> (<c>_ew_row_address</c>), a non-null Int64
    /// packing <c>(fileOrdinal &lt;&lt; PositionBits) | absolutePosition</c>. For a host whose own rowid must
    /// be a single <c>BIGINT</c>. Snapshot-scoped: an ADDRESS, not an identity — unpack it with
    /// <see cref="RowSelection.FromRowAddresses"/>, passing the snapshot it was read from.
    ///
    /// <para>Unlike the other two this column is NOT prefixed: it has no Spark counterpart to borrow a name
    /// from.</para>
    /// </summary>
    RowAddress = 1 << 0,

    /// <summary>
    /// Appends <c>{prefix}file_path</c> (Utf8, the <c>add.path</c> exactly as the snapshot records it) and
    /// <c>{prefix}row_index</c> (Int64, ABSOLUTE — counting rows a deletion vector hides, per Spark's
    /// <c>_metadata.row_index</c> semantics). The same address as <see cref="RowAddress"/>, unpacked and
    /// path-keyed — and therefore the form the DML consumes directly:
    /// <see cref="RowSelection.FromLocatorColumns"/> takes these batches as they come.
    /// </summary>
    Locator = 1 << 1,

    /// <summary>
    /// Appends <c>{prefix}row_id</c> and <c>{prefix}row_commit_version</c> (both nullable Int64): Delta row
    /// tracking's STABLE identity, resolved per row as the file's materialized value where it has one, else
    /// <c>add.baseRowId + position</c> / <c>add.defaultRowCommitVersion</c>. Survives UPDATE, copy-on-write
    /// DELETE, OVERWRITE and compaction, and matches what a conformant engine reads for the same rows.
    ///
    /// <para>Requires <c>delta.enableRowTracking=true</c> and throws otherwise — a table that does not track
    /// identity is refused rather than served two all-null columns, which would read as "these rows have no
    /// identity" instead of "this table does not track identity".</para>
    /// </summary>
    RowTracking = 1 << 2,
}
