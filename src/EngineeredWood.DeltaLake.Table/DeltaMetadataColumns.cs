// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The names of the per-row metadata columns a read can append — the single place they are spelled. Each is
/// a SUFFIX applied to a caller-chosen prefix (<see cref="DeltaReadOptions.MetadataPrefix"/>), so a table
/// whose own columns collide with the defaults can move them out of the way.
///
/// <para>The default prefix matches Spark's <c>_metadata</c> struct, and under it the emitted names are
/// exactly <c>_metadata.file_path</c>, <c>_metadata.row_index</c>, <c>_metadata.row_id</c> and
/// <c>_metadata.row_commit_version</c> — the names Spark exposes for the same four values.
/// <see cref="DeltaRowMetadata.RowAddress"/> is the exception: it is engineered-wood's own
/// snapshot-scoped address with no Spark counterpart, so it keeps its unprefixed
/// <see cref="TransientRowAddress.ColumnName"/> rather than borrowing a name that would read as a promise
/// of durability it cannot keep.</para>
/// </summary>
public static class DeltaMetadataColumns
{
    /// <summary>The default metadata prefix, matching Spark's <c>_metadata</c> struct.</summary>
    public const string DefaultPrefix = "_metadata.";

    /// <summary>The file's <c>add.path</c>, exactly as the snapshot records it (Utf8, non-null).</summary>
    public const string FilePathSuffix = "file_path";

    /// <summary>The row's ABSOLUTE in-file position — the parquet row index, counting rows a deletion vector
    /// hides, per Spark's <c>_metadata.row_index</c> semantics (Int64, non-null).</summary>
    public const string RowIndexSuffix = "row_index";

    /// <summary>Delta row tracking's STABLE row id (Int64, nullable — a file predating row tracking on the
    /// table carries no <c>baseRowId</c>, so its rows have no derivable id).</summary>
    public const string RowIdSuffix = "row_id";

    /// <summary>The version that last changed the row (Int64, nullable, on the same terms as
    /// <see cref="RowIdSuffix"/>).</summary>
    public const string RowCommitVersionSuffix = "row_commit_version";
}
