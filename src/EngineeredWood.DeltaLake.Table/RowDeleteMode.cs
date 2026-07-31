// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// How <see cref="DeltaTable.DeleteRowsAsync"/> removes the selected rows. Both produce the same logical
/// result; they differ in what they write and in what a reader must support.
/// </summary>
public enum RowDeleteMode
{
    /// <summary>
    /// Write a deletion vector: <c>remove</c>(old file + old DV) + <c>add</c>(the SAME file, new DV). No data
    /// is rewritten, so this is cheap and — with <c>rowLevelRetry</c> — composes row-by-row with a concurrent
    /// delete of different rows in the same file. Requires <c>delta.enableDeletionVectors</c> on the table,
    /// and a reader that understands deletion vectors.
    /// </summary>
    DeletionVector,

    /// <summary>
    /// Rewrite each affected file without the selected rows, committed as plain <c>remove</c>/<c>add</c>.
    /// Works on ANY table — no deletion vectors, no row-tracking feature — so the result is maximally
    /// reader-compatible. Row tracking, where enabled, is preserved: survivors keep their materialized id and
    /// commit version.
    /// </summary>
    CopyOnWrite,
}
