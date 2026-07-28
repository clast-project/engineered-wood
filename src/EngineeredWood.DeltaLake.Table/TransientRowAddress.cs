// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The read side's row ADDRESS: a single <see cref="long"/> packing a file's path-sorted ordinal in a
/// snapshot's active set together with a row's absolute position inside that file. It is the coordinate the
/// row-level DML surface speaks — <see cref="DeltaTable.ReadAllWithRowIdsAsync"/> emits it,
/// <see cref="DeltaTable.ReadRowsByRowIdsAsync"/>, <see cref="DeltaTable.DeleteByRowIdsAsync"/> and
/// <see cref="DeltaTable.UpdateByRowIdsAsync"/> consume it — so a host can read rows, keep their addresses,
/// then mutate exactly those rows.
///
/// <para><b>An address, not an identity.</b> It says WHERE a row currently sits, and only within the snapshot
/// it came from: a concurrent append inserts a path into the sort order and renumbers every ordinal after it,
/// and any rewrite moves rows to new positions. Never persist one, never compare two from different
/// snapshots. A row's durable identity is Delta ROW TRACKING's <c>baseRowId</c>-derived stable id — a
/// different number entirely, read as a column by <see cref="DeltaTable.ReadAllWithRowTrackingAsync"/>
/// (named <see cref="DeltaLake.RowTracking.RowTrackingConfig.RowIdColumnName"/>) and reported out-of-band by
/// <see cref="DeltaTable.ReadRowsByRowIdsAsync"/>' <c>sourceRowTrackingOut</c>. Reach for that one to
/// remember a row; reach for this one to locate the rows you just read.</para>
///
/// <para>Deliberately NOT tied to the row-tracking feature: the address works on a plain table with no
/// deletion vectors and no row-tracking declared, which is the point — it is the maximally
/// reader-compatible way for a host to address rows.</para>
/// </summary>
public static class TransientRowAddress
{
    /// <summary>
    /// The column <see cref="DeltaTable.ReadAllWithRowIdsAsync"/> appends, and the default key column of
    /// <see cref="DeltaTable.UpdateByRowIdsAsync"/>. Named to stay clear of Spark's <c>_metadata</c> struct,
    /// whose <c>_metadata.row_id</c> is the STABLE row-tracking id — the same name for a different number
    /// would be read as a promise of durability this value cannot keep.
    /// </summary>
    public const string ColumnName = "_ew_row_address";

    /// <summary>
    /// Bits the in-file position occupies; the file ordinal takes the rest of the (non-negative) long.
    /// Allows ~1.1e12 rows per file and ~8.4M active files per snapshot.
    /// </summary>
    public const int PositionBits = 40;

    private const long PositionMask = (1L << PositionBits) - 1;

    /// <summary>The largest addressable in-file row position.</summary>
    public const long MaxPosition = PositionMask;

    /// <summary>The largest addressable file ordinal (the active-file count a snapshot can address).</summary>
    public const int MaxFileOrdinal = int.MaxValue >> (PositionBits - 32);

    /// <summary>Packs a file ordinal and an absolute in-file row position into one address.</summary>
    public static long Pack(int fileOrdinal, long position) =>
        ((long)fileOrdinal << PositionBits) | position;

    /// <summary>The file's ordinal in the snapshot's PATH-SORTED active set — the same ordinal
    /// <see cref="PlannedFile.FileOrdinal"/> reports.</summary>
    public static int FileOrdinal(long address) => (int)(address >> PositionBits);

    /// <summary>The row's ABSOLUTE position in its file: the parquet row index, counting rows a deletion
    /// vector hides, so repeated deletes against one file compose.</summary>
    public static long Position(long address) => address & PositionMask;
}
