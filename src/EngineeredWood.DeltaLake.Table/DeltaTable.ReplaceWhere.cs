// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.DeltaLake.Table;

public sealed partial class DeltaTable
{
    /// <summary>
    /// Replaces the rows matching <paramref name="predicate"/> with <paramref name="batches"/>, in one commit:
    /// Spark's <c>replaceWhere</c> (<c>INSERT INTO … REPLACE WHERE</c>) on any column, not only partition columns.
    /// The usual use is reprocessing a window, such as a week of dates, on a table not partitioned by it.
    ///
    /// <para>The table's rows the predicate is TRUE for are deleted as <see cref="DeleteAsync(Expressions.Predicate, CancellationToken)"/>
    /// deletes them: a file no row of which can match is skipped unread, one every row of which matches is removed
    /// whole, and one that matches in part gets a deletion vector where the table enables them and is otherwise
    /// rewritten without the matched rows. A row the predicate is NULL for is kept. The new rows are appended in
    /// the same commit, so no reader sees the window empty.</para>
    ///
    /// <para>Every new row must satisfy the predicate, or nothing is written and
    /// <see cref="DeltaTableErrorCodes.ReplaceWhereMismatch"/> is raised: a row it is false or NULL for would land
    /// outside the window, where replacing the window again would not remove it. The rows are checked as the table
    /// will store them, generated columns included. Spark checks the same way by default.</para>
    ///
    /// <para>The commit records the operation <c>WRITE</c>, as Spark's does, with Spark's metrics for it, and is
    /// not a blind append. The predicate is its read set: a concurrent commit that adds a file it could match, or
    /// removes one it read, is a conflict, precise to the isolation level. An append-only table is refused even
    /// when no row matches, as Spark refuses it. On a Change Data Feed table the replaced rows are recorded as
    /// <c>delete</c> and the new ones as <c>insert</c>.</para>
    /// </summary>
    /// <param name="batches">The new rows. None at all deletes the window and adds nothing.</param>
    /// <param name="predicate">The window: which rows are replaced, and which new rows are allowed.</param>
    /// <param name="cancellationToken">Cancels the read, the writes or the commit.</param>
    /// <param name="commitProperties">The caller's own metadata for the commit.</param>
    /// <returns>The committed version.</returns>
    /// <exception cref="DeltaFormatException">A new row does not satisfy the predicate
    /// (<see cref="DeltaTableErrorCodes.ReplaceWhereMismatch"/>), the predicate names a column the table does not
    /// have (<see cref="DeltaTableErrorCodes.UnresolvedColumn"/>), or the table is append-only
    /// (<see cref="DeltaTableErrorCodes.CannotModifyAppendOnly"/>).</exception>
    /// <exception cref="DeltaConflictException">A concurrent commit invalidated what this read.</exception>
    public async ValueTask<long> OverwriteWhereAsync(
        IReadOnlyList<RecordBatch> batches,
        Expressions.Predicate predicate,
        CancellationToken cancellationToken = default,
        CommitProperties? commitProperties = null)
    {
        ThrowIfDisposed();
        // As DELETE commits: through the optimistic-concurrency loop, and an abort takes back what was written.
        await using var transaction = StartTransaction();
        transaction.CommitProperties = commitProperties;
        await transaction.OverwriteWhereAsync(batches, predicate, cancellationToken).ConfigureAwait(false);
        return await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Refuses <paramref name="predicate"/> as a replaceWhere window over <paramref name="snapshot"/>'s table before
    /// anything is read or written: a column it names that the table does not have, or types Spark's analyzer
    /// would refuse. Spark refuses both at analysis (<c>UNRESOLVED_COLUMN</c>,
    /// <c>DATATYPE_MISMATCH</c>), even on a table with no rows.
    /// </summary>
    internal static void RefuseReplaceWhere(Expressions.Predicate predicate, Snapshot.Snapshot snapshot) =>
        RefuseUnanalyzable(predicate, snapshot);

    /// <summary>
    /// A copy of a batch being written, as an expression over the table's logical columns should see it: renamed to
    /// logical names (logical-first, physical-second at every level, the binding the write itself gives it, so a
    /// column the caller named by its column-mapping physical name keeps its values), then with every column or
    /// nullable struct field it leaves out backfilled as the NULL it reads back as. The batch written is not changed.
    /// </summary>
    /// <remarks>
    /// The rename has to come first: the backfill drops a column the schema does not name, and backfills the logical
    /// one as NULL, so a physical alias of <c>v = -1</c> passed <c>v IS NULL OR v &gt; 0</c>.
    /// </remarks>
    internal static RecordBatch AsStored(RecordBatch batch, Snapshot.Snapshot snapshot)
    {
        var logical = DeltaLake.Schema.ColumnMappingRecursive.ToLogical(
            batch, snapshot.Schema, DeltaLake.Schema.ColumnMapping.GetMode(snapshot.Metadata.Configuration),
            inputIsPhysical: false);
        return SchemaEvolution.BackfillMissingColumns(logical, snapshot.ArrowSchema.FieldsList);
    }

    /// <summary>
    /// Refuses replaceWhere rows the predicate is not TRUE for: false, or NULL, as a CHECK constraint refuses them
    /// and as Spark's replaceWhere constraint check does. A write may leave out a nullable column, or a nullable
    /// struct field, which then reads back as NULL; the predicate sees it so, from <see cref="AsStored"/>'s copy of
    /// <paramref name="batch"/>. The batch written is not changed.
    /// </summary>
    private static void ThrowIfOutsideReplaceWhere(
        RecordBatch batch, Snapshot.Snapshot snapshot, Expressions.Predicate predicate)
    {
        var asStored = AsStored(batch, snapshot);
        long outside = 0;
        using (var mask = RowEvaluator.EvaluatePredicate(predicate, asStored))
        {
            for (int row = 0; row < asStored.Length; row++)
            {
                if (mask.GetValue(row) is not true)
                    outside++;
            }
        }

        if (outside > 0)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.ReplaceWhereMismatch,
                $"Written data does not conform to the replaceWhere condition {predicate}: {outside} of the new rows "
                + "do not satisfy it (a row it is null for does not either). Nothing was written.");
        }
    }
}
