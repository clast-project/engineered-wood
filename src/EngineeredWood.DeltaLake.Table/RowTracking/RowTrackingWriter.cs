// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.DeltaLake.Table.RowTracking;

/// <summary>
/// Adds and strips the hidden MATERIALIZED row-tracking columns (row id + row commit version) on a
/// copy-on-write rewrite. A freshly-appended file needs NO materialized column — a row's id is
/// <c>add.baseRowId + position</c> and its version is <c>add.defaultRowCommitVersion</c>. Only rows RELOCATED
/// by a rewrite (UPDATE / compaction) carry per-row materialized values, written into the hidden physical
/// columns whose names live in table metadata
/// (<c>delta.rowTracking.materializedRowIdColumnName</c> / <c>…materializedRowCommitVersionColumnName</c>).
/// A non-null materialized value OVERRIDES the default for that row (spec).
/// </summary>
internal static class RowTrackingWriter
{
    /// <summary>
    /// Legacy internal column name a pre-1.0 writer used for the materialized row id. Retained ONLY so the
    /// strip path still recognizes such a file; new files write the metadata-declared physical name.
    /// </summary>
    public const string RowIdColumn = "__delta_row_id";

    /// <summary>Legacy internal column name for the materialized row commit version (see
    /// <see cref="RowIdColumn"/>).</summary>
    public const string RowCommitVersionColumn = "__delta_row_commit_version";

    /// <summary>
    /// Appends a materialized row-id column named <paramref name="rowIdName"/> carrying EXPLICIT per-row ids
    /// (one per row) — used on a copy-on-write rewrite so each row's ORIGINAL stable id survives being moved
    /// to a new file (a spec reader honors the materialized column over <c>baseRowId + position</c>).
    /// <paramref name="rowIds"/>.Length must equal <paramref name="batch"/>.Length.
    /// </summary>
    public static RecordBatch AddRowIdColumn(
        RecordBatch batch, Int64Array rowIds, string rowIdName, bool nullable = false)
    {
        var columns = new IArrowArray[batch.ColumnCount + 1];
        var fields = new List<Field>(batch.ColumnCount + 1);
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            columns[i] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }
        columns[batch.ColumnCount] = rowIds;
        fields.Add(new Field(rowIdName, Int64Type.Default, nullable));

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);
        return new RecordBatch(schema.Build(), columns, batch.Length);
    }

    /// <summary>
    /// Appends BOTH the materialized row-id (<paramref name="rowIdName"/>) and row-commit-version
    /// (<paramref name="rowCommitVersionName"/>) columns carrying EXPLICIT per-row values — used on a
    /// COMPACTION or UPDATE rewrite, where rows from several source files (or an in-place update) mix, so a
    /// single <c>baseRowId</c> / <c>defaultRowCommitVersion</c> on the new <c>add</c> cannot represent them.
    /// Materializing both preserves each row's ORIGINAL stable id AND commit version across the rewrite. Both
    /// arrays' length must equal <paramref name="batch"/>.Length.
    /// </summary>
    public static RecordBatch AddRowIdAndCommitVersionColumns(
        RecordBatch batch, Int64Array rowIds, Int64Array commitVersions,
        string rowIdName, string rowCommitVersionName, bool nullable = false)
    {
        // nullable: a rewrite may carry per-row NULLs (a source file predating row tracking has no derivable
        // original id — a reader then falls back to the new file's baseRowId + position, a fresh id for
        // exactly that row).
        var columns = new IArrowArray[batch.ColumnCount + 2];
        var fields = new List<Field>(batch.ColumnCount + 2);
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            columns[i] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }
        columns[batch.ColumnCount] = rowIds;
        fields.Add(new Field(rowIdName, Int64Type.Default, nullable));
        columns[batch.ColumnCount + 1] = commitVersions;
        fields.Add(new Field(rowCommitVersionName, Int64Type.Default, nullable));

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);
        return new RecordBatch(schema.Build(), columns, batch.Length);
    }

    /// <summary>
    /// Removes the hidden materialized row-tracking columns from a batch, returning the clean (user-column)
    /// batch plus the captured id / commit-version arrays (null when the column is absent). Recognizes both
    /// the metadata-declared physical names (<paramref name="rowIdName"/> /
    /// <paramref name="rowCommitVersionName"/>) and the legacy internal names, so a file written by any
    /// EngineeredWood vintage strips cleanly. When neither name is present the batch is returned unchanged.
    /// </summary>
    public static (RecordBatch Batch, Int64Array? RowIds, Int64Array? Versions) StripMaterializedColumns(
        RecordBatch batch, string? rowIdName, string? rowCommitVersionName)
    {
        Int64Array? ids = null, versions = null;
        var keepColumns = new List<IArrowArray>(batch.ColumnCount);
        var keepFields = new List<Field>(batch.ColumnCount);
        bool removedAny = false;

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            string name = batch.Schema.FieldsList[i].Name;
            if (name == RowIdColumn || (rowIdName is not null && name == rowIdName))
            {
                ids = batch.Column(i) as Int64Array;
                removedAny = true;
                continue;
            }
            if (name == RowCommitVersionColumn || (rowCommitVersionName is not null && name == rowCommitVersionName))
            {
                versions = batch.Column(i) as Int64Array;
                removedAny = true;
                continue;
            }
            keepColumns.Add(batch.Column(i));
            keepFields.Add(batch.Schema.FieldsList[i]);
        }

        if (!removedAny)
            return (batch, null, null);

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in keepFields)
            schema.Field(f);
        return (new RecordBatch(schema.Build(), keepColumns, batch.Length), ids, versions);
    }

    /// <summary>
    /// Strips the legacy internal <c>__delta_row_id</c> column from a batch (back-compat shim over
    /// <see cref="StripMaterializedColumns"/>); returns the batch without it plus the row-id array.
    /// </summary>
    public static (RecordBatch Batch, Int64Array? RowIds) StripRowIdColumn(RecordBatch batch)
    {
        var (clean, ids, _) = StripMaterializedColumns(batch, null, null);
        return (clean, ids);
    }
}
