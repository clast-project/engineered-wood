// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.DeltaLake.Table.RowTracking;

/// <summary>
/// Adds materialized row ID columns to RecordBatches for row tracking.
/// </summary>
internal static class RowTrackingWriter
{
    /// <summary>
    /// The internal column name used in Parquet files to store materialized row IDs.
    /// </summary>
    public const string RowIdColumn = "__delta_row_id";

    /// <summary>
    /// Adds a <c>__delta_row_id</c> column to a batch with sequential IDs
    /// starting from <paramref name="baseRowId"/>.
    /// </summary>
    public static RecordBatch AddRowIdColumn(RecordBatch batch, long baseRowId)
    {
        var rowIds = new Int64Array.Builder();
        for (int i = 0; i < batch.Length; i++)
            rowIds.Append(baseRowId + i);

        var columns = new IArrowArray[batch.ColumnCount + 1];
        var fields = new List<Field>(batch.ColumnCount + 1);

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            columns[i] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }

        columns[batch.ColumnCount] = rowIds.Build();
        fields.Add(new Field(RowIdColumn, Int64Type.Default, false));

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);

        return new RecordBatch(schema.Build(), columns, batch.Length);
    }

    /// <summary>
    /// Strips the internal <c>__delta_row_id</c> column from a batch,
    /// returning the batch without it. Also returns the row ID array
    /// for use in compaction.
    /// </summary>
    public static (RecordBatch Batch, Int64Array? RowIds) StripRowIdColumn(RecordBatch batch)
    {
        int rowIdIdx = -1;
        for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            if (batch.Schema.FieldsList[i].Name == RowIdColumn)
            {
                rowIdIdx = i;
                break;
            }
        }

        if (rowIdIdx < 0)
            return (batch, null);

        var rowIds = (Int64Array)batch.Column(rowIdIdx);

        var columns = new IArrowArray[batch.ColumnCount - 1];
        var fields = new List<Field>(batch.ColumnCount - 1);
        int outIdx = 0;

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            if (i == rowIdIdx)
                continue;
            columns[outIdx] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
            outIdx++;
        }

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);

        return (new RecordBatch(schema.Build(), columns, batch.Length), rowIds);
    }

    /// <summary>
    /// Reads the materialized row ID column from a batch if present,
    /// or generates default IDs from <paramref name="baseRowId"/>.
    /// </summary>
    public static Int64Array GetOrGenerateRowIds(RecordBatch batch, long baseRowId)
    {
        // Check if the batch has a materialized row ID column
        for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            if (batch.Schema.FieldsList[i].Name == RowIdColumn)
                return (Int64Array)batch.Column(i);
        }

        // Generate default IDs
        var builder = new Int64Array.Builder();
        for (int i = 0; i < batch.Length; i++)
            builder.Append(baseRowId + i);
        return builder.Build();
    }

    /// <summary>
    /// Builds a constant Int64 array for the commit version virtual column.
    /// </summary>
    public static Int64Array BuildCommitVersionArray(long commitVersion, int length)
    {
        var builder = new Int64Array.Builder();
        for (int i = 0; i < length; i++)
            builder.Append(commitVersion);
        return builder.Build();
    }
}
