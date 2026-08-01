// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.ChangeDataFeed;

/// <summary>
/// Writes Change Data Feed (CDC) files during operations that modify data.
/// CDC files are stored in <c>_change_data/</c> and contain rows with an
/// additional <c>_change_type</c> column.
/// </summary>
internal static class CdfWriter
{
    /// <summary>
    /// Writes a CDC file for a set of changed rows (given in the table's LOGICAL shape) and returns the
    /// <see cref="CdcFile"/> action. Under column mapping the row bytes are renamed to PHYSICAL names + parquet
    /// field ids first, so the <c>_change_data</c> file follows the table's data-file layout exactly (a
    /// spec reader — Spark's <c>table_changes</c>, delta-kernel — resolves the feed through the same mapping).
    /// The synthetic <c>_change_type</c> column is added AFTER the rename, so it stays an unmapped, plainly-named
    /// column. Partition columns are NOT stored in the file bytes (data-file convention) — they ride on the
    /// returned action's <paramref name="partitionValues"/> and the reader re-materializes them.
    /// <para>On a ROW TRACKING table the two hidden materialized columns go in as well, carrying
    /// <paramref name="rowIds"/> / <paramref name="rowCommitVersions"/> — see
    /// <see cref="AddMaterializedRowTracking"/> for why a cdc file cannot do without them.</para>
    /// </summary>
    /// <param name="written">Records this change file, so a transaction that stages it and never commits can
    /// take it back. Null wherever the caller is not a transaction.</param>
    public static async ValueTask<CdcFile> WriteAsync(
        ITableFileSystem fs,
        EngineeredWood.DeltaLake.Snapshot.Snapshot snapshot,
        RecordBatch rows,
        string changeType,
        IReadOnlyDictionary<string, string> partitionValues,
        ParquetWriteOptions? parquetOptions,
        CancellationToken cancellationToken,
        Int64Array? rowIds = null,
        Int64Array? rowCommitVersions = null,
        WrittenFileLedger? written = null)
    {
        // Partition columns never live in the file bytes — they ride on the action's partitionValues and the
        // reader re-materializes them POSITIONALLY against the table schema. A caller whose rows came from the
        // read path (which materializes partition columns) would otherwise write them into the file, and the
        // reader would both duplicate the partition column and shift every column after it out of the feed.
        rows = Partitioning.PartitionUtils.RemovePartitionColumns(rows, snapshot.Metadata.PartitionColumns);

        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        if (mappingMode != ColumnMappingMode.None)
            rows = ColumnMappingRecursive.ToPhysical(rows, snapshot.Schema, mappingMode);

        // Row-tracking identity, under the declared physical names — after the rename (the names are already
        // physical) and before _change_type, which is the layout Spark writes.
        rows = AddMaterializedRowTracking(rows, snapshot, rowIds, rowCommitVersions);

        // Add _change_type column (unmapped — added after the physical rename)
        var batchWithChangeType = AddRunEncodedChangeTypeColumn(rows, changeType);

        string fileName = $"{CdfConfig.ChangeDataDir}/{Guid.NewGuid():N}.parquet";
        // Recorded before the write, so a write that fails part-way still leaves a deletable path behind.
        written?.Record(fileName);
        long fileSize;

        await using (var file = await fs.CreateAsync(
            fileName, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            await using var writer = new ParquetFileWriter(
                file, ownsFile: false, parquetOptions);
            await writer.WriteRowGroupAsync(batchWithChangeType, cancellationToken)
                .ConfigureAwait(false);
            await writer.DisposeAsync().ConfigureAwait(false);
            fileSize = file.Position;
        }

        return new CdcFile
        {
            Path = EngineeredWood.DeltaLake.DeltaPath.Encode(fileName),
            PartitionValues = CopyDict(partitionValues),
            Size = fileSize,
            DataChange = false,
        };
    }

    /// <summary>
    /// Puts each change row's STABLE row id + row commit version into the table's declared hidden
    /// materialized columns, when the table tracks row identity. A no-op otherwise, so a table without row
    /// tracking writes exactly the file it always did.
    ///
    /// <para>Materializing is the ONLY way identity reaches a change file: a <c>cdc</c> action has no
    /// <c>baseRowId</c> / <c>defaultRowCommitVersion</c> fields (unlike <c>add</c> and <c>remove</c>), so
    /// there is no positional fallback for a reader to resolve against. Absent these columns a change row
    /// simply has no id.</para>
    ///
    /// <para>MEASURED against Spark 4.0.1 / delta-spark 4.0.0 on a row-tracking + CDF table: its change files
    /// carry both columns, positioned after the user columns and before <c>_change_type</c> — the layout
    /// written here. Spark leaves a value NULL where the default would supply it (an <c>update_postimage</c>
    /// row's commit version, which is the version being committed); engineered-wood writes that version
    /// explicitly instead, which resolves to the same number without depending on a reader implementing the
    /// fallback. A null is still written where a value is genuinely unknown — a row from a file predating
    /// row tracking has no original id to carry.</para>
    ///
    /// <para>Both columns are written whenever the table declares them, even with no values supplied (the
    /// host seam <c>DeltaTable.WriteChangeDataFileAsync</c> may not have ids in hand), so every change file
    /// in a version has the same schema. A reader unioning them then sees one shape rather than two.</para>
    /// </summary>
    private static RecordBatch AddMaterializedRowTracking(
        RecordBatch rows,
        EngineeredWood.DeltaLake.Snapshot.Snapshot snapshot,
        Int64Array? rowIds,
        Int64Array? rowCommitVersions)
    {
        if (!RowTrackingConfig.IsEnabled(snapshot.Metadata.Configuration))
            return rows;

        var (rowIdName, rowVerName) =
            RowTrackingConfig.TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        if (rowIdName is null || rowVerName is null)
        {
            // A spec-invalid table declaring row tracking without its column names. Writing under a guessed
            // name would put an unreadable column in the file; the rewrite paths refuse such a table outright
            // (DeltaTable.RejectRowTrackingWrite) and the feed follows suit by carrying no identity.
            return rows;
        }

        if (rowIds is not null && rowIds.Length != rows.Length)
            throw new ArgumentException(
                $"Row-tracking ids for a change file must have one value per row: got {rowIds.Length} for "
                + $"{rows.Length} rows.", nameof(rowIds));
        if (rowCommitVersions is not null && rowCommitVersions.Length != rows.Length)
            throw new ArgumentException(
                $"Row-tracking commit versions for a change file must have one value per row: got "
                + $"{rowCommitVersions.Length} for {rows.Length} rows.", nameof(rowCommitVersions));

        return RowTracking.RowTrackingWriter.AddRowIdAndCommitVersionColumns(
            rows,
            rowIds ?? AllNullInt64(rows.Length),
            rowCommitVersions ?? AllNullInt64(rows.Length),
            rowIdName, rowVerName, nullable: true);
    }

    /// <summary>An all-null Int64 column of <paramref name="length"/> rows — "this row's identity is not
    /// known", which is distinct from the column being absent only in that the file's schema stays uniform.
    /// </summary>
    private static Int64Array AllNullInt64(int length) =>
        (Int64Array)ArrowCompute.MakeNullArray(Int64Type.Default, length);

    /// <summary>
    /// Adds a <c>_change_type</c> column to a RecordBatch as a plain string column — one value slot per
    /// row. This is the shape a CALLER sees: <see cref="CdfReader"/> uses it to synthesize the column on
    /// the read path, where the batch is handed to whoever asked for the feed and must carry the type the
    /// Delta spec names.
    /// </summary>
    public static RecordBatch AddChangeTypeColumn(RecordBatch batch, string changeType)
    {
        // One change type per batch, so the column is constant: the UTF-8 bytes are tiled across it rather
        // than appended a row at a time. Repeat also allocates no validity buffer, which suits a column the
        // field below declares non-nullable.
        var changeTypeColumn = ArrowCompute.Repeat(
            StringType.Default, System.Text.Encoding.UTF8.GetBytes(changeType), batch.Length);

        return AppendChangeType(batch, changeTypeColumn, StringType.Default);
    }

    /// <summary>
    /// The write-path form of the same column: one RUN rather than one value per row, so a million-row
    /// change batch spends ~32 bytes on <c>_change_type</c> instead of the 24 MB of tiled UTF-8 and offsets
    /// that <see cref="AddChangeTypeColumn"/> materializes.
    ///
    /// <para>Nothing outside this method sees the difference. The batch goes straight to the Parquet
    /// writer, which maps a run-end encoded column to its VALUE type — so the file holds a plain required
    /// UTF8 column, byte for byte what the tiled form produced, and Spark and delta-rs read the feed
    /// through the same spec-defined column they always did. That containment is the reason the read path
    /// keeps the plain form: its batches go to callers, whose schema expectations we do not get to
    /// change.</para>
    /// </summary>
    private static RecordBatch AddRunEncodedChangeTypeColumn(RecordBatch batch, string changeType)
    {
        var changeTypeColumn = RunEndEncoding.Constant(
            StringType.Default, System.Text.Encoding.UTF8.GetBytes(changeType), batch.Length);

        return AppendChangeType(batch, changeTypeColumn, changeTypeColumn.Data.DataType);
    }

    /// <summary>Appends the <c>_change_type</c> column and its field to a batch, preserving the rest.</summary>
    private static RecordBatch AppendChangeType(
        RecordBatch batch, IArrowArray changeTypeColumn, IArrowType changeTypeFieldType)
    {
        var columns = new IArrowArray[batch.ColumnCount + 1];
        var fields = new List<Field>(batch.ColumnCount + 1);

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            columns[i] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }

        columns[batch.ColumnCount] = changeTypeColumn;
        fields.Add(new Field(CdfConfig.ChangeTypeColumn, changeTypeFieldType, false));

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);

        return new RecordBatch(schema.Build(), columns, batch.Length);
    }

    private static Dictionary<string, string> CopyDict(IReadOnlyDictionary<string, string> source)
    {
        var result = new Dictionary<string, string>();
        foreach (var kvp in source)
            result[kvp.Key] = kvp.Value;
        return result;
    }
}
