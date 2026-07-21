// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
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
    /// </summary>
    public static async ValueTask<CdcFile> WriteAsync(
        ITableFileSystem fs,
        EngineeredWood.DeltaLake.Snapshot.Snapshot snapshot,
        RecordBatch rows,
        string changeType,
        IReadOnlyDictionary<string, string> partitionValues,
        ParquetWriteOptions? parquetOptions,
        CancellationToken cancellationToken)
    {
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        if (mappingMode != ColumnMappingMode.None)
            rows = ColumnMappingRecursive.ToPhysical(rows, snapshot.Schema, mappingMode);

        // Add _change_type column (unmapped — added after the physical rename)
        var batchWithChangeType = AddChangeTypeColumn(rows, changeType);

        string fileName = $"{CdfConfig.ChangeDataDir}/{Guid.NewGuid():N}.parquet";
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
    /// Adds a <c>_change_type</c> column to a RecordBatch.
    /// </summary>
    public static RecordBatch AddChangeTypeColumn(RecordBatch batch, string changeType)
    {
        var changeTypeBuilder = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            changeTypeBuilder.Append(changeType);

        var columns = new IArrowArray[batch.ColumnCount + 1];
        var fields = new List<Field>(batch.ColumnCount + 1);

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            columns[i] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }

        columns[batch.ColumnCount] = changeTypeBuilder.Build();
        fields.Add(new Field(CdfConfig.ChangeTypeColumn, StringType.Default, false));

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
