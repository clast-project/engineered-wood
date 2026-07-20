// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
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
    /// Writes a CDC file for a set of changed rows and returns the <see cref="CdcFile"/> action.
    /// </summary>
    public static async ValueTask<CdcFile> WriteAsync(
        ITableFileSystem fs,
        RecordBatch rows,
        string changeType,
        IReadOnlyDictionary<string, string> partitionValues,
        ParquetWriteOptions? parquetOptions,
        CancellationToken cancellationToken)
    {
        // Add _change_type column
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
