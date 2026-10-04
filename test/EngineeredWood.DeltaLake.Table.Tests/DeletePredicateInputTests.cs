// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using X = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A DELETE predicate must see each file's rows in the shape a scan returns them: the table's current
/// schema, with a column added after the file was written backfilled as NULL, partition columns
/// re-materialized, and widened types widened. The DV DELETE path reads data files itself, and handed
/// the predicate the file's own shape instead. A predicate on such a column then threw, or found nothing.
/// </summary>
public class DeletePredicateInputTests : IDisposable
{
    private readonly string _tempDir;

    public DeletePredicateInputTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_delinput_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Task<List<string>> ReadAsync(DeltaTable table) => ColumnMappingNameReuseTests.ReadAsync(table);

    private static Func<RecordBatch, BooleanArray> Where(string column, Func<IArrowArray, int, bool> test) => batch =>
    {
        var array = batch.Column(batch.Schema.GetFieldIndex(column));
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            mask.Append(test(array, i));
        return mask.Build();
    };

    private async Task<DeltaTable> AddedColumnTableAsync()
    {
        var v1 = new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, true)).Build();
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), v1, enableDeletionVectors: true);
        await table.WriteAsync([new RecordBatch(v1, [new Int64Array.Builder().AppendRange([1L, 2L]).Build()], 2)]);
        await table.AddColumnAsync(new Field("extra", Int64Type.Default, true));
        var v2 = new Apache.Arrow.Schema.Builder()
            .Field(v1.FieldsList[0]).Field(new Field("extra", Int64Type.Default, true)).Build();
        await table.WriteAsync([new RecordBatch(v2,
            [new Int64Array.Builder().AppendRange([3L, 4L]).Build(), new Int64Array.Builder().AppendRange([30L, 40L]).Build()],
            2)]);
        return table;
    }

    [Fact]
    public async Task Delete_FunctionPredicateOnColumnAddedLater_SeesNullForOlderFiles()
    {
        await using var table = await AddedColumnTableAsync();

        // The old file has no "extra" at all; under the current schema its rows are NULL there.
        var (deleted, _) = await table.DeleteAsync(Where("extra", (a, i) =>
            a.IsNull(i) || ((Int64Array)a).GetValue(i) == 40));

        Assert.Equal(3, deleted);
        Assert.Equal(["id=3 extra=30"], await ReadAsync(table));
    }

    [Fact]
    public async Task Delete_ExpressionPredicateOnColumnAddedLater_SeesNullForOlderFiles()
    {
        await using var table = await AddedColumnTableAsync();

        var (deleted, _) = await table.DeleteAsync(X.IsNull("extra"));

        Assert.Equal(2, deleted);
        Assert.Equal(["id=3 extra=30", "id=4 extra=40"], await ReadAsync(table));
    }

    [Fact]
    public async Task Delete_PredicateOnPartitionColumn_SeesThePartitionValue()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("region", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, partitionColumns: ["region"], enableDeletionVectors: true);
        await table.WriteAsync([new RecordBatch(schema,
            [
                new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build(),
                new StringArray.Builder().Append("us").Append("us").Append("eu").Build(),
            ], 3)]);

        // Matches part of the "us" file only, so the predicate runs row by row over that file's batch.
        var (deleted, _) = await table.DeleteAsync(batch =>
        {
            var id = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var region = (StringArray)batch.Column(batch.Schema.GetFieldIndex("region"));
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                mask.Append(region.GetString(i) == "us" && id.GetValue(i) == 1);
            return mask.Build();
        });

        Assert.Equal(1, deleted);
        Assert.Equal(["id=2 region=us", "id=3 region=eu"], await ReadAsync(table));
    }

    [Fact]
    public async Task Delete_PredicateOnWidenedColumn_SeesTheWidenedType()
    {
        // The file stores id as int32; the table has since widened it to long.
        var fs = new LocalTableFileSystem(_tempDir);
        var int32Schema = new Apache.Arrow.Schema.Builder().Field(new Field("id", Int32Type.Default, true)).Build();
        await using (var file = await fs.CreateAsync("data_int32.parquet"))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(
                int32Schema, [new Int32Array.Builder().AppendRange([10, 20, 30]).Build()], 3));
        }

        await new TransactionLog(fs).WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["typeWidening", "deletionVectors"],
                WriterFeatures = ["typeWidening", "deletionVectors"],
            },
            new MetadataAction
            {
                Id = "widened",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{"delta.typeChanges":"[{\"fromType\":\"integer\",\"toType\":\"long\"}]"}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { Schema.TypeWidening.EnableKey, "true" },
                    { DeletionVectors.DeletionVectorConfig.EnableKey, "true" },
                },
            },
            new AddFile
            {
                Path = "data_int32.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = new FileInfo(Path.Combine(_tempDir, "data_int32.parquet")).Length,
                ModificationTime = 1000,
                DataChange = true,
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        var (deleted, _) = await table.DeleteAsync(Where("id", (a, i) => ((Int64Array)a).GetValue(i) == 20));

        Assert.Equal(1, deleted);
        Assert.Equal(["id=10", "id=30"], await ReadAsync(table));
    }
}
