// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A write goes to as few data files as <see cref="DeltaTableOptions.TargetFileSize"/> allows, however the caller
/// batched its rows (#559). It used to write one file per input batch, each with a single row group the size of
/// that batch, and <c>TargetFileSize</c> was read by nothing.
/// </summary>
public class TargetFileSizeTests : IDisposable
{
    private readonly string _tempDir;

    public TargetFileSizeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_target_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static Apache.Arrow.Schema BuildSchema(bool idNullable = false) => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, idNullable))
        .Field(new Field("part", StringType.Default, true))
        .Build();

    /// <summary>Rows <paramref name="startId"/>.. of partition values cycling through
    /// <paramref name="parts"/>.</summary>
    private static RecordBatch Batch(long startId, int count, string[]? parts = null, bool idNullable = false)
    {
        var ids = new Int64Array.Builder();
        var part = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            part.Append(parts is null ? "p" : parts[i % parts.Length]);
        }
        return new RecordBatch(BuildSchema(idNullable), [ids.Build(), part.Build()], count);
    }

    private static List<RecordBatch> Batches(int batches, int rowsEach, string[]? parts = null)
    {
        var list = new List<RecordBatch>();
        for (int b = 0; b < batches; b++)
            list.Add(Batch(b * (long)rowsEach, rowsEach, parts));
        return list;
    }

    private string[] DataFilesOnDisk() =>
        [.. Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
            .Where(p => !p.Contains("_delta_log", StringComparison.Ordinal))];

    private async Task<int> RowGroupCountAsync(string relativePath)
    {
        await using var file = await Fs.OpenReadAsync(DeltaPath.Decode(relativePath));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        return (await reader.ReadMetadataAsync()).RowGroups.Count;
    }

    private static async Task<List<long>> ReadIdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    private static List<long> Range(long start, long count) =>
        [.. Enumerable.Range(0, checked((int)count)).Select(i => start + i)];

    [Fact]
    public async Task ManySmallBatches_GoToOneFileWithOneRowGroup()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync(Batches(batches: 40, rowsEach: 250));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal(1, await RowGroupCountAsync(file.Path));
        Assert.Equal(Range(0, 10_000), await ReadIdsAsync(table));
    }

    /// <summary>The file's statistics cover every batch in it, not just one.</summary>
    [Fact]
    public async Task TheFilesStatistics_SpanEveryBatchInIt()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(500, 10), Batch(0, 10), Batch(1000, 10)]);

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        using var stats = JsonDocument.Parse(file.GetStatsJson()!);
        Assert.Equal(30, stats.RootElement.GetProperty("numRecords").GetInt64());
        Assert.Equal(0, stats.RootElement.GetProperty("minValues").GetProperty("id").GetInt64());
        Assert.Equal(1009, stats.RootElement.GetProperty("maxValues").GetProperty("id").GetInt64());
    }

    /// <summary>
    /// Batches join into row groups of at most <see cref="ParquetWriteOptions.RowGroupMaxRows"/> rows, and a file
    /// closes once it reaches the target. A target of one byte closes every file after its first row group.
    /// </summary>
    [Fact]
    public async Task AFileClosesAtTheTarget_AfterARowGroup()
    {
        var options = new DeltaTableOptions
        {
            TargetFileSize = 1,
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 1000 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), options);
        await table.WriteAsync(Batches(batches: 10, rowsEach: 250));

        var files = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        Assert.Equal(3, files.Count); // 1000 + 1000 + 500 rows
        foreach (var file in files)
            Assert.Equal(1, await RowGroupCountAsync(file.Path));
        Assert.Equal(new long[] { 500, 1000, 1000 },
            files.Select(f => JsonDocument.Parse(f.GetStatsJson()!).RootElement.GetProperty("numRecords").GetInt64())
                .OrderBy(n => n));
        Assert.Equal(Range(0, 2500), await ReadIdsAsync(table));
    }

    /// <summary>
    /// One batch larger than a row group is cut at row-group boundaries too, so the target is checked after each
    /// row group, and the files come out as they do from the same rows in small batches. Handed whole to the Parquet
    /// writer, it split into three row groups in one file.
    /// </summary>
    [Fact]
    public async Task AnOversizedBatch_IsCutAtRowGroups_AndRollsLikeSmallBatches()
    {
        var options = new DeltaTableOptions
        {
            TargetFileSize = 1,
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 1000 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), options);
        await table.WriteAsync([Batch(0, 2500)]);

        var files = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        Assert.Equal(3, files.Count);
        foreach (var file in files)
            Assert.Equal(1, await RowGroupCountAsync(file.Path));
        Assert.Equal(Range(0, 2500), await ReadIdsAsync(table));
    }

    /// <summary>Batches that straddle a row-group boundary are cut there, so every row group but the last is
    /// full.</summary>
    [Fact]
    public async Task BatchesCrossingARowGroupBoundary_FillEachRowGroupExactly()
    {
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 1000 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), options);
        await table.WriteAsync(Batches(batches: 7, rowsEach: 300));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        await using var stream = await Fs.OpenReadAsync(DeltaPath.Decode(file.Path));
        using var reader = new ParquetFileReader(stream, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(new long[] { 1000, 1000, 100 }, metadata.RowGroups.Select(g => g.NumRows));
        Assert.Equal(Range(0, 2100), await ReadIdsAsync(table));
    }

    private static readonly Decimal128Type Amount = new(10, 2);

    private static Apache.Arrow.Schema DecimalSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("amount", Amount, true))
        .Build();

    /// <summary>Rows <paramref name="startId"/>..; amount is id / 100, null on every seventh row.</summary>
    private static RecordBatch DecimalBatch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var amounts = new Decimal128Array.Builder(Amount);
        for (int i = 0; i < count; i++)
        {
            long id = startId + i;
            ids.Append(id);
            if (id % 7 == 0)
                amounts.AppendNull();
            else
                amounts.Append(id / 100m);
        }
        return new RecordBatch(DecimalSchema(), [ids.Build(), amounts.Build()], count);
    }

    /// <summary>
    /// Where a batch is cut between two files, each file's statistics describe exactly the rows it holds: row
    /// count, null count and bounds, checked against the file's own contents.
    /// </summary>
    [Fact]
    public async Task WhereABatchIsCutBetweenFiles_EachFilesStatisticsMatchItsRows()
    {
        var options = new DeltaTableOptions
        {
            TargetFileSize = 1,
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 1000 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, DecimalSchema(), options);
        await table.WriteAsync([DecimalBatch(0, 600), DecimalBatch(600, 600), DecimalBatch(1200, 600)]);

        var files = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        Assert.Equal(2, files.Count); // 1000 + 800 rows
        foreach (var file in files)
        {
            var ids = new List<long>();
            var amounts = new List<decimal?>();
            await using var stream = await Fs.OpenReadAsync(DeltaPath.Decode(file.Path));
            using var reader = new ParquetFileReader(stream, ownsFile: false);
            await foreach (var batch in reader.ReadAllAsync())
            {
                var id = (Int64Array)batch.Column("id");
                // The raw reader hands back the narrowest decimal that fits the file's column.
                var amount = batch.Column("amount");
                for (int i = 0; i < batch.Length; i++)
                {
                    ids.Add(id.GetValue(i)!.Value);
                    amounts.Add(amount switch
                    {
                        Decimal64Array d64 => d64.GetValue(i),
                        Decimal128Array d128 => d128.GetValue(i),
                        _ => throw new InvalidOperationException(amount.Data.DataType.Name),
                    });
                }
            }

            using var stats = JsonDocument.Parse(file.GetStatsJson()!);
            var root = stats.RootElement;
            Assert.Equal(ids.Count, root.GetProperty("numRecords").GetInt64());
            Assert.Equal(ids.Min(), root.GetProperty("minValues").GetProperty("id").GetInt64());
            Assert.Equal(ids.Max(), root.GetProperty("maxValues").GetProperty("id").GetInt64());
            Assert.Equal(amounts.Count(a => a is null), root.GetProperty("nullCount").GetProperty("amount").GetInt64());
            Assert.Equal(amounts.Min(), root.GetProperty("minValues").GetProperty("amount").GetDecimal());
            Assert.Equal(amounts.Max(), root.GetProperty("maxValues").GetProperty("amount").GetDecimal());
        }
    }

    /// <summary>
    /// A sliced decimal column gets its bounds from its own rows. The Decimal128 collector read the value buffer
    /// from slot 0, ignoring the slice's offset, so a caller's sliced batch got bounds from rows it never wrote, and
    /// pruning could skip the file for a value it holds.
    /// </summary>
    [Fact]
    public async Task ASlicedDecimalColumn_GetsBoundsFromItsOwnRows()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, DecimalSchema());
        await table.WriteAsync([DecimalBatch(1, 20).Slice(10, 5)]); // ids 11..15, amounts 0.11..0.15

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        using var stats = JsonDocument.Parse(file.GetStatsJson()!);
        Assert.Equal(0.11m, stats.RootElement.GetProperty("minValues").GetProperty("amount").GetDecimal());
        Assert.Equal(0.15m, stats.RootElement.GetProperty("maxValues").GetProperty("amount").GetDecimal());
    }

    /// <summary>
    /// A sliced struct column's leaf bounds come from the slice's own rows, at every depth. A struct's children are
    /// not sliced with it, and the leaf collector read the whole child: bounds from every row the batch was sliced
    /// from. Never wrong to prune by, since they contain the slice's values, but looser than the file's rows, and a
    /// write slices batches at every row-group boundary.
    /// </summary>
    [Fact]
    public async Task ASlicedStructColumn_GetsLeafBoundsFromItsOwnRows()
    {
        var innerType = new StructType([new Field("w", Int64Type.Default, true)]);
        var structType = new StructType(
        [
            new Field("v", Int64Type.Default, true),
            new Field("d", Amount, true),
            new Field("inner", innerType, true),
        ]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("s", structType, true))
            .Build();

        const int rows = 20;
        var ids = new Int64Array.Builder();
        var v = new Int64Array.Builder();
        var d = new Decimal128Array.Builder(Amount);
        var w = new Int64Array.Builder();
        for (int i = 0; i < rows; i++)
        {
            ids.Append(i);
            v.Append(i);
            d.Append(i / 100m);
            w.Append(1000 + i);
        }
        var inner = new StructArray(innerType, rows, [w.Build()], ArrowBuffer.Empty, nullCount: 0);
        var s = new StructArray(structType, rows, [v.Build(), d.Build(), inner], ArrowBuffer.Empty, nullCount: 0);
        var batch = new RecordBatch(schema, [ids.Build(), s], rows);

        await using var table = await DeltaTable.CreateAsync(Fs, schema);
        await table.WriteAsync([batch.Slice(10, 5)]); // ids 10..14

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        using var stats = JsonDocument.Parse(file.GetStatsJson()!);
        var min = stats.RootElement.GetProperty("minValues").GetProperty("s");
        var max = stats.RootElement.GetProperty("maxValues").GetProperty("s");
        Assert.Equal(10, min.GetProperty("v").GetInt64());
        Assert.Equal(14, max.GetProperty("v").GetInt64());
        Assert.Equal(0.10m, min.GetProperty("d").GetDecimal());
        Assert.Equal(0.14m, max.GetProperty("d").GetDecimal());
        Assert.Equal(1010, min.GetProperty("inner").GetProperty("w").GetInt64());
        Assert.Equal(1014, max.GetProperty("inner").GetProperty("w").GetInt64());
    }

    /// <summary>Under a large target, the same row groups share one file.</summary>
    [Fact]
    public async Task UnderTheTarget_RowGroupsShareAFile()
    {
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 1000 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), options);
        await table.WriteAsync(Batches(batches: 10, rowsEach: 250));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal(3, await RowGroupCountAsync(file.Path));
    }

    /// <summary>Each partition gets its own file, gathering its rows from every batch.</summary>
    [Fact]
    public async Task APartitionedWrite_GivesEachPartitionOneFile()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), partitionColumns: ["part"]);
        await table.WriteAsync(Batches(batches: 12, rowsEach: 100, parts: ["a", "b", "c"]));

        var files = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        Assert.Equal(3, files.Count);
        Assert.Equal(new[] { "a", "b", "c" }, files.Select(f => f.PartitionValues["part"]).OrderBy(p => p));
        Assert.Equal(Range(0, 1200), await ReadIdsAsync(table));
    }

    /// <summary>Row tracking assigns each file its own contiguous range: every row's id is distinct, and the
    /// high-water mark covers them all.</summary>
    [Fact]
    public async Task OnARowTrackingTable_EveryRowGetsADistinctId()
    {
        var options = new DeltaTableOptions
        {
            TargetFileSize = 1,
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 300 },
        };
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), options, partitionColumns: ["part"], enableRowTracking: true);
        await table.WriteAsync(Batches(batches: 8, rowsEach: 100, parts: ["a", "b"]));

        Assert.True(table.CurrentSnapshot.FileCount > 2);
        var rowIds = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var col = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
                rowIds.Add(col.GetValue(i)!.Value);
        }
        Assert.Equal(Range(0, 800), rowIds.OrderBy(id => id).ToList());
        Assert.Equal(800, table.CurrentSnapshot.RowIdHighWaterMark);
    }

    /// <summary>
    /// A batch of a different Arrow layout cannot always share the open file's Parquet schema, so it starts a new
    /// file rather than being refused. Here the first batch declares <c>id</c> non-nullable, so the file's column
    /// is required, and the next one holds a null in it, which a required column cannot store.
    /// </summary>
    [Fact]
    public async Task ABatchOfADifferentLayout_StartsANewFile()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(idNullable: true));
        var withNull = new RecordBatch(BuildSchema(idNullable: true),
            [new Int64Array.Builder().Append(10).AppendNull().Build(),
             new StringArray.Builder().Append("p").Append("p").Build()], 2);
        await table.WriteAsync([Batch(0, 10, idNullable: false), withNull, Batch(20, 10, idNullable: true)]);

        Assert.Equal(2, table.CurrentSnapshot.FileCount);
        long rows = 0, nulls = 0;
        await foreach (var batch in table.ReadAllAsync())
        {
            rows += batch.Length;
            nulls += batch.Column("id").NullCount;
        }
        Assert.Equal(22, rows);
        Assert.Equal(1, nulls);
    }

    /// <summary>In a transaction every rolled file is on the ledger, so an abort takes them all back.</summary>
    [Fact]
    public async Task InATransaction_AnAbortDeletesEveryRolledFile()
    {
        var options = new DeltaTableOptions
        {
            TargetFileSize = 1,
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 100 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), options);

        var txn = table.StartTransaction();
        await txn.WriteAsync(Batches(batches: 5, rowsEach: 100));
        Assert.Equal(5, DataFilesOnDisk().Length);
        await txn.AbortAsync();

        Assert.Empty(DataFilesOnDisk());
    }

    /// <summary>
    /// A host writer is handed each file's batches as they came, to cut its own row groups. Its output size is
    /// known only once it returns, so its files are cut by the batches' Arrow size.
    /// </summary>
    [Fact]
    public async Task AHostWriter_GetsEachFilesBatches_CutByTheirArrowSize()
    {
        var host = new RecordingWriter(Fs);
        long batchBytes = RollingDataFileWriter.EstimatedBytes(Batch(0, 100));
        var options = new DeltaTableOptions { DataFileWriter = host, TargetFileSize = 3 * batchBytes };
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), options);
        await table.WriteAsync(Batches(batches: 7, rowsEach: 100));

        Assert.Equal(new[] { 3, 3, 1 }, host.BatchCounts);
        Assert.Equal(3, table.CurrentSnapshot.FileCount);
        Assert.Equal(Range(0, 700), await ReadIdsAsync(table));
    }

    /// <summary>The host-staging write cuts files the same way, and its materialized row ids keep to their
    /// rows.</summary>
    [Fact]
    public async Task WriteDataFilesAsync_PutsTheBatchesInOneFile_KeepingMaterializedIds()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        var batches = new List<RecordBatch> { Batch(0, 3), Batch(3, 3) };

        var files = await table.WriteDataFilesAsync(
            batches, materializedRowIds: [100, 101, 102, null, 104, 105]);

        var file = Assert.Single(files);
        Assert.Equal(6, file.NumRecords);
        Assert.Equal(1, await RowGroupCountAsync(file.RelativePath));

        string matRowId = RowTrackingConfig
            .TryGetMaterializedColumnNames(table.CurrentSnapshot.Metadata.Configuration).RowIdColumnName!;
        await using var stream = await Fs.OpenReadAsync(file.RelativePath);
        using var reader = new ParquetFileReader(stream, ownsFile: false);
        var written = await reader.ReadRowGroupAsync(0);
        var ids = (Int64Array)written.Column("id");
        var rowIds = (Int64Array)written.Column(matRowId);
        var byId = Enumerable.Range(0, written.Length).ToDictionary(i => ids.GetValue(i)!.Value, i => rowIds.GetValue(i));
        Assert.Equal(new long?[] { 100, 101, 102, null, 104, 105 }, Range(0, 6).Select(id => byId[id]));
    }

    private sealed class RecordingWriter(ITableFileSystem fs) : IDataFileWriter
    {
        public List<int> BatchCounts { get; } = [];

        public async ValueTask<long> WriteAsync(
            IAsyncEnumerable<RecordBatch> batches, string relativePath, CancellationToken cancellationToken)
        {
            await using var file = await fs.CreateAsync(relativePath, cancellationToken: cancellationToken);
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            int count = 0;
            await foreach (var b in batches.WithCancellation(cancellationToken))
            {
                await writer.WriteRowGroupAsync(b, cancellationToken);
                count++;
            }
            await writer.CloseAsync(cancellationToken);
            BatchCounts.Add(count);
            return file.Position;
        }
    }
}
