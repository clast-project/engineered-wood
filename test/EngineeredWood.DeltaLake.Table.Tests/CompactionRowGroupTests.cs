// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Compaction joins its sources' rows into row groups of the configured size, and rolls files on their actual size
/// (#572). It used to write one row group per source batch, so compacting 100 files of 10,000 rows gave one file
/// of 100 row groups of 10,000 rows.
/// </summary>
public class CompactionRowGroupTests : IDisposable
{
    private readonly string _tempDir;

    public CompactionRowGroupTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_compact_rg_{Guid.NewGuid():N}");
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

    private static Apache.Arrow.Schema Schema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append($"v{startId + i}");
        }
        return new RecordBatch(Schema, [ids.Build(), values.Build()], count);
    }

    /// <summary>Appends <paramref name="files"/> files of <paramref name="rowsEach"/> rows, ids 0.. in order.</summary>
    private static async Task AppendFilesAsync(DeltaTable table, int files, int rowsEach)
    {
        for (int f = 0; f < files; f++)
            await table.WriteAsync([Batch(f * (long)rowsEach, rowsEach)]);
    }

    private async Task<List<long>> RowGroupSizesAsync(string relativePath)
    {
        await using var file = await Fs.OpenReadAsync(DeltaPath.Decode(relativePath));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        return [.. (await reader.ReadMetadataAsync()).RowGroups.Select(rg => rg.NumRows)];
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

    private static long NumRecords(AddFile file)
    {
        using var stats = JsonDocument.Parse(file.GetStatsJson()!);
        return stats.RootElement.GetProperty("numRecords").GetInt64();
    }

    private static List<long> Range(long start, long count) =>
        [.. Enumerable.Range(0, checked((int)count)).Select(i => start + i)];

    private static readonly CompactionOptions AllFiles = new() { MinFileSize = long.MaxValue };

    [Fact]
    public async Task ManySmallFiles_CompactIntoOneRowGroup()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema);
        await AppendFilesAsync(table, files: 20, rowsEach: 500);

        Assert.NotNull(await table.CompactAsync(AllFiles));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal([10_000L], await RowGroupSizesAsync(file.Path));
        Assert.Equal(10_000, NumRecords(file));
        Assert.Equal(Range(0, 10_000), await ReadIdsAsync(table));
    }

    /// <summary>Row groups are cut at <see cref="ParquetWriteOptions.RowGroupMaxRows"/>, across source files.</summary>
    [Fact]
    public async Task RowGroups_AreCutAtRowGroupMaxRows_AcrossSourceFiles()
    {
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 1_000 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, Schema, options);
        await AppendFilesAsync(table, files: 15, rowsEach: 300); // 4,500 rows

        Assert.NotNull(await table.CompactAsync(AllFiles));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal([1_000L, 1_000, 1_000, 1_000, 500], await RowGroupSizesAsync(file.Path));
        Assert.Equal(Range(0, 4_500), await ReadIdsAsync(table));
    }

    /// <summary>
    /// A file closes once its written size reaches the target, after a row group. With a one-byte target every
    /// output file holds exactly one row group, each with stats for its own rows.
    /// </summary>
    [Fact]
    public async Task Files_RollOnTheirWrittenSize()
    {
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 1_000 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, Schema, options);
        await AppendFilesAsync(table, files: 10, rowsEach: 250); // 2,500 rows

        Assert.NotNull(await table.CompactAsync(AllFiles with { TargetFileSize = 1 }));

        var files = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        Assert.Equal(3, files.Count); // 1,000 + 1,000 + 500
        foreach (var file in files)
        {
            var groups = await RowGroupSizesAsync(file.Path);
            Assert.Equal(NumRecords(file), Assert.Single(groups));
            Assert.Equal(new FileInfo(Path.Combine(_tempDir, DeltaPath.Decode(file.Path))).Length, file.Size);
        }
        Assert.Equal(new long[] { 500, 1_000, 1_000 }, files.Select(NumRecords).OrderBy(n => n));
        Assert.Equal(Range(0, 2_500), await ReadIdsAsync(table));
    }

    /// <summary>
    /// Rolled into several files, every row keeps its original id (materialized, since rows of many sources share
    /// a file), and each file reserves its own run of fresh base row ids past the high-water mark.
    /// </summary>
    [Fact]
    public async Task RowTracking_SurvivesRollingIntoSeveralFiles()
    {
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 1_000 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, Schema, options, enableRowTracking: true);
        await AppendFilesAsync(table, files: 10, rowsEach: 250); // row ids 0..2,499
        long markBefore = RowTrackingConfig.TryReadHighWaterMark(table.GetDomainMetadata())!.Value;
        Assert.Equal(2_499, markBefore);

        Assert.NotNull(await table.CompactAsync(AllFiles with { TargetFileSize = 1 }));

        var files = table.CurrentSnapshot.ActiveFiles.Values.OrderBy(f => f.BaseRowId).ToList();
        Assert.Equal(3, files.Count);
        long expectedBase = markBefore + 1;
        foreach (var file in files)
        {
            Assert.Equal(expectedBase, file.BaseRowId);
            expectedBase += NumRecords(file);
        }
        Assert.Equal(expectedBase - 1, RowTrackingConfig.TryReadHighWaterMark(table.GetDomainMetadata()));

        // The materialized ids: the row with user id n was appended as row id n.
        var config = table.CurrentSnapshot.Metadata.Configuration!;
        string idCol = config[RowTrackingConfig.MaterializedRowIdColumnNameKey];
        var pairs = new List<(long User, long RowId)>();
        foreach (var file in files)
        {
            await using var f = await Fs.OpenReadAsync(DeltaPath.Decode(file.Path));
            using var reader = new ParquetFileReader(f, ownsFile: false);
            await foreach (var batch in reader.ReadAllAsync())
            {
                var user = (Int64Array)batch.Column("id");
                var rowIds = (Int64Array)batch.Column(idCol);
                for (int i = 0; i < batch.Length; i++)
                    pairs.Add((user.GetValue(i)!.Value, rowIds.GetValue(i)!.Value));
            }
        }
        Assert.Equal(2_500, pairs.Count);
        Assert.All(pairs, p => Assert.Equal(p.User, p.RowId));
    }

    private static RecordBatch NestedBatch(long id, bool idNullable, bool childNullable)
    {
        var x = new Int64Array.Builder().Append(id * 10).Build();
        var structType = new StructType([new Field("x", Int64Type.Default, childNullable)]);
        var s = new StructArray(structType, 1, [x], ArrowBuffer.Empty, nullCount: 0);
        var schema = new Apache.Arrow.Schema(
            [new Field("id", Int64Type.Default, idNullable), new Field("s", structType, true)], null);
        return new RecordBatch(schema, [new Int64Array.Builder().Append(id).Build(), s], 1);
    }

    private async Task<Apache.Arrow.Schema> FileSchemaAsync(string relativePath)
    {
        await using var file = await Fs.OpenReadAsync(DeltaPath.Decode(relativePath));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        return await reader.GetArrowSchemaAsync();
    }

    /// <summary>
    /// Sources whose Arrow nullability disagrees (an append that declared <c>id</c> non-nullable beside one that
    /// did not, at the top level and inside a struct) still compact into one file and one row group, labelled as
    /// the table declares the columns.
    /// </summary>
    [Fact]
    public async Task SourcesDifferingOnlyInNullability_ShareOneFile()
    {
        var tableSchema = NestedBatch(0, idNullable: true, childNullable: true).Schema;
        await using var table = await DeltaTable.CreateAsync(Fs, tableSchema);
        await table.WriteAsync([NestedBatch(1, idNullable: false, childNullable: false)]);
        await table.WriteAsync([NestedBatch(2, idNullable: true, childNullable: true)]);
        await table.WriteAsync([NestedBatch(3, idNullable: false, childNullable: true)]);

        Assert.NotNull(await table.CompactAsync(AllFiles));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal([3L], await RowGroupSizesAsync(file.Path));
        var schema = await FileSchemaAsync(file.Path);
        Assert.True(schema.GetFieldByName("id").IsNullable);
        Assert.True(((StructType)schema.GetFieldByName("s").DataType).Fields[0].IsNullable);
        Assert.Equal([1L, 2, 3], await ReadIdsAsync(table));
    }

    /// <summary>A NOT NULL column is written required once compacted, whatever flag its sources carried.</summary>
    [Fact]
    public async Task ANotNullColumn_IsWrittenRequired()
    {
        var tableSchema = NestedBatch(0, idNullable: false, childNullable: true).Schema;
        await using var table = await DeltaTable.CreateAsync(Fs, tableSchema);
        await table.WriteAsync([NestedBatch(1, idNullable: true, childNullable: true)]);
        await table.WriteAsync([NestedBatch(2, idNullable: false, childNullable: true)]);

        Assert.NotNull(await table.CompactAsync(AllFiles));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.False((await FileSchemaAsync(file.Path)).GetFieldByName("id").IsNullable);
        Assert.Equal([1L, 2], await ReadIdsAsync(table));
    }

    /// <summary>
    /// Deletion-vector survivors from many files are joined too: a heavily deleted source no longer leaves its
    /// small remnant as a row group of its own, and no deleted row comes back.
    /// </summary>
    [Fact]
    public async Task DeletionVectorSurvivors_JoinIntoOneRowGroup()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema, enableDeletionVectors: true);
        await AppendFilesAsync(table, files: 10, rowsEach: 1_000);
        // Keep the first 100 rows of every file.
        var (deleted, _) = await table.DeleteAsync(b =>
        {
            var ids = (Int64Array)b.Column("id");
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < b.Length; i++)
                mask.Append(ids.GetValue(i)!.Value % 1_000 >= 100);
            return mask.Build();
        }, RowDeleteMode.DeletionVector);
        Assert.Equal(9_000, deleted);
        Assert.All(table.CurrentSnapshot.ActiveFiles.Values, f => Assert.NotNull(f.DeletionVector));

        Assert.NotNull(await table.CompactAsync(AllFiles));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Null(file.DeletionVector);
        Assert.Equal([1_000L], await RowGroupSizesAsync(file.Path));
        var expected = Enumerable.Range(0, 10).SelectMany(f => Range(f * 1_000L, 100)).ToList();
        Assert.Equal(expected, await ReadIdsAsync(table));
    }
}
