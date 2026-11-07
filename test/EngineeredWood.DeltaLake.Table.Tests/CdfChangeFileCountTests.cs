// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// How many change files a DML operation writes (#723). Every change row of one operation, of every change type,
/// shares the operation's change files: one per partition, rolled at <see cref="DeltaTableOptions.TargetFileSize"/>.
/// Before, every rewritten data file wrote a change file per change type, so an UPDATE of ten files committed twenty
/// <c>cdc</c> actions and every reader of the feed paid for each.
///
/// <para>The table is ten files of ten rows each: file <c>f</c> holds ids <c>f*10 .. f*10+9</c>, with value
/// <c>v{id}</c> and category <c>c{id % 10}</c>, so a predicate on a category touches every file.</para>
/// </summary>
public class CdfChangeFileCountTests : IDisposable
{
    private const int Files = 10;
    private const int RowsPerFile = 10;
    private readonly string _tempDir;

    public CdfChangeFileCountTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdfcount_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema FlatSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("category", StringType.Default, true))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static readonly Apache.Arrow.Schema PartitionedSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("category", StringType.Default, true))
        .Field(new Field("value", StringType.Default, true))
        .Field(new Field("p", StringType.Default, true))
        .Build();

    private LocalTableFileSystem Fs => new(_tempDir);

    /// <summary>The ten-file table; partitioned by <c>p</c> (<c>even</c> / <c>odd</c> by id) when asked.</summary>
    private async Task<DeltaTable> CreateAsync(
        bool deletionVectors = false, bool partitioned = false, bool rowTracking = false,
        DeltaTableOptions? options = null)
    {
        var schema = partitioned ? PartitionedSchema : FlatSchema;
        var table = await DeltaTable.CreateAsync(
            Fs, schema, options ?? new DeltaTableOptions { CheckpointInterval = 0 },
            partitionColumns: partitioned ? ["p"] : null,
            enableDeletionVectors: deletionVectors, enableRowTracking: rowTracking,
            configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" });
        for (int f = 0; f < Files; f++)
            await table.WriteAsync([Rows(schema, Enumerable.Range(f * RowsPerFile, RowsPerFile).Select(i => (long)i))]);
        return table;
    }

    private static RecordBatch Rows(Apache.Arrow.Schema schema, IEnumerable<long> ids)
    {
        var id = new Int64Array.Builder();
        var category = new StringArray.Builder();
        var value = new StringArray.Builder();
        var p = new StringArray.Builder();
        int n = 0;
        foreach (long i in ids)
        {
            id.Append(i);
            category.Append($"c{i % 10}");
            value.Append($"v{i}");
            p.Append(i % 2 == 0 ? "even" : "odd");
            n++;
        }
        IArrowArray[] columns = schema.FieldsList.Count == 4
            ? [id.Build(), category.Build(), value.Build(), p.Build()]
            : [id.Build(), category.Build(), value.Build()];
        return new RecordBatch(schema, columns, n);
    }

    /// <summary>Sets <c>value</c> to <c>x</c>, keeping every other column.</summary>
    private static RecordBatch SetValue(RecordBatch batch)
    {
        int index = batch.Schema.GetFieldIndex("value");
        var x = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            x.Append("x");
        var columns = Enumerable.Range(0, batch.ColumnCount)
            .Select(c => c == index ? x.Build() : batch.Column(c)).ToArray();
        return new RecordBatch(batch.Schema, columns, batch.Length);
    }

    private async Task<List<CdcFile>> ChangeFilesAsync(long version) =>
        (await new TransactionLog(Fs).ReadCommitAsync(version)).OfType<CdcFile>().ToList();

    /// <summary>Every row of the feed at <paramref name="version"/>, as (id, change type, value), sorted.</summary>
    private static async Task<List<(long Id, string ChangeType, string Value)>> FeedAsync(DeltaTable table, long version)
    {
        var rows = new List<(long, string, string)>();
        await foreach (var b in table.ReadChangesAsync(
                           new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var types = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            var values = (StringArray)b.Column(b.Schema.GetFieldIndex("value"));
            for (int i = 0; i < b.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, types.GetString(i), values.GetString(i)));
        }
        return rows.OrderBy(r => r.Item1).ThenBy(r => r.Item2, StringComparer.Ordinal).ToList();
    }

    /// <summary>The feed an UPDATE setting <c>value = 'x'</c> on <paramref name="ids"/> must produce.</summary>
    private static List<(long, string, string)> UpdateFeed(IEnumerable<long> ids) =>
        ids.SelectMany(i => new[]
            {
                (i, CdfConfig.UpdatePostimage, "x"),
                (i, CdfConfig.UpdatePreimage, $"v{i}"),
            })
            .OrderBy(r => r.i).ThenBy(r => r.Item2, StringComparer.Ordinal).ToList();

    private static IEnumerable<long> Category(int c) => Enumerable.Range(0, Files).Select(f => (long)(f * RowsPerFile + c));

    // The delta-dotnet benchmark's change-feed fixture: an UPDATE touching every file wrote 20 change files.
    [Theory]
    [InlineData(RowDeleteMode.CopyOnWrite)]
    [InlineData(RowDeleteMode.DeletionVector)]
    public async Task AnUpdateOfEveryFile_WritesOneChangeFile(RowDeleteMode mode)
    {
        await using var table = await CreateAsync(deletionVectors: mode == RowDeleteMode.DeletionVector);
        var (updated, version) = await table.UpdateAsync(Ex.Equal("category", "c3"), SetValue, mode: mode);

        Assert.Equal(Files, updated);
        Assert.Single(await ChangeFilesAsync(version));
        Assert.Equal(UpdateFeed(Category(3)), await FeedAsync(table, version));
    }

    // The same fixture's DELETE wrote 10.
    [Theory]
    [InlineData(RowDeleteMode.CopyOnWrite)]
    [InlineData(RowDeleteMode.DeletionVector)]
    public async Task ADeleteFromEveryFile_WritesOneChangeFile(RowDeleteMode mode)
    {
        await using var table = await CreateAsync(deletionVectors: mode == RowDeleteMode.DeletionVector);
        var (deleted, version) = await table.DeleteAsync(Ex.Equal("category", "c5"), commitProperties: null, mode);

        Assert.Equal(Files, deleted);
        Assert.Single(await ChangeFilesAsync(version));
        Assert.Equal(
            Category(5).Select(i => (i, CdfConfig.Delete, $"v{i}")).ToList(),
            await FeedAsync(table, version));
    }

    [Fact]
    public async Task AHostDrivenDeleteOfSeveralFiles_WritesOneChangeFile()
    {
        await using var table = await CreateAsync();
        var selection = table.CurrentSnapshot.ActiveFiles.Values.Take(3)
            .ToDictionary(f => f.Path, f => (IReadOnlyCollection<long>)[0L, 1L]);
        var (deleted, version) = await table.DeleteRowsAsync(RowSelection.ByPath(selection), RowDeleteMode.CopyOnWrite);

        Assert.Equal(6, deleted);
        Assert.Single(await ChangeFilesAsync(version));
        var feed = await FeedAsync(table, version);
        Assert.Equal(6, feed.Count);
        Assert.All(feed, r => Assert.Equal(CdfConfig.Delete, r.ChangeType));
    }

    [Fact]
    public async Task AHostDrivenUpdateOfSeveralFiles_WritesOneChangeFile()
    {
        await using var table = await CreateAsync();
        var selection = table.CurrentSnapshot.ActiveFiles.Values.Take(3)
            .ToDictionary(f => f.Path, f => (IReadOnlyCollection<long>)[2L]);
        long version = await table.UpdateRowsAsync(
            RowSelection.ByPath(selection), (_, batches, _) => [.. batches.Select(SetValue)]);

        Assert.Single(await ChangeFilesAsync(version));
        var feed = await FeedAsync(table, version);
        Assert.Equal(3, feed.Count(r => r.ChangeType == CdfConfig.UpdatePreimage));
        Assert.Equal(3, feed.Count(r => r.ChangeType == CdfConfig.UpdatePostimage && r.Value == "x"));
    }

    // As Spark's MERGE writes it (measured): update pre- and post-images and deletes side by side in one file.
    [Theory]
    [InlineData(RowDeleteMode.CopyOnWrite)]
    [InlineData(RowDeleteMode.DeletionVector)]
    public async Task AMergeThatUpdatesAndDeletes_WritesOneChangeFile(RowDeleteMode mode)
    {
        await using var table = await CreateAsync(deletionVectors: mode == RowDeleteMode.DeletionVector);
        // Ids 0, 7, 14, ... from most files: those of category c1 (21 and 91) deleted, the rest updated.
        var touched = Enumerable.Range(0, Files * RowsPerFile).Where(i => i % 7 == 0).Select(i => (long)i).ToList();
        var (_, version) = await table.MergeAsync([Rows(FlatSchema, touched)], new MergeOptions
        {
            Keys = ["id"],
            DeleteMode = mode,
            WhenMatched =
            [
                MergeAction.Delete.When(Ex.Equal(UnboundReference.FromParts(["source", "category"]), Ex.Lit("c1"))),
                MergeAction.UpdateSet(new MergeAssignment("value", Ex.Lit("x"))),
            ],
        });

        Assert.Single(await ChangeFilesAsync(version));
        var feed = await FeedAsync(table, version);
        Assert.Equal([21L, 91L], feed.Where(r => r.ChangeType == CdfConfig.Delete).Select(r => r.Id));
        Assert.Equal(touched.Count - 2, feed.Count(r => r.ChangeType == CdfConfig.UpdatePreimage));
        Assert.Equal(touched.Count - 2, feed.Count(r => r.ChangeType == CdfConfig.UpdatePostimage));
        Assert.All(feed.Where(r => r.ChangeType == CdfConfig.UpdatePostimage), r => Assert.Equal("x", r.Value));
    }

    // A post-image belongs to the partition its row now lives in, so moving rows touch both partitions' files.
    [Fact]
    public async Task AnUpdateMovingRowsAcrossPartitions_WritesOneChangeFilePerPartition()
    {
        await using var table = await CreateAsync(partitioned: true);
        // Every even row with category c4 moves to "odd".
        var (updated, version) = await table.UpdateAsync(
            Ex.Equal("category", "c4"),
            batch =>
            {
                int pIndex = batch.Schema.GetFieldIndex("p");
                var odd = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    odd.Append("odd");
                var columns = Enumerable.Range(0, batch.ColumnCount)
                    .Select(c => c == pIndex ? odd.Build() : batch.Column(c)).ToArray();
                return new RecordBatch(batch.Schema, columns, batch.Length);
            });

        Assert.Equal(Files, updated);
        var cdc = await ChangeFilesAsync(version);
        Assert.Equal(2, cdc.Count);
        Assert.Equal(["even", "odd"], cdc.Select(c => c.PartitionValues["p"]).OrderBy(p => p, StringComparer.Ordinal));

        var pFeed = new List<(long Id, string ChangeType, string P)>();
        await foreach (var b in table.ReadChangesAsync(
                           new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var types = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            var ps = (StringArray)b.Column(b.Schema.GetFieldIndex("p"));
            for (int i = 0; i < b.Length; i++)
                pFeed.Add((ids.GetValue(i)!.Value, types.GetString(i), ps.GetString(i)));
        }
        Assert.Equal(Files * 2, pFeed.Count);
        Assert.All(pFeed.Where(r => r.ChangeType == CdfConfig.UpdatePreimage), r => Assert.Equal("even", r.P));
        Assert.All(pFeed.Where(r => r.ChangeType == CdfConfig.UpdatePostimage), r => Assert.Equal("odd", r.P));
    }

    // A pre-image and its post-image keep the row's stable id when they share a file.
    [Fact]
    public async Task OnARowTrackingTable_ThePreAndPostImageOfARow_CarryItsId()
    {
        await using var table = await CreateAsync(rowTracking: true);
        var (_, version) = await table.UpdateAsync(Ex.Equal("category", "c3"), SetValue);
        Assert.Single(await ChangeFilesAsync(version));

        var byType = new Dictionary<string, Dictionary<long, long>>(StringComparer.Ordinal);
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions
                       {
                           StartVersion = version, EndVersion = version, Metadata = DeltaRowMetadata.RowTracking,
                       }))
        {
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var types = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            var rowIds = (Int64Array)b.Column(b.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < b.Length; i++)
            {
                if (!byType.TryGetValue(types.GetString(i), out var map))
                    byType[types.GetString(i)] = map = new Dictionary<long, long>();
                map[ids.GetValue(i)!.Value] = rowIds.GetValue(i)!.Value;
            }
        }

        var pre = byType[CdfConfig.UpdatePreimage];
        Assert.Equal(Files, pre.Count);
        Assert.Equal(pre, byType[CdfConfig.UpdatePostimage]);
        // Ids follow the append order, one per row from 0: the row with id i was appended i-th.
        Assert.All(pre, kv => Assert.Equal(kv.Key, kv.Value));
    }

    // The files roll as data files do: past the target, the next row group starts a new file.
    [Fact]
    public async Task ChangeFiles_RollAtTheTargetFileSize()
    {
        var options = new DeltaTableOptions
        {
            CheckpointInterval = 0,
            TargetFileSize = 1,
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 5 },
        };
        await using var table = await CreateAsync(options: options);
        var (_, version) = await table.UpdateAsync(b => AllTrue(b.Length), SetValue);

        // 100 pre-images and 100 post-images, five rows a row group, and every row group past the target.
        Assert.Equal(2 * Files * RowsPerFile / 5, (await ChangeFilesAsync(version)).Count);
        Assert.Equal(UpdateFeed(Enumerable.Range(0, Files * RowsPerFile).Select(i => (long)i)), await FeedAsync(table, version));
    }

    // Across partitions only one row group's worth of rows is held: past it the fullest partition is written.
    [Fact]
    public async Task HeldRows_AreCappedAtOneRowGroupAcrossPartitions()
    {
        var options = new DeltaTableOptions
        {
            CheckpointInterval = 0,
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxBytes = 1 },
        };
        await using var table = await CreateAsync(partitioned: true, options: options);
        var (_, version) = await table.UpdateAsync(b => AllTrue(b.Length), SetValue);

        var cdc = await ChangeFilesAsync(version);
        Assert.Equal(2, cdc.Count);
        // Every batch went out as soon as it came: one row group per batch added, not one per file.
        foreach (var file in cdc)
        {
            await using var stream = new LocalRandomAccessFile(Path.Combine(_tempDir, DeltaPath.Decode(file.Path)));
            using var reader = new ParquetFileReader(stream, ownsFile: false);
            Assert.True((await reader.ReadMetadataAsync()).RowGroups.Count > 1);
        }
        Assert.Equal(UpdateFeed(Enumerable.Range(0, Files * RowsPerFile).Select(i => (long)i)), await FeedAsync(table, version));
    }

    private static BooleanArray AllTrue(int length)
    {
        var all = new BooleanArray.Builder();
        for (int i = 0; i < length; i++)
            all.Append(true);
        return all.Build();
    }
}
