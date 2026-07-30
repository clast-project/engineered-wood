// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Row tracking ON THE CHANGE DATA FEED: a change row carries the same stable identity the table read
/// reports for it, so a feed consumer can join a change to its row and pair an <c>update_preimage</c> with
/// its <c>update_postimage</c>.
///
/// <para>The write half is the load-bearing one. A <c>cdc</c> action has no <c>baseRowId</c> /
/// <c>defaultRowCommitVersion</c> (unlike <c>add</c> and <c>remove</c>), so a change file that does not
/// MATERIALIZE the two hidden columns leaves its rows with no derivable id at all — there is nothing for a
/// reader to fall back to. Spark 4.0.1 was measured writing exactly those columns into its own change files;
/// these tests pin engineered-wood to the same shape and to the values it puts in them.</para>
///
/// <para>The strip is the other half, and it was a live defect (fixed in its own commit): the hidden columns
/// reached the feed as two extra Int64 columns on an UNPARTITIONED table. A partitioned one consumes data
/// columns positionally when it re-materializes its partition columns, which takes exactly the user columns
/// and leaves the hidden ones off the end — so there they were already being dropped, by accident. The
/// partitioned test below therefore earns its keep on the IDS, not on the strip.</para>
/// </summary>
public class CdfRowTrackingTests : IDisposable
{
    private readonly string _tempDir;

    public CdfRowTrackingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdfrt_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private Task<DeltaTable> CreateAsync(bool rowTracking = true, IReadOnlyList<string>? partitionColumns = null)
        => DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), TableSchema,
            partitionColumns: partitionColumns,
            enableRowTracking: rowTracking,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" })
            .AsTask();

    private static RecordBatch Rows(params (long Id, string Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var vals = new StringArray.Builder();
        foreach (var (id, value) in rows) { ids.Append(id); vals.Append(value); }
        return new RecordBatch(TableSchema, [ids.Build(), vals.Build()], rows.Length);
    }

    // Rewrites the "value" of every row whose id is in `ids`, leaving the rest untouched.
    private static Task<(long RowsUpdated, long Version)> UpdateValueAsync(
        DeltaTable table, string newValue, params long[] ids)
    {
        var target = new HashSet<long>(ids);
        return table.UpdateAsync(
            b =>
            {
                var col = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < b.Length; i++)
                    mask.Append(target.Contains(col.GetValue(i)!.Value));
                return mask.Build();
            },
            b =>
            {
                var vals = new StringArray.Builder();
                for (int i = 0; i < b.Length; i++) vals.Append(newValue);
                var columns = new IArrowArray[b.ColumnCount];
                for (int i = 0; i < b.ColumnCount; i++) columns[i] = b.Column(i);
                columns[b.Schema.GetFieldIndex("value")] = vals.Build();
                return new RecordBatch(b.Schema, columns, b.Length);
            }).AsTask();
    }

    private static async Task<List<RecordBatch>> CollectAsync(IAsyncEnumerable<RecordBatch> source)
    {
        var list = new List<RecordBatch>();
        await foreach (var b in source)
            list.Add(b);
        return list;
    }

    /// <summary>Every change row as (changeType, id, rowId, rowCommitVersion, commitVersion), flattened.</summary>
    private static async Task<List<(string Change, long Id, long? RowId, long? RowVersion, long Commit)>>
        ReadFeedWithTrackingAsync(DeltaTable table, long from, long to)
    {
        var result = new List<(string, long, long?, long?, long)>();
        foreach (var b in await CollectAsync(table.ReadChangesWithRowTrackingAsync(from, to)))
        {
            var change = (StringArray)b.Column(b.Schema.GetFieldIndex("_change_type"));
            var id = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var rowId = (Int64Array)b.Column(b.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            var rowVer = (Int64Array)b.Column(
                b.Schema.GetFieldIndex(RowTrackingConfig.RowCommitVersionColumnName));
            var commit = (Int64Array)b.Column(b.Schema.GetFieldIndex("_commit_version"));
            for (int i = 0; i < b.Length; i++)
            {
                result.Add((
                    change.GetString(i), id.GetValue(i)!.Value,
                    rowId.IsNull(i) ? null : rowId.GetValue(i),
                    rowVer.IsNull(i) ? null : rowVer.GetValue(i),
                    commit.GetValue(i)!.Value));
            }
        }
        return result;
    }

    private IEnumerable<string> ChangeDataFiles() =>
        Directory.Exists(Path.Combine(_tempDir, "_change_data"))
            ? Directory.GetFiles(Path.Combine(_tempDir, "_change_data"), "*.parquet")
            : [];

    private static async Task<List<string>> ParquetColumnNamesAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        await foreach (var batch in reader.ReadAllAsync())
            return batch.Schema.FieldsList.Select(f => f.Name).ToList();
        return [];
    }

    [Fact]
    public async Task Update_OnRowTrackingTable_ChangeFileMaterializesBothColumns()
    {
        await using var table = await CreateAsync();
        var (matIdName, matVerName) = RowTrackingConfig.TryGetMaterializedColumnNames(
            table.CurrentSnapshot.Metadata.Configuration);
        Assert.NotNull(matIdName);
        Assert.NotNull(matVerName);

        await table.WriteAsync([Rows((1, "a"), (2, "b"), (3, "c"))]);
        await UpdateValueAsync(table, "B", 2);

        // The change files are the whole claim here: without these columns the feed's rows have no identity,
        // because a cdc action carries no baseRowId to derive one from.
        var files = ChangeDataFiles().ToList();
        Assert.NotEmpty(files);
        foreach (var f in files)
        {
            var names = await ParquetColumnNamesAsync(f);
            Assert.Contains(matIdName!, names);
            Assert.Contains(matVerName!, names);
            // Spark's measured layout: identity after the user columns, _change_type last.
            Assert.Equal("_change_type", names[^1]);
        }
    }

    [Fact]
    public async Task Update_PreAndPostImage_ShareTheRowIdAndDifferInCommitVersion()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a"), (2, "b"), (3, "c"))]);
        long appendVersion = table.CurrentSnapshot.Version;
        var (_, updateVersion) = await UpdateValueAsync(table, "B", 2);

        var feed = await ReadFeedWithTrackingAsync(table, updateVersion, updateVersion);
        var pre = Assert.Single(feed, r => r.Change == "update_preimage");
        var post = Assert.Single(feed, r => r.Change == "update_postimage");

        // The point of the whole feature: the two images are the SAME row, and say so.
        Assert.Equal(pre.RowId, post.RowId);
        Assert.Equal(1L, pre.RowId); // id 2 was the second row appended -> baseRowId 0 + position 1
        // The pre-image belongs to the version that last wrote the row; the post-image to this one.
        Assert.Equal(appendVersion, pre.RowVersion);
        Assert.Equal(updateVersion, post.RowVersion);
    }

    [Fact]
    public async Task Insert_ResolvesPositionallyFromTheAddAction()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a"), (2, "b"), (3, "c"))]);
        long appendVersion = table.CurrentSnapshot.Version;

        // An append writes no change file — the feed is INFERRED from the add, which carries baseRowId, so
        // the ids come from position exactly as the main read path resolves them.
        var feed = await ReadFeedWithTrackingAsync(table, appendVersion, appendVersion);
        Assert.Equal(3, feed.Count);
        Assert.All(feed, r => Assert.Equal("insert", r.Change));
        Assert.Equal([0L, 1L, 2L], feed.OrderBy(r => r.Id).Select(r => r.RowId).ToArray());
        Assert.All(feed, r => Assert.Equal(appendVersion, r.RowVersion));
    }

    [Fact]
    public async Task FeedIdsMatchTheTableReadIdsForTheSameRows()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a"), (2, "b"), (3, "c"))]);
        var (_, updateVersion) = await UpdateValueAsync(table, "B", 2);

        // The identity a feed consumer sees must be the identity the table reports, or it cannot join the two.
        var byId = new Dictionary<long, long?>();
        foreach (var b in await CollectAsync(table.ReadAllWithRowTrackingAsync(null, null)))
        {
            var id = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var rowId = (Int64Array)b.Column(b.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < b.Length; i++)
                byId[id.GetValue(i)!.Value] = rowId.IsNull(i) ? null : rowId.GetValue(i);
        }

        var post = Assert.Single(
            await ReadFeedWithTrackingAsync(table, updateVersion, updateVersion),
            r => r.Change == "update_postimage");
        Assert.Equal(byId[post.Id], post.RowId);
    }

    [Fact]
    public async Task Delete_ReportsTheRowsOriginalIdAndVersion()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a"), (2, "b"), (3, "c"))]);
        long appendVersion = table.CurrentSnapshot.Version;

        // Deleting the whole file needs no deletion vector, so this exercises the plain DELETE path.
        var (_, deleteVersion) = await table.DeleteAsync(
            b =>
            {
                var col = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < b.Length; i++) mask.Append(true);
                _ = col;
                return mask.Build();
            });

        var feed = await ReadFeedWithTrackingAsync(table, deleteVersion, deleteVersion);
        Assert.Equal(3, feed.Count);
        Assert.All(feed, r => Assert.Equal("delete", r.Change));
        // A deleted row reports the identity it HAD — not the version that deleted it.
        Assert.Equal([0L, 1L, 2L], feed.OrderBy(r => r.Id).Select(r => r.RowId).ToArray());
        Assert.All(feed, r => Assert.Equal(appendVersion, r.RowVersion));
    }

    [Fact]
    public async Task Overwrite_InferredFeed_ResolvesIdsThroughARewrittenSourceFile()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a"), (2, "b"), (3, "c"))]);
        await UpdateValueAsync(table, "B", 2); // the file now CARRIES materialized columns

        long overwriteVersion = table.CurrentSnapshot.Version + 1;
        await table.WriteAsync([Rows((9, "z"))], DeltaWriteMode.Overwrite);

        // An overwrite writes no change file, so the feed is inferred — the "delete" rows come from reading
        // the removed file itself, whose materialized columns must be read and stripped, and the "insert"
        // rows from the fresh add's baseRowId.
        var feed = await ReadFeedWithTrackingAsync(table, overwriteVersion, overwriteVersion);
        var deletes = feed.Where(r => r.Change == "delete").OrderBy(r => r.Id).ToList();
        Assert.Equal(3, deletes.Count);
        Assert.Equal([0L, 1L, 2L], deletes.Select(r => r.RowId).ToArray());

        var insert = Assert.Single(feed, r => r.Change == "insert");
        Assert.NotNull(insert.RowId);
        Assert.Equal(overwriteVersion, insert.RowVersion);
        // A fresh row gets a fresh id, never one of the ids the overwrite retired.
        Assert.DoesNotContain(insert.RowId, deletes.Select(r => r.RowId));
    }

    [Fact]
    public async Task ReadChanges_DoesNotLeakTheHiddenMaterializedColumns()
    {
        await using var table = await CreateAsync();
        var (matIdName, matVerName) = RowTrackingConfig.TryGetMaterializedColumnNames(
            table.CurrentSnapshot.Metadata.Configuration);
        await table.WriteAsync([Rows((1, "a"), (2, "b"), (3, "c"))]);
        await UpdateValueAsync(table, "B", 2);
        await table.WriteAsync([Rows((9, "z"))], DeltaWriteMode.Overwrite);

        // Both feed sources are covered: the change files the UPDATE wrote, and the data files the OVERWRITE
        // version is inferred from (whose source carries materialized columns from that same UPDATE).
        var batches = await CollectAsync(table.ReadChangesAsync(0, table.CurrentSnapshot.Version));
        Assert.NotEmpty(batches);
        foreach (var b in batches)
        {
            var names = b.Schema.FieldsList.Select(f => f.Name).ToList();
            Assert.DoesNotContain(matIdName!, names);
            Assert.DoesNotContain(matVerName!, names);
            // The plain feed is the user columns plus exactly the three the spec names.
            Assert.Equal(["id", "value", "_change_type", "_commit_version", "_commit_timestamp"], names);
        }
    }

    [Fact]
    public async Task Partitioned_FeedKeepsItsColumnsAndResolvesIds()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field("region", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema,
            partitionColumns: ["region"], enableRowTracking: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });

        RecordBatch Batch(params (long Id, string Value, string Region)[] rows)
        {
            var ids = new Int64Array.Builder();
            var vals = new StringArray.Builder();
            var regions = new StringArray.Builder();
            foreach (var r in rows) { ids.Append(r.Id); vals.Append(r.Value); regions.Append(r.Region); }
            return new RecordBatch(schema, [ids.Build(), vals.Build(), regions.Build()], rows.Length);
        }

        await table.WriteAsync([Batch((1, "a", "west"), (2, "b", "west"))]);
        long updateVersion = table.CurrentSnapshot.Version + 1;
        await table.UpdateAsync(
            b =>
            {
                var col = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < b.Length; i++) mask.Append(col.GetValue(i) == 2);
                return mask.Build();
            },
            b =>
            {
                var vals = new StringArray.Builder();
                for (int i = 0; i < b.Length; i++) vals.Append("B");
                var columns = new IArrowArray[b.ColumnCount];
                for (int i = 0; i < b.ColumnCount; i++) columns[i] = b.Column(i);
                columns[b.Schema.GetFieldIndex("value")] = vals.Build();
                return new RecordBatch(b.Schema, columns, b.Length);
            });

        foreach (var b in await CollectAsync(
                     table.ReadChangesWithRowTrackingAsync(updateVersion, updateVersion)))
        {
            var names = b.Schema.FieldsList.Select(f => f.Name).ToList();
            // The partition interleave walks the table schema pulling data columns positionally, so a hidden
            // column left in the batch shifts or drops one of these rather than merely showing up.
            Assert.Equal("id", names[0]);
            Assert.Equal("value", names[1]);
            Assert.Equal("region", names[2]);
            var region = (StringArray)b.Column(2);
            for (int i = 0; i < b.Length; i++)
                Assert.Equal("west", region.GetString(i));
        }

        var feed = await ReadFeedWithTrackingAsync(table, updateVersion, updateVersion);
        var pre = Assert.Single(feed, r => r.Change == "update_preimage");
        var post = Assert.Single(feed, r => r.Change == "update_postimage");
        Assert.Equal(pre.RowId, post.RowId);
        Assert.NotNull(pre.RowId);
    }

    [Fact]
    public async Task StagedChangeData_OnAPartitionedTable_KeepsEachRowsIdWithThatRow()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema,
            partitionColumns: ["region"], enableRowTracking: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });

        RecordBatch Batch(params (long Id, string Region)[] rows)
        {
            var ids = new Int64Array.Builder();
            var regions = new StringArray.Builder();
            foreach (var r in rows) { ids.Append(r.Id); regions.Append(r.Region); }
            return new RecordBatch(schema, [ids.Build(), regions.Build()], rows.Length);
        }

        await table.WriteAsync([Batch((1, "west"), (2, "east"), (3, "west"))]);

        // The staged rows interleave two partitions, so the split REORDERS them. If the ids were passed
        // through unsplit, row 3 would come back wearing row 2's id — the failure this test exists for.
        var txn = table.StartTransaction();
        long version = table.CurrentSnapshot.Version + 1;
        await txn.StageChangeDataAsync(
            Batch((1, "west"), (2, "east"), (3, "west")), "delete", default,
            new Int64Array.Builder().Append(100).Append(200).Append(300).Build(),
            new Int64Array.Builder().Append(7).Append(8).Append(9).Build());
        await txn.CommitAsync();

        var feed = await ReadFeedWithTrackingAsync(table, version, version);
        Assert.Equal(3, feed.Count);
        Assert.All(feed, r => Assert.Equal("delete", r.Change));
        var byId = feed.ToDictionary(r => r.Id);
        Assert.Equal(100L, byId[1].RowId);
        Assert.Equal(200L, byId[2].RowId);
        Assert.Equal(300L, byId[3].RowId);
        Assert.Equal(7L, byId[1].RowVersion);
        Assert.Equal(8L, byId[2].RowVersion);
        Assert.Equal(9L, byId[3].RowVersion);
    }

    [Fact]
    public async Task ReadChangesWithRowTracking_OnANonRowTrackingTable_Throws()
    {
        await using var table = await CreateAsync(rowTracking: false);
        await table.WriteAsync([Rows((1, "a"))]);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await CollectAsync(table.ReadChangesWithRowTrackingAsync(0, table.CurrentSnapshot.Version)));
        Assert.Contains("delta.enableRowTracking", ex.Message);
    }

    [Fact]
    public async Task ChangeFileWithoutMaterializedColumns_ReportsNullIdsRatherThanWrongOnes()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a"), (2, "b"))]);

        // A change file a writer produced without the row-tracking columns (an older engineered-wood, or any
        // writer that omits them). There is no baseRowId on a cdc action to fall back to, so the honest answer
        // is NULL — the alternative would be inventing a position-derived id that means nothing.
        long version = table.CurrentSnapshot.Version + 1;
        var cdc = await table.WriteChangeDataFileAsync(Rows((7, "seven")), "insert");
        await table.CommitDataFilesAsync([], extraActions: [cdc]);

        var feed = await ReadFeedWithTrackingAsync(table, version, version);
        var row = Assert.Single(feed, r => r.Id == 7);
        Assert.Null(row.RowId);
        // The commit version still resolves: a change belongs to the version that recorded it.
        Assert.Equal(version, row.RowVersion);
    }
}
