// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A MERGE's inserted rows share the MERGE's change files with its updates and deletes (#732), as Spark's MERGE
/// writes them: MEASURED against pyspark 4.0.4 / delta-spark 4.0.0 and 4.1.1 / 4.1.0 and 4.1.2 / 4.3.1, one change
/// file holding <c>update_preimage</c>, <c>update_postimage</c>, <c>delete</c> and <c>insert</c> rows, and no change
/// file at all for a MERGE that only inserts. Before, each inserted data file got a change file of its own, written by
/// reading the data file back at commit.
///
/// <para>On a row-tracking table Spark leaves an inserted row's id NULL in the change file. Here it carries the id
/// its add gives it, so a rebase that moves the add's <c>baseRowId</c> must rewrite the shared file.</para>
/// </summary>
public class CdfMergeInsertChangeTests : IDisposable
{
    private readonly string _tempDir;

    public CdfMergeInsertChangeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdfmergeins_{Guid.NewGuid():N}");
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

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    private static readonly Apache.Arrow.Schema FlatSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static readonly Apache.Arrow.Schema PartitionedSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Field(new Field("p", StringType.Default, true))
        .Build();

    /// <summary>Rows <c>(id, value, p)</c>, with <c>p</c> = <c>even</c> / <c>odd</c> by id when partitioned.</summary>
    private static RecordBatch Rows(bool partitioned, string value, params long[] ids)
    {
        var id = new Int64Array.Builder();
        var v = new StringArray.Builder();
        var p = new StringArray.Builder();
        foreach (long i in ids)
        {
            id.Append(i);
            v.Append(value == "v" ? $"v{i}" : value);
            p.Append(i % 2 == 0 ? "even" : "odd");
        }
        return partitioned
            ? new RecordBatch(PartitionedSchema, [id.Build(), v.Build(), p.Build()], ids.Length)
            : new RecordBatch(FlatSchema, [id.Build(), v.Build()], ids.Length);
    }

    /// <summary>Three files, ids 1-3, 11-13 and 21-23, each row's value <c>v{id}</c>.</summary>
    private async Task<DeltaTable> CreateAsync(
        bool partitioned = false, bool rowTracking = false, bool deletionVectors = false)
    {
        var table = await DeltaTable.CreateAsync(
            Fs, partitioned ? PartitionedSchema : FlatSchema, new DeltaTableOptions { CheckpointInterval = 0 },
            partitionColumns: partitioned ? ["p"] : null,
            enableDeletionVectors: deletionVectors, enableRowTracking: rowTracking,
            configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" });
        await table.WriteAsync([Rows(partitioned, "v", 1, 2, 3)]);
        await table.WriteAsync([Rows(partitioned, "v", 11, 12, 13)]);
        await table.WriteAsync([Rows(partitioned, "v", 21, 22, 23)]);
        return table;
    }

    /// <summary>Upsert: a source row whose id matches sets the value, one with value <c>DEL</c> deletes, the rest
    /// are inserted.</summary>
    private static MergeOptions Upsert(
        RowDeleteMode? mode = null, IReadOnlyList<MergeAction>? notMatched = null) => new()
    {
        Keys = ["id"],
        DeleteMode = mode,
        WhenMatched =
        [
            MergeAction.Delete.When(EngineeredWood.Expressions.Expressions.Equal(
                EngineeredWood.Expressions.UnboundReference.FromParts(["source", "value"]),
                EngineeredWood.Expressions.Expressions.Lit("DEL"))),
            MergeAction.Update,
        ],
        WhenNotMatched = notMatched ?? [MergeAction.Insert],
    };

    private async Task<List<CdcFile>> ChangeFilesAsync(long version) =>
        (await new TransactionLog(Fs).ReadCommitAsync(version)).OfType<CdcFile>().ToList();

    private sealed record Change(long Id, string Type, string Value, long? RowId, long? RowVersion);

    private static async Task<List<Change>> FeedAsync(DeltaTable table, long version, bool rowTracking = false)
    {
        var rows = new List<Change>();
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions
        {
            StartVersion = version, EndVersion = version,
            Metadata = rowTracking ? DeltaRowMetadata.RowTracking : DeltaRowMetadata.None,
        }))
        {
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var types = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            var values = (StringArray)b.Column(b.Schema.GetFieldIndex("value"));
            var rowIds = rowTracking ? (Int64Array)b.Column(b.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName)) : null;
            var rowVers = rowTracking
                ? (Int64Array)b.Column(b.Schema.GetFieldIndex(RowTrackingConfig.RowCommitVersionColumnName))
                : null;
            for (int i = 0; i < b.Length; i++)
            {
                rows.Add(new Change(
                    ids.GetValue(i)!.Value, types.GetString(i), values.GetString(i),
                    rowIds?.GetValue(i), rowVers?.GetValue(i)));
            }
        }
        return rows.OrderBy(r => r.Id).ThenBy(r => r.Type, StringComparer.Ordinal).ToList();
    }

    /// <summary>Each live row's stable id, by <c>id</c>.</summary>
    private static async Task<Dictionary<long, long>> CommittedRowIdsAsync(DeltaTable table)
    {
        var committed = new Dictionary<long, long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var id = (Int64Array)batch.Column("id");
            var rowId = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
                committed[id.GetValue(i)!.Value] = rowId.GetValue(i)!.Value;
        }
        return committed;
    }

    /// <summary>The change files on disk that no commit names.</summary>
    private async Task<List<string>> OrphanChangeFilesAsync()
    {
        string dir = Path.Combine(_tempDir, "_change_data");
        if (!Directory.Exists(dir))
            return [];
        await using var check = await OpenAsync();
        var log = new TransactionLog(Fs);
        var referenced = new HashSet<string>(StringComparer.Ordinal);
        for (long v = 0; v <= check.CurrentSnapshot.Version; v++)
        {
            foreach (var cdc in (await log.ReadCommitAsync(v)).OfType<CdcFile>())
                referenced.Add(Path.GetFileName(DeltaPath.Decode(cdc.Path)));
        }
        return Directory.GetFiles(dir).Select(Path.GetFileName).Where(f => !referenced.Contains(f!)).ToList()!;
    }

    [Theory]
    [InlineData(RowDeleteMode.CopyOnWrite)]
    [InlineData(RowDeleteMode.DeletionVector)]
    public async Task AMergeThatUpdatesDeletesAndInserts_WritesOneChangeFile(RowDeleteMode mode)
    {
        await using var table = await CreateAsync(deletionVectors: mode == RowDeleteMode.DeletionVector);
        var source = new RecordBatch(FlatSchema,
            [new Int64Array.Builder().AppendRange([2, 12, 31, 32, 33]).Build(),
             new StringArray.Builder().AppendRange(["A", "DEL", "n31", "n32", "n33"]).Build()], 5);
        // Two not-matched clauses, so the inserts reach the write as two batches.
        var options = Upsert(mode,
        [
            MergeAction.Insert.When(EngineeredWood.Expressions.Expressions.Equal(
                EngineeredWood.Expressions.UnboundReference.FromParts(["source", "id"]),
                EngineeredWood.Expressions.Expressions.Lit(31L))),
            MergeAction.Insert,
        ]);
        var (metrics, version) = await table.MergeAsync([source], options);

        Assert.Equal(3, metrics.NumTargetRowsInserted);
        Assert.Single(await ChangeFilesAsync(version));
        var feed = await FeedAsync(table, version);
        Assert.Equal(
            [
                new Change(2, CdfConfig.UpdatePostimage, "A", null, null),
                new Change(2, CdfConfig.UpdatePreimage, "v2", null, null),
                new Change(12, CdfConfig.Delete, "v12", null, null),
                new Change(31, CdfConfig.Insert, "n31", null, null),
                new Change(32, CdfConfig.Insert, "n32", null, null),
                new Change(33, CdfConfig.Insert, "n33", null, null),
            ],
            feed);
    }

    // As Spark leaves it (measured): the feed of a MERGE that changed no target row is inferred from its adds.
    [Fact]
    public async Task AnInsertOnlyMerge_WritesNoChangeFile()
    {
        await using var table = await CreateAsync();
        var (_, version) = await table.MergeAsync([Rows(false, "n", 40, 41)], Upsert());

        Assert.Empty(await ChangeFilesAsync(version));
        Assert.Equal(
            [new Change(40, CdfConfig.Insert, "n", null, null), new Change(41, CdfConfig.Insert, "n", null, null)],
            await FeedAsync(table, version));
    }

    // Each partition's change rows, inserts included, share one file.
    [Fact]
    public async Task OnAPartitionedTable_InsertsJoinTheirPartitionsChangeFile()
    {
        await using var table = await CreateAsync(partitioned: true);
        // Updates in "even" only; inserts in both.
        var (_, version) = await table.MergeAsync([Rows(true, "n", 2, 22, 40, 41, 43)], Upsert());

        var cdc = await ChangeFilesAsync(version);
        Assert.Equal(["even", "odd"], cdc.Select(c => c.PartitionValues["p"]).OrderBy(p => p, StringComparer.Ordinal));
        var feed = await FeedAsync(table, version);
        Assert.Equal([40L, 41L, 43L], feed.Where(r => r.Type == CdfConfig.Insert).Select(r => r.Id));
        Assert.Equal([2L, 22L], feed.Where(r => r.Type == CdfConfig.UpdatePostimage).Select(r => r.Id));
    }

    // An INSERT naming only some columns, or naming them out of order, writes rows of another layout than the update
    // rows read back from the table. They are brought to the table's layout first, so they still share the file.
    [Theory]
    [InlineData("value", "id")]
    [InlineData("id")]
    public async Task InsertsOfAPartialOrReorderedColumnList_ShareTheChangeFile(params string[] columns)
    {
        await using var table = await CreateAsync();
        var (_, version) = await table.MergeAsync([Rows(false, "n", 2, 40, 41)], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = [MergeAction.Update],
            WhenNotMatched = [MergeAction.InsertColumns(columns)],
        });

        Assert.Single(await ChangeFilesAsync(version));
        string? inserted = columns.Contains("value") ? "n" : null;
        Assert.Equal(
            [
                new Change(2, CdfConfig.UpdatePostimage, "n", null, null),
                new Change(2, CdfConfig.UpdatePreimage, "v2", null, null),
                new Change(40, CdfConfig.Insert, inserted!, null, null),
                new Change(41, CdfConfig.Insert, inserted!, null, null),
            ],
            await FeedAsync(table, version));
    }

    // Without per-column statistics an add records no numRecords; the inserted rows' ids still come out right.
    [Fact]
    public async Task OnARowTrackingTableWithoutStatistics_InsertedRows_CarryTheirIds()
    {
        var table = await DeltaTable.CreateAsync(
            Fs, FlatSchema, new DeltaTableOptions { CheckpointInterval = 0, CollectStats = false },
            enableRowTracking: true,
            configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" });
        await using (table)
        {
            await table.WriteAsync([Rows(false, "v", 1, 2, 3)]);
            var (_, version) = await table.MergeAsync([Rows(false, "n", 2, 40, 41)], Upsert());

            Assert.Single(await ChangeFilesAsync(version));
            var committed = await CommittedRowIdsAsync(table);
            var inserts = (await FeedAsync(table, version, rowTracking: true))
                .Where(r => r.Type == CdfConfig.Insert).ToList();
            Assert.Equal([40L, 41L], inserts.Select(r => r.Id));
            Assert.All(inserts, r => Assert.Equal(committed[r.Id], r.RowId));
        }
    }

    // The change rows are the rows as stored: an identity column the source leaves out is the value the table generated.
    [Fact]
    public async Task InsertedRows_CarryTheGeneratedIdentityValues()
    {
        string idMeta = JsonSerializer.Serialize(IdentityColumn.CreateMetadata(100, 1, allowExplicitInsert: false));
        string schema = "{\"type\":\"struct\",\"fields\":["
            + $"{{\"name\":\"rid\",\"type\":\"long\",\"nullable\":true,\"metadata\":{idMeta}}},"
            + "{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
        await new TransactionLog(Fs).WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["identityColumns", "changeDataFeed"],
            },
            new MetadataAction
            {
                Id = "merge-identity", Format = Format.Parquet, SchemaString = schema, PartitionColumns = [],
                Configuration = new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" },
            },
        });
        await using var table = await DeltaTable.OpenAsync(Fs);
        await table.WriteAsync([Rows(false, "v", 1, 2)]);

        var (_, version) = await table.MergeAsync([Rows(false, "n", 2, 5, 6)], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = [MergeAction.UpdateColumns("value")],
            WhenNotMatched = [MergeAction.InsertColumns("id", "value")],
        });

        Assert.Single(await ChangeFilesAsync(version));
        var stored = new Dictionary<long, long>();
        await foreach (var b in table.ReadAsync())
        {
            var id = (Int64Array)b.Column("id");
            var rid = (Int64Array)b.Column("rid");
            for (int i = 0; i < b.Length; i++)
                stored[id.GetValue(i)!.Value] = rid.GetValue(i)!.Value;
        }
        var inserted = new Dictionary<long, long>();
        await foreach (var b in table.ReadChangesAsync(
                           new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var id = (Int64Array)b.Column("id");
            var rid = (Int64Array)b.Column("rid");
            var type = (StringArray)b.Column(CdfConfig.ChangeTypeColumn);
            for (int i = 0; i < b.Length; i++)
            {
                if (type.GetString(i) == CdfConfig.Insert)
                    inserted[id.GetValue(i)!.Value] = rid.GetValue(i)!.Value;
            }
        }
        Assert.Equal([5L, 6L], inserted.Keys.OrderBy(k => k));
        Assert.All(inserted, kv => Assert.Equal(stored[kv.Key], kv.Value));
    }

    // An inserted row's change carries the id the table gives it, and its commit version.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OnARowTrackingTable_InsertedRows_CarryTheirIds(bool partitioned)
    {
        await using var table = await CreateAsync(partitioned, rowTracking: true);
        var (_, version) = await table.MergeAsync([Rows(partitioned, "n", 2, 40, 41, 43)], Upsert());

        Assert.Equal(partitioned ? 2 : 1, (await ChangeFilesAsync(version)).Count);
        var committed = await CommittedRowIdsAsync(table);
        var feed = await FeedAsync(table, version, rowTracking: true);
        var inserts = feed.Where(r => r.Type == CdfConfig.Insert).ToList();
        Assert.Equal([40L, 41L, 43L], inserts.Select(r => r.Id));
        Assert.All(inserts, r =>
        {
            Assert.Equal(committed[r.Id], r.RowId);
            Assert.Equal(version, r.RowVersion);
        });
        // And the updated row keeps its own.
        Assert.Equal(committed[2], feed.Single(r => r.Type == CdfConfig.UpdatePostimage).RowId);
    }

    /// <summary>A concurrent append takes the ids the MERGE reserved for its inserts, so the rebase moves their adds'
    /// <c>baseRowId</c>; the shared change file is rewritten to follow, its other rows unchanged, and the file the
    /// losing attempt wrote is not left behind.</summary>
    [Theory]
    [InlineData(false, RowDeleteMode.CopyOnWrite)]
    [InlineData(true, RowDeleteMode.CopyOnWrite)]
    [InlineData(false, RowDeleteMode.DeletionVector)]
    public async Task OnARowTrackingTable_ARebase_MovesTheInsertedRowsIds(bool partitioned, RowDeleteMode mode)
    {
        await using var table = await CreateAsync(
            partitioned, rowTracking: true, deletionVectors: mode == RowDeleteMode.DeletionVector);
        long before = table.CurrentSnapshot.Version;

        long version;
        await using (var txn = table.StartTransaction())
        {
            await txn.MergeAsync([Rows(partitioned, "n", 2, 12, 40, 41, 43)], Upsert(mode));

            // The racer appends to a file the MERGE did not read, taking the ids the MERGE reserved.
            await using (var racer = await OpenAsync())
                await racer.WriteAsync([Rows(partitioned, "v", 100, 101, 102)]);

            version = await txn.CommitAsync();
        }
        Assert.Equal(before + 2, version);

        await using var check = await OpenAsync();
        var committed = await CommittedRowIdsAsync(check);
        var feed = await FeedAsync(check, version, rowTracking: true);
        var inserts = feed.Where(r => r.Type == CdfConfig.Insert).ToList();
        Assert.Equal([40L, 41L, 43L], inserts.Select(r => r.Id));
        Assert.All(inserts, r =>
        {
            Assert.Equal(committed[r.Id], r.RowId);
            Assert.Equal(version, r.RowVersion);
        });
        // The racer's rows and the inserts hold different ids.
        Assert.Equal(committed.Count, committed.Values.Distinct().Count());
        // The updated rows' pre- and post-images keep the ids they had.
        foreach (long id in new long[] { 2, 12 })
        {
            var images = feed.Where(r => r.Id == id).ToList();
            Assert.Equal(2, images.Count);
            Assert.All(images, r => Assert.Equal(committed[id], r.RowId));
        }
        Assert.Equal(partitioned ? 2 : 1, (await ChangeFilesAsync(version)).Count);
        Assert.Empty(await OrphanChangeFilesAsync());
    }

    // Without a racer the rebase moves nothing and writes nothing again.
    [Fact]
    public async Task OnARowTrackingTable_ARebaseThatMovesNoId_KeepsTheFiles()
    {
        await using var table = await CreateAsync(rowTracking: true);
        long version;
        List<string> staged;
        await using (var txn = table.StartTransaction())
        {
            await txn.MergeAsync([Rows(false, "n", 2, 40)], Upsert());
            staged = Directory.GetFiles(Path.Combine(_tempDir, "_change_data")).Select(Path.GetFileName).ToList()!;
            // A concurrent commit that adds no rows, so the MERGE's ids stay where they were.
            await using (var racer = await OpenAsync())
            {
                var t = racer.StartTransaction();
                t.StageActions([new DomainMetadata { Domain = "test.domain", Configuration = "{}", Removed = false }]);
                await t.CommitAsync();
            }
            version = await txn.CommitAsync();
        }

        var committedFiles = (await ChangeFilesAsync(version))
            .Select(c => Path.GetFileName(DeltaPath.Decode(c.Path))).ToList();
        Assert.Equal(staged, committedFiles);
        Assert.Empty(await OrphanChangeFilesAsync());
    }
}
