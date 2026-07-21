// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Change Data Feed on a COLUMN-MAPPING (and/or PARTITIONED) table: the <c>_change_data</c> files (and the
/// data files a no-cdc version infers changes from) are stored in the PHYSICAL layout — physical names + field
/// ids, partition columns absent — so <see cref="DeltaTable.ReadChangesAsync"/> must map them back to LOGICAL
/// names and re-materialize the partition columns. Regression cover for the round-trip; the Spark side (a
/// conformant reader resolving the physical layout) is pinned in the interop suite.
/// </summary>
public class CdfColumnMappingTests : IDisposable
{
    private readonly string _tempDir;

    public CdfColumnMappingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdfcm_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    // Creates a Name-mode column-mapping table with CDF enabled (CreateAsync has no CDF switch, so the property
    // is patched into the generated — correctly mapped — metadata as a follow-up metadata commit).
    private async Task<DeltaTable> CreateMappedCdfTableAsync(
        Apache.Arrow.Schema schema, IReadOnlyList<string>? partitionColumns = null)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        long next;
        MetadataAction meta;
        await using (var created = await DeltaTable.CreateAsync(
            fs, schema, partitionColumns: partitionColumns, columnMappingMode: ColumnMappingMode.Name))
        {
            meta = created.CurrentSnapshot.Metadata;
            next = created.CurrentSnapshot.Version + 1;
        }
        var cfg = meta.Configuration!.ToDictionary(kv => kv.Key, kv => kv.Value);
        cfg[CdfConfig.EnableKey] = "true";
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(next, new List<DeltaAction> { meta with { Configuration = cfg } });
        return await DeltaTable.OpenAsync(fs);
    }

    private static async Task<List<RecordBatch>> ReadChangesAsync(DeltaTable t, long from, long to)
    {
        var list = new List<RecordBatch>();
        await foreach (var b in t.ReadChangesAsync(from, to))
            list.Add(b);
        return list;
    }

    private static IEnumerable<string> Names(RecordBatch b) => b.Schema.FieldsList.Select(f => f.Name);

    private static StringArray Col(RecordBatch b, string name) =>
        (StringArray)b.Column(b.Schema.GetFieldIndex(name));

    [Fact]
    public async Task Update_OnMappedTable_FeedHasLogicalNamesNotPhysical()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();
        await using var table = await CreateMappedCdfTableAsync(schema);

        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build(),
             new StringArray.Builder().Append("a").Append("b").Build()], 2)]);
        long updateVersion = table.CurrentSnapshot.Version + 1;

        await table.UpdateAsync(
            b => { var id = (Int64Array)b.Column(0); var mask = new BooleanArray.Builder();
                   for (int i = 0; i < b.Length; i++) mask.Append(id.GetValue(i) == 1); return mask.Build(); },
            b => new RecordBatch(b.Schema,
                [b.Column(0), new StringArray.Builder().Append("updated").Build()], b.Length));

        var changes = await ReadChangesAsync(table, updateVersion, updateVersion);
        Assert.NotEmpty(changes);
        foreach (var b in changes)
        {
            // The crux: logical names, and no physical col-<guid> leaked into the feed.
            Assert.Contains("id", Names(b));
            Assert.Contains("value", Names(b));
            Assert.DoesNotContain(Names(b), n => n.StartsWith("col-", StringComparison.Ordinal));
        }

        // The pre-image carries the old value, the post-image the new — both under the logical "value" name.
        var pre = changes.Where(b => Col(b, CdfConfig.ChangeTypeColumn).GetString(0) == CdfConfig.UpdatePreimage).ToList();
        var post = changes.Where(b => Col(b, CdfConfig.ChangeTypeColumn).GetString(0) == CdfConfig.UpdatePostimage).ToList();
        Assert.Contains(pre, b => Col(b, "value").GetString(0) == "a");
        Assert.Contains(post, b => Col(b, "value").GetString(0) == "updated");
    }

    [Fact]
    public async Task Insert_OnMappedPartitionedTable_FeedHasLogicalNamesAndPartitionValue()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field("region", StringType.Default, true))
            .Build();
        await using var table = await CreateMappedCdfTableAsync(schema, partitionColumns: ["region"]);

        long insertVersion = table.CurrentSnapshot.Version + 1;
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build(),
             new StringArray.Builder().Append("a").Append("b").Build(),
             new StringArray.Builder().Append("emea").Append("emea").Build()], 2)]);

        // Insert feed is INFERRED from the AddFile (no cdc file) — the physical data file omits the partition
        // column, so the reader must re-materialize "region" from the file's partitionValues.
        var changes = await ReadChangesAsync(table, insertVersion, insertVersion);
        long rows = 0;
        foreach (var b in changes)
        {
            Assert.Contains("id", Names(b));
            Assert.Contains("region", Names(b));
            Assert.DoesNotContain(Names(b), n => n.StartsWith("col-", StringComparison.Ordinal));
            var region = Col(b, "region");
            for (int i = 0; i < b.Length; i++)
                Assert.Equal("emea", region.GetString(i));
            Assert.All(Enumerable.Range(0, b.Length),
                i => Assert.Equal(CdfConfig.Insert, Col(b, CdfConfig.ChangeTypeColumn).GetString(i)));
            rows += b.Length;
        }
        Assert.Equal(2, rows);
    }

    [Fact]
    public async Task Update_OnMappedPartitionedTable_CdcFeedHasLogicalNamesAndPartitionValue()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field("region", StringType.Default, true))
            .Build();
        await using var table = await CreateMappedCdfTableAsync(schema, partitionColumns: ["region"]);

        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build(),
             new StringArray.Builder().Append("a").Build(),
             new StringArray.Builder().Append("apac").Build()], 1)]);
        long updateVersion = table.CurrentSnapshot.Version + 1;

        await table.UpdateAsync(
            b => { var mask = new BooleanArray.Builder();
                   for (int i = 0; i < b.Length; i++) mask.Append(true); return mask.Build(); },
            b =>
            {
                // keep id + region, change value
                int vIdx = b.Schema.GetFieldIndex("value");
                var cols = new IArrowArray[b.ColumnCount];
                for (int i = 0; i < b.ColumnCount; i++)
                    cols[i] = i == vIdx ? new StringArray.Builder().Append("z").Build() : b.Column(i);
                return new RecordBatch(b.Schema, cols, b.Length);
            });

        var changes = await ReadChangesAsync(table, updateVersion, updateVersion);
        Assert.NotEmpty(changes);
        foreach (var b in changes)
        {
            Assert.Contains("region", Names(b));
            Assert.DoesNotContain(Names(b), n => n.StartsWith("col-", StringComparison.Ordinal));
            var region = Col(b, "region");
            for (int i = 0; i < b.Length; i++)
                Assert.Equal("apac", region.GetString(i)); // partition value re-materialized on the cdc-file path
        }
        Assert.Contains(changes, b => Col(b, CdfConfig.ChangeTypeColumn).GetString(0) == CdfConfig.UpdatePreimage
                                      && Col(b, "value").GetString(0) == "a");
        Assert.Contains(changes, b => Col(b, CdfConfig.ChangeTypeColumn).GetString(0) == CdfConfig.UpdatePostimage
                                      && Col(b, "value").GetString(0) == "z");
    }
}
