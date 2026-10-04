// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Issue #447: one change feed has ONE schema, and it is the one Spark picks. A file written before an ADD
/// COLUMN has no column for it, so the feed must backfill it as NULL, as a scan does. The schema is the
/// latest one without column mapping and the end version's under mapping (delta-spark 4.0's rule). The
/// Spark side of the same table is pinned in <c>SparkInteropTests</c>.
/// </summary>
public class CdfSchemaEvolutionTests : IDisposable
{
    private readonly string _tempDir;

    public CdfSchemaEvolutionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdfse_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly string[] FeedColumns =
        [CdfConfig.ChangeTypeColumn, CdfConfig.CommitVersionColumn, CdfConfig.CommitTimestampColumn];

    private async Task<DeltaTable> CreateAsync(
        ColumnMappingMode mode, Apache.Arrow.Schema schema, IReadOnlyList<string>? partitionColumns = null)
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, partitionColumns: partitionColumns,
            columnMappingMode: mode,
            configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" });
        Assert.Equal("true", table.CurrentSnapshot.Metadata.Configuration![CdfConfig.EnableKey]);
        return table;
    }

    private static Apache.Arrow.Schema IdOnly() =>
        new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, false)).Build();

    private static Apache.Arrow.Schema IdExtra() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("extra", Int64Type.Default, true))
        .Build();

    private static RecordBatch Ids(params long[] ids)
    {
        var b = new Int64Array.Builder();
        foreach (var id in ids) b.Append(id);
        return new RecordBatch(IdOnly(), [b.Build()], ids.Length);
    }

    private static RecordBatch IdsExtra(long id, long extra) => new(IdExtra(),
        [new Int64Array.Builder().Append(id).Build(), new Int64Array.Builder().Append(extra).Build()], 1);

    private static async Task<List<RecordBatch>> ReadChangesAsync(DeltaTable t, long from, long to)
    {
        var list = new List<RecordBatch>();
        await foreach (var b in t.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = from, EndVersion = to }))
        {
            list.Add(b);
        }
        return list;
    }

    private static string[] Names(RecordBatch b) => b.Schema.FieldsList.Select(f => f.Name).ToArray();

    /// <summary>(id, extra) for every row of the feed, with extra null where the row has none.</summary>
    private static List<(long Id, long? Extra, string ChangeType)> Rows(IEnumerable<RecordBatch> batches)
    {
        var rows = new List<(long, long?, string)>();
        foreach (var b in batches)
        {
            var id = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            int extraIdx = b.Schema.GetFieldIndex("extra");
            var extra = extraIdx < 0 ? null : (Int64Array)b.Column(extraIdx);
            var ct = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            for (int i = 0; i < b.Length; i++)
                rows.Add((id.GetValue(i)!.Value, extra?.GetValue(i), ct.GetString(i)));
        }
        return rows.OrderBy(r => r.Item1).ThenBy(r => r.Item3, StringComparer.Ordinal).ToList();
    }

    // v1 inserts id = 1, v2 adds `extra`, v3 inserts (2, 20): the table of the issue.
    private async Task<(DeltaTable Table, long V1, long V3)> BuildAddColumnHistoryAsync(ColumnMappingMode mode)
    {
        var table = await CreateAsync(mode, IdOnly());
        long v1 = await table.WriteAsync([Ids(1)]);
        await table.AddColumnAsync(new Field("extra", Int64Type.Default, true));
        long v3 = await table.WriteAsync([IdsExtra(2, 20)]);
        return (table, v1, v3);
    }

    [Theory]
    [InlineData(ColumnMappingMode.None)]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task RangeSpanningAddColumn_EveryBatchHasOneSchema_OlderRowsNull(ColumnMappingMode mode)
    {
        var (table, v1, v3) = await BuildAddColumnHistoryAsync(mode);
        await using var _ = table;

        var changes = await ReadChangesAsync(table, v1, v3);

        Assert.Equal(2, changes.Count);
        foreach (var b in changes)
            Assert.Equal(["id", "extra", .. FeedColumns], Names(b));
        Assert.Equal(
            [(1L, (long?)null, CdfConfig.Insert), (2L, 20L, CdfConfig.Insert)],
            Rows(changes));
    }

    [Fact]
    public async Task OldVersionOnly_NoMapping_UsesLatestSchema()
    {
        var (table, v1, _) = await BuildAddColumnHistoryAsync(ColumnMappingMode.None);
        await using var _t = table;

        var changes = await ReadChangesAsync(table, v1, v1);

        var b = Assert.Single(changes);
        Assert.Equal(["id", "extra", .. FeedColumns], Names(b));
        Assert.Equal([(1L, (long?)null, CdfConfig.Insert)], Rows(changes));
    }

    [Theory]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task OldVersionOnly_ColumnMapping_UsesEndVersionSchema(ColumnMappingMode mode)
    {
        var (table, v1, _) = await BuildAddColumnHistoryAsync(mode);
        await using var _t = table;

        var changes = await ReadChangesAsync(table, v1, v1);

        var b = Assert.Single(changes);
        Assert.Equal(["id", .. FeedColumns], Names(b));
    }

    [Fact]
    public async Task ColumnMapping_RenameAfterEndVersion_FeedUsesTheNameAtTheEndVersion()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();
        await using var table = await CreateAsync(ColumnMappingMode.Name, schema);
        long v1 = await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build(), new StringArray.Builder().Append("a").Build()], 1)]);
        long v2 = await table.RenameColumnAsync("value", "label");

        var before = Assert.Single(await ReadChangesAsync(table, v1, v1));
        Assert.Equal(["id", "value", .. FeedColumns], Names(before));
        Assert.Equal("a", ((StringArray)before.Column(1)).GetString(0));

        var through = Assert.Single(await ReadChangesAsync(table, v1, v2));
        Assert.Equal(["id", "label", .. FeedColumns], Names(through));
        Assert.Equal("a", ((StringArray)through.Column(1)).GetString(0));
    }

    [Fact]
    public async Task ChangeFileWrittenBeforeAddColumn_IsBackfilled()
    {
        // The UPDATE writes a _change_data file under the schema of its day, which has no `extra`; the read
        // after the ADD must backfill it there too, not only in batches inferred from data files.
        await using var table = await CreateAsync(ColumnMappingMode.None, IdOnly());
        long v1 = await table.WriteAsync([Ids(1, 2)]);
        await table.UpdateAsync(
            b =>
            {
                var id = (Int64Array)b.Column(0);
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < b.Length; i++) mask.Append(id.GetValue(i) == 1);
                return mask.Build();
            },
            b => new RecordBatch(b.Schema, [new Int64Array.Builder().Append(10).Build()], b.Length));
        long v3 = await table.AddColumnAsync(new Field("extra", Int64Type.Default, true));

        var changes = await ReadChangesAsync(table, v1, v3);

        Assert.Contains(changes, b => Names(b).Contains(CdfConfig.ChangeTypeColumn)
            && ((StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn))).GetString(0)
                == CdfConfig.UpdatePreimage);
        foreach (var b in changes)
            Assert.Equal(["id", "extra", .. FeedColumns], Names(b));
        Assert.All(Rows(changes), r => Assert.Null(r.Extra));
    }

    [Fact]
    public async Task PartitionedTable_RangeSpanningAddColumn_PartitionColumnKeepsItsPlace()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, true))
            .Build();
        await using var table = await CreateAsync(ColumnMappingMode.Name, schema, partitionColumns: ["region"]);
        long v1 = await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build(), new StringArray.Builder().Append("emea").Build()], 1)]);
        long v2 = await table.AddColumnAsync(new Field("extra", Int64Type.Default, true));

        var b = Assert.Single(await ReadChangesAsync(table, v1, v2));

        Assert.Equal(["id", "region", "extra", .. FeedColumns], Names(b));
        Assert.Equal("emea", ((StringArray)b.Column(1)).GetString(0));
        Assert.True(b.Column(2).IsNull(0));
    }
}
