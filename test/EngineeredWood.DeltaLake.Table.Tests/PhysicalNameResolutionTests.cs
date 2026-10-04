// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using ArrowStructType = Apache.Arrow.Types.StructType;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #451 and #452: every place that looks a column up in what a FILE recorded (its statistics, its partition
/// values, its column names) resolves the column's PHYSICAL name. The fixtures are tables upgraded to name mode,
/// which keep their original names as physical names (what Spark leaves); there, after renaming <c>a</c> to
/// <c>x</c> and then <c>b</c> to <c>a</c>, the logical name <c>a</c> is the physical name of <c>x</c>, and after
/// a column is dropped and re-added under the same name, the dropped column's physical name is the new one's
/// logical name. A lookup that tried the logical name first bound the other column.
/// </summary>
public class PhysicalNameResolutionTests : IDisposable
{
    private readonly string _tempDir;

    public PhysicalNameResolutionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_physres_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // A field as an upgraded table records it: physical name = the original (logical) name, unless given.
    private static StructField Mapped(string name, DeltaDataType type, int id, string? physical = null) => new()
    {
        Name = name,
        Type = type,
        Nullable = true,
        Metadata = new Dictionary<string, string>
        {
            [ColumnMapping.FieldIdKey] = id.ToString(),
            [ColumnMapping.PhysicalNameKey] = physical ?? name,
        },
    };

    private static PrimitiveType Long => new() { TypeName = "long" };
    private static PrimitiveType Str => new() { TypeName = "string" };

    private static Apache.Arrow.Schema LongSchema(params string[] names)
    {
        var b = new Apache.Arrow.Schema.Builder();
        foreach (var n in names)
            b.Field(new Field(n, Int64Type.Default, true));
        return b.Build();
    }

    private static RecordBatch LongRow(Apache.Arrow.Schema schema, params long[] values)
    {
        var arrays = new List<IArrowArray>();
        foreach (var v in values)
            arrays.Add(new Int64Array.Builder().Append(v).Build());
        return new RecordBatch(schema, arrays, 1);
    }

    // Upgraded name-mode table a: long, b: long, holding one row a = 1, b = 100.
    private async Task<DeltaTable> CreateUpgradedAbAsync()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), LongSchema("a", "b"), columnMappingMode: ColumnMappingMode.Name,
            preAssignedSchema: new DeltaStructType { Fields = [Mapped("a", Long, 1), Mapped("b", Long, 2)] });
        await table.WriteAsync([LongRow(LongSchema("a", "b"), 1, 100)]);
        return table;
    }

    private static async Task<List<RecordBatch>> CollectAsync(DeltaTable table, Predicate? filter = null)
    {
        var list = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync(columns: null, filter))
            list.Add(b);
        return list;
    }

    // ReadAllAsync skips files but does not filter rows, so a count below the file's row count means a file
    // holding matching rows was pruned.
    private static async Task<int> CountAsync(DeltaTable table, Predicate filter) =>
        (await CollectAsync(table, filter)).Sum(b => b.Length);

    private static AddFile MakeAdd(string? stats, Dictionary<string, string>? partitionValues = null) => new()
    {
        Path = "part-0.parquet",
        PartitionValues = partitionValues ?? new Dictionary<string, string>(),
        Size = 1,
        ModificationTime = 0,
        DataChange = true,
        Stats = stats,
    };

    // ── File statistics ──────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Pruning_AfterChainedRenames_UsesTheColumnsOwnStats()
    {
        await using var table = await CreateUpgradedAbAsync();
        await table.RenameColumnAsync("a", "x"); // x = physical a = 1
        await table.RenameColumnAsync("b", "a"); // a = physical b = 100

        Assert.Equal(1, await CountAsync(table, Ex.Equal("a", 100L)));
        Assert.Equal(1, await CountAsync(table, Ex.Equal("x", 1L)));
        // ... and pruning still works: these bounds are the right column's.
        Assert.Equal(0, await CountAsync(table, Ex.Equal("a", 1L)));
    }

    [Fact]
    public void Pruner_PhysicalKeyWinsOverAnotherColumnsLogicalName()
    {
        // Logical a is physical "b"; logical c is physical "a".
        var schema = new DeltaStructType { Fields = [Mapped("c", Long, 1, "a"), Mapped("a", Long, 2, "b")] };
        var add = MakeAdd("""{"numRecords":1,"minValues":{"a":1,"b":100},"maxValues":{"a":1,"b":100},"nullCount":{"a":0,"b":0}}""");

        var pruner = new DeltaFilePruner(schema, []);
        Assert.True(pruner.ShouldInclude(add, Ex.Equal("a", LiteralValue.Of(100L))));
        Assert.False(pruner.ShouldInclude(add, Ex.Equal("a", LiteralValue.Of(1L))));
    }

    // After DROP + re-ADD of b, the old file's statistics still carry the DROPPED column under "b" (its
    // physical name on an upgraded table). They say nothing about the new b, which is NULL in that file.
    [Fact]
    public async Task Pruning_AfterDropAndReAdd_DoesNotUseTheDroppedColumnsStats()
    {
        await using var table = await CreateUpgradedAbAsync();
        await table.DropColumnAsync("b");
        await table.AddColumnAsync(new Field("b", Int64Type.Default, true));

        Assert.Equal(1, await CountAsync(table, Ex.IsNull("b")));
    }

    // The price of resolving statistics by physical name only: a file an older engineered-wood wrote with
    // LOGICAL keys is not pruned on a mapped column. It is read, never wrongly skipped.
    [Fact]
    public void Pruner_LogicalKeyedStats_OnAMappedColumn_KeepTheFile()
    {
        var schema = new DeltaStructType { Fields = [Mapped("a", Long, 1, "col-a")] };
        var add = MakeAdd("""{"numRecords":1,"minValues":{"a":5},"maxValues":{"a":5},"nullCount":{"a":0}}""");

        Assert.True(new DeltaFilePruner(schema, []).ShouldInclude(add, Ex.Equal("a", LiteralValue.Of(99L))));
    }

    // ── Statistics written by rewrites ───────────────────────────────────────────────────────────────

    private static List<string> MinKeys(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("minValues").EnumerateObject()
            .Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal).ToList();

    [Theory]
    [InlineData(false)] // UpdateAsync's rewrite
    [InlineData(true)]  // DeleteRowsAsync(CopyOnWrite)'s rewrite
    public async Task Rewrite_OnNameModeTable_KeysStatsByPhysicalName(bool delete)
    {
        var schema = LongSchema("id", "v");
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build(), new Int64Array.Builder().Append(10).Append(20).Build()],
            2)]);
        string appendStats = table.CurrentSnapshot.ActiveFiles.Values.Single().GetStatsJson()!;

        if (delete)
        {
            var doomed = new List<RecordBatch>();
            await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
            {
                var id = (Int64Array)batch.Column("id");
                var rows = Enumerable.Range(0, batch.Length).Where(i => id.GetValue(i) == 1).ToList();
                doomed.Add(EngineeredWood.Arrow.ArrowCompute.Take(batch, batch.Schema, rows));
            }
            await table.DeleteRowsAsync(RowSelection.FromLocatorColumns(doomed), RowDeleteMode.CopyOnWrite);
        }
        else
            await table.UpdateAsync(Ex.Equal("id", 1L), b => b);

        string rewriteStats = table.CurrentSnapshot.ActiveFiles.Values.Single().GetStatsJson()!;
        var physical = table.CurrentSnapshot.Schema.Fields
            .Select(f => f.Metadata![ColumnMapping.PhysicalNameKey]).OrderBy(n => n, StringComparer.Ordinal);
        Assert.Equal(physical, MinKeys(rewriteStats));
        Assert.Equal(MinKeys(appendStats), MinKeys(rewriteStats));
    }

    [Fact]
    public async Task Update_ThenDropAndReAdd_IsNullStillFindsTheRow()
    {
        var schema = LongSchema("a", "b");
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([LongRow(schema, 1, 100)]);
        await table.UpdateAsync(Ex.Equal("a", 1L), b => b);
        await table.DropColumnAsync("b");
        await table.AddColumnAsync(new Field("b", Int64Type.Default, true));

        Assert.Equal(1, await CountAsync(table, Ex.IsNull("b")));
    }

    [Fact]
    public async Task Update_ThenRename_PruningStillFindsTheRow()
    {
        await using var table = await CreateUpgradedAbAsync();
        await table.RenameColumnAsync("a", "x"); // x = physical a
        await table.RenameColumnAsync("b", "a"); // a = physical b
        await table.UpdateAsync(Ex.Equal("x", 1L), b => b);
        await table.RenameColumnAsync("x", "y"); // y = physical a

        Assert.Equal(1, await CountAsync(table, Ex.Equal("y", 1L)));
        Assert.Equal(1, await CountAsync(table, Ex.Equal("a", 100L)));
    }

    // ── Checkpoint stats_parsed ──────────────────────────────────────────────────────────────────────

    private async Task<AddFile> CheckpointedAddAsync(string schemaJson, string stats)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 2, MinWriterVersion = 5 },
            new MetadataAction
            {
                Id = "physres", Format = Format.Parquet, SchemaString = schemaJson, PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    [ColumnMapping.ModeKey] = "name",
                    ["delta.checkpoint.writeStatsAsStruct"] = "true",
                },
            },
            MakeAdd(stats),
        ]);
        var snapshot = await SnapshotBuilder.BuildAsync(log);
        await new CheckpointWriter(fs).WriteCheckpointAsync(snapshot);
        var reader = new CheckpointReader(fs);
        return (await reader.ReadCheckpointAsync((await reader.ReadLastCheckpointAsync())!))
            .OfType<AddFile>().Single();
    }

    private static string MappedSchemaJson(params (string Logical, string Physical, int Id)[] cols) =>
        "{\"type\":\"struct\",\"fields\":[" + string.Join(",", cols.Select(c =>
            $"{{\"name\":\"{c.Logical}\",\"type\":\"long\",\"nullable\":true,\"metadata\":{{"
            + $"\"delta.columnMapping.id\":{c.Id},\"delta.columnMapping.physicalName\":\"{c.Physical}\"}}}}")) + "]}";

    // With uuid physical names the typed columns were laid out under logical names and came out all null; the
    // typed path, preferred by default, then answered Unknown for everything.
    [Fact]
    public async Task CheckpointStatsParsed_UuidPhysicalNames_Prunes()
    {
        string schemaJson = MappedSchemaJson(("a", "col-1111", 1));
        var add = await CheckpointedAddAsync(schemaJson,
            """{"numRecords":1,"minValues":{"col-1111":5},"maxValues":{"col-1111":5},"nullCount":{"col-1111":0}}""");
        Assert.NotNull(add.TypedStats);

        var schema = DeltaSchemaSerializer.Parse(schemaJson);
        var typedOnly = add with { Stats = null };
        Assert.False(new DeltaFilePruner(schema, []).ShouldInclude(typedOnly, Ex.Equal("a", LiteralValue.Of(99L))));
        Assert.True(new DeltaFilePruner(schema, []).ShouldInclude(typedOnly, Ex.Equal("a", LiteralValue.Of(5L))));
    }

    [Fact]
    public async Task CheckpointStatsParsed_PhysicalNameEqualToAnotherLogical_KeepsTheMatchingFile()
    {
        // Logical a is physical "b" (= 100); logical c is physical "a" (= 1).
        string schemaJson = MappedSchemaJson(("c", "a", 1), ("a", "b", 2));
        var add = await CheckpointedAddAsync(schemaJson,
            """{"numRecords":1,"minValues":{"a":1,"b":100},"maxValues":{"a":1,"b":100},"nullCount":{"a":0,"b":0}}""");

        var schema = DeltaSchemaSerializer.Parse(schemaJson);
        var typedOnly = add with { Stats = null };
        var pruner = new DeltaFilePruner(schema, []);
        Assert.True(pruner.ShouldInclude(typedOnly, Ex.Equal("a", LiteralValue.Of(100L))));
        Assert.False(pruner.ShouldInclude(typedOnly, Ex.Equal("a", LiteralValue.Of(1L))));
    }

    // ── Partition values ─────────────────────────────────────────────────────────────────────────────

    private static Apache.Arrow.Schema P1P2VSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("p1", StringType.Default, true))
        .Field(new Field("p2", StringType.Default, true))
        .Field(new Field("v", Int64Type.Default, true))
        .Build();

    private static Apache.Arrow.Schema XP1VSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("x", StringType.Default, true))
        .Field(new Field("p1", StringType.Default, true))
        .Field(new Field("v", Int64Type.Default, true))
        .Build();

    private static RecordBatch PRow(Apache.Arrow.Schema schema, string first, string second, long v) =>
        new(schema,
            [
                new StringArray.Builder().Append(first).Build(),
                new StringArray.Builder().Append(second).Build(),
                new Int64Array.Builder().Append(v).Build(),
            ], 1);

    // Upgraded table partitioned by p1, p2, then p1 renamed to x and p2 renamed to p1: logical p1 is now the
    // physical key p2, and the physical key p1 belongs to x.
    private async Task<DeltaTable> CreateRenamedPartitionedAsync(params (string P1, string P2, long V)[] rows)
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), P1P2VSchema(), partitionColumns: ["p1", "p2"],
            columnMappingMode: ColumnMappingMode.Name,
            preAssignedSchema: new DeltaStructType
            {
                Fields = [Mapped("p1", Str, 1), Mapped("p2", Str, 2), Mapped("v", Long, 3)],
            });
        foreach (var (p1, p2, v) in rows)
            await table.WriteAsync([PRow(P1P2VSchema(), p1, p2, v)]);
        await table.RenameColumnAsync("p1", "x");
        await table.RenameColumnAsync("p2", "p1");
        return table;
    }

    private static List<string> Rows(IEnumerable<RecordBatch> batches)
    {
        var rows = new List<string>();
        foreach (var b in batches)
        {
            for (int i = 0; i < b.Length; i++)
            {
                rows.Add($"x={((StringArray)b.Column("x")).GetString(i)} "
                    + $"p1={((StringArray)b.Column("p1")).GetString(i)} v={((Int64Array)b.Column("v")).GetValue(i)}");
            }
        }
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    [Fact]
    public async Task PartitionValues_AfterChainedRenames_FollowTheirColumn()
    {
        await using var table = await CreateRenamedPartitionedAsync(("A", "B", 1));

        Assert.Equal(["x=A p1=B v=1"], Rows(await CollectAsync(table)));
        // The partition branch of the pruner resolves the same way.
        Assert.Equal(1, await CountAsync(table, Ex.Equal("p1", "B")));
        Assert.Equal(0, await CountAsync(table, Ex.Equal("p1", "A")));
    }

    // Two partitions whose values are each other's swapped. Translating already-physical keys as if they were
    // logical gave both the same grouping key, and they were compacted into one file stamped with the first's
    // partition values.
    [Fact]
    public async Task Compaction_AfterChainedRenames_KeepsPartitionsApart()
    {
        await using var table = await CreateRenamedPartitionedAsync(("A", "B", 1), ("B", "A", 2), ("A", "B", 3));
        await table.CompactAsync();

        Assert.Equal(2, table.CurrentSnapshot.ActiveFiles.Count);
        Assert.Equal(["x=A p1=B v=1", "x=A p1=B v=3", "x=B p1=A v=2"], Rows(await CollectAsync(table)));
    }

    [Fact]
    public async Task OverwritePartitions_AfterChainedRenames_ReplacesTheNamedPartition()
    {
        await using var table = await CreateRenamedPartitionedAsync(("A", "B", 1), ("C", "A", 2));

        // Logical p1 = "B" is exactly the first file.
        await table.OverwritePartitionsAsync(
            [PRow(XP1VSchema(), "Z", "B", 9)], new Dictionary<string, string> { ["p1"] = "B" });

        Assert.Equal(["x=C p1=A v=2", "x=Z p1=B v=9"], Rows(await CollectAsync(table)));
    }

    [Fact]
    public async Task DynamicOverwrite_AfterChainedRenames_ReplacesOnlyTheWrittenPartition()
    {
        await using var table = await CreateRenamedPartitionedAsync(("A", "B", 1), ("B", "A", 2));

        await table.DynamicOverwriteAsync([PRow(XP1VSchema(), "A", "B", 9)]);

        Assert.Equal(["x=A p1=B v=9", "x=B p1=A v=2"], Rows(await CollectAsync(table)));
    }

    // Files written before engineered-wood keyed partitionValues physically still resolve, by logical name.
    [Fact]
    public void Pruner_LogicalKeyedPartitionValues_StillResolve()
    {
        var schema = new DeltaStructType { Fields = [Mapped("p", Str, 1, "col-p"), Mapped("v", Long, 2, "col-v")] };
        var add = MakeAdd(null, new Dictionary<string, string> { ["p"] = "A" });

        var pruner = new DeltaFilePruner(schema, ["p"]);
        Assert.True(pruner.ShouldInclude(add, Ex.Equal("p", LiteralValue.Of("A"))));
        Assert.False(pruner.ShouldInclude(add, Ex.Equal("p", LiteralValue.Of("B"))));
    }

    // ── #452: a dropped column's values do not come back under a re-added name ───────────────────────

    private static Apache.Arrow.Schema IdAmountStructSchema(bool withStruct)
    {
        var b = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("amount", Int64Type.Default, true));
        if (withStruct)
        {
            b.Field(new Field("s", new ArrowStructType(
                [new Field("a", Int64Type.Default, true), new Field("x", StringType.Default, true)]), true));
        }
        return b.Build();
    }

    // Upgraded name-mode table holding one row: id = 1, amount = 100 (and s = {a: 10, x: "p"}).
    private async Task<DeltaTable> CreateUpgradedAmountAsync(bool withStruct)
    {
        var arrow = IdAmountStructSchema(withStruct);
        var fields = new List<StructField> { Mapped("id", Long, 1), Mapped("amount", Long, 2) };
        if (withStruct)
            fields.Add(Mapped("s", new DeltaStructType { Fields = [Mapped("a", Long, 4), Mapped("x", Str, 5)] }, 3));
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), arrow, columnMappingMode: ColumnMappingMode.Name,
            preAssignedSchema: new DeltaStructType { Fields = fields });
        var cols = new List<IArrowArray>
        {
            new Int64Array.Builder().Append(1).Build(),
            new Int64Array.Builder().Append(100).Build(),
        };
        if (withStruct)
        {
            cols.Add(new StructArray((ArrowStructType)arrow.FieldsList[2].DataType, 1,
                [new Int64Array.Builder().Append(10).Build(), new StringArray.Builder().Append("p").Build()],
                ArrowBuffer.Empty));
        }
        await table.WriteAsync([new RecordBatch(arrow, cols, 1)]);
        return table;
    }

    [Theory]
    [InlineData(false)] // flat path: ColumnMapping.RenameColumns
    [InlineData(true)]  // recursive path: ColumnMappingRecursive.ToLogical
    public async Task DropThenReAdd_TopLevel_TheNewColumnIsNull(bool withStruct)
    {
        await using var table = await CreateUpgradedAmountAsync(withStruct);
        await table.DropColumnAsync("amount");
        await table.AddColumnAsync(new Field("amount", Int64Type.Default, true));

        var batch = Assert.Single(await CollectAsync(table));
        Assert.True(((Int64Array)batch.Column("amount")).IsNull(0));
        Assert.Equal(1L, ((Int64Array)batch.Column("id")).GetValue(0));

        // OPTIMIZE agrees (it already did), and a new row's value is the new column's.
        await table.WriteAsync([LongRow(LongSchema("id", "amount"), 2, 7)]);
        await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        var amounts = new List<long?>();
        foreach (var b in await CollectAsync(table))
        {
            for (int i = 0; i < b.Length; i++)
                amounts.Add(((Int64Array)b.Column("amount")).GetValue(i));
        }
        Assert.Equal(new long?[] { null, 7 }, amounts.OrderBy(v => v ?? long.MinValue));
    }

    [Fact]
    public async Task DropThenReAdd_Nested_TheNewFieldIsNull()
    {
        await using var table = await CreateUpgradedAmountAsync(withStruct: true);
        await table.DropFieldAsync(["s", "a"]);
        await table.AddFieldAsync(["s"], new Field("a", Int64Type.Default, true));

        var batch = Assert.Single(await CollectAsync(table));
        var s = (StructArray)batch.Column("s");
        var st = (ArrowStructType)s.Data.DataType;
        Assert.Equal(["x", "a"], st.Fields.Select(f => f.Name).ToArray());
        Assert.True(s.Fields[st.GetFieldIndex("a")].IsNull(0));
        Assert.Equal("p", ((StringArray)s.Fields[st.GetFieldIndex("x")]).GetString(0));
    }

    [Fact]
    public async Task DropThenReAdd_DeletePredicate_SeesTheNewColumn()
    {
        await using var table = await CreateUpgradedAmountAsync(withStruct: false);
        await table.DropColumnAsync("amount");
        await table.AddColumnAsync(new Field("amount", Int64Type.Default, true));

        // The old row's amount is NULL, so this deletes nothing.
        await table.DeleteAsync(Ex.Equal("amount", 100L));

        var batch = Assert.Single(await CollectAsync(table));
        Assert.Equal(1, batch.Length);
    }

    [Fact]
    public async Task DropThenReAdd_ChangeFeed_ReportsTheNewColumnAsNull()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var created = await DeltaTable.CreateAsync(
            fs, LongSchema("id", "amount"), columnMappingMode: ColumnMappingMode.Name,
            preAssignedSchema: new DeltaStructType { Fields = [Mapped("id", Long, 1), Mapped("amount", Long, 2)] },
            configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" }))
        {
            await created.WriteAsync([LongRow(LongSchema("id", "amount"), 1, 100)]);
            await created.DropColumnAsync("amount");
            await created.AddColumnAsync(new Field("amount", Int64Type.Default, true));
        }

        await using var table = await DeltaTable.OpenAsync(fs);
        var inserts = new List<long?>();
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions
            { StartVersion = 1, EndVersion = table.CurrentSnapshot.Version }))
        {
            for (int i = 0; i < b.Length; i++)
                inserts.Add(((Int64Array)b.Column("amount")).GetValue(i));
        }
        Assert.Equal(new long?[] { null }, inserts);
    }
}
