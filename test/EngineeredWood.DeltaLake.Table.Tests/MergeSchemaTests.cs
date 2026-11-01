// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Opt-in schema merge on append (#568): <c>mergeSchema: true</c> evolves the schema to fit the batch in the same
/// commit as the data; without it an extra column is refused, as before.
/// </summary>
public class MergeSchemaTests : IDisposable
{
    private readonly string _tempDir;

    public MergeSchemaTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_merge_schema_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static Apache.Arrow.Schema Schema(params Field[] fields)
    {
        var b = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            b.Field(f);
        return b.Build();
    }

    private static Field Long(string name, bool nullable = false) => new(name, Int64Type.Default, nullable);
    private static Field Int(string name, bool nullable = false) => new(name, Int32Type.Default, nullable);
    private static Field Str(string name, bool nullable = true) => new(name, StringType.Default, nullable);

    private static Int64Array Longs(params long[] values)
    {
        var b = new Int64Array.Builder();
        foreach (var v in values)
            b.Append(v);
        return b.Build();
    }

    private static Int32Array Ints(params int[] values)
    {
        var b = new Int32Array.Builder();
        foreach (var v in values)
            b.Append(v);
        return b.Build();
    }

    private static StringArray Strings(params string[] values)
    {
        var b = new StringArray.Builder();
        foreach (var v in values)
            b.Append(v);
        return b.Build();
    }

    /// <summary>Every row of the table, keyed by the long or int <c>id</c> column, as column name to value.</summary>
    private static async Task<Dictionary<long, Dictionary<string, object?>>> RowsByIdAsync(DeltaTable table)
    {
        var rows = new Dictionary<long, Dictionary<string, object?>>();
        await foreach (var batch in table.ReadAllAsync())
        {
            for (int i = 0; i < batch.Length; i++)
            {
                var row = new Dictionary<string, object?>();
                for (int c = 0; c < batch.ColumnCount; c++)
                    row[batch.Schema.FieldsList[c].Name] = Value(batch.Column(c), i);
                rows[Convert.ToInt64(row["id"])] = row;
            }
        }
        return rows;
    }

    private static object? Value(IArrowArray array, int i)
    {
        if (array.IsNull(i))
            return null;
        return array switch
        {
            Int64Array a => a.GetValue(i),
            Int32Array a => a.GetValue(i),
            StringArray a => a.GetString(i),
            StructArray a => Enumerable.Range(0, a.Fields.Count).ToDictionary(
                f => ((Apache.Arrow.Types.StructType)a.Data.DataType).Fields[f].Name, f => Value(a.Fields[f], i)),
            _ => throw new NotSupportedException(array.GetType().Name),
        };
    }

    /// <summary>The action kinds of one commit file, in order (commitInfo excluded).</summary>
    private List<string> ActionKinds(long version)
    {
        var path = Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json");
        var kinds = new List<string>();
        foreach (var line in File.ReadAllLines(path))
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;
            using var doc = JsonDocument.Parse(line);
            foreach (var p in doc.RootElement.EnumerateObject())
            {
                if (p.Name != "commitInfo")
                    kinds.Add(p.Name);
            }
        }
        return kinds;
    }

    private async Task<DeltaTable> CreateIdLongTableAsync(
        ColumnMappingMode mode = ColumnMappingMode.None, IReadOnlyDictionary<string, string>? configuration = null)
    {
        var schema = Schema(Long("id"));
        var table = await DeltaTable.CreateAsync(
            Fs, schema, columnMappingMode: mode, configuration: configuration);
        await table.WriteAsync([new RecordBatch(schema, [Longs(1, 2)], 2)]);
        return table;
    }

    // ── The default is unchanged ──

    [Fact]
    public async Task WithoutMergeSchema_AnExtraColumnIsStillRefused()
    {
        await using var table = await CreateIdLongTableAsync();
        var schema = Schema(Long("id"), Str("name"));
        long before = table.CurrentSnapshot.Version;

        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.WriteAsync([new RecordBatch(schema, [Longs(3), Strings("c")], 1)]));
        Assert.Contains("'name'", ex.Message);
        Assert.Equal(before, table.CurrentSnapshot.Version);
        Assert.Single(table.CurrentSnapshot.Schema.Fields);
    }

    // ── New columns ──

    [Fact]
    public async Task NewTopLevelColumn_IsAddedNullable_InTheSameCommitAsTheData()
    {
        await using var table = await CreateIdLongTableAsync();
        long before = table.CurrentSnapshot.Version;

        // Declared NOT NULL in the batch, and added nullable anyway: the rows already in the table have no value.
        var schema = Schema(Long("id"), Str("name", nullable: false));
        long version = await table.WriteAsync(
            [new RecordBatch(schema, [Longs(3), Strings("c")], 1)], mergeSchema: true);

        Assert.Equal(before + 1, version);
        var kinds = ActionKinds(version);
        Assert.Equal(1, kinds.Count(k => k == "metaData"));
        Assert.Contains("add", kinds);
        Assert.True(kinds.IndexOf("metaData") < kinds.IndexOf("add"));

        var fields = table.CurrentSnapshot.Schema.Fields;
        Assert.Equal(["id", "name"], fields.Select(f => f.Name));
        Assert.True(fields[1].Nullable);

        var rows = await RowsByIdAsync(table);
        Assert.Null(rows[1]["name"]);
        Assert.Null(rows[2]["name"]);
        Assert.Equal("c", rows[3]["name"]);
    }

    [Fact]
    public async Task MissingAndNewColumns_TogetherMissingReadsNull()
    {
        var schema = Schema(Long("id"), Str("a"));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);
        await table.WriteAsync([new RecordBatch(schema, [Longs(1), Strings("x")], 1)]);

        var batchSchema = Schema(Long("id"), Str("b"));
        await table.WriteAsync([new RecordBatch(batchSchema, [Longs(2), Strings("y")], 1)], mergeSchema: true);

        Assert.Equal(["id", "a", "b"], table.CurrentSnapshot.Schema.Fields.Select(f => f.Name));
        var rows = await RowsByIdAsync(table);
        Assert.Null(rows[2]["a"]);
        Assert.Equal("y", rows[2]["b"]);
        Assert.Null(rows[1]["b"]);
    }

    [Fact]
    public async Task NothingNew_CommitsNoMetadata()
    {
        await using var table = await CreateIdLongTableAsync();
        string schemaBefore = table.CurrentSnapshot.Metadata.SchemaString;

        var schema = Schema(Long("id"));
        long version = await table.WriteAsync([new RecordBatch(schema, [Longs(3)], 1)], mergeSchema: true);

        Assert.DoesNotContain("metaData", ActionKinds(version));
        Assert.Equal(schemaBefore, table.CurrentSnapshot.Metadata.SchemaString);
    }

    [Fact]
    public async Task ColumnMapping_NewColumnGetsAnIdAndPhysicalName()
    {
        await using var table = await CreateIdLongTableAsync(ColumnMappingMode.Name);
        int maxBefore = int.Parse(table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);

        var schema = Schema(Long("id"), Str("name"));
        await table.WriteAsync([new RecordBatch(schema, [Longs(3), Strings("c")], 1)], mergeSchema: true);

        var added = table.CurrentSnapshot.Schema.Fields[1];
        Assert.Equal((maxBefore + 1).ToString(), added.Metadata![ColumnMapping.FieldIdKey]);
        string physical = added.Metadata![ColumnMapping.PhysicalNameKey];
        Assert.StartsWith("col-", physical);
        Assert.Equal(
            (maxBefore + 1).ToString(),
            table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);

        // The data file stores the column under its physical name, and reads back under the logical one.
        var rows = await RowsByIdAsync(table);
        Assert.Equal("c", rows[3]["name"]);
        Assert.Null(rows[1]["name"]);
    }

    [Fact]
    public async Task NewStructField_IsAddedInsideTheStruct()
    {
        var inner = new Apache.Arrow.Types.StructType([Int("a", nullable: true)]);
        var schema = Schema(Long("id"), new Field("s", inner, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([new RecordBatch(schema,
            [Longs(1), new StructArray(inner, 1, [Ints(10)], ArrowBuffer.Empty)], 1)]);

        var wider = new Apache.Arrow.Types.StructType([Int("a", nullable: true), Str("b")]);
        var batchSchema = Schema(Long("id"), new Field("s", wider, true));
        await table.WriteAsync([new RecordBatch(batchSchema,
            [Longs(2), new StructArray(wider, 1, [Ints(20), Strings("bee")], ArrowBuffer.Empty)], 1)],
            mergeSchema: true);

        var s = Assert.IsType<DeltaLake.Schema.StructType>(table.CurrentSnapshot.Schema.Fields[1].Type);
        Assert.Equal(["a", "b"], s.Fields.Select(f => f.Name));
        Assert.True(s.Fields[1].Metadata!.ContainsKey(ColumnMapping.PhysicalNameKey));

        var rows = await RowsByIdAsync(table);
        var s1 = (Dictionary<string, object?>)rows[1]["s"]!;
        var s2 = (Dictionary<string, object?>)rows[2]["s"]!;
        Assert.Equal(10, s1["a"]);
        Assert.Null(s1["b"]);
        Assert.Equal(20, s2["a"]);
        Assert.Equal("bee", s2["b"]);
    }

    [Fact]
    public async Task NewFieldInsideAnArrayElement_IsRefused()
    {
        var inner = new Apache.Arrow.Types.StructType([Int("a", nullable: true)]);
        var schema = Schema(Long("id"), new Field("xs", new ListType(new Field("element", inner, true)), true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);
        long before = table.CurrentSnapshot.Version;

        var wider = new Apache.Arrow.Types.StructType([Int("a", nullable: true), Str("b")]);
        var listType = new ListType(new Field("element", wider, true));
        var values = new StructArray(wider, 1, [Ints(1), Strings("x")], ArrowBuffer.Empty);
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Build();
        var list = new ListArray(listType, 1, offsets, values, ArrowBuffer.Empty);
        var batchSchema = Schema(Long("id"), new Field("xs", listType, true));

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync([new RecordBatch(batchSchema, [Longs(1), list], 1)], mergeSchema: true));
        Assert.Equal(DeltaTableErrorCodes.UnsupportedSchemaMerge, ex.ErrorCode);
        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WideningInsideAnArrayElement_IsRefusedAsUnsupported_WhateverTheTableSetting(bool widening)
    {
        var schema = Schema(Long("id"), new Field("xs", new ListType(Int("element", nullable: true)), true));
        await using var table = await DeltaTable.CreateAsync(
            Fs, schema, configuration: widening ? Widening : null);

        var listType = new ListType(Long("element", nullable: true));
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Build();
        var list = new ListArray(listType, 1, offsets, Longs(5), ArrowBuffer.Empty);
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync(
                [new RecordBatch(Schema(Long("id"), new Field("xs", listType, true)), [Longs(1), list], 1)],
                mergeSchema: true));
        Assert.Equal(DeltaTableErrorCodes.UnsupportedSchemaMerge, ex.ErrorCode);
    }

    [Fact]
    public async Task CaseOnlyDifference_IsRefused()
    {
        await using var table = await CreateIdLongTableAsync();
        long before = table.CurrentSnapshot.Version;

        var schema = Schema(Long("ID"));
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync([new RecordBatch(schema, [Longs(3)], 1)], mergeSchema: true));
        Assert.Equal(DeltaTableErrorCodes.DuplicateColumnsFound, ex.ErrorCode);
        Assert.Contains("'ID'", ex.Message);
        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task ReadMetadataColumn_IsNotAdded()
    {
        await using var table = await CreateIdLongTableAsync();
        var schema = Schema(Long("id"), Long(DeltaMetadataColumns.DefaultPrefix + "row_id"));

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.WriteAsync([new RecordBatch(schema, [Longs(3), Longs(9)], 1)], mergeSchema: true));
        Assert.Single(table.CurrentSnapshot.Schema.Fields);
    }

    [Fact]
    public async Task Overwrite_WithMergeSchema_AddsTheColumn()
    {
        await using var table = await CreateIdLongTableAsync();

        var schema = Schema(Long("id"), Str("name"));
        await table.WriteAsync(
            [new RecordBatch(schema, [Longs(7), Strings("seven")], 1)], DeltaWriteMode.Overwrite, mergeSchema: true);

        var rows = await RowsByIdAsync(table);
        Assert.Equal("seven", Assert.Single(rows).Value["name"]);
    }

    // ── Type widening ──

    private static readonly Dictionary<string, string> Widening = new() { [DeltaLake.Schema.TypeWidening.EnableKey] = "true" };

    private async Task<DeltaTable> CreateIdIntTableAsync(IReadOnlyDictionary<string, string>? configuration)
    {
        var schema = Schema(Int("id"), Str("p"));
        var table = await DeltaTable.CreateAsync(Fs, schema, configuration: configuration);
        await table.WriteAsync([new RecordBatch(schema, [Ints(1, 2), Strings("a", "b")], 2)]);
        return table;
    }

    [Fact]
    public async Task WiderType_IsWidened_WhenTheTableEnablesTypeWidening()
    {
        await using var table = await CreateIdIntTableAsync(Widening);

        var schema = Schema(Long("id"), Str("p"));
        long version = await table.WriteAsync(
            [new RecordBatch(schema, [Longs(5_000_000_000L), Strings("c")], 1)], mergeSchema: true);

        var kinds = ActionKinds(version);
        Assert.Contains("protocol", kinds);
        Assert.Contains("metaData", kinds);

        var id = table.CurrentSnapshot.Schema.Fields[0];
        Assert.Equal("long", Assert.IsType<PrimitiveType>(id.Type).TypeName);
        var change = Assert.Single(DeltaLake.Schema.TypeWidening.GetTypeChanges(id));
        Assert.Equal(("integer", "long"), (change.FromType, change.ToType));
        Assert.Contains("typeWidening", table.CurrentSnapshot.Protocol.ReaderFeatures!);
        Assert.Contains("typeWidening", table.CurrentSnapshot.Protocol.WriterFeatures!);

        // The old file is read widened.
        var rows = await RowsByIdAsync(table);
        Assert.Equal([1L, 2L, 5_000_000_000L], rows.Keys.OrderBy(k => k));
        Assert.All(rows.Values, r => Assert.IsType<long>(r["id"]));
    }

    [Fact]
    public async Task WiderType_WithoutTypeWidening_IsRefused()
    {
        await using var table = await CreateIdIntTableAsync(configuration: null);
        long before = table.CurrentSnapshot.Version;

        var schema = Schema(Long("id"), Str("p"));
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync([new RecordBatch(schema, [Longs(3), Strings("c")], 1)], mergeSchema: true));
        Assert.Equal(DeltaTableErrorCodes.MergeIncompatibleDataType, ex.ErrorCode);
        Assert.Contains(DeltaLake.Schema.TypeWidening.EnableKey, ex.Message);
        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task IntegerToDouble_IsNotAnAutomaticWidening()
    {
        await using var table = await CreateIdIntTableAsync(Widening);
        long before = table.CurrentSnapshot.Version;

        var doubles = new DoubleArray.Builder().Append(1.5).Build();
        var schema = Schema(new Field("id", DoubleType.Default, false), Str("p"));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.WriteAsync([new RecordBatch(schema, [doubles, Strings("c")], 1)], mergeSchema: true));
        Assert.Equal(before, table.CurrentSnapshot.Version);
        Assert.Equal("integer", ((PrimitiveType)table.CurrentSnapshot.Schema.Fields[0].Type).TypeName);
    }

    [Fact]
    public async Task NarrowerType_IsStillRefused()
    {
        await using var table = await CreateIdLongTableAsync(configuration: Widening);

        var schema = Schema(Int("id"));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.WriteAsync([new RecordBatch(schema, [Ints(3)], 1)], mergeSchema: true));
    }

    [Fact]
    public async Task PartitionColumn_IsNotWidened()
    {
        var schema = Schema(Long("id"), Int("part"));
        await using var table = await DeltaTable.CreateAsync(
            Fs, schema, partitionColumns: ["part"], configuration: Widening);
        await table.WriteAsync([new RecordBatch(schema, [Longs(1), Ints(1)], 1)]);

        var wider = Schema(Long("id"), Long("part"));
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync([new RecordBatch(wider, [Longs(2), Longs(2)], 1)], mergeSchema: true));
        Assert.Equal(DeltaTableErrorCodes.MergeIncompatibleDataType, ex.ErrorCode);
        Assert.Contains("partition column", ex.Message);
    }

    [Fact]
    public async Task ColumnACheckConstraintReads_IsNotWidened()
    {
        var config = new Dictionary<string, string>(Widening) { ["delta.constraints.positive"] = "id > 0" };
        await using var table = await CreateIdIntTableAsync(config);

        var schema = Schema(Long("id"), Str("p"));
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync([new RecordBatch(schema, [Longs(3), Strings("c")], 1)], mergeSchema: true));
        Assert.Equal(DeltaTableErrorCodes.ConstraintDependentColumnChange, ex.ErrorCode);
    }

    [Theory]
    [InlineData("byte", "short", false, true)]
    [InlineData("integer", "integer", false, false)]
    [InlineData("short", "long", false, true)]
    [InlineData("long", "integer", false, false)]
    [InlineData("integer", "double", false, false)]
    [InlineData("integer", "decimal(20,0)", false, false)]
    [InlineData("float", "double", false, true)]
    [InlineData("date", "timestamp_ntz", false, true)]
    [InlineData("date", "timestamp", false, false)]
    [InlineData("decimal(10,2)", "decimal(12,4)", false, true)]
    [InlineData("decimal(10,2)", "decimal(11,3)", false, true)]
    [InlineData("decimal(10,2)", "decimal(11,4)", false, false)]
    [InlineData("decimal(10,2)", "decimal(10,2)", false, false)]
    [InlineData("decimal(10,2)", "decimal(12,2)", true, true)]
    [InlineData("decimal(10,2)", "decimal(12,4)", true, false)]
    [InlineData("short", "integer", true, false)]
    [InlineData("integer", "long", true, true)]
    [InlineData("date", "timestamp_ntz", true, false)]
    public void SchemaEvolutionWidenings_AreSparks(string from, string to, bool iceberg, bool expected) =>
        Assert.Equal(expected, DeltaTable.IsSchemaEvolutionWidening(from, to, iceberg));

    // ── Transactions ──

    [Fact]
    public async Task Transaction_MergesCompose_AndLaterAppendsSeeTheMergedSchema()
    {
        await using var table = await CreateIdLongTableAsync(ColumnMappingMode.Name);
        long before = table.CurrentSnapshot.Version;

        await using (var txn = table.StartTransaction())
        {
            await txn.WriteAsync(
                [new RecordBatch(Schema(Long("id"), Str("a")), [Longs(3), Strings("a3")], 1)], mergeSchema: true);
            await txn.WriteAsync(
                [new RecordBatch(Schema(Long("id"), Str("b")), [Longs(4), Strings("b4")], 1)], mergeSchema: true);
            // No merge asked for: the column the first merge added is declared now.
            await txn.WriteAsync([new RecordBatch(Schema(Long("id"), Str("a")), [Longs(5), Strings("a5")], 1)]);
            long version = await txn.CommitAsync();
            Assert.Equal(before + 1, version);
            Assert.Equal(1, ActionKinds(version).Count(k => k == "metaData"));
        }

        Assert.Equal(["id", "a", "b"], table.CurrentSnapshot.Schema.Fields.Select(f => f.Name));
        var rows = await RowsByIdAsync(table);
        Assert.Equal(("a3", null), ((string?)rows[3]["a"], (string?)rows[3]["b"]));
        Assert.Equal((null, "b4"), ((string?)rows[4]["a"], (string?)rows[4]["b"]));
        Assert.Equal(("a5", null), ((string?)rows[5]["a"], (string?)rows[5]["b"]));
    }

    [Fact]
    public async Task Transaction_MergeAfterAStagedSchemaChange_IsRefused()
    {
        await using var table = await CreateIdLongTableAsync(ColumnMappingMode.Name);
        await using var txn = table.StartTransaction();
        txn.StageSchemaChange(table.ComputeAddColumn(Str("other")));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await txn.WriteAsync(
                [new RecordBatch(Schema(Long("id"), Str("a")), [Longs(3), Strings("a3")], 1)], mergeSchema: true));
    }

    [Fact]
    public async Task Transaction_StagedSchemaChangeAfterAMerge_IsRefused()
    {
        await using var table = await CreateIdLongTableAsync(ColumnMappingMode.Name);
        await using var txn = table.StartTransaction();
        await txn.WriteAsync(
            [new RecordBatch(Schema(Long("id"), Str("a")), [Longs(3), Strings("a3")], 1)], mergeSchema: true);

        Assert.Throws<InvalidOperationException>(() => txn.StageSchemaChange(table.ComputeAddColumn(Str("other"))));
    }

    // ── Identity columns ──

    [Fact]
    public async Task IdentityHighWaterMark_SurvivesTheMerge()
    {
        var fs = Fs;
        var idMeta = JsonSerializer.Serialize(IdentityColumn.CreateMetadata(1, 1, false));
        string schemaString =
            $$$"""{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{{{idMeta}}}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""";
        await new TransactionLog(fs).WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["identityColumns"] },
            new MetadataAction { Id = "merge-identity", Format = Format.Parquet, SchemaString = schemaString, PartitionColumns = [] },
        });
        await using var table = await DeltaTable.OpenAsync(fs);

        var schema = Schema(Str("value"), Str("extra"));
        long version = await table.WriteAsync(
            [new RecordBatch(schema, [Strings("a", "b"), Strings("x", "y")], 2)], mergeSchema: true);
        Assert.Equal(1, ActionKinds(version).Count(k => k == "metaData"));

        var fields = table.CurrentSnapshot.Schema.Fields;
        Assert.Equal(["id", "value", "extra"], fields.Select(f => f.Name));
        Assert.Equal(2, IdentityColumn.GetConfig(fields[0])!.HighWaterMark);

        // A later plain write continues from the mark.
        await table.WriteAsync([new RecordBatch(schema, [Strings("c"), Strings("z")], 1)]);
        Assert.Equal(3, IdentityColumn.GetConfig(table.CurrentSnapshot.Schema.Fields[0])!.HighWaterMark);
    }

    private int DataFileCount() =>
        Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
            .Count(f => !f.Contains("_delta_log"));

    [Fact]
    public async Task Transaction_IdentityMergeAfterASchemaMerge_IsRefused_AndLeavesNoFiles()
    {
        var fs = Fs;
        var idMeta = JsonSerializer.Serialize(IdentityColumn.CreateMetadata(1, 1, false));
        string schemaString =
            $$$"""{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{{{idMeta}}}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""";
        await new TransactionLog(fs).WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["identityColumns"] },
            new MetadataAction { Id = "merge-identity-txn", Format = Format.Parquet, SchemaString = schemaString, PartitionColumns = [] },
        });
        await using var table = await DeltaTable.OpenAsync(fs);

        await using var txn = table.StartTransaction();
        await txn.WriteAsync(
            [new RecordBatch(Schema(Str("value"), Str("extra")), [Strings("a"), Strings("x")], 1)], mergeSchema: true);
        int files = DataFileCount();

        // The MERGE inserts a row, so it advances the identity mark: a metaData of its own, which the commit
        // cannot carry beside the merge's. Its insert file is written before that is known, and must go again.
        var source = new RecordBatch(Schema(Str("value")), [Strings("b")], 1);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await txn.MergeAsync([source], new MergeOptions { Keys = ["value"], WhenNotMatched = MergeAction.InsertColumns("value") }));
        Assert.Equal(files, DataFileCount());

        // The earlier work still commits.
        await txn.CommitAsync();
        Assert.Equal(["id", "value", "extra"], table.CurrentSnapshot.Schema.Fields.Select(f => f.Name));
        Assert.Equal(files, DataFileCount());
    }

    // ── Names with dots ──

    [Fact]
    public async Task ADottedColumnName_IsNotMistakenForAFieldOfAColumnTheMergeAdds()
    {
        // A top-level column literally named "a.b", and a batch that adds a column "a" before widening "a.b":
        // "a.b" is not a field of the new "a", so it is widened.
        await using var table = await DeltaTable.CreateAsync(
            Fs, Schema(Long("id"), Int("a.b", nullable: true)), configuration: Widening);
        await table.WriteAsync(
            [new RecordBatch(Schema(Long("id"), Int("a.b", nullable: true)), [Longs(1), Ints(10)], 1)]);

        var schema = Schema(Long("id"), Str("a"), Long("a.b", nullable: true));
        await table.WriteAsync([new RecordBatch(schema, [Longs(2), Strings("x"), Longs(5_000_000_000L)], 1)],
            mergeSchema: true);

        var fields = table.CurrentSnapshot.Schema.Fields;
        Assert.Equal(["id", "a.b", "a"], fields.Select(f => f.Name));
        Assert.Equal("long", ((PrimitiveType)fields[1].Type).TypeName);
        var rows = await RowsByIdAsync(table);
        Assert.Equal(10L, rows[1]["a.b"]);
        Assert.Equal(5_000_000_000L, rows[2]["a.b"]);
    }

    // ── Duplicate names in the batch ──

    public static TheoryData<bool> MergeOrNot => new() { false, true };

    [Theory]
    [MemberData(nameof(MergeOrNot))]
    public async Task ADuplicateDeclaredColumn_IsRefused(bool mergeSchema)
    {
        await using var table = await CreateIdLongTableAsync();
        long before = table.CurrentSnapshot.Version;

        var schema = Schema(Long("id"), Long("id"));
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.WriteAsync([new RecordBatch(schema, [Longs(3), Longs(4)], 1)], mergeSchema: mergeSchema));
        Assert.Contains("'id' more than once", ex.Message);
        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task ADuplicateNewColumn_IsRefused_AndNothingIsAdded()
    {
        await using var table = await CreateIdLongTableAsync();

        var schema = Schema(Long("id"), Str("extra"), Str("extra"));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.WriteAsync(
                [new RecordBatch(schema, [Longs(3), Strings("a"), Strings("b")], 1)], mergeSchema: true));
        Assert.DoesNotContain(table.CurrentSnapshot.Schema.Fields, f => f.Name == "extra");
    }

    [Fact]
    public async Task ADuplicateThatDiffersOnlyInCase_IsRefused()
    {
        await using var table = await CreateIdLongTableAsync();

        var schema = Schema(Long("id"), Str("extra"), Str("EXTRA"));
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.WriteAsync(
                [new RecordBatch(schema, [Longs(3), Strings("a"), Strings("b")], 1)], mergeSchema: true));
        Assert.Contains("case-insensitive", ex.Message);
    }

    [Fact]
    public async Task ADuplicateNestedField_IsRefused()
    {
        var inner = new Apache.Arrow.Types.StructType([Int("a", nullable: true)]);
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Long("id"), new Field("s", inner, true)));

        var dup = new Apache.Arrow.Types.StructType([Int("a", nullable: true), Int("b", nullable: true), Int("b", nullable: true)]);
        var s = new StructArray(dup, 1, [Ints(1), Ints(2), Ints(3)], ArrowBuffer.Empty);
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.WriteAsync(
                [new RecordBatch(Schema(Long("id"), new Field("s", dup, true)), [Longs(3), s], 1)], mergeSchema: true));
        Assert.Contains("'s.b' more than once", ex.Message);
    }

    [Theory]
    [MemberData(nameof(MergeOrNot))]
    public async Task ALogicalNameBesideItsPhysicalName_IsADuplicate(bool mergeSchema)
    {
        await using var table = await CreateIdLongTableAsync(ColumnMappingMode.Name);
        string physical = table.CurrentSnapshot.Schema.Fields[0].Metadata![ColumnMapping.PhysicalNameKey];

        var schema = Schema(Long("id"), Long(physical));
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.WriteAsync([new RecordBatch(schema, [Longs(3), Longs(4)], 1)], mergeSchema: mergeSchema));
        Assert.Contains("logical and physical names", ex.Message);
    }

    [Fact]
    public async Task WriteDataFilesAsync_RefusesADuplicateColumn()
    {
        await using var table = await CreateIdLongTableAsync();

        var schema = Schema(Long("id"), Long("id"));
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.WriteDataFilesAsync([new RecordBatch(schema, [Longs(3), Longs(4)], 1)]));
        Assert.Contains("'id' more than once", ex.Message);
    }

    [Fact]
    public async Task AMergeSourceWithADuplicateColumn_IsRefused()
    {
        await using var table = await CreateIdLongTableAsync();

        var source = new RecordBatch(Schema(Long("id"), Long("id")), [Longs(3), Longs(4)], 1);
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.MergeAsync([source], new MergeOptions { Keys = ["id"], WhenNotMatched = MergeAction.Insert }));
        Assert.Contains("'id' more than once", ex.Message);
    }
}
