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
            MapArray a => Enumerable.Range(a.ValueOffsets[i], a.GetValueLength(i)).ToDictionary(
                e => Value(a.Keys, e)!, e => Value(a.Values, e)),
            ListArray a => Enumerable.Range(a.ValueOffsets[i], a.GetValueLength(i))
                .Select(e => Value(a.Values, e)).ToList(),
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

    // ── Inside arrays and maps (#603) ──

    /// <summary>A one-row list column holding <paramref name="values"/> as its single row's elements.</summary>
    private static ListArray OneRowList(ListType type, IArrowArray values)
    {
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(values.Length).Build();
        return new ListArray(type, 1, offsets, values, ArrowBuffer.Empty);
    }

    /// <summary>A one-row map column holding the single entry <paramref name="key"/> → <paramref name="value"/>.</summary>
    private static MapArray OneEntryMap(Apache.Arrow.Types.MapType type, IArrowArray key, IArrowArray value)
    {
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Build();
        var entries = new StructArray(type.KeyValueType, 1, [key, value], ArrowBuffer.Empty);
        return new MapArray(type, 1, offsets, entries, ArrowBuffer.Empty);
    }

    private static Field ListOf(string name, Field element) => new(name, new ListType(element), true);

    private static StructField Column(DeltaTable table, string name) =>
        table.CurrentSnapshot.Schema.Fields.Single(f => f.Name == name);

    [Theory]
    [InlineData(ColumnMappingMode.None)]
    [InlineData(ColumnMappingMode.Name)]
    public async Task NewFieldInsideAnArrayElement_IsAdded(ColumnMappingMode mode)
    {
        var inner = new Apache.Arrow.Types.StructType([Int("a", nullable: true)]);
        var schema = Schema(Long("id"), ListOf("xs", new Field("element", inner, true)));
        await using var table = await DeltaTable.CreateAsync(Fs, schema, columnMappingMode: mode);
        await table.WriteAsync([new RecordBatch(schema,
            [Longs(1), OneRowList(new ListType(new Field("element", inner, true)),
                new StructArray(inner, 1, [Ints(10)], ArrowBuffer.Empty))], 1)]);
        int maxBefore = mode == ColumnMappingMode.None
            ? 0
            : int.Parse(table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);

        var wider = new Apache.Arrow.Types.StructType([Int("a", nullable: true), Str("b", nullable: false)]);
        var listType = new ListType(new Field("element", wider, true));
        var list = OneRowList(listType, new StructArray(wider, 1, [Ints(20), Strings("bee")], ArrowBuffer.Empty));
        long version = await table.WriteAsync(
            [new RecordBatch(Schema(Long("id"), ListOf("xs", new Field("element", wider, true))), [Longs(2), list], 1)],
            mergeSchema: true);
        Assert.Contains("metaData", ActionKinds(version));

        var element = Assert.IsType<DeltaLake.Schema.StructType>(Assert.IsType<DeltaLake.Schema.ArrayType>(Column(table, "xs").Type).ElementType);
        Assert.Equal(["a", "b"], element.Fields.Select(f => f.Name));
        var b = element.Fields[1];
        Assert.True(b.Nullable); // added nullable, whatever the batch declares
        if (mode == ColumnMappingMode.Name)
        {
            Assert.Equal((maxBefore + 1).ToString(), b.Metadata![ColumnMapping.FieldIdKey]);
            Assert.StartsWith("col-", b.Metadata![ColumnMapping.PhysicalNameKey]);
            Assert.Equal((maxBefore + 1).ToString(),
                table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);
        }

        // The old file's elements read the new field as NULL.
        var rows = await RowsByIdAsync(table);
        var old = (Dictionary<string, object?>)Assert.Single((List<object?>)rows[1]["xs"]!)!;
        Assert.Equal(10, old["a"]);
        Assert.Null(old["b"]);
        var added = (Dictionary<string, object?>)Assert.Single((List<object?>)rows[2]["xs"]!)!;
        Assert.Equal(20, added["a"]);
        Assert.Equal("bee", added["b"]);
    }

    [Fact]
    public async Task NewFieldInsideAMapValue_IsAdded()
    {
        var inner = new Apache.Arrow.Types.StructType([Int("a", nullable: true)]);
        var schema = Schema(Long("id"), new Field("m", new Apache.Arrow.Types.MapType(Str("key", nullable: false), new Field("value", inner, true)), true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);

        var wider = new Apache.Arrow.Types.StructType([Int("a", nullable: true), Str("b")]);
        var mapType = new Apache.Arrow.Types.MapType(Str("key", nullable: false), new Field("value", wider, true));
        var map = OneEntryMap(mapType, Strings("k"), new StructArray(wider, 1, [Ints(1), Strings("x")], ArrowBuffer.Empty));
        await table.WriteAsync(
            [new RecordBatch(Schema(Long("id"), new Field("m", mapType, true)), [Longs(1), map], 1)], mergeSchema: true);

        var value = Assert.IsType<DeltaLake.Schema.StructType>(Assert.IsType<DeltaLake.Schema.MapType>(Column(table, "m").Type).ValueType);
        Assert.Equal(["a", "b"], value.Fields.Select(f => f.Name));
    }

    [Fact]
    public async Task WideningInsideAnArrayElement_IsRecordedOnTheColumn_WithAFieldPath()
    {
        var schema = Schema(Long("id"), ListOf("xs", Int("element", nullable: true)));
        await using var table = await DeltaTable.CreateAsync(Fs, schema, configuration: Widening);
        await table.WriteAsync([new RecordBatch(schema,
            [Longs(1), OneRowList(new ListType(Int("element", nullable: true)), Ints(7))], 1)]);

        var listType = new ListType(Long("element", nullable: true));
        await table.WriteAsync(
            [new RecordBatch(Schema(Long("id"), new Field("xs", listType, true)),
                [Longs(2), OneRowList(listType, Longs(5_000_000_000L))], 1)],
            mergeSchema: true);

        var xs = Column(table, "xs");
        var array = Assert.IsType<DeltaLake.Schema.ArrayType>(xs.Type);
        Assert.Equal("long", Assert.IsType<PrimitiveType>(array.ElementType).TypeName);
        Assert.True(array.ContainsNull);
        var change = Assert.Single(DeltaLake.Schema.TypeWidening.GetTypeChanges(xs));
        Assert.Equal(("integer", "long", "element"), (change.FromType, change.ToType, change.FieldPath));

        // The old file's elements are read widened.
        var rows = await RowsByIdAsync(table);
        Assert.Equal(new object?[] { 7L }, (List<object?>)rows[1]["xs"]!);
        Assert.Equal(new object?[] { 5_000_000_000L }, (List<object?>)rows[2]["xs"]!);
    }

    [Fact]
    public async Task WideningAMapsKeyAndValue_RecordsBothOnTheColumn()
    {
        var narrow = new Apache.Arrow.Types.MapType(Int("key"), Int("value", nullable: true));
        var schema = Schema(Long("id"), new Field("m", narrow, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema, configuration: Widening);
        await table.WriteAsync([new RecordBatch(schema, [Longs(1), OneEntryMap(narrow, Ints(1), Ints(2))], 1)]);

        var wide = new Apache.Arrow.Types.MapType(Long("key"), Long("value", nullable: true));
        await table.WriteAsync(
            [new RecordBatch(Schema(Long("id"), new Field("m", wide, true)), [Longs(2), OneEntryMap(wide, Longs(3), Longs(4))], 1)],
            mergeSchema: true);

        var m = Column(table, "m");
        var map = Assert.IsType<DeltaLake.Schema.MapType>(m.Type);
        Assert.Equal(("long", "long"),
            (((PrimitiveType)map.KeyType).TypeName, ((PrimitiveType)map.ValueType).TypeName));
        Assert.Equal(
            [("integer", "long", "key"), ("integer", "long", "value")],
            DeltaLake.Schema.TypeWidening.GetTypeChanges(m).Select(c => (c.FromType, c.ToType, c.FieldPath!)));

        var rows = await RowsByIdAsync(table);
        Assert.Equal(new Dictionary<object, object?> { [1L] = 2L }, rows[1]["m"]);
        Assert.Equal(new Dictionary<object, object?> { [3L] = 4L }, rows[2]["m"]);
    }

    [Fact]
    public async Task WideningInsideNestedCollections_JoinsTheFieldPath()
    {
        var narrow = new ListType(new Field("element", new ListType(Int("element", nullable: true)), true));
        var schema = Schema(Long("id"), new Field("xss", narrow, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema, configuration: Widening);

        var wide = new ListType(new Field("element", new ListType(Long("element", nullable: true)), true));
        var inner = OneRowList((ListType)wide.ValueDataType, Longs(1));
        await table.WriteAsync(
            [new RecordBatch(Schema(Long("id"), new Field("xss", wide, true)), [Longs(1), OneRowList(wide, inner)], 1)],
            mergeSchema: true);

        var change = Assert.Single(DeltaLake.Schema.TypeWidening.GetTypeChanges(Column(table, "xss")));
        Assert.Equal("element.element", change.FieldPath);
    }

    [Fact]
    public async Task WideningInsideAStructInsideAnArray_IsRecordedOnTheNearestStructField()
    {
        // xs: array<struct<a: array<int>>> — the change goes on `a`, with fieldPath "element", not on `xs`.
        var narrowA = ListOf("a", Int("element", nullable: true));
        var narrowElement = new Apache.Arrow.Types.StructType([narrowA]);
        var schema = Schema(Long("id"), ListOf("xs", new Field("element", narrowElement, true)));
        await using var table = await DeltaTable.CreateAsync(Fs, schema, configuration: Widening);

        var wideAType = new ListType(Long("element", nullable: true));
        var wideElement = new Apache.Arrow.Types.StructType([new Field("a", wideAType, true)]);
        var xsType = new ListType(new Field("element", wideElement, true));
        var element = new StructArray(wideElement, 1, [OneRowList(wideAType, Longs(9))], ArrowBuffer.Empty);
        await table.WriteAsync(
            [new RecordBatch(Schema(Long("id"), new Field("xs", xsType, true)), [Longs(1), OneRowList(xsType, element)], 1)],
            mergeSchema: true);

        var xs = Column(table, "xs");
        Assert.Empty(DeltaLake.Schema.TypeWidening.GetTypeChanges(xs));
        var a = Assert.IsType<DeltaLake.Schema.StructType>(((DeltaLake.Schema.ArrayType)xs.Type).ElementType).Fields[0];
        var change = Assert.Single(DeltaLake.Schema.TypeWidening.GetTypeChanges(a));
        Assert.Equal(("integer", "long", "element"), (change.FromType, change.ToType, change.FieldPath));
    }

    [Fact]
    public async Task AStructFieldNamedElement_IsNotTakenForAnArrayElement()
    {
        // s: struct<element: int> widened is s.element's own change, with no fieldPath.
        var schema = Schema(Long("id"), new Field("s", new Apache.Arrow.Types.StructType([Int("element", nullable: true)]), true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema, configuration: Widening);

        var wide = new Apache.Arrow.Types.StructType([Long("element", nullable: true)]);
        await table.WriteAsync(
            [new RecordBatch(Schema(Long("id"), new Field("s", wide, true)),
                [Longs(1), new StructArray(wide, 1, [Longs(1)], ArrowBuffer.Empty)], 1)],
            mergeSchema: true);

        var field = Assert.IsType<DeltaLake.Schema.StructType>(Column(table, "s").Type).Fields[0];
        var change = Assert.Single(DeltaLake.Schema.TypeWidening.GetTypeChanges(field));
        Assert.Null(change.FieldPath);
    }

    [Fact]
    public async Task WideningInsideAnArray_WithoutTypeWidening_IsRefused()
    {
        var schema = Schema(Long("id"), ListOf("xs", Int("element", nullable: true)));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);
        long before = table.CurrentSnapshot.Version;

        var listType = new ListType(Long("element", nullable: true));
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync(
                [new RecordBatch(Schema(Long("id"), new Field("xs", listType, true)), [Longs(1), OneRowList(listType, Longs(5))], 1)],
                mergeSchema: true));
        Assert.Equal(DeltaTableErrorCodes.MergeIncompatibleDataType, ex.ErrorCode);
        Assert.Contains("'xs.element'", ex.Message);
        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task WideningInsideAnArrayACheckConstraintReads_IsRefused()
    {
        var config = new Dictionary<string, string>(Widening) { ["delta.constraints.first"] = "xs[0] > 0" };
        var schema = Schema(Long("id"), ListOf("xs", Int("element", nullable: true)));
        await using var table = await DeltaTable.CreateAsync(Fs, schema, configuration: config);

        var listType = new ListType(Long("element", nullable: true));
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync(
                [new RecordBatch(Schema(Long("id"), new Field("xs", listType, true)), [Longs(1), OneRowList(listType, Longs(5))], 1)],
                mergeSchema: true));
        Assert.Equal(DeltaTableErrorCodes.ConstraintDataTypeMismatch, ex.ErrorCode);
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

    /// <summary>
    /// <c>delta.enableTypeWidening=true</c> declares the <c>typeWidening</c> reader+writer feature from version 0, as
    /// Spark does (#605; the Spark twins are in <c>ProtocolFloorPropertyInteropTests</c>), not only once a column is
    /// widened.
    /// </summary>
    [Fact]
    public async Task Create_WithTypeWidening_DeclaresTheFeature()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Int("id")), configuration: Widening);

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((3, 7), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        Assert.Equal(["typeWidening"], protocol.ReaderFeatures!);
        Assert.Equal(
            ["appendOnly", "invariants", "typeWidening"],
            protocol.WriterFeatures!.OrderBy(f => f, StringComparer.Ordinal));
    }

    [Theory]
    [InlineData("false")]
    [InlineData("nonsense")]
    public async Task Create_WithTypeWideningNotEnabled_DeclaresNothing(string value)
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, Schema(Int("id")),
            configuration: new Dictionary<string, string> { [DeltaLake.Schema.TypeWidening.EnableKey] = value });

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((1, 2), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        Assert.Null(protocol.ReaderFeatures);
        Assert.Null(protocol.WriterFeatures);
    }

    [Fact]
    public async Task Replace_WithTypeWidening_DeclaresTheFeature()
    {
        await using (await DeltaTable.CreateAsync(Fs, Schema(Int("id"))))
        {
        }

        await using var table = await DeltaTable.CreateOrReplaceAsync(Fs, Schema(Int("id")), [], configuration: Widening);

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((3, 7), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        Assert.Contains("typeWidening", protocol.ReaderFeatures!);
        Assert.Contains("typeWidening", protocol.WriterFeatures!);
    }

    [Fact]
    public async Task WiderType_IsWidened_WhenTheTableEnablesTypeWidening()
    {
        await using var table = await CreateIdIntTableAsync(Widening);

        var schema = Schema(Long("id"), Str("p"));
        long version = await table.WriteAsync(
            [new RecordBatch(schema, [Longs(5_000_000_000L), Strings("c")], 1)], mergeSchema: true);

        var kinds = ActionKinds(version);
        // The table declared typeWidening when it was created (#605), so the widening needs no protocol change.
        Assert.DoesNotContain("protocol", kinds);
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
        Assert.Equal(DeltaTableErrorCodes.ConstraintDataTypeMismatch, ex.ErrorCode);
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
    public async Task Transaction_IdentityMergeAfterASchemaMerge_BuildsOnTheMergedSchema()
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

        // The MERGE inserts a row, so it advances the identity mark: its metaData is computed from the merged schema,
        // as a later write's is, and replaces the merge's in the commit without dropping the merged column.
        var source = new RecordBatch(Schema(Str("value"), Str("extra")), [Strings("b"), Strings("y")], 1);
        await txn.MergeAsync([source], new MergeOptions { Keys = ["value"], WhenNotMatched = [MergeAction.InsertColumns("value", "extra")] });
        long version = await txn.CommitAsync();

        Assert.Equal(["id", "value", "extra"], table.CurrentSnapshot.Schema.Fields.Select(f => f.Name));
        Assert.Equal(2, IdentityColumn.GetConfig(table.CurrentSnapshot.Schema.Fields[0])!.HighWaterMark);
        Assert.Single(ActionKinds(version), k => k == "metaData");
        var rows = await RowsByIdAsync(table);
        Assert.Equal(["a", "b"], rows.Values.Select(r => (string?)r["value"]).OrderBy(v => v));
        Assert.Equal(["x", "y"], rows.Values.Select(r => (string?)r["extra"]).OrderBy(v => v));
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
            await table.MergeAsync([source], new MergeOptions { Keys = ["id"], WhenNotMatched = [MergeAction.Insert] }));
        Assert.Contains("'id' more than once", ex.Message);
    }
}
