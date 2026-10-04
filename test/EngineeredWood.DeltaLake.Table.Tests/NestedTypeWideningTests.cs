// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table.TypeWidening;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ArrowMapType = Apache.Arrow.Types.MapType;
using ArrowStructType = Apache.Arrow.Types.StructType;
using DeltaArrayType = EngineeredWood.DeltaLake.Schema.ArrayType;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #450: old files read against a schema that changed BELOW the top level. A field widened inside a struct, a
/// list element or a map key/value must come back wide, and a struct child added inside a list element or map
/// value must be backfilled. Tables are built by hand, as Spark leaves them, because EW has no ALTER COLUMN TYPE
/// and its own nested ALTERs do not reach into lists or maps.
/// </summary>
public class NestedTypeWideningTests : IDisposable
{
    private readonly string _tempDir;

    public NestedTypeWideningTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_ntw_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    private static async Task WriteFileAsync(LocalTableFileSystem fs, string name, RecordBatch batch)
    {
        await using var file = await fs.CreateAsync(name);
        await using var writer = new ParquetFileWriter(file, ownsFile: false);
        await writer.WriteRowGroupAsync(batch);
    }

    // Commits version 0: the typeWidening feature, `schemaString`, and one AddFile per data file.
    private async Task CommitAsync(
        LocalTableFileSystem fs, string schemaString, string? typeWideningConfig, params string[] dataFiles)
    {
        var config = new Dictionary<string, string>();
        if (typeWideningConfig is not null)
            config[Schema.TypeWidening.EnableKey] = typeWideningConfig;
        var actions = new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["typeWidening"],
                WriterFeatures = ["typeWidening"],
            },
            new MetadataAction
            {
                Id = "ntw",
                Format = Format.Parquet,
                SchemaString = schemaString,
                PartitionColumns = [],
                Configuration = config,
            },
        };
        foreach (var path in dataFiles)
        {
            actions.Add(new AddFile
            {
                Path = path,
                PartitionValues = new Dictionary<string, string>(),
                Size = new FileInfo(Path.Combine(_tempDir, path)).Length,
                ModificationTime = 1000,
                DataChange = true,
            });
        }
        await new TransactionLog(fs).WriteCommitAsync(0, actions);
    }

    private static async Task<List<RecordBatch>> ReadAllAsync(DeltaTable table)
    {
        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);
        return batches;
    }

    private static async Task<RecordBatch> ReadSingleAsync(LocalTableFileSystem fs)
    {
        await using var table = await DeltaTable.OpenAsync(fs);
        return Assert.Single(await ReadAllAsync(table));
    }

    private static ArrowBuffer Offsets(params int[] offsets)
    {
        var b = new ArrowBuffer.Builder<int>();
        foreach (var o in offsets)
            b.Append(o);
        return b.Build();
    }

    private static ArrowBuffer Validity(params bool[] valid)
    {
        var b = new ArrowBuffer.BitmapBuilder();
        foreach (var v in valid)
            b.Append(v);
        return b.Build();
    }

    private const string StructChildWidenedSchema = """
        {"type":"struct","fields":[
          {"name":"id","type":"long","nullable":true,"metadata":{}},
          {"name":"s","type":{"type":"struct","fields":[
            {"name":"a","type":"long","nullable":true,"metadata":{
              "delta.typeChanges":"[{\"fromType\":\"integer\",\"toType\":\"long\"}]"}}]},
           "nullable":true,"metadata":{}}]}
        """;

    // id: [1, 2]; s.a: [10, 20], both as int.
    private static RecordBatch NarrowStructBatch()
    {
        var st = new ArrowStructType([new Field("a", Int32Type.Default, true)]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("s", st, true))
            .Build();
        var s = new StructArray(st, 2, [new Int32Array.Builder().Append(10).Append(20).Build()], ArrowBuffer.Empty);
        return new RecordBatch(schema, [new Int64Array.Builder().Append(1).Append(2).Build(), s], 2);
    }

    // ── Reads ────────────────────────────────────────────────────────────────────────────────────────

    // Spark records the change on the struct CHILD's own metadata. The table property may have been turned off
    // since; the recorded change still governs how old files read.
    [Theory]
    [InlineData("true")]
    [InlineData("false")]
    [InlineData(null)]
    public async Task StructChild_IntToLong_IsWidenedOnRead(string? config)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await WriteFileAsync(fs, "old.parquet", NarrowStructBatch());
        await CommitAsync(fs, StructChildWidenedSchema, config, "old.parquet");

        var batch = await ReadSingleAsync(fs);
        var a = Assert.IsType<Int64Array>(((StructArray)batch.Column("s")).Fields[0]);
        Assert.Equal([10L, 20L], [a.GetValue(0)!.Value, a.GetValue(1)!.Value]);
        var schemaChild = ((ArrowStructType)batch.Schema.GetFieldByName("s").DataType).Fields[0];
        Assert.IsType<Int64Type>(schemaChild.DataType);
    }

    [Fact]
    public async Task StructChild_NullParentAndNullChild_KeepTheirValidity()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var st = new ArrowStructType([new Field("a", Int32Type.Default, true)]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("s", st, true))
            .Build();
        // Rows: s = {a: 1}, s = null, s = {a: null}.
        var child = new Int32Array.Builder().Append(1).Append(0).AppendNull().Build();
        var s = new StructArray(st, 3, [child], Validity(true, false, true), nullCount: 1);
        await WriteFileAsync(fs, "old.parquet", new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Append(3).Build(), s], 3));
        await CommitAsync(fs, StructChildWidenedSchema, "true", "old.parquet");

        var batch = await ReadSingleAsync(fs);
        var got = (StructArray)batch.Column("s");
        var a = Assert.IsType<Int64Array>(got.Fields[0]);
        Assert.False(got.IsNull(0));
        Assert.Equal(1L, a.GetValue(0));
        Assert.True(got.IsNull(1));
        Assert.False(got.IsNull(2));
        Assert.True(a.IsNull(2));
    }

    [Fact]
    public async Task ListElement_IntToLong_IsWidenedOnRead()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var lt = new ListType(new Field("element", Int32Type.Default, true));
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("l", lt, true)).Build();
        var values = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
        var list = new ListArray(lt, 2, Offsets(0, 2, 3), values, ArrowBuffer.Empty, 0);
        await WriteFileAsync(fs, "old.parquet", new RecordBatch(schema, [list], 2));

        const string schemaString = """
        {"type":"struct","fields":[{"name":"l","type":{"type":"array","elementType":"long","containsNull":true},
          "nullable":true,"metadata":{"delta.typeChanges":"[{\"fromType\":\"integer\",\"toType\":\"long\",\"fieldPath\":\"element\"}]"}}]}
        """;
        await CommitAsync(fs, schemaString, "true", "old.parquet");

        var batch = await ReadSingleAsync(fs);
        var l = Assert.IsType<ListArray>(batch.Column(0));
        var got = Assert.IsType<Int64Array>(l.Values);
        Assert.Equal([1L, 2L, 3L], got.Values.ToArray());
        Assert.Equal(2, l.GetValueLength(0));
        Assert.IsType<Int64Type>(((ListType)batch.Schema.FieldsList[0].DataType).ValueDataType);
    }

    [Fact]
    public async Task ListOfStruct_ChildIntToLong_IsWidenedOnRead()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var elem = new ArrowStructType([new Field("a", Int32Type.Default, true)]);
        var lt = new ListType(new Field("element", elem, true));
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("l", lt, true)).Build();
        var structs = new StructArray(elem, 3,
            [new Int32Array.Builder().Append(4).Append(5).Append(6).Build()], ArrowBuffer.Empty);
        await WriteFileAsync(fs, "old.parquet", new RecordBatch(schema,
            [new ListArray(lt, 2, Offsets(0, 1, 3), structs, ArrowBuffer.Empty, 0)], 2));

        const string schemaString = """
        {"type":"struct","fields":[{"name":"l","type":{"type":"array","elementType":{"type":"struct","fields":[
          {"name":"a","type":"long","nullable":true,"metadata":{"delta.typeChanges":"[{\"fromType\":\"integer\",\"toType\":\"long\"}]"}}
        ]},"containsNull":true},"nullable":true,"metadata":{}}]}
        """;
        await CommitAsync(fs, schemaString, "false", "old.parquet");

        var batch = await ReadSingleAsync(fs);
        var s = Assert.IsType<StructArray>(((ListArray)batch.Column(0)).Values);
        var a = Assert.IsType<Int64Array>(s.Fields[0]);
        Assert.Equal([4L, 5L, 6L], a.Values.ToArray());
    }

    [Fact]
    public async Task MapValue_IntToLong_IsWidenedOnRead()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var mt = new ArrowMapType(StringType.Default, Int32Type.Default);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("m", mt, true)).Build();
        var entries = new StructArray(mt.KeyValueType, 2,
        [
            new StringArray.Builder().Append("x").Append("y").Build(),
            new Int32Array.Builder().Append(10).Append(20).Build(),
        ], ArrowBuffer.Empty);
        await WriteFileAsync(fs, "old.parquet", new RecordBatch(schema,
            [new MapArray(mt, 1, Offsets(0, 2), entries, ArrowBuffer.Empty, 0)], 1));

        const string schemaString = """
        {"type":"struct","fields":[{"name":"m","type":{"type":"map","keyType":"string","valueType":"long","valueContainsNull":true},
          "nullable":true,"metadata":{"delta.typeChanges":"[{\"fromType\":\"integer\",\"toType\":\"long\",\"fieldPath\":\"value\"}]"}}]}
        """;
        await CommitAsync(fs, schemaString, "true", "old.parquet");

        var batch = await ReadSingleAsync(fs);
        var m = Assert.IsType<MapArray>(batch.Column(0));
        Assert.Equal([10L, 20L], Assert.IsType<Int64Array>(m.Values).Values.ToArray());
        Assert.Equal("y", ((StringArray)m.Keys).GetString(1));
    }

    [Fact]
    public async Task MapKey_IntToLong_IsWidenedOnRead()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var mt = new ArrowMapType(Int32Type.Default, StringType.Default);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("m", mt, true)).Build();
        var entries = new StructArray(mt.KeyValueType, 2,
        [
            new Int32Array.Builder().Append(1).Append(2).Build(),
            new StringArray.Builder().Append("x").Append("y").Build(),
        ], ArrowBuffer.Empty);
        await WriteFileAsync(fs, "old.parquet", new RecordBatch(schema,
            [new MapArray(mt, 1, Offsets(0, 2), entries, ArrowBuffer.Empty, 0)], 1));

        const string schemaString = """
        {"type":"struct","fields":[{"name":"m","type":{"type":"map","keyType":"long","valueType":"string","valueContainsNull":true},
          "nullable":true,"metadata":{"delta.typeChanges":"[{\"fromType\":\"integer\",\"toType\":\"long\",\"fieldPath\":\"key\"}]"}}]}
        """;
        await CommitAsync(fs, schemaString, "true", "old.parquet");

        var batch = await ReadSingleAsync(fs);
        var m = Assert.IsType<MapArray>(batch.Column(0));
        Assert.Equal([1L, 2L], Assert.IsType<Int64Array>(m.Keys).Values.ToArray());
    }

    // ── Reconcile inside lists and maps ──────────────────────────────────────────────────────────────

    // Spark: ALTER TABLE t ADD COLUMNS (l.element.b STRING)
    [Fact]
    public async Task ListOfStruct_MissingChild_IsBackfilled()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var elem = new ArrowStructType([new Field("a", Int64Type.Default, true)]);
        var lt = new ListType(new Field("element", elem, true));
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("l", lt, true)).Build();
        var structs = new StructArray(elem, 3,
            [new Int64Array.Builder().Append(1).Append(2).Append(3).Build()], ArrowBuffer.Empty);
        await WriteFileAsync(fs, "old.parquet", new RecordBatch(schema,
            [new ListArray(lt, 2, Offsets(0, 2, 3), structs, ArrowBuffer.Empty, 0)], 2));

        const string schemaString = """
        {"type":"struct","fields":[{"name":"l","type":{"type":"array","elementType":{"type":"struct","fields":[
          {"name":"a","type":"long","nullable":true,"metadata":{}},
          {"name":"b","type":"string","nullable":true,"metadata":{}}]},"containsNull":true},"nullable":true,"metadata":{}}]}
        """;
        await CommitAsync(fs, schemaString, null, "old.parquet");

        var batch = await ReadSingleAsync(fs);
        var s = Assert.IsType<StructArray>(((ListArray)batch.Column(0)).Values);
        Assert.Equal(2, s.Fields.Count);
        Assert.Equal([1L, 2L, 3L], ((Int64Array)s.Fields[0]).Values.ToArray());
        Assert.Equal(3, s.Fields[1].Length);
        Assert.Equal(3, s.Fields[1].NullCount);
        var elemType = (ArrowStructType)((ListType)batch.Schema.FieldsList[0].DataType).ValueDataType;
        Assert.Equal(["a", "b"], elemType.Fields.Select(f => f.Name).ToArray());
    }

    [Fact]
    public async Task MapOfStruct_MissingChild_IsBackfilled()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var valueType = new ArrowStructType([new Field("a", Int64Type.Default, true)]);
        var mt = new ArrowMapType(new Field("key", StringType.Default, false), new Field("value", valueType, true));
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("m", mt, true)).Build();
        var vals = new StructArray(valueType, 1, [new Int64Array.Builder().Append(5).Build()], ArrowBuffer.Empty);
        var entries = new StructArray(mt.KeyValueType, 1,
            [new StringArray.Builder().Append("k").Build(), vals], ArrowBuffer.Empty);
        await WriteFileAsync(fs, "old.parquet", new RecordBatch(schema,
            [new MapArray(mt, 1, Offsets(0, 1), entries, ArrowBuffer.Empty, 0)], 1));

        const string schemaString = """
        {"type":"struct","fields":[{"name":"m","type":{"type":"map","keyType":"string","valueType":{"type":"struct","fields":[
          {"name":"a","type":"long","nullable":true,"metadata":{}},
          {"name":"b","type":"string","nullable":true,"metadata":{}}]},"valueContainsNull":true},"nullable":true,"metadata":{}}]}
        """;
        await CommitAsync(fs, schemaString, null, "old.parquet");

        var batch = await ReadSingleAsync(fs);
        var m = Assert.IsType<MapArray>(batch.Column(0));
        var v = Assert.IsType<StructArray>(m.Values);
        Assert.Equal(2, v.Fields.Count);
        Assert.Equal(5L, ((Int64Array)v.Fields[0]).GetValue(0));
        Assert.True(v.Fields[1].IsNull(0));
        Assert.Equal("k", ((StringArray)m.Keys).GetString(0));
    }

    private static StructField Mapped(string name, DeltaDataType type, int id, string physical) => new()
    {
        Name = name,
        Type = type,
        Nullable = true,
        Metadata = new Dictionary<string, string>
        {
            [ColumnMapping.FieldIdKey] = id.ToString(),
            [ColumnMapping.PhysicalNameKey] = physical,
        },
    };

    // Spark: drop arr.element.b, then add arr.element.c. In name mode the dropped field used to leak out under
    // its physical name; in id mode the added one was missing.
    [Theory]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task ArrayOfStruct_NestedAddAndDrop_OldFileReadsInTableShape(ColumnMappingMode mode)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var elem = new ArrowStructType(
            [new Field("a", Int64Type.Default, true), new Field("b", StringType.Default, true)]);
        var arrow = new Apache.Arrow.Schema.Builder()
            .Field(new Field("arr", new ListType(new Field("element", elem, true)), true))
            .Build();
        var longType = new PrimitiveType { TypeName = "long" };
        StructField Arr(params StructField[] children) => Mapped("arr", new DeltaArrayType
        {
            ElementType = new DeltaStructType { Fields = children },
            ContainsNull = true,
        }, 1, "col-arr");
        var pre = new DeltaStructType
        {
            Fields = [Arr(Mapped("a", longType, 2, "col-a"),
                Mapped("b", new PrimitiveType { TypeName = "string" }, 3, "col-b"))],
        };

        long next;
        MetadataAction meta;
        await using (var t = await DeltaTable.CreateAsync(
            fs, arrow, columnMappingMode: mode, preAssignedSchema: pre))
        {
            var values = new StructArray(elem, 1,
                [new Int64Array.Builder().Append(7).Build(), new StringArray.Builder().Append("p").Build()],
                ArrowBuffer.Empty);
            var list = new ListArray(arrow.FieldsList[0].DataType, 1, Offsets(0, 1), values, ArrowBuffer.Empty);
            await t.WriteAsync([new RecordBatch(arrow, [list], 1)]);
            meta = t.CurrentSnapshot.Metadata;
            next = t.CurrentSnapshot.Version + 1;
        }

        var evolved = new DeltaStructType
        {
            Fields = [Arr(Mapped("a", longType, 2, "col-a"), Mapped("c", longType, 4, "col-c"))],
        };
        var cfg = meta.Configuration!.ToDictionary(kv => kv.Key, kv => kv.Value);
        cfg[ColumnMapping.MaxColumnIdKey] = "4";
        await new TransactionLog(fs).WriteCommitAsync(next, new List<DeltaAction>
        {
            meta with { SchemaString = DeltaSchemaSerializer.Serialize(evolved), Configuration = cfg },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        var batch = Assert.Single(await ReadAllAsync(table));
        var gotElem = (ArrowStructType)((ListType)batch.Schema.FieldsList[0].DataType).ValueDataType;
        Assert.Equal(["a", "c"], gotElem.Fields.Select(f => f.Name).ToArray());
        var s = (StructArray)((ListArray)batch.Column(0)).Values;
        Assert.Equal(7L, ((Int64Array)s.Fields[0]).GetValue(0));
        Assert.True(s.Fields[1].IsNull(0));
    }

    // ── Rewrites ─────────────────────────────────────────────────────────────────────────────────────

    // Compaction used to fix the column as Int32 from the old file's first row group and then truncate the new
    // file's 5,000,000,000 to 705,032,704, permanently: the source files are removed.
    [Fact]
    public async Task Compaction_OldAndNewStructChildEras_KeepsEveryValue()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await WriteFileAsync(fs, "old.parquet", NarrowStructBatch());
        await CommitAsync(fs, StructChildWidenedSchema, "true", "old.parquet");

        await using var table = await DeltaTable.OpenAsync(fs);
        var wideStruct = new ArrowStructType([new Field("a", Int64Type.Default, true)]);
        var wide = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("s", wideStruct, true))
            .Build();
        var s = new StructArray(wideStruct, 1, [new Int64Array.Builder().Append(5_000_000_000L).Build()],
            ArrowBuffer.Empty);
        await table.WriteAsync([new RecordBatch(wide, [new Int64Array.Builder().Append(3).Build(), s], 1)]);

        await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });

        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        var values = new List<long>();
        foreach (var b in await ReadAllAsync(table))
        {
            var a = Assert.IsType<Int64Array>(((StructArray)b.Column("s")).Fields[0]);
            for (int i = 0; i < b.Length; i++)
                values.Add(a.GetValue(i)!.Value);
        }
        values.Sort();
        Assert.Equal([10L, 20L, 5_000_000_000L], values);
    }

    [Theory]
    [InlineData("true")]
    [InlineData("false")]
    public async Task Update_OfAFileWithANarrowNestedField_RewritesItWide(string config)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await WriteFileAsync(fs, "old.parquet", NarrowStructBatch());
        await CommitAsync(fs, StructChildWidenedSchema, config, "old.parquet");

        await using var table = await DeltaTable.OpenAsync(fs);
        await table.UpdateAsync(Expressions.Expressions.Equal("id", 1L), b => b);

        var values = new List<long>();
        foreach (var b in await ReadAllAsync(table))
        {
            var a = Assert.IsType<Int64Array>(((StructArray)b.Column("s")).Fields[0]);
            for (int i = 0; i < b.Length; i++)
                values.Add(a.GetValue(i)!.Value);
        }
        values.Sort();
        Assert.Equal([10L, 20L], values);
    }

    // ── ValueWidener directly: offsets ───────────────────────────────────────────────────────────────

    // A sliced list (offset 1) and a struct whose child carries its own offset: the widened arrays must keep
    // addressing the same elements.
    [Fact]
    public void WidenBatch_SlicedNestedArrays_KeepTheirElements()
    {
        var lt = new ListType(new Field("element", Int32Type.Default, true));
        var list = new ListArray(lt, 3, Offsets(0, 1, 3, 6),
            new Int32Array.Builder().AppendRange([1, 2, 3, 4, 5, 6]).Build(), ArrowBuffer.Empty, 0);
        var slicedList = (ListArray)list.Slice(1, 2); // [[2, 3], [4, 5, 6]]

        var st = new ArrowStructType([new Field("a", Int32Type.Default, true)]);
        var child = (Int32Array)new Int32Array.Builder().AppendRange([0, 7, 8]).Build().Slice(1, 2);
        var str = new StructArray(st, 2, [child], ArrowBuffer.Empty); // [{a:7}, {a:8}]

        var batch = new RecordBatch(new Apache.Arrow.Schema.Builder()
            .Field(new Field("l", lt, true)).Field(new Field("s", st, true)).Build(), [slicedList, str], 2);
        var target = new Apache.Arrow.Schema.Builder()
            .Field(new Field("l", new ListType(new Field("element", Int64Type.Default, true)), true))
            .Field(new Field("s", new ArrowStructType([new Field("a", Int64Type.Default, true)]), true))
            .Build();

        var widened = ValueWidener.WidenBatch(batch, target);

        var l = (ListArray)widened.Column(0);
        var row0 = (Int64Array)l.GetSlicedValues(0);
        var row1 = (Int64Array)l.GetSlicedValues(1);
        Assert.Equal([2L, 3L], row0.ToArray().Select(v => v!.Value).ToArray());
        Assert.Equal([4L, 5L, 6L], row1.ToArray().Select(v => v!.Value).ToArray());
        var a = (Int64Array)((StructArray)widened.Column(1)).Fields[0];
        Assert.Equal([7L, 8L], a.ToArray().Select(v => v!.Value).ToArray());
        Assert.IsType<Int64Type>(((ListType)widened.Schema.FieldsList[0].DataType).ValueDataType);
    }
}
