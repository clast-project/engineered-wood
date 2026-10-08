// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet.Schema;
using ArrowMapType = Apache.Arrow.Types.MapType;
using ArrowStructType = Apache.Arrow.Types.StructType;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #467: IcebergCompatV2 gives every array element and map key/value a column id, recorded under
/// <c>delta.columnMapping.nested.ids</c> on the nearest ancestor field and written as the Parquet <c>field_id</c>
/// of the <c>element</c>/<c>key</c>/<c>value</c> node. EW assigned none, so a UniForm conversion could not map
/// them. The expected ids follow delta-spark's and delta-kernel's <c>rewriteFieldIdsForIceberg</c>: every column
/// id first, then the nested ids in schema order, each assigned before descending.
/// </summary>
public class IcebergCompatNestedIdsTests : IDisposable
{
    private readonly string _tempDir;

    public IcebergCompatNestedIdsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_nested_ids_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Dictionary<string, string> V2 = new() { [IcebergCompat.EnableV2Key] = "true" };

    private static Field ListOf(string name, IArrowType element) =>
        new(name, new ListType(new Field("element", element, true)), true);

    private static Field MapOf(string name, IArrowType key, IArrowType value) =>
        new(name, new ArrowMapType(new Field("key", key, false), new Field("value", value, true)), true);

    private Task<DeltaTable> CreateAsync(
        Apache.Arrow.Schema schema, ColumnMappingMode mode = ColumnMappingMode.Name,
        Dictionary<string, string>? configuration = null) =>
        DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema,
            columnMappingMode: mode, configuration: configuration ?? V2).AsTask();

    private static string Physical(StructField field) =>
        ColumnMapping.GetPhysicalName(field, ColumnMappingMode.Name);

    private static int Id(StructField field) => ColumnMapping.GetFieldId(field)!.Value;

    private static Dictionary<string, int> Nested(StructField field) =>
        ColumnMapping.GetNestedIds(field).ToDictionary(e => e.Key, e => e.Value);

    private static int MaxColumnId(DeltaTable table) =>
        int.Parse(table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);

    [Fact]
    public async Task Create_ArrayColumn_RecordsTheElementId()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(ListOf("arr", Int64Type.Default))
            .Build();
        await using var table = await CreateAsync(schema);

        var fields = table.CurrentSnapshot.Schema.Fields;
        Assert.Equal([1, 2], fields.Select(Id));
        Assert.Empty(Nested(fields[0]));
        Assert.Equal(new Dictionary<string, int> { [Physical(fields[1]) + ".element"] = 3 }, Nested(fields[1]));
        Assert.Equal(3, MaxColumnId(table));

        // Written as the JSON object (of numbers) the spec types it as, not as a string.
        Assert.Contains(
            $"\"{ColumnMapping.NestedIdsKey}\":{{\"{Physical(fields[1])}.element\":3}}",
            table.CurrentSnapshot.Metadata.SchemaString);
    }

    [Fact]
    public async Task Create_MapOfArrays_RecordsEveryLevelInOrder()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(MapOf("m", StringType.Default, new ListType(new Field("element", Int32Type.Default, true))))
            .Build();
        await using var table = await CreateAsync(schema);

        var m = table.CurrentSnapshot.Schema.Fields.Single();
        string p = Physical(m);
        Assert.Equal(
            [
                new KeyValuePair<string, int>(p + ".key", 2),
                new KeyValuePair<string, int>(p + ".value", 3),
                new KeyValuePair<string, int>(p + ".value.element", 4),
            ],
            ColumnMapping.GetNestedIds(m));
        Assert.Equal(4, MaxColumnId(table));
    }

    [Fact]
    public async Task Create_StructBelowAnArray_RecordsItsOwnFieldsIdsOnThem()
    {
        // arr: array<struct<a: array<int>>>. The struct starts over: `a` records its own element id, under its own
        // physical name, and `arr` records only the level above the struct.
        var inner = new ArrowStructType([ListOf("a", Int32Type.Default)]);
        var schema = new Apache.Arrow.Schema.Builder().Field(ListOf("arr", inner)).Build();
        await using var table = await CreateAsync(schema);

        var arr = table.CurrentSnapshot.Schema.Fields.Single();
        var a = ((DeltaStructType)((ArrayType)arr.Type).ElementType).Fields.Single();
        Assert.Equal((1, 2), (Id(arr), Id(a)));
        Assert.Equal(new Dictionary<string, int> { [Physical(arr) + ".element"] = 3 }, Nested(arr));
        Assert.Equal(new Dictionary<string, int> { [Physical(a) + ".element"] = 4 }, Nested(a));
        Assert.Equal(4, MaxColumnId(table));
    }

    [Fact]
    public async Task Create_WithoutIcebergCompat_RecordsNoneAndBurnsNoIds()
    {
        // Spark and Kernel assign nested ids only under IcebergCompatV2. Column mapping alone gives only struct
        // fields ids — and EW used to burn ids on the element/key/value it never recorded.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(ListOf("arr", Int64Type.Default))
            .Field(MapOf("m", StringType.Default, Int64Type.Default))
            .Build();
        await using var table = await CreateAsync(schema, configuration: new Dictionary<string, string>());

        var fields = table.CurrentSnapshot.Schema.Fields;
        Assert.Equal([1, 2], fields.Select(Id));
        Assert.All(fields, f => Assert.Empty(Nested(f)));
        Assert.Equal(2, MaxColumnId(table));
        Assert.DoesNotContain(ColumnMapping.NestedIdsKey, table.CurrentSnapshot.Metadata.SchemaString);
    }

    [Fact]
    public async Task AddColumn_ContinuesPastTheNestedIds_AndKeepsTheExistingOnes()
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(ListOf("arr", Int64Type.Default)).Build();
        await using var table = await CreateAsync(schema);
        var before = Nested(table.CurrentSnapshot.Schema.Fields[0]);

        await table.AddColumnAsync(MapOf("m", StringType.Default, Int64Type.Default));

        var fields = table.CurrentSnapshot.Schema.Fields;
        Assert.Equal(before, Nested(fields[0]));
        Assert.Equal(3, Id(fields[1]));
        string p = Physical(fields[1]);
        Assert.Equal(new Dictionary<string, int> { [p + ".key"] = 4, [p + ".value"] = 5 }, Nested(fields[1]));
        Assert.Equal(5, MaxColumnId(table));
    }

    [Fact]
    public async Task AddField_ArrayInsideAStruct_RecordsItsIdOnTheNewField()
    {
        var s = new ArrowStructType([new Field("x", Int32Type.Default, true)]);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("s", s, true)).Build();
        await using var table = await CreateAsync(schema);

        await table.AddFieldAsync(["s"], ListOf("y", StringType.Default));

        var y = ((DeltaStructType)table.CurrentSnapshot.Schema.Fields[0].Type).Fields[1];
        Assert.Equal(3, Id(y));
        Assert.Equal(new Dictionary<string, int> { [Physical(y) + ".element"] = 4 }, Nested(y));
        Assert.Empty(Nested(table.CurrentSnapshot.Schema.Fields[0]));
        Assert.Equal(4, MaxColumnId(table));
    }

    [Fact]
    public async Task SetSchema_AssignsNestedIdsPastTheOldMaximum()
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(ListOf("arr", Int64Type.Default)).Build();
        await using var table = await CreateAsync(schema);

        await table.SetSchemaAsync(new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(ListOf("tags", StringType.Default))
            .Build());

        var tags = table.CurrentSnapshot.Schema.Fields[1];
        Assert.Equal(4, Id(tags));
        Assert.Equal(new Dictionary<string, int> { [Physical(tags) + ".element"] = 5 }, Nested(tags));
        Assert.Equal(5, MaxColumnId(table));
    }

    [Fact]
    public async Task RenameColumn_KeepsTheNestedIds()
    {
        // The paths are physical, so a rename leaves them valid.
        var schema = new Apache.Arrow.Schema.Builder().Field(ListOf("arr", Int64Type.Default)).Build();
        await using var table = await CreateAsync(schema);
        var before = table.CurrentSnapshot.Schema.Fields[0];

        await table.RenameColumnAsync("arr", "renamed");

        Assert.Equal(Nested(before), Nested(table.CurrentSnapshot.Schema.Fields[0]));
    }

    [Theory]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task Write_StampsTheNestedIdsOnTheParquetNodes_AndReadsBack(ColumnMappingMode mode)
    {
        var inner = new ArrowStructType([ListOf("a", Int32Type.Default)]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(ListOf("arr", Int64Type.Default))
            .Field(MapOf("m", StringType.Default, Int64Type.Default))
            .Field(ListOf("structs", inner))
            .Build();
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await CreateAsync(schema, mode);

        var arrValues = new Int64Array.Builder().Append(1).Append(2).Build();
        var arr = new ListArray(schema.FieldsList[0].DataType, 1,
            new ArrowBuffer.Builder<int>().Append(0).Append(2).Build(), arrValues, ArrowBuffer.Empty);
        var entries = new StructArray(
            new ArrowStructType([((ArrowMapType)schema.FieldsList[1].DataType).KeyField,
                ((ArrowMapType)schema.FieldsList[1].DataType).ValueField]),
            1, [new StringArray.Builder().Append("k").Build(), new Int64Array.Builder().Append(7).Build()],
            ArrowBuffer.Empty);
        var map = new MapArray(schema.FieldsList[1].DataType, 1,
            new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(), entries, ArrowBuffer.Empty);
        var a = new ListArray(inner.Fields[0].DataType, 1,
            new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(),
            new Int32Array.Builder().Append(9).Build(), ArrowBuffer.Empty);
        var structs = new ListArray(schema.FieldsList[2].DataType, 1,
            new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(),
            new StructArray(inner, 1, [a], ArrowBuffer.Empty), ArrowBuffer.Empty);
        await table.WriteAsync([new RecordBatch(schema, [arr, map, structs], 1)]);

        var fields = table.CurrentSnapshot.Schema.Fields;
        var nestedA = ((DeltaStructType)((ArrayType)fields[2].Type).ElementType).Fields[0];
        var addFile = table.CurrentSnapshot.ActiveFiles.Values.Single();
        await using (var file = await fs.OpenReadAsync(addFile.Path))
        {
            using var reader = new EngineeredWood.Parquet.ParquetFileReader(file, ownsFile: false);
            var root = (await reader.GetSchemaAsync()).Root;
            SchemaNode Column(StructField f) => root.Children.Single(c => c.Name == Physical(f));

            // LIST -> repeated "list" -> element; MAP -> repeated "key_value" -> key, value.
            Assert.Equal(Nested(fields[0])[Physical(fields[0]) + ".element"],
                Column(fields[0]).Children[0].Children[0].Element.FieldId);
            var keyValue = Column(fields[1]).Children[0];
            Assert.Equal(Nested(fields[1])[Physical(fields[1]) + ".key"], keyValue.Children[0].Element.FieldId);
            Assert.Equal(Nested(fields[1])[Physical(fields[1]) + ".value"], keyValue.Children[1].Element.FieldId);

            var structElement = Column(fields[2]).Children[0].Children[0];
            Assert.Equal(Nested(fields[2])[Physical(fields[2]) + ".element"], structElement.Element.FieldId);
            var aNode = structElement.Children.Single(c => c.Name == Physical(nestedA));
            Assert.Equal(Id(nestedA), aNode.Element.FieldId);
            Assert.Equal(Nested(nestedA)[Physical(nestedA) + ".element"], aNode.Children[0].Children[0].Element.FieldId);
        }

        // The element ids are no struct-field ids, so neither mode's read binds anything by them.
        var rows = new List<RecordBatch>();
        await foreach (var batch in table.ReadAllAsync())
            rows.Add(batch);
        var read = rows.Single();
        Assert.Equal(["arr", "m", "structs"], read.Schema.FieldsList.Select(f => f.Name));
        var readArr = (ListArray)read.Column(0);
        Assert.Equal([1L, 2L], ((Int64Array)readArr.Values).Values.ToArray());
        var readMap = (MapArray)read.Column(1);
        Assert.Equal("k", ((StringArray)readMap.Keys).GetString(0));
        Assert.Equal(7L, ((Int64Array)readMap.Values).GetValue(0));
        var readStructs = (StructArray)((ListArray)read.Column(2)).Values;
        Assert.Equal(9, ((Int32Array)((ListArray)readStructs.Fields[0]).Values).GetValue(0));
        // ...and, like every other file id, they do not reach the caller.
        var readMapType = (ArrowMapType)read.Schema.FieldsList[1].DataType;
        Assert.All(
            [((ListType)read.Schema.FieldsList[0].DataType).ValueField, readMapType.KeyField, readMapType.ValueField,
             ((ListType)read.Schema.FieldsList[2].DataType).ValueField],
            f => Assert.Null(ColumnMapping.GetParquetFieldId(f)));
    }

    [Fact]
    public void GetMaxColumnId_CountsNestedIds()
    {
        var schema = DeltaSchemaSerializer.Parse(
            """
            {"type":"struct","fields":[{"name":"arr","type":{"type":"array","elementType":"long","containsNull":true},
             "nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-1",
             "delta.columnMapping.nested.ids":{"col-1.element":7}}}]}
            """);

        Assert.Equal(7, ColumnMapping.GetMaxColumnId(schema));
    }

    [Fact]
    public void AssignNestedIds_KeepsRecordedIds_AndToleratesSparksEmptyMaps()
    {
        // Spark writes an empty nested-ids map on every field; it must read as "nothing recorded".
        var schema = DeltaSchemaSerializer.Parse(
            """
            {"type":"struct","fields":[
             {"name":"id","type":"long","nullable":true,"metadata":{"delta.columnMapping.id":1,
              "delta.columnMapping.physicalName":"col-1","delta.columnMapping.nested.ids":{}}},
             {"name":"m","type":{"type":"map","keyType":"string","valueType":"long","valueContainsNull":true},
              "nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-2",
              "delta.columnMapping.nested.ids":{"col-2.key":10}}}]}
            """);

        var (assigned, max) = ColumnMapping.AssignNestedIds(schema, ColumnMapping.GetMaxColumnId(schema));

        Assert.Empty(ColumnMapping.GetNestedIds(assigned.Fields[0]));
        Assert.Equal(
            [new KeyValuePair<string, int>("col-2.key", 10), new KeyValuePair<string, int>("col-2.value", 11)],
            ColumnMapping.GetNestedIds(assigned.Fields[1]));
        Assert.Equal(11, max);

        // Nothing left to assign: the same instance back.
        var (again, sameMax) = ColumnMapping.AssignNestedIds(assigned, max);
        Assert.Same(assigned, again);
        Assert.Equal(11, sameMax);
    }

    [Fact]
    public void AssignNestedIds_StartsPastTheIdsTheSchemaRecords()
    {
        // A caller's startId can lag a schema that carries ids of its own; 3 would be assigned below key's 10.
        var schema = DeltaSchemaSerializer.Parse(
            """
            {"type":"struct","fields":[
             {"name":"m","type":{"type":"map","keyType":"string","valueType":"long","valueContainsNull":true},
              "nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-2",
              "delta.columnMapping.nested.ids":{"col-2.key":10}}}]}
            """);

        var (assigned, max) = ColumnMapping.AssignNestedIds(schema, startId: 2);

        Assert.Equal(11, Nested(assigned.Fields[0])["col-2.value"]);
        Assert.Equal(11, max);
    }

    [Fact]
    public async Task Replace_PreAssignedSchemaReusingANestedId_IsRefused()
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(ListOf("arr", Int64Type.Default)).Build();
        var fs = new LocalTableFileSystem(_tempDir);
        await (await CreateAsync(schema)).DisposeAsync(); // ids 1 (arr) and 2 (arr.element)

        // A fresh column id above the old maximum, but the element keeps an id the old table used.
        var preAssigned = DeltaSchemaSerializer.Parse(
            """
            {"type":"struct","fields":[{"name":"arr","type":{"type":"array","elementType":"long","containsNull":true},
             "nullable":true,"metadata":{"delta.columnMapping.id":3,"delta.columnMapping.physicalName":"col-new",
             "delta.columnMapping.nested.ids":{"col-new.element":2}}}]}
            """);
        var error = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateOrReplaceAsync(fs, schema, [], columnMappingMode: ColumnMappingMode.Name,
                configuration: V2, preAssignedSchema: preAssigned));
        Assert.Contains("col-new.element", error.Message);
    }

    [Fact]
    public async Task Create_PreAssignedSchemaWithOnlyNestedIds_IsRefused()
    {
        // GetMaxColumnId counts nested ids, so the "no column ids" guard must look at column ids themselves.
        var preAssigned = DeltaSchemaSerializer.Parse(
            """
            {"type":"struct","fields":[{"name":"arr","type":{"type":"array","elementType":"long","containsNull":true},
             "nullable":true,"metadata":{"delta.columnMapping.nested.ids":{"arr.element":5}}}]}
            """);
        var schema = new Apache.Arrow.Schema.Builder().Field(ListOf("arr", Int64Type.Default)).Build();

        var error = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema,
                columnMappingMode: ColumnMappingMode.Id, configuration: V2, preAssignedSchema: preAssigned));
        Assert.Contains("declares no column-mapping field ids", error.Message);
    }

    [Theory]
    [InlineData("""{"col-x.element":1}""")] // a nested id equal to a column id
    [InlineData("""{"col-x.element":2,"col-x.element.element":2}""")] // two nested ids
    public async Task Create_PreAssignedSchemaRepeatingAnId_IsRefused(string nestedIds)
    {
        var preAssigned = DeltaSchemaSerializer.Parse(
            """
            {"type":"struct","fields":[{"name":"arr","type":{"type":"array","elementType":
             {"type":"array","elementType":"long","containsNull":true},"containsNull":true},"nullable":true,
             "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-x",
             "delta.columnMapping.nested.ids":
            """ + nestedIds + "}}]}");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(ListOf("arr", new ListType(new Field("element", Int64Type.Default, true))))
            .Build();

        var error = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema,
                columnMappingMode: ColumnMappingMode.Name, configuration: V2, preAssignedSchema: preAssigned));
        Assert.Contains("every id must be distinct", error.Message);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RenameOrDrop_FillsInTheIdsAnOlderWriterLeftOut(bool rename)
    {
        // A V2 table as EW wrote it before #467: column ids only, and maxColumnId past an id it burned.
        var fs = new LocalTableFileSystem(_tempDir);
        await new EngineeredWood.DeltaLake.Log.TransactionLog(fs).WriteCommitAsync(0,
        [
            new EngineeredWood.DeltaLake.Actions.ProtocolAction
            {
                MinReaderVersion = 2, MinWriterVersion = 7,
                ReaderFeatures = ["columnMapping"], WriterFeatures = ["columnMapping", "icebergCompatV2"],
            },
            new EngineeredWood.DeltaLake.Actions.MetadataAction
            {
                Id = "older-ew",
                Format = EngineeredWood.DeltaLake.Actions.Format.Parquet,
                SchemaString = """
                    {"type":"struct","fields":[
                     {"name":"id","type":"long","nullable":true,"metadata":{"delta.columnMapping.id":1,
                      "delta.columnMapping.physicalName":"col-1"}},
                     {"name":"arr","type":{"type":"array","elementType":"long","containsNull":true},"nullable":true,
                      "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-2"}}]}
                    """,
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    [ColumnMapping.ModeKey] = "name",
                    [ColumnMapping.MaxColumnIdKey] = "4",
                    [IcebergCompat.EnableV2Key] = "true",
                },
            },
        ]);
        await using var table = await DeltaTable.OpenAsync(fs);

        if (rename)
            await table.RenameColumnAsync("id", "key");
        else
            await table.DropColumnAsync("id");

        var arr = table.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "arr");
        Assert.Equal(new Dictionary<string, int> { ["col-2.element"] = 5 }, Nested(arr));
        Assert.Equal(5, MaxColumnId(table));
    }

    [Theory]
    [InlineData("\"not an object\"")]
    [InlineData("{\"col-1.element\":\"7\"}")]
    [InlineData("[7]")]
    public void GetNestedIds_RefusesAMalformedValue(string json)
    {
        var schema = DeltaSchemaSerializer.Parse(
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"elementType\":\"long\","
            + "\"containsNull\":true},\"nullable\":true,\"metadata\":{\"delta.columnMapping.nested.ids\":" + json + "}}]}");

        Assert.Throws<DeltaFormatException>(() => ColumnMapping.GetNestedIds(schema.Fields[0]));
    }
}
