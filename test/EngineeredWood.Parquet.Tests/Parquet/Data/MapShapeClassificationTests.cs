// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// How a MAP-annotated group is classified when the file does not live up to the annotation. Arrow's
/// <c>MapType</c> demands more than the Parquet spec does, so the reader has to decide what a group that
/// falls short actually is — see issue #156, and the two gaps the review of that fix surfaced.
/// </summary>
public class MapShapeClassificationTests : IDisposable
{
    private readonly string _tempDir;

    public MapShapeClassificationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-mapshape-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    // A MAP annotation over a group with no REPEATED child describes a nesting level the file does not
    // have. Neither assembler can read it: both take their repetition and definition thresholds off that
    // repeated group, so the offsets and validity would be computed against a level that is not there —
    // wrong values rather than a missing column. It is a plain group, so it reads as a struct.
    [Fact]
    public void MapAnnotation_WithoutRepeatedGroup_ReadsAsStruct()
    {
        var field = ConvertSingleField(MapAnnotatedNode(
            keyValueRepetition: FieldRepetitionType.Optional,
            childNames: ["key", "value"]));

        var structType = Assert.IsType<StructType>(field.DataType);
        var keyValue = Assert.Single(structType.Fields);
        Assert.Equal("key_value", keyValue.Name);
        Assert.IsType<StructType>(keyValue.DataType);
    }

    // The same group WITH the repeated child is a map, which is what pins the check above to the
    // repetition and not to something else about the shape.
    [Fact]
    public void MapAnnotation_WithRepeatedKeyAndValue_ReadsAsMap()
    {
        var field = ConvertSingleField(MapAnnotatedNode(
            keyValueRepetition: FieldRepetitionType.Repeated,
            childNames: ["key", "value"]));

        Assert.IsType<MapType>(field.DataType);
    }

    // Issue #156 itself, at the classifier level: repeated group, but no value child.
    [Fact]
    public void MapAnnotation_RepeatedGroupWithOnlyAKey_ReadsAsList()
    {
        var field = ConvertSingleField(MapAnnotatedNode(
            keyValueRepetition: FieldRepetitionType.Repeated,
            childNames: ["key"]));

        var listType = Assert.IsType<ListType>(field.DataType);
        Assert.Equal("key", listType.ValueField.Name);
    }

    // Above two children the list rules make the repeated group itself a struct element, which keeps every
    // child rather than dropping the ones past the second as the map path did.
    [Fact]
    public void MapAnnotation_RepeatedGroupWithThreeChildren_ReadsAsListOfStruct()
    {
        var field = ConvertSingleField(MapAnnotatedNode(
            keyValueRepetition: FieldRepetitionType.Repeated,
            childNames: ["key", "value", "extra"]));

        var listType = Assert.IsType<ListType>(field.DataType);
        var elementType = Assert.IsType<StructType>(listType.ValueField.DataType);
        Assert.Equal(["key", "value", "extra"], elementType.Fields.Select(f => f.Name));
    }

    // A map whose KEY is a struct spans more than one leaf, so the value's definition levels do not sit at
    // `firstLeafIndex + 1` — that index lands inside the key's own subtree. The null map row is what makes
    // it observable: it creates the phantom entry that the value array has to be filtered against, and
    // filtering it against another column's levels drops the wrong rows.
    [Fact]
    public async Task Map_WithStructKey_FiltersTheValueAgainstItsOwnLevels()
    {
        string path = TempPath("struct_key_map.parquet");

        var keyType = new StructType([
            new Field("a", Int32Type.Default, nullable: false),
            new Field("b", Int32Type.Default, nullable: false),
        ]);
        var keyField = new Field("key", keyType, nullable: false);
        var valueField = new Field("value", Int32Type.Default, nullable: true);
        var mapType = new MapType(keyField, valueField);

        // Row 0: two entries, Row 1: null map, Row 2: one entry.
        int[] aValues = [10, 20, 30];
        int[] bValues = [11, 21, 31];
        int[] entryValues = [100, 200, 300];

        var keys = new StructArray(
            keyType, 3,
            [
                new Int32Array.Builder().AppendRange(aValues).Build(),
                new Int32Array.Builder().AppendRange(bValues).Build(),
            ],
            ArrowBuffer.Empty, nullCount: 0);
        var values = new Int32Array.Builder().AppendRange(entryValues).Build();

        var entries = new StructArray(
            new StructType([keyField, valueField]), 3, [keys, values], ArrowBuffer.Empty, nullCount: 0);

        var offsets = new ArrowBuffer.Builder<int>();
        offsets.Append(0).Append(2).Append(2).Append(3);

        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(true).Append(false).Append(true);

        var map = new MapArray(mapType, 3, offsets.Build(), entries, validity.Build(), nullCount: 1);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("m", mapType, nullable: true)).Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [map], 3));
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        var read = Assert.IsType<MapArray>(batch.Column(0));
        Assert.Equal(3, read.Length);
        Assert.True(read.IsNull(1));

        var readKeys = Assert.IsType<StructArray>(read.Keys);
        var readA = Assert.IsType<Int32Array>(readKeys.Fields[0]);
        var readB = Assert.IsType<Int32Array>(readKeys.Fields[1]);
        var readValues = Assert.IsType<Int32Array>(read.Values);

        // Three entries survive the null row, and the value column must still line up with the keys.
        Assert.Equal(3, readValues.Length);
        Assert.Equal(aValues, Enumerable.Range(0, 3).Select(i => readA.GetValue(i)!.Value));
        Assert.Equal(bValues, Enumerable.Range(0, 3).Select(i => readB.GetValue(i)!.Value));
        Assert.Equal(entryValues, Enumerable.Range(0, 3).Select(i => readValues.GetValue(i)!.Value));
    }

    /// <summary>
    /// A MAP-annotated group wrapping one <c>key_value</c> group of int32 children, built directly so the
    /// shapes below can be malformed in ways no writer would produce.
    /// </summary>
    private static SchemaNode MapAnnotatedNode(
        FieldRepetitionType keyValueRepetition, string[] childNames)
    {
        var children = childNames.Select(name => new SchemaNode
        {
            Element = new SchemaElement
            {
                Name = name,
                Type = PhysicalType.Int32,
                RepetitionType = name == "key" ? FieldRepetitionType.Required : FieldRepetitionType.Optional,
            },
            Children = [],
        }).ToArray();

        var keyValue = new SchemaNode
        {
            Element = new SchemaElement
            {
                Name = "key_value",
                RepetitionType = keyValueRepetition,
                NumChildren = children.Length,
            },
            Children = children,
        };

        return new SchemaNode
        {
            Element = new SchemaElement
            {
                Name = "m",
                RepetitionType = FieldRepetitionType.Optional,
                NumChildren = 1,
                LogicalType = new LogicalType.MapType(),
                ConvertedType = ConvertedType.Map,
            },
            Children = [keyValue],
        };
    }

    private static Apache.Arrow.Field ConvertSingleField(SchemaNode node)
    {
        var root = new SchemaNode
        {
            Element = new SchemaElement { Name = "schema", NumChildren = 1 },
            Children = [node],
        };

        return Assert.Single(ArrowSchemaConverter.ToArrowFields(root));
    }
}
