// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Tests;

public class AvroDenseUnionTests
{
    /// <summary>
    /// Round-trips a two-branch DenseUnion ["int", "string"] through Avro OCF.
    /// </summary>
    [Fact]
    public void RoundTrip_IntStringUnion()
    {
        // Avro schema: { "type": "record", "name": "Test", "fields":
        //   [{"name": "value", "type": ["int", "string"]}] }
        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": ["int", "string"]}
            ]
        }
        """;

        var unionType = new UnionType(
            [new Field("branch0", Int32Type.Default, false),
             new Field("branch1", StringType.Default, false)],
            [0, 1], UnionMode.Dense);

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", unionType, false))
            .Build();

        // Build DenseUnionArray: rows = [int(42), string("hello"), int(7), string("world")]
        var unionArray = BuildIntStringUnion(unionType,
            [(0, 42, null), (1, 0, "hello"), (0, 7, null), (1, 0, "world")]);

        var batch = new RecordBatch(arrowSchema, [unionArray], 4);
        var result = WriteAndReadWithSchema(arrowSchema, batch, avroSchemaJson);

        Assert.Equal(4, result.Length);
        var resultUnion = (DenseUnionArray)result.Column(0);

        // Verify type IDs
        Assert.Equal(0, resultUnion.TypeIds[0]);
        Assert.Equal(1, resultUnion.TypeIds[1]);
        Assert.Equal(0, resultUnion.TypeIds[2]);
        Assert.Equal(1, resultUnion.TypeIds[3]);

        // Verify values via child arrays + offsets
        var intChild = (Int32Array)resultUnion.Fields[0];
        var strChild = (StringArray)resultUnion.Fields[1];
        Assert.Equal(42, intChild.GetValue(resultUnion.ValueOffsets[0]));
        Assert.Equal("hello", strChild.GetString(resultUnion.ValueOffsets[1]));
        Assert.Equal(7, intChild.GetValue(resultUnion.ValueOffsets[2]));
        Assert.Equal("world", strChild.GetString(resultUnion.ValueOffsets[3]));
    }

    /// <summary>
    /// Round-trips a three-branch union ["null", "int", "string"] which is NOT treated
    /// as a simple nullable (since it has 3 branches, not 2 with one being null).
    /// </summary>
    [Fact]
    public void RoundTrip_ThreeBranchUnionWithNull()
    {
        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": ["null", "int", "string"]}
            ]
        }
        """;

        var unionType = new UnionType(
            [new Field("branch0", NullType.Default, true),
             new Field("branch1", Int32Type.Default, false),
             new Field("branch2", StringType.Default, false)],
            [0, 1, 2], UnionMode.Dense);

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", unionType, false))
            .Build();

        // Build: [null, int(99), string("abc"), null, int(1)]
        var typeIds = new byte[] { 0, 1, 2, 0, 1 };
        var offsets = new int[] { 0, 0, 0, 1, 1 };

        var nullChild = new NullArray(2);
        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(99);
        intBuilder.Append(1);
        var intChild = intBuilder.Build();
        var strBuilder = new StringArray.Builder();
        strBuilder.Append("abc");
        var strChild = strBuilder.Build();

        var unionArray = new DenseUnionArray(unionType, 5,
            [nullChild, intChild, strChild],
            new ArrowBuffer(typeIds),
            new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()));

        var batch = new RecordBatch(arrowSchema, [unionArray], 5);
        var result = WriteAndReadWithSchema(arrowSchema, batch, avroSchemaJson);

        Assert.Equal(5, result.Length);
        var resultUnion = (DenseUnionArray)result.Column(0);

        Assert.Equal(0, resultUnion.TypeIds[0]); // null
        Assert.Equal(1, resultUnion.TypeIds[1]); // int
        Assert.Equal(2, resultUnion.TypeIds[2]); // string
        Assert.Equal(0, resultUnion.TypeIds[3]); // null
        Assert.Equal(1, resultUnion.TypeIds[4]); // int

        var resultInt = (Int32Array)resultUnion.Fields[1];
        Assert.Equal(99, resultInt.GetValue(resultUnion.ValueOffsets[1]));
        Assert.Equal(1, resultInt.GetValue(resultUnion.ValueOffsets[4]));

        var resultStr = (StringArray)resultUnion.Fields[2];
        Assert.Equal("abc", resultStr.GetString(resultUnion.ValueOffsets[2]));
    }

    /// <summary>
    /// Tests that Arrow UnionType(Dense) → Avro → Arrow round-trips the schema correctly.
    /// </summary>
    [Fact]
    public void SchemaConversion_ArrowToAvroAndBack()
    {
        var unionType = new UnionType(
            [new Field("branch0", Int32Type.Default, false),
             new Field("branch1", StringType.Default, false),
             new Field("branch2", BooleanType.Default, false)],
            [0, 1, 2], UnionMode.Dense);

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("u", unionType, false))
            .Build();

        // Arrow → Avro
        var avroRecord = ArrowSchemaConverter.FromArrow(arrowSchema, "Test");
        Assert.Single(avroRecord.Fields);

        var unionSchema = avroRecord.Fields[0].Schema;
        Assert.IsType<AvroUnionSchema>(unionSchema);
        var avroUnion = (AvroUnionSchema)unionSchema;
        Assert.Equal(3, avroUnion.Branches.Count);
        Assert.Equal(AvroType.Int, avroUnion.Branches[0].Type);
        Assert.Equal(AvroType.String, avroUnion.Branches[1].Type);
        Assert.Equal(AvroType.Boolean, avroUnion.Branches[2].Type);

        // Avro → Arrow
        var (roundTripped, nullable) = ArrowSchemaConverter.ToArrowType(avroUnion);
        Assert.False(nullable);
        Assert.IsType<UnionType>(roundTripped);
        var rtUnion = (UnionType)roundTripped;
        Assert.Equal(UnionMode.Dense, rtUnion.Mode);
        Assert.Equal(3, rtUnion.Fields.Count);
    }

    /// <summary>
    /// Avro schema parser should parse ["int", "string"] as a general union, not nullable.
    /// </summary>
    [Fact]
    public void SchemaParsing_GeneralUnionNotNullable()
    {
        var schemaJson = """["int", "string"]""";
        var parsed = AvroSchemaParser.Parse(schemaJson);

        Assert.IsType<AvroUnionSchema>(parsed);
        var union = (AvroUnionSchema)parsed;
        Assert.Equal(2, union.Branches.Count);
        Assert.False(union.IsNullable(out _, out _));
        Assert.Equal(AvroType.Int, union.Branches[0].Type);
        Assert.Equal(AvroType.String, union.Branches[1].Type);
    }

    /// <summary>
    /// Round-trips a DenseUnion with all values in the same branch.
    /// </summary>
    [Fact]
    public void RoundTrip_AllSameBranch()
    {
        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": ["int", "string"]}
            ]
        }
        """;

        var unionType = new UnionType(
            [new Field("branch0", Int32Type.Default, false),
             new Field("branch1", StringType.Default, false)],
            [0, 1], UnionMode.Dense);

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", unionType, false))
            .Build();

        // All ints
        var unionArray = BuildIntStringUnion(unionType,
            [(0, 1, null), (0, 2, null), (0, 3, null)]);

        var batch = new RecordBatch(arrowSchema, [unionArray], 3);
        var result = WriteAndReadWithSchema(arrowSchema, batch, avroSchemaJson);

        Assert.Equal(3, result.Length);
        var resultUnion = (DenseUnionArray)result.Column(0);
        for (int i = 0; i < 3; i++)
            Assert.Equal(0, resultUnion.TypeIds[i]);

        var intChild = (Int32Array)resultUnion.Fields[0];
        Assert.Equal(1, intChild.GetValue(0));
        Assert.Equal(2, intChild.GetValue(1));
        Assert.Equal(3, intChild.GetValue(2));
    }

    /// <summary>
    /// Round-trips a DenseUnion with boolean and double branches.
    /// </summary>
    [Fact]
    public void RoundTrip_BooleanDoubleUnion()
    {
        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": ["boolean", "double"]}
            ]
        }
        """;

        var unionType = new UnionType(
            [new Field("branch0", BooleanType.Default, false),
             new Field("branch1", DoubleType.Default, false)],
            [0, 1], UnionMode.Dense);

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", unionType, false))
            .Build();

        // [true, 3.14, false, 2.718]
        var typeIds = new byte[] { 0, 1, 0, 1 };
        var offsets = new int[] { 0, 0, 1, 1 };

        var boolBuilder = new BooleanArray.Builder();
        boolBuilder.Append(true);
        boolBuilder.Append(false);

        var doubleBuilder = new DoubleArray.Builder();
        doubleBuilder.Append(3.14);
        doubleBuilder.Append(2.718);

        var unionArray = new DenseUnionArray(unionType, 4,
            [boolBuilder.Build(), doubleBuilder.Build()],
            new ArrowBuffer(typeIds),
            new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()));

        var batch = new RecordBatch(arrowSchema, [unionArray], 4);
        var result = WriteAndReadWithSchema(arrowSchema, batch, avroSchemaJson);

        Assert.Equal(4, result.Length);
        var resultUnion = (DenseUnionArray)result.Column(0);

        var boolChild = (BooleanArray)resultUnion.Fields[0];
        var doubleChild = (DoubleArray)resultUnion.Fields[1];

        Assert.True(boolChild.GetValue(resultUnion.ValueOffsets[0]));
        Assert.Equal(3.14, doubleChild.GetValue(resultUnion.ValueOffsets[1]));
        Assert.False(boolChild.GetValue(resultUnion.ValueOffsets[2]));
        Assert.Equal(2.718, doubleChild.GetValue(resultUnion.ValueOffsets[3]));
    }

    /// <summary>
    /// Verifies the Avro binary encoding for a union: branch index + value.
    /// </summary>
    [Fact]
    public void BinaryEncoding_UnionIndexPlusValue()
    {
        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": ["int", "string"]}
            ]
        }
        """;

        var unionType = new UnionType(
            [new Field("branch0", Int32Type.Default, false),
             new Field("branch1", StringType.Default, false)],
            [0, 1], UnionMode.Dense);

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", unionType, false))
            .Build();

        // Single row: string("hi")
        var unionArray = BuildIntStringUnion(unionType,
            [(1, 0, "hi")]);

        var batch = new RecordBatch(arrowSchema, [unionArray], 1);

        // Write to OCF and read back — if it round-trips, encoding is correct
        var result = WriteAndReadWithSchema(arrowSchema, batch, avroSchemaJson);
        Assert.Equal(1, result.Length);
        var resultUnion = (DenseUnionArray)result.Column(0);
        Assert.Equal(1, resultUnion.TypeIds[0]);
        var strChild = (StringArray)resultUnion.Fields[1];
        Assert.Equal("hi", strChild.GetString(resultUnion.ValueOffsets[0]));
    }

    /// <summary>
    /// Round-trips an empty batch with a union column.
    /// </summary>
    [Fact]
    public void RoundTrip_EmptyBatch()
    {
        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": ["int", "string"]}
            ]
        }
        """;

        var unionType = new UnionType(
            [new Field("branch0", Int32Type.Default, false),
             new Field("branch1", StringType.Default, false)],
            [0, 1], UnionMode.Dense);

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", unionType, false))
            .Build();

        var unionArray = new DenseUnionArray(unionType, 0,
            [new Int32Array.Builder().Build(), new StringArray.Builder().Build()],
            new ArrowBuffer(System.Array.Empty<byte>()),
            new ArrowBuffer(System.Array.Empty<byte>()));

        var batch = new RecordBatch(arrowSchema, [unionArray], 0);
        var result = WriteAndReadWithSchema(arrowSchema, batch, avroSchemaJson);
        Assert.Equal(0, result.Length);
    }

    /// <summary>
    /// Round-trips a union alongside other columns.
    /// </summary>
    [Fact]
    public void RoundTrip_UnionWithOtherColumns()
    {
        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "payload", "type": ["int", "string"]},
                {"name": "tag", "type": "string"}
            ]
        }
        """;

        var unionType = new UnionType(
            [new Field("branch0", Int32Type.Default, false),
             new Field("branch1", StringType.Default, false)],
            [0, 1], UnionMode.Dense);

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("payload", unionType, false))
            .Field(new Field("tag", StringType.Default, false))
            .Build();

        var idBuilder = new Int64Array.Builder();
        idBuilder.Append(100);
        idBuilder.Append(200);
        idBuilder.Append(300);

        var unionArray = BuildIntStringUnion(unionType,
            [(0, 42, null), (1, 0, "hello"), (0, 7, null)]);

        var tagBuilder = new StringArray.Builder();
        tagBuilder.Append("a");
        tagBuilder.Append("b");
        tagBuilder.Append("c");

        var batch = new RecordBatch(arrowSchema,
            [idBuilder.Build(), unionArray, tagBuilder.Build()], 3);
        var result = WriteAndReadWithSchema(arrowSchema, batch, avroSchemaJson);

        Assert.Equal(3, result.Length);

        // Verify non-union columns
        var ids = (Int64Array)result.Column(0);
        Assert.Equal(100, ids.GetValue(0));
        Assert.Equal(200, ids.GetValue(1));
        Assert.Equal(300, ids.GetValue(2));

        var tags = (StringArray)result.Column(2);
        Assert.Equal("a", tags.GetString(0));
        Assert.Equal("b", tags.GetString(1));
        Assert.Equal("c", tags.GetString(2));

        // Verify union column
        var resultUnion = (DenseUnionArray)result.Column(1);
        Assert.Equal(0, resultUnion.TypeIds[0]);
        Assert.Equal(1, resultUnion.TypeIds[1]);
        Assert.Equal(0, resultUnion.TypeIds[2]);

        var intChild = (Int32Array)resultUnion.Fields[0];
        Assert.Equal(42, intChild.GetValue(resultUnion.ValueOffsets[0]));
        Assert.Equal(7, intChild.GetValue(resultUnion.ValueOffsets[2]));

        var strChild = (StringArray)resultUnion.Fields[1];
        Assert.Equal("hello", strChild.GetString(resultUnion.ValueOffsets[1]));
    }

    // ─── Helpers ───

    /// <summary>
    /// Builds a DenseUnionArray with int (branch 0) and string (branch 1) children.
    /// Each tuple is (branchIndex, intValue, stringValue) — only the relevant value is used.
    /// </summary>
    private static DenseUnionArray BuildIntStringUnion(
        UnionType unionType, (int branch, int intVal, string? strVal)[] rows)
    {
        var typeIds = new byte[rows.Length];
        var offsets = new int[rows.Length];
        var intValues = new List<int>();
        var strValues = new List<string>();

        for (int i = 0; i < rows.Length; i++)
        {
            var (branch, intVal, strVal) = rows[i];
            typeIds[i] = (byte)branch;
            if (branch == 0)
            {
                offsets[i] = intValues.Count;
                intValues.Add(intVal);
            }
            else
            {
                offsets[i] = strValues.Count;
                strValues.Add(strVal!);
            }
        }

        var intBuilder = new Int32Array.Builder();
        foreach (var v in intValues) intBuilder.Append(v);

        var strBuilder = new StringArray.Builder();
        foreach (var v in strValues) strBuilder.Append(v);

        return new DenseUnionArray(unionType, rows.Length,
            [intBuilder.Build(), strBuilder.Build()],
            new ArrowBuffer(typeIds),
            new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()));
    }

    private static RecordBatch WriteAndReadWithSchema(
        Apache.Arrow.Schema arrowSchema, RecordBatch batch, string avroSchemaJson)
    {
        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(arrowSchema)
            .WithAvroSchema(new AvroSchema(avroSchemaJson))
            .Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        return result;
    }
}
