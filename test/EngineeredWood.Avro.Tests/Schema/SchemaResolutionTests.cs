// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Tests.Schema;

public class SchemaResolutionTests
{
    // ─── Helpers ───

    private static AvroRecordSchema ParseRecord(string json)
    {
        var node = AvroSchemaParser.Parse(json);
        return (AvroRecordSchema)node;
    }

    private static byte[] WriteTestData(Apache.Arrow.Schema arrowSchema, RecordBatch batch,
        string? avroSchemaJson = null)
    {
        using var ms = new MemoryStream();
        var writerBuilder = new AvroWriterBuilder(arrowSchema);
        if (avroSchemaJson != null)
            writerBuilder = writerBuilder.WithAvroSchema(new AvroSchema(avroSchemaJson));
        using (var writer = writerBuilder.Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }
        return ms.ToArray();
    }

    private static RecordBatch ReadWithSchema(byte[] data, AvroSchema readerSchema)
    {
        using var ms = new MemoryStream(data);
        using var reader = new AvroReaderBuilder()
            .WithReaderSchema(readerSchema)
            .Build(ms);
        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        return result;
    }

    private static RecordBatch ReadWithoutSchema(byte[] data)
    {
        using var ms = new MemoryStream(data);
        using var reader = new AvroReaderBuilder().Build(ms);
        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        return result;
    }

    // ─── Field reordering ───

    [Fact]
    public void FieldReordering_ReadsFieldsInReaderOrder()
    {
        // Writer: {a: int, b: string}
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, false))
            .Field(new Field("b", StringType.Default, false))
            .Build();

        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(42);
        var strBuilder = new StringArray.Builder();
        strBuilder.Append("hello");
        var batch = new RecordBatch(writerSchema,
            [intBuilder.Build(), strBuilder.Build()], 1);

        var data = WriteTestData(writerSchema, batch);

        // Reader: {b: string, a: int} — reversed
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "b", "type": "string"},
                {"name": "a", "type": "int"}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));

        Assert.Equal(1, result.Length);
        Assert.Equal(2, result.ColumnCount);
        // Reader order: b first, a second
        Assert.Equal("b", result.Schema.FieldsList[0].Name);
        Assert.Equal("a", result.Schema.FieldsList[1].Name);
        Assert.Equal("hello", ((StringArray)result.Column(0)).GetString(0));
        Assert.Equal(42, ((Int32Array)result.Column(1)).GetValue(0));
    }

    // ─── Missing writer fields with defaults ───

    [Fact]
    public void MissingWriterField_UsesIntDefault()
    {
        // Writer: {a: int}
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, false))
            .Build();
        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(10);
        intBuilder.Append(20);
        var batch = new RecordBatch(writerSchema, [intBuilder.Build()], 2);
        var data = WriteTestData(writerSchema, batch);

        // Reader: {a: int, b: int (default=99)}
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "int", "default": 99}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));

        Assert.Equal(2, result.Length);
        Assert.Equal(2, result.ColumnCount);
        var aArr = (Int32Array)result.Column(0);
        var bArr = (Int32Array)result.Column(1);
        Assert.Equal(10, aArr.GetValue(0));
        Assert.Equal(20, aArr.GetValue(1));
        Assert.Equal(99, bArr.GetValue(0));
        Assert.Equal(99, bArr.GetValue(1));
    }

    [Fact]
    public void MissingWriterField_UsesStringDefault()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Build();
        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(1);
        var batch = new RecordBatch(writerSchema, [intBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string", "default": "unknown"}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(1, result.Length);
        Assert.Equal("unknown", ((StringArray)result.Column(1)).GetString(0));
    }

    [Fact]
    public void MissingWriterField_UsesNullDefault()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, false))
            .Build();
        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(5);
        var batch = new RecordBatch(writerSchema, [intBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        // Reader has a nullable field with null default
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "opt", "type": ["null", "int"], "default": null}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(1, result.Length);
        Assert.False(result.Column(1).IsValid(0)); // opt should be null
    }

    [Fact]
    public void MissingWriterField_UsesBooleanDefault()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, false))
            .Build();
        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(1);
        var batch = new RecordBatch(writerSchema, [intBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "x", "type": "int"},
                {"name": "flag", "type": "boolean", "default": true}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(true, ((BooleanArray)result.Column(1)).GetValue(0));
    }

    [Fact]
    public void MissingWriterField_UsesDoubleDefault()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, false))
            .Build();
        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(1);
        var batch = new RecordBatch(writerSchema, [intBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "x", "type": "int"},
                {"name": "score", "type": "double", "default": 3.14}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(3.14, ((DoubleArray)result.Column(1)).GetValue(0));
    }

    // ─── Extra writer fields (skipped) ───

    [Fact]
    public void ExtraWriterField_IsSkipped()
    {
        // Writer: {a: int, b: string, c: long}
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, false))
            .Field(new Field("b", StringType.Default, false))
            .Field(new Field("c", Int64Type.Default, false))
            .Build();

        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(42);
        var strBuilder = new StringArray.Builder();
        strBuilder.Append("skip_me");
        var longBuilder = new Int64Array.Builder();
        longBuilder.Append(999L);
        var batch = new RecordBatch(writerSchema,
            [intBuilder.Build(), strBuilder.Build(), longBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        // Reader only wants {a: int, c: long} — b is skipped
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "c", "type": "long"}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(1, result.Length);
        Assert.Equal(2, result.ColumnCount);
        Assert.Equal(42, ((Int32Array)result.Column(0)).GetValue(0));
        Assert.Equal(999L, ((Int64Array)result.Column(1)).GetValue(0));
    }

    [Fact]
    public void ExtraWriterField_SkipsComplexTypes()
    {
        // Writer has a record field that reader doesn't want
        var writerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "nested", "type": {
                    "type": "record", "name": "Inner",
                    "fields": [
                        {"name": "x", "type": "string"},
                        {"name": "y", "type": "int"}
                    ]
                }},
                {"name": "value", "type": "string"}
            ]
        }
        """;

        // Build Arrow schema matching writer
        var innerStructType = new StructType([
            new Field("x", StringType.Default, false),
            new Field("y", Int32Type.Default, false),
        ]);
        var writerArrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("nested", innerStructType, false))
            .Field(new Field("value", StringType.Default, false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        idBuilder.Append(1);
        var xBuilder = new StringArray.Builder();
        xBuilder.Append("nested_str");
        var yBuilder = new Int32Array.Builder();
        yBuilder.Append(7);
        var structArr = new StructArray(innerStructType, 1,
            [xBuilder.Build(), yBuilder.Build()], ArrowBuffer.Empty);
        var valBuilder = new StringArray.Builder();
        valBuilder.Append("keep_me");
        var batch = new RecordBatch(writerArrowSchema,
            [idBuilder.Build(), structArr, valBuilder.Build()], 1);

        var data = WriteTestData(writerArrowSchema, batch, writerSchemaJson);

        // Reader only wants {id, value}
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": "string"}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(1, result.Length);
        Assert.Equal(2, result.ColumnCount);
        Assert.Equal(1, ((Int32Array)result.Column(0)).GetValue(0));
        Assert.Equal("keep_me", ((StringArray)result.Column(1)).GetString(0));
    }

    // ─── Field aliases ───

    [Fact]
    public void FieldAlias_MatchesWriterFieldByReaderAlias()
    {
        // Writer: {old_name: int}
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("old_name", Int32Type.Default, false))
            .Build();
        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(77);
        var batch = new RecordBatch(writerSchema, [intBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        // Reader: {new_name: int, aliases: ["old_name"]}
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "new_name", "type": "int", "aliases": ["old_name"]}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(1, result.Length);
        Assert.Equal("new_name", result.Schema.FieldsList[0].Name);
        Assert.Equal(77, ((Int32Array)result.Column(0)).GetValue(0));
    }

    // ─── Type promotion ───

    [Fact]
    public void TypePromotion_IntToLong()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", Int32Type.Default, false))
            .Build();
        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(42);
        intBuilder.Append(-100);
        var batch = new RecordBatch(writerSchema, [intBuilder.Build()], 2);
        var data = WriteTestData(writerSchema, batch);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "val", "type": "long"}]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        var arr = (Int64Array)result.Column(0);
        Assert.Equal(42L, arr.GetValue(0));
        Assert.Equal(-100L, arr.GetValue(1));
    }

    [Fact]
    public void TypePromotion_IntToFloat()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", Int32Type.Default, false))
            .Build();
        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(7);
        var batch = new RecordBatch(writerSchema, [intBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "val", "type": "float"}]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(7.0f, ((FloatArray)result.Column(0)).GetValue(0));
    }

    [Fact]
    public void TypePromotion_IntToDouble()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", Int32Type.Default, false))
            .Build();
        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(123);
        var batch = new RecordBatch(writerSchema, [intBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "val", "type": "double"}]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(123.0, ((DoubleArray)result.Column(0)).GetValue(0));
    }

    [Fact]
    public void TypePromotion_LongToDouble()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", Int64Type.Default, false))
            .Build();
        var longBuilder = new Int64Array.Builder();
        longBuilder.Append(9876543210L);
        var batch = new RecordBatch(writerSchema, [longBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "val", "type": "double"}]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal((double)9876543210L, ((DoubleArray)result.Column(0)).GetValue(0));
    }

    [Fact]
    public void TypePromotion_LongToFloat()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", Int64Type.Default, false))
            .Build();
        var longBuilder = new Int64Array.Builder();
        longBuilder.Append(42L);
        var batch = new RecordBatch(writerSchema, [longBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "val", "type": "float"}]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(42.0f, ((FloatArray)result.Column(0)).GetValue(0));
    }

    [Fact]
    public void TypePromotion_FloatToDouble()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", FloatType.Default, false))
            .Build();
        var floatBuilder = new FloatArray.Builder();
        floatBuilder.Append(2.5f);
        var batch = new RecordBatch(writerSchema, [floatBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "val", "type": "double"}]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(2.5, ((DoubleArray)result.Column(0)).GetValue(0));
    }

    // ─── String ↔ Bytes promotion ───

    [Fact]
    public void TypePromotion_StringToBytes()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", StringType.Default, false))
            .Build();
        var strBuilder = new StringArray.Builder();
        strBuilder.Append("hello");
        var batch = new RecordBatch(writerSchema, [strBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "val", "type": "bytes"}]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        var arr = (BinaryArray)result.Column(0);
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes("hello"), arr.GetBytes(0).ToArray());
    }

    [Fact]
    public void TypePromotion_BytesToString()
    {
        var writerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "val", "type": "bytes"}]
        }
        """;
        var writerArrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", BinaryType.Default, false))
            .Build();
        var binBuilder = new BinaryArray.Builder();
        binBuilder.Append(System.Text.Encoding.UTF8.GetBytes("world"));
        var batch = new RecordBatch(writerArrowSchema, [binBuilder.Build()], 1);
        var data = WriteTestData(writerArrowSchema, batch, writerSchemaJson);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "val", "type": "string"}]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal("world", ((StringArray)result.Column(0)).GetString(0));
    }

    // ─── Nullable type promotion ───

    [Fact]
    public void TypePromotion_NullableIntToNullableLong()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", Int32Type.Default, true))
            .Build();
        var intBuilder = new Int32Array.Builder();
        intBuilder.Append(10);
        intBuilder.AppendNull();
        intBuilder.Append(30);
        var batch = new RecordBatch(writerSchema, [intBuilder.Build()], 3);
        var data = WriteTestData(writerSchema, batch);

        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "val", "type": ["null", "long"]}]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        var arr = (Int64Array)result.Column(0);
        Assert.Equal(3, result.Length);
        Assert.Equal(10L, arr.GetValue(0));
        Assert.False(arr.IsValid(1));
        Assert.Equal(30L, arr.GetValue(2));
    }

    // ─── Combined: skip + default + reorder ───

    [Fact]
    public void Combined_SkipDefaultAndReorder()
    {
        // Writer: {a: int, b: string, c: long}
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, false))
            .Field(new Field("b", StringType.Default, false))
            .Field(new Field("c", Int64Type.Default, false))
            .Build();
        var aBuilder = new Int32Array.Builder();
        aBuilder.Append(1);
        var bBuilder = new StringArray.Builder();
        bBuilder.Append("drop");
        var cBuilder = new Int64Array.Builder();
        cBuilder.Append(100L);
        var batch = new RecordBatch(writerSchema,
            [aBuilder.Build(), bBuilder.Build(), cBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        // Reader: {c: long, d: float (default=1.5), a: int}
        // - b: skipped
        // - d: new field with default
        // - reordered: c first, then d, then a
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "c", "type": "long"},
                {"name": "d", "type": "float", "default": 1.5},
                {"name": "a", "type": "int"}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(1, result.Length);
        Assert.Equal(3, result.ColumnCount);
        Assert.Equal(100L, ((Int64Array)result.Column(0)).GetValue(0));
        Assert.Equal(1.5f, ((FloatArray)result.Column(1)).GetValue(0));
        Assert.Equal(1, ((Int32Array)result.Column(2)).GetValue(0));
    }

    // ─── Error cases ───

    [Fact]
    public void MissingFieldWithNoDefault_Throws()
    {
        var writerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "a", "type": "int"}]
        }
        """;
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "int"}
            ]
        }
        """;

        var writerRecord = ParseRecord(writerSchemaJson);
        var readerRecord = ParseRecord(readerSchemaJson);

        var ex = Assert.Throws<InvalidOperationException>(
            () => SchemaResolver.Resolve(writerRecord, readerRecord));
        Assert.Contains("b", ex.Message);
        Assert.Contains("no default", ex.Message);
    }

    [Fact]
    public void IncompatibleTypes_Throws()
    {
        var writerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "a", "type": "int"}]
        }
        """;
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [{"name": "a", "type": "boolean"}]
        }
        """;

        var writerRecord = ParseRecord(writerSchemaJson);
        var readerRecord = ParseRecord(readerSchemaJson);

        Assert.Throws<InvalidOperationException>(
            () => SchemaResolver.Resolve(writerRecord, readerRecord));
    }

    // ─── Multiple rows ───

    [Fact]
    public void MultipleRows_WithDefaultsAndSkips()
    {
        // Writer: {x: int, y: int}
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, false))
            .Field(new Field("y", Int32Type.Default, false))
            .Build();
        var xBuilder = new Int32Array.Builder();
        var yBuilder = new Int32Array.Builder();
        for (int i = 0; i < 5; i++)
        {
            xBuilder.Append(i * 10);
            yBuilder.Append(i * 100);
        }
        var batch = new RecordBatch(writerSchema,
            [xBuilder.Build(), yBuilder.Build()], 5);
        var data = WriteTestData(writerSchema, batch);

        // Reader: {x: int, z: long (default=0)} — y is skipped, z gets default
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "x", "type": "int"},
                {"name": "z", "type": "long", "default": 0}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(5, result.Length);
        var xArr = (Int32Array)result.Column(0);
        var zArr = (Int64Array)result.Column(1);
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(i * 10, xArr.GetValue(i));
            Assert.Equal(0L, zArr.GetValue(i));
        }
    }

    // ─── Same schema (no-op resolution) ───

    [Fact]
    public void SameSchema_WorksIdentically()
    {
        var writerSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, false))
            .Field(new Field("b", StringType.Default, false))
            .Build();
        var aBuilder = new Int32Array.Builder();
        aBuilder.Append(1);
        var bBuilder = new StringArray.Builder();
        bBuilder.Append("test");
        var batch = new RecordBatch(writerSchema,
            [aBuilder.Build(), bBuilder.Build()], 1);
        var data = WriteTestData(writerSchema, batch);

        // Same schema for reader
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "string"}
            ]
        }
        """;

        var withEvolution = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        var without = ReadWithoutSchema(data);

        Assert.Equal(without.Length, withEvolution.Length);
        Assert.Equal(
            ((Int32Array)without.Column(0)).GetValue(0),
            ((Int32Array)withEvolution.Column(0)).GetValue(0));
        Assert.Equal(
            ((StringArray)without.Column(1)).GetString(0),
            ((StringArray)withEvolution.Column(1)).GetString(0));
    }

    // ─── Schema resolution unit tests (no I/O) ───

    [Fact]
    public void IsPromotable_ValidPromotions()
    {
        Assert.True(SchemaResolver.IsPromotable(AvroType.Int, AvroType.Long));
        Assert.True(SchemaResolver.IsPromotable(AvroType.Int, AvroType.Float));
        Assert.True(SchemaResolver.IsPromotable(AvroType.Int, AvroType.Double));
        Assert.True(SchemaResolver.IsPromotable(AvroType.Long, AvroType.Float));
        Assert.True(SchemaResolver.IsPromotable(AvroType.Long, AvroType.Double));
        Assert.True(SchemaResolver.IsPromotable(AvroType.Float, AvroType.Double));
        Assert.True(SchemaResolver.IsPromotable(AvroType.String, AvroType.Bytes));
        Assert.True(SchemaResolver.IsPromotable(AvroType.Bytes, AvroType.String));
    }

    [Fact]
    public void IsPromotable_InvalidPromotions()
    {
        Assert.False(SchemaResolver.IsPromotable(AvroType.Long, AvroType.Int));
        Assert.False(SchemaResolver.IsPromotable(AvroType.Double, AvroType.Float));
        Assert.False(SchemaResolver.IsPromotable(AvroType.Int, AvroType.Boolean));
        Assert.False(SchemaResolver.IsPromotable(AvroType.String, AvroType.Int));
    }

    [Fact]
    public void Resolve_WriterActionsCorrect()
    {
        var writerSchemaJson = """
        {
            "type": "record", "name": "R",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "long"}
            ]
        }
        """;
        var readerSchemaJson = """
        {
            "type": "record", "name": "R",
            "fields": [
                {"name": "c", "type": "long"},
                {"name": "a", "type": "int"}
            ]
        }
        """;

        var resolution = SchemaResolver.Resolve(
            ParseRecord(writerSchemaJson), ParseRecord(readerSchemaJson));

        // Writer field 0 (a) → reader field 1
        Assert.Equal(1, resolution.WriterActions[0].ReaderFieldIndex);
        Assert.False(resolution.WriterActions[0].Skip);

        // Writer field 1 (b) → skip
        Assert.True(resolution.WriterActions[1].Skip);

        // Writer field 2 (c) → reader field 0
        Assert.Equal(0, resolution.WriterActions[2].ReaderFieldIndex);
        Assert.False(resolution.WriterActions[2].Skip);

        // No defaults needed
        Assert.Empty(resolution.DefaultFields);
    }
}
