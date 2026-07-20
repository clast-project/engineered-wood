// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// Tests for DefaultValueApplicator — verifies default values are applied correctly
/// for logical types during schema evolution (reader adds new fields with defaults).
/// </summary>
public class AvroDefaultValueTests
{
    // ─── Helpers ───

    private static byte[] WriteTestData(Apache.Arrow.Schema arrowSchema, RecordBatch batch,
        string? avroSchemaJson = null)
    {
        using var ms = new MemoryStream();
        var builder = new AvroWriterBuilder(arrowSchema);
        if (avroSchemaJson != null)
            builder = builder.WithAvroSchema(new AvroSchema(avroSchemaJson));
        using (var writer = builder.Build(ms))
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

    /// <summary>
    /// Writes data with a simple {id: int} writer schema, then reads with a reader schema
    /// that has an additional field with a default. Returns the result batch.
    /// </summary>
    private static RecordBatch WriteAndReadWithExtraField(string readerSchemaJson, int rowCount = 3)
    {
        // Writer schema: just {id: int}
        var writerSchemaJson = """
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"}
            ]
        }
        """;

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        for (int i = 0; i < rowCount; i++)
            idBuilder.Append(i + 1);

        var batch = new RecordBatch(arrowSchema, [idBuilder.Build()], rowCount);
        var data = WriteTestData(arrowSchema, batch, writerSchemaJson);
        return ReadWithSchema(data, new AvroSchema(readerSchemaJson));
    }

    // ═══════════════════════════════════════════════════════════════
    // Primitive defaults
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public void Default_Int()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "count", "type": "int", "default": 42}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (Int32Array)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.Equal(42, arr.GetValue(i));
    }

    [Fact]
    public void Default_Long()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "big", "type": "long", "default": 9999999999}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (Int64Array)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.Equal(9999999999L, arr.GetValue(i));
    }

    [Fact]
    public void Default_Boolean()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "flag", "type": "boolean", "default": true}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (BooleanArray)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.True(arr.GetValue(i));
    }

    [Fact]
    public void Default_Float()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "score", "type": "float", "default": 1.5}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (FloatArray)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.Equal(1.5f, arr.GetValue(i));
    }

    [Fact]
    public void Default_Double()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "score", "type": "double", "default": 3.14}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (DoubleArray)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.Equal(3.14, arr.GetValue(i));
    }

    [Fact]
    public void Default_String()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "tag", "type": "string", "default": "hello"}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (StringArray)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.Equal("hello", arr.GetString(i));
    }

    [Fact]
    public void Default_NullableUnion_Null()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "opt", "type": ["null", "int"], "default": null}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (Int32Array)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.False(arr.IsValid(i));
    }

    // ═══════════════════════════════════════════════════════════════
    // Logical type defaults
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public void Default_Date()
    {
        // date is days since epoch; default = 18628 (2021-01-01)
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "dt", "type": {"type": "int", "logicalType": "date"}, "default": 18628}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (Date32Array)result.Column(1);
        for (int i = 0; i < 3; i++)
        {
            var val = arr.GetDateTimeOffset(i);
            Assert.NotNull(val);
            Assert.Equal(new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero), val.Value);
        }
    }

    [Fact]
    public void Default_TimeMillis()
    {
        // time-millis default = 45000 (45 seconds = 45000 ms)
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "t", "type": {"type": "int", "logicalType": "time-millis"}, "default": 45000}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (Time32Array)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.Equal(45000, arr.GetValue(i));
    }

    [Fact]
    public void Default_TimeMicros()
    {
        // time-micros: microseconds since midnight
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "t", "type": {"type": "long", "logicalType": "time-micros"}, "default": 45000000}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (Time64Array)result.Column(1);
        Assert.Equal(TimeUnit.Microsecond, ((Time64Type)arr.Data.DataType).Unit);
        for (int i = 0; i < 3; i++)
            Assert.Equal(45000000L, arr.GetValue(i));
    }

    [Fact]
    public void Default_TimeNanos()
    {
        // time-nanos: nanoseconds since midnight
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "t", "type": {"type": "long", "logicalType": "time-nanos"}, "default": 45000000000}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (Time64Array)result.Column(1);
        Assert.Equal(TimeUnit.Nanosecond, ((Time64Type)arr.Data.DataType).Unit);
        for (int i = 0; i < 3; i++)
            Assert.Equal(45000000000L, arr.GetValue(i));
    }

    [Fact]
    public void Default_TimestampMillis()
    {
        // timestamp-millis: milliseconds since epoch
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}, "default": 1609459200000}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (TimestampArray)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.Equal(1609459200000L, arr.GetValue(i));
    }

    [Fact]
    public void Default_TimestampMicros()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-micros"}, "default": 1609459200000000}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (TimestampArray)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.Equal(1609459200000000L, arr.GetValue(i));
    }

    [Fact]
    public void Default_LocalTimestampMillis()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "ts", "type": {"type": "long", "logicalType": "local-timestamp-millis"}, "default": 1609459200000}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (TimestampArray)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.Equal(1609459200000L, arr.GetValue(i));
    }

    [Fact]
    public void Default_Uuid()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "uid", "type": {"type": "string", "logicalType": "uuid"}, "default": "550e8400-e29b-41d4-a716-446655440000"}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (StringArray)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.Equal("550e8400-e29b-41d4-a716-446655440000", arr.GetString(i));
    }

    [Fact]
    public void Default_Enum()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "color", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}, "default": "GREEN"}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (DictionaryArray)result.Column(1);
        var indices = (Int32Array)arr.Indices;
        var dict = (StringArray)arr.Dictionary;
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(1, indices.GetValue(i)); // GREEN is index 1
            Assert.Equal("GREEN", dict.GetString(indices.GetValue(i)!.Value));
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Nullable logical type defaults
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public void Default_NullableDate_WithNull()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "dt", "type": ["null", {"type": "int", "logicalType": "date"}], "default": null}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (Date32Array)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.False(arr.IsValid(i));
    }

    [Fact]
    public void Default_NullableTimestampMicros_WithNull()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "ts", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}], "default": null}
            ]
        }
        """);

        Assert.Equal(3, result.Length);
        var arr = (TimestampArray)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.False(arr.IsValid(i));
    }

    // ═══════════════════════════════════════════════════════════════
    // Multiple defaults together
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public void Default_MultipleLogicalTypes_Together()
    {
        var result = WriteAndReadWithExtraField("""
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "dt", "type": {"type": "int", "logicalType": "date"}, "default": 18628},
                {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}, "default": 1609459200000},
                {"name": "tag", "type": "string", "default": "test"},
                {"name": "t", "type": {"type": "long", "logicalType": "time-nanos"}, "default": 123456789}
            ]
        }
        """);

        Assert.Equal(3, result.Length);

        // id column: 1, 2, 3
        var ids = (Int32Array)result.Column(0);
        for (int i = 0; i < 3; i++)
            Assert.Equal(i + 1, ids.GetValue(i));

        // date defaults
        var dates = (Date32Array)result.Column(1);
        for (int i = 0; i < 3; i++)
        {
            var val = dates.GetDateTimeOffset(i);
            Assert.NotNull(val);
            Assert.Equal(new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero), val.Value);
        }

        // timestamp defaults
        var ts = (TimestampArray)result.Column(2);
        for (int i = 0; i < 3; i++)
            Assert.Equal(1609459200000L, ts.GetValue(i));

        // string defaults
        var tags = (StringArray)result.Column(3);
        for (int i = 0; i < 3; i++)
            Assert.Equal("test", tags.GetString(i));

        // time-nanos defaults
        var times = (Time64Array)result.Column(4);
        for (int i = 0; i < 3; i++)
            Assert.Equal(123456789L, times.GetValue(i));
    }
}
