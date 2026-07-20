// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace EngineeredWood.Avro.Tests;

public class AvroRoundTripTests
{
    [Fact]
    public void RoundTrip_PrimitiveColumns_Uncompressed()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("bool_col", BooleanType.Default, false))
            .Field(new Field("int_col", Int32Type.Default, false))
            .Field(new Field("long_col", Int64Type.Default, false))
            .Field(new Field("float_col", FloatType.Default, false))
            .Field(new Field("double_col", DoubleType.Default, false))
            .Field(new Field("string_col", StringType.Default, false))
            .Build();

        var batch = MakePrimitiveBatch(schema, 100);
        var result = WriteAndRead(schema, batch, AvroCodec.Null);
        AssertBatchesEqual(batch, result);
    }

    [Theory]
    [InlineData(AvroCodec.Deflate)]
    [InlineData(AvroCodec.Snappy)]
    [InlineData(AvroCodec.Zstandard)]
    public void RoundTrip_PrimitiveColumns_Compressed(AvroCodec codec)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("int_col", Int32Type.Default, false))
            .Field(new Field("string_col", StringType.Default, false))
            .Build();

        var batch = MakePrimitiveBatch(schema, 50);
        var result = WriteAndRead(schema, batch, codec);
        AssertBatchesEqual(batch, result);
    }

    [Fact]
    public void RoundTrip_NullableColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("nullable_int", Int32Type.Default, true))
            .Build();

        var intBuilder = new Int32Array.Builder();
        for (int i = 0; i < 20; i++)
        {
            if (i % 3 == 0)
                intBuilder.AppendNull();
            else
                intBuilder.Append(i * 10);
        }

        var batch = new RecordBatch(schema, [intBuilder.Build()], 20);
        var result = WriteAndRead(schema, batch, AvroCodec.Null);

        var srcArr = (Int32Array)batch.Column(0);
        var dstArr = (Int32Array)result.Column(0);
        Assert.Equal(batch.Length, result.Length);
        for (int i = 0; i < batch.Length; i++)
        {
            Assert.Equal(srcArr.IsValid(i), dstArr.IsValid(i));
            if (srcArr.IsValid(i))
                Assert.Equal(srcArr.GetValue(i), dstArr.GetValue(i));
        }
    }

    [Fact]
    public void RoundTrip_EmptyBatch()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, false))
            .Build();

        var batch = new RecordBatch(schema, [new Int32Array.Builder().Build()], 0);
        var result = WriteAndRead(schema, batch, AvroCodec.Null);
        Assert.Equal(0, result.Length);
    }

    [Fact]
    public void RoundTrip_MultipleBatches()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var batches = new List<RecordBatch>();
        for (int b = 0; b < 3; b++)
        {
            var builder = new Int64Array.Builder();
            for (int i = 0; i < 10; i++)
                builder.Append(b * 10 + i);
            batches.Add(new RecordBatch(schema, [builder.Build()], 10));
        }

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(schema).Build(ms))
        {
            foreach (var batch in batches)
                writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        var reader = new AvroReaderBuilder().Build(ms);
        var results = reader.ToList();

        Assert.Equal(3, results.Count);
        for (int b = 0; b < 3; b++)
        {
            Assert.Equal(10, results[b].Length);
            var arr = (Int64Array)results[b].Column(0);
            for (int i = 0; i < 10; i++)
                Assert.Equal(b * 10 + i, arr.GetValue(i));
        }
    }

    [Fact]
    public void RoundTrip_BytesColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("data", BinaryType.Default, false))
            .Build();

        var builder = new BinaryArray.Builder();
        builder.Append(new byte[] { 1, 2, 3 });
        builder.Append(new byte[] { });
        builder.Append(new byte[] { 255, 0, 128 });

        var batch = new RecordBatch(schema, [builder.Build()], 3);
        var result = WriteAndRead(schema, batch, AvroCodec.Null);

        var src = (BinaryArray)batch.Column(0);
        var dst = (BinaryArray)result.Column(0);
        Assert.Equal(3, result.Length);
        for (int i = 0; i < 3; i++)
            Assert.Equal(src.GetBytes(i).ToArray(), dst.GetBytes(i).ToArray());
    }

    [Fact]
    public void Reader_ExposesCodecAndMetadata()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, false))
            .Build();

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(schema)
            .WithCompression(AvroCodec.Deflate)
            .Build(ms))
        {
            var batch = new RecordBatch(schema, [new Int32Array.Builder().Append(1).Build()], 1);
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        Assert.Equal(AvroCodec.Deflate, reader.Codec);
        Assert.True(reader.Metadata.ContainsKey("avro.schema"));
    }

    private static RecordBatch MakePrimitiveBatch(Apache.Arrow.Schema schema, int rowCount)
    {
        var arrays = new List<IArrowArray>();
        foreach (var field in schema.FieldsList)
        {
            arrays.Add(field.DataType switch
            {
                BooleanType => BuildBooleans(rowCount),
                Int32Type => BuildInt32s(rowCount),
                Int64Type => BuildInt64s(rowCount),
                FloatType => BuildFloats(rowCount),
                DoubleType => BuildDoubles(rowCount),
                StringType => BuildStrings(rowCount),
                _ => throw new NotSupportedException(),
            });
        }
        return new RecordBatch(schema, arrays, rowCount);
    }

    private static IArrowArray BuildBooleans(int n)
    {
        var b = new BooleanArray.Builder();
        for (int i = 0; i < n; i++) b.Append(i % 2 == 0);
        return b.Build();
    }

    private static IArrowArray BuildInt32s(int n)
    {
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++) b.Append(i * 7 - 50);
        return b.Build();
    }

    private static IArrowArray BuildInt64s(int n)
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < n; i++) b.Append((long)i * 100_000 - 500_000);
        return b.Build();
    }

    private static IArrowArray BuildFloats(int n)
    {
        var b = new FloatArray.Builder();
        for (int i = 0; i < n; i++) b.Append(i * 0.5f);
        return b.Build();
    }

    private static IArrowArray BuildDoubles(int n)
    {
        var b = new DoubleArray.Builder();
        for (int i = 0; i < n; i++) b.Append(i * 1.23456789);
        return b.Build();
    }

    private static IArrowArray BuildStrings(int n)
    {
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++) b.Append($"row_{i}");
        return b.Build();
    }

    private static RecordBatch WriteAndRead(Apache.Arrow.Schema schema, RecordBatch batch, AvroCodec codec)
    {
        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(schema)
            .WithCompression(codec)
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

    [Fact]
    public void RoundTrip_EnumColumn()
    {
        var dictType = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("color", dictType, false))
            .Build();

        var dictBuilder = new StringArray.Builder();
        dictBuilder.Append("RED");
        dictBuilder.Append("GREEN");
        dictBuilder.Append("BLUE");
        var dictionary = dictBuilder.Build();

        var indexBuilder = new Int32Array.Builder();
        indexBuilder.Append(0); // RED
        indexBuilder.Append(1); // GREEN
        indexBuilder.Append(2); // BLUE
        indexBuilder.Append(0); // RED
        var indices = indexBuilder.Build();

        var dictArray = new DictionaryArray(dictType, indices, dictionary);
        var batch = new RecordBatch(arrowSchema, [dictArray], 4);

        // Must provide Avro schema explicitly since Arrow DictionaryType doesn't carry symbols
        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "color", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}}
            ]
        }
        """;

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
        var result = reader.ReadNextBatch()!;

        Assert.Equal(4, result.Length);
        var resultDict = (DictionaryArray)result.Column(0);
        var resultIndices = (Int32Array)resultDict.Indices;
        Assert.Equal(0, resultIndices.GetValue(0));
        Assert.Equal(1, resultIndices.GetValue(1));
        Assert.Equal(2, resultIndices.GetValue(2));
        Assert.Equal(0, resultIndices.GetValue(3));
    }

    [Fact]
    public void RoundTrip_ArrayColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("tags", new ListType(StringType.Default), false))
            .Build();

        var valueBuilder = new StringArray.Builder();
        var offsetBuilder = new List<int> { 0 };

        // Row 0: ["a", "b"]
        valueBuilder.Append("a");
        valueBuilder.Append("b");
        offsetBuilder.Add(2);

        // Row 1: []
        offsetBuilder.Add(2);

        // Row 2: ["c"]
        valueBuilder.Append("c");
        offsetBuilder.Add(3);

        var listArray = new ListArray(new ListType(StringType.Default),
            3, ToOffsetBuffer(offsetBuilder),
            valueBuilder.Build(), ArrowBuffer.Empty);
        var batch = new RecordBatch(schema, [listArray], 3);
        var result = WriteAndRead(schema, batch, AvroCodec.Null);

        Assert.Equal(3, result.Length);
        var resultList = (ListArray)result.Column(0);
        Assert.Equal(2, resultList.GetValueLength(0)); // ["a", "b"]
        Assert.Equal(0, resultList.GetValueLength(1)); // []
        Assert.Equal(1, resultList.GetValueLength(2)); // ["c"]

        var values = (StringArray)resultList.Values;
        Assert.Equal("a", values.GetString(0));
        Assert.Equal("b", values.GetString(1));
        Assert.Equal("c", values.GetString(2));
    }

    [Fact]
    public void RoundTrip_MapColumn()
    {
        var mapType = new MapType(StringType.Default, Int32Type.Default);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("labels", mapType, false))
            .Build();

        var keyBuilder = new StringArray.Builder();
        var valueBuilder = new Int32Array.Builder();
        var offsets = new List<int> { 0 };

        // Row 0: {"x": 1, "y": 2}
        keyBuilder.Append("x");
        valueBuilder.Append(1);
        keyBuilder.Append("y");
        valueBuilder.Append(2);
        offsets.Add(2);

        // Row 1: {}
        offsets.Add(2);

        // Row 2: {"z": 3}
        keyBuilder.Append("z");
        valueBuilder.Append(3);
        offsets.Add(3);

        var structType = new StructType([
            new Field("key", StringType.Default, false),
            new Field("value", Int32Type.Default, true),
        ]);
        var structArray = new StructArray(structType, 3,
            [keyBuilder.Build(), valueBuilder.Build()], ArrowBuffer.Empty);

        var mapArray = new MapArray(mapType, 3,
            ToOffsetBuffer(offsets), structArray, ArrowBuffer.Empty);
        var batch = new RecordBatch(schema, [mapArray], 3);
        var result = WriteAndRead(schema, batch, AvroCodec.Null);

        Assert.Equal(3, result.Length);
        var resultMap = (MapArray)result.Column(0);
        Assert.Equal(2, resultMap.GetValueLength(0));
        Assert.Equal(0, resultMap.GetValueLength(1));
        Assert.Equal(1, resultMap.GetValueLength(2));
    }

    [Fact]
    public void RoundTrip_FixedColumn()
    {
        var fixedType = new FixedSizeBinaryType(4);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("hash", fixedType, false))
            .Build();

        var values = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }; // 2 values of size 4
        var data = new ArrayData(fixedType, 2, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(values)]);
        var fixedArray = ArrowArrayFactory.BuildArray(data);

        var batch = new RecordBatch(schema, [fixedArray], 2);
        var result = WriteAndRead(schema, batch, AvroCodec.Null);

        Assert.Equal(2, result.Length);
        var resultFixed = (FixedSizeBinaryArray)result.Column(0);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, resultFixed.GetBytes(0).ToArray());
        Assert.Equal(new byte[] { 5, 6, 7, 8 }, resultFixed.GetBytes(1).ToArray());
    }

    [Fact]
    public void RoundTrip_StructColumn()
    {
        var structType = new StructType([
            new Field("city", StringType.Default, false),
            new Field("zip", Int32Type.Default, false),
        ]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("address", structType, false))
            .Build();

        var cityBuilder = new StringArray.Builder();
        cityBuilder.Append("NYC");
        cityBuilder.Append("LA");
        var zipBuilder = new Int32Array.Builder();
        zipBuilder.Append(10001);
        zipBuilder.Append(90001);

        var structArray = new StructArray(structType, 2,
            [cityBuilder.Build(), zipBuilder.Build()], ArrowBuffer.Empty);
        var batch = new RecordBatch(schema, [structArray], 2);
        var result = WriteAndRead(schema, batch, AvroCodec.Null);

        Assert.Equal(2, result.Length);
        var resultStruct = (StructArray)result.Column(0);
        var cities = (StringArray)resultStruct.Fields[0];
        var zips = (Int32Array)resultStruct.Fields[1];
        Assert.Equal("NYC", cities.GetString(0));
        Assert.Equal("LA", cities.GetString(1));
        Assert.Equal(10001, zips.GetValue(0));
        Assert.Equal(90001, zips.GetValue(1));
    }

    [Fact]
    public void RoundTrip_DateColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", Date32Type.Default, false))
            .Build();

        var builder = new Date32Array.Builder();
#if NET6_0_OR_GREATER
        builder.Append(new DateOnly(2024, 1, 15));
        builder.Append(new DateOnly(1970, 1, 1));
        builder.Append(new DateOnly(2000, 6, 30));
#else
        builder.Append(new DateTime(2024, 1, 15));
        builder.Append(new DateTime(1970, 1, 1));
        builder.Append(new DateTime(2000, 6, 30));
#endif

        var batch = new RecordBatch(schema, [builder.Build()], 3);
        var result = WriteAndRead(schema, batch, AvroCodec.Null);

        Assert.Equal(3, result.Length);
        var src = (Date32Array)batch.Column(0);
        var dst = (Date32Array)result.Column(0);
        for (int i = 0; i < 3; i++)
#if NET6_0_OR_GREATER
            Assert.Equal(src.GetDateOnly(i), dst.GetDateOnly(i));
#else
            Assert.Equal(src.GetDateTime(i), dst.GetDateTime(i));
#endif
    }

    [Fact]
    public void RoundTrip_TimestampColumn()
    {
        var tsType = new TimestampType(TimeUnit.Millisecond, (string?)null);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("ts", tsType, false))
            .Build();

        var builder = new TimestampArray.Builder(tsType);
#if NET8_0_OR_GREATER
        builder.Append(DateTimeOffset.UnixEpoch);
#else
        builder.Append(new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero));
#endif
        builder.Append(new DateTimeOffset(2024, 6, 15, 12, 30, 0, TimeSpan.Zero));

        var batch = new RecordBatch(schema, [builder.Build()], 2);
        var result = WriteAndRead(schema, batch, AvroCodec.Null);

        Assert.Equal(2, result.Length);
        var src = (TimestampArray)batch.Column(0);
        var dst = (TimestampArray)result.Column(0);
        Assert.Equal(src.GetValue(0), dst.GetValue(0));
        Assert.Equal(src.GetValue(1), dst.GetValue(1));
    }

    private static void AssertBatchesEqual(RecordBatch expected, RecordBatch actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        Assert.Equal(expected.ColumnCount, actual.ColumnCount);

        for (int col = 0; col < expected.ColumnCount; col++)
        {
            var srcArr = expected.Column(col);
            var dstArr = actual.Column(col);

            for (int row = 0; row < expected.Length; row++)
            {
                Assert.Equal(srcArr.IsValid(row), dstArr.IsValid(row));
                if (!srcArr.IsValid(row)) continue;

                switch (srcArr)
                {
                    case BooleanArray s:
                        Assert.Equal(s.GetValue(row), ((BooleanArray)dstArr).GetValue(row));
                        break;
                    case Int32Array s:
                        Assert.Equal(s.GetValue(row), ((Int32Array)dstArr).GetValue(row));
                        break;
                    case Int64Array s:
                        Assert.Equal(s.GetValue(row), ((Int64Array)dstArr).GetValue(row));
                        break;
                    case FloatArray s:
                        Assert.Equal(s.GetValue(row), ((FloatArray)dstArr).GetValue(row));
                        break;
                    case DoubleArray s:
                        Assert.Equal(s.GetValue(row), ((DoubleArray)dstArr).GetValue(row));
                        break;
                    case StringArray s:
                        Assert.Equal(s.GetString(row), ((StringArray)dstArr).GetString(row));
                        break;
                }
            }
        }
    }

    private static ArrowBuffer ToOffsetBuffer(List<int> offsets)
    {
        var bytes = new byte[offsets.Count * 4];
        for (int i = 0; i < offsets.Count; i++)
#if NET8_0_OR_GREATER
            BitConverter.TryWriteBytes(bytes.AsSpan(i * 4), offsets[i]);
#else
            Buffer.BlockCopy(BitConverter.GetBytes(offsets[i]), 0, bytes, i * 4, 4);
#endif
        return new ArrowBuffer(bytes);
    }
}
