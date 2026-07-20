// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace EngineeredWood.Avro.Tests;

public class AvroProjectionTests
{
    private static Apache.Arrow.Schema FiveColumnSchema => new Apache.Arrow.Schema.Builder()
        .Field(new Field("col_a", Int32Type.Default, false))
        .Field(new Field("col_b", StringType.Default, false))
        .Field(new Field("col_c", DoubleType.Default, false))
        .Field(new Field("col_d", BooleanType.Default, false))
        .Field(new Field("col_e", Int64Type.Default, false))
        .Build();

    private static RecordBatch MakeFiveColumnBatch(int rowCount)
    {
        var colA = new Int32Array.Builder();
        var colB = new StringArray.Builder();
        var colC = new DoubleArray.Builder();
        var colD = new BooleanArray.Builder();
        var colE = new Int64Array.Builder();

        for (int i = 0; i < rowCount; i++)
        {
            colA.Append(i);
            colB.Append($"str_{i}");
            colC.Append(i * 1.5);
            colD.Append(i % 2 == 0);
            colE.Append((long)i * 100);
        }

        return new RecordBatch(FiveColumnSchema,
            [colA.Build(), colB.Build(), colC.Build(), colD.Build(), colE.Build()],
            rowCount);
    }

    private static byte[] WriteToBytes(Apache.Arrow.Schema schema, RecordBatch batch, AvroCodec codec = AvroCodec.Null)
    {
        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(schema)
            .WithCompression(codec)
            .Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }
        return ms.ToArray();
    }

    [Fact]
    public void WithProjection_SelectsTwoOfFiveColumns()
    {
        var batch = MakeFiveColumnBatch(50);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithProjection(0, 2)
            .Build(ms);

        Assert.Equal(2, reader.Schema.FieldsList.Count);
        Assert.Equal("col_a", reader.Schema.FieldsList[0].Name);
        Assert.Equal("col_c", reader.Schema.FieldsList[1].Name);

        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(50, result.Length);
        Assert.Equal(2, result.ColumnCount);

        var ints = (Int32Array)result.Column(0);
        var doubles = (DoubleArray)result.Column(1);
        for (int i = 0; i < 50; i++)
        {
            Assert.Equal(i, ints.GetValue(i));
            Assert.Equal(i * 1.5, doubles.GetValue(i));
        }
    }

    [Fact]
    public void WithSkipFields_ExcludesNamedColumn()
    {
        var batch = MakeFiveColumnBatch(30);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithSkipFields("col_b")
            .Build(ms);

        Assert.Equal(4, reader.Schema.FieldsList.Count);
        Assert.DoesNotContain(reader.Schema.FieldsList, f => f.Name == "col_b");

        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(30, result.Length);
        Assert.Equal(4, result.ColumnCount);
    }

    [Fact]
    public void WithSkipFields_ExcludesMultipleColumns()
    {
        var batch = MakeFiveColumnBatch(20);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithSkipFields("col_a", "col_c", "col_e")
            .Build(ms);

        Assert.Equal(2, reader.Schema.FieldsList.Count);
        Assert.Equal("col_b", reader.Schema.FieldsList[0].Name);
        Assert.Equal("col_d", reader.Schema.FieldsList[1].Name);

        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(20, result.Length);
    }

    [Fact]
    public void AvroSchema_Project_ReturnsProjectedSchema()
    {
        var avroSchema = AvroSchema.FromArrowSchema(FiveColumnSchema, "TestRecord");
        var projected = avroSchema.Project([1, 3]);

        var arrowSchema = projected.ToArrowSchema();
        Assert.Equal(2, arrowSchema.FieldsList.Count);
        Assert.Equal("col_b", arrowSchema.FieldsList[0].Name);
        Assert.Equal("col_d", arrowSchema.FieldsList[1].Name);
    }

    [Fact]
    public void AvroSchema_Project_OutOfRange_Throws()
    {
        var avroSchema = AvroSchema.FromArrowSchema(FiveColumnSchema, "TestRecord");
        Assert.Throws<ArgumentOutOfRangeException>(() => avroSchema.Project([0, 99]));
    }

    [Fact]
    public void AvroSchema_Project_NegativeIndex_Throws()
    {
        var avroSchema = AvroSchema.FromArrowSchema(FiveColumnSchema, "TestRecord");
        Assert.Throws<ArgumentOutOfRangeException>(() => avroSchema.Project([-1]));
    }

    [Theory]
    [InlineData(AvroCodec.Snappy)]
    [InlineData(AvroCodec.Deflate)]
    [InlineData(AvroCodec.Zstandard)]
    public void Projection_WithCompression(AvroCodec codec)
    {
        var batch = MakeFiveColumnBatch(40);
        var bytes = WriteToBytes(FiveColumnSchema, batch, codec);

        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithProjection(1, 4)
            .Build(ms);

        Assert.Equal(2, reader.Schema.FieldsList.Count);
        Assert.Equal("col_b", reader.Schema.FieldsList[0].Name);
        Assert.Equal("col_e", reader.Schema.FieldsList[1].Name);

        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(40, result.Length);

        var strings = (StringArray)result.Column(0);
        var longs = (Int64Array)result.Column(1);
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal($"str_{i}", strings.GetString(i));
            Assert.Equal((long)i * 100, longs.GetValue(i));
        }
    }

    [Fact]
    public void Projection_NestedType_SkipsStructColumn()
    {
        var structType = new StructType([
            new Field("city", StringType.Default, false),
            new Field("zip", Int32Type.Default, false),
        ]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("address", structType, false))
            .Field(new Field("name", StringType.Default, false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var cityBuilder = new StringArray.Builder();
        var zipBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        for (int i = 0; i < 10; i++)
        {
            idBuilder.Append(i);
            cityBuilder.Append($"city_{i}");
            zipBuilder.Append(10000 + i);
            nameBuilder.Append($"name_{i}");
        }

        var structArray = new StructArray(structType, 10,
            [cityBuilder.Build(), zipBuilder.Build()], ArrowBuffer.Empty);
        var batch = new RecordBatch(schema, [idBuilder.Build(), structArray, nameBuilder.Build()], 10);
        var bytes = WriteToBytes(schema, batch);

        // Project away the struct column (index 1), keep id and name
        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithProjection(0, 2)
            .Build(ms);

        Assert.Equal(2, reader.Schema.FieldsList.Count);
        Assert.Equal("id", reader.Schema.FieldsList[0].Name);
        Assert.Equal("name", reader.Schema.FieldsList[1].Name);

        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(10, result.Length);

        var ids = (Int32Array)result.Column(0);
        var names = (StringArray)result.Column(1);
        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(i, ids.GetValue(i));
            Assert.Equal($"name_{i}", names.GetString(i));
        }
    }

    [Fact]
    public void Projection_NestedType_SkipsArrayColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("tags", new ListType(StringType.Default), false))
            .Field(new Field("score", DoubleType.Default, false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var scoreBuilder = new DoubleArray.Builder();
        var valueBuilder = new StringArray.Builder();
        var offsets = new List<int> { 0 };

        for (int i = 0; i < 5; i++)
        {
            idBuilder.Append(i);
            scoreBuilder.Append(i * 2.5);
            valueBuilder.Append($"tag_{i}");
            offsets.Add(offsets[^1] + 1);
        }

        var listArray = new ListArray(new ListType(StringType.Default),
            5, ToOffsetBuffer(offsets), valueBuilder.Build(), ArrowBuffer.Empty);
        var batch = new RecordBatch(schema, [idBuilder.Build(), listArray, scoreBuilder.Build()], 5);
        var bytes = WriteToBytes(schema, batch);

        // Skip the list column
        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithSkipFields("tags")
            .Build(ms);

        Assert.Equal(2, reader.Schema.FieldsList.Count);
        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(5, result.Length);

        var ids = (Int32Array)result.Column(0);
        var scores = (DoubleArray)result.Column(1);
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(i, ids.GetValue(i));
            Assert.Equal(i * 2.5, scores.GetValue(i));
        }
    }

    [Fact]
    public async Task Async_Projection()
    {
        var batch = MakeFiveColumnBatch(25);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        var reader = await new AvroReaderBuilder()
            .WithProjection(0, 4)
            .BuildAsync(ms);

        Assert.Equal(2, reader.Schema.FieldsList.Count);
        Assert.Equal("col_a", reader.Schema.FieldsList[0].Name);
        Assert.Equal("col_e", reader.Schema.FieldsList[1].Name);

        var result = await reader.ReadNextBatchAsync();
        Assert.NotNull(result);
        Assert.Equal(25, result.Length);

        var ints = (Int32Array)result.Column(0);
        var longs = (Int64Array)result.Column(1);
        for (int i = 0; i < 25; i++)
        {
            Assert.Equal(i, ints.GetValue(i));
            Assert.Equal((long)i * 100, longs.GetValue(i));
        }
    }

    [Fact]
    public void WithProjection_ByName_SelectsTwoOfFiveColumns()
    {
        var batch = MakeFiveColumnBatch(50);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithProjection("col_a", "col_c")
            .Build(ms);

        Assert.Equal(2, reader.Schema.FieldsList.Count);
        Assert.Equal("col_a", reader.Schema.FieldsList[0].Name);
        Assert.Equal("col_c", reader.Schema.FieldsList[1].Name);

        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(50, result.Length);
        Assert.Equal(2, result.ColumnCount);

        var ints = (Int32Array)result.Column(0);
        var doubles = (DoubleArray)result.Column(1);
        for (int i = 0; i < 50; i++)
        {
            Assert.Equal(i, ints.GetValue(i));
            Assert.Equal(i * 1.5, doubles.GetValue(i));
        }
    }

    [Fact]
    public void WithProjection_ByName_UnknownField_Throws()
    {
        var batch = MakeFiveColumnBatch(10);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        Assert.Throws<ArgumentException>(() =>
            new AvroReaderBuilder().WithProjection("col_a", "nonexistent").Build(ms));
    }

    [Fact]
    public void WithProjection_ByName_SingleColumn()
    {
        var batch = MakeFiveColumnBatch(15);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithProjection("col_d")
            .Build(ms);

        Assert.Single(reader.Schema.FieldsList);
        Assert.Equal("col_d", reader.Schema.FieldsList[0].Name);

        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(15, result.Length);

        var bools = (BooleanArray)result.Column(0);
        for (int i = 0; i < 15; i++)
            Assert.Equal(i % 2 == 0, bools.GetValue(i));
    }

    [Fact]
    public void WithProjection_OutOfRange_Throws()
    {
        var batch = MakeFiveColumnBatch(10);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new AvroReaderBuilder().WithProjection(0, 10).Build(ms));
    }

    [Fact]
    public void WithProjection_SingleColumn()
    {
        var batch = MakeFiveColumnBatch(15);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithProjection(3)
            .Build(ms);

        Assert.Single(reader.Schema.FieldsList);
        Assert.Equal("col_d", reader.Schema.FieldsList[0].Name);

        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(15, result.Length);
        Assert.Equal(1, result.ColumnCount);

        var bools = (BooleanArray)result.Column(0);
        for (int i = 0; i < 15; i++)
            Assert.Equal(i % 2 == 0, bools.GetValue(i));
    }

    [Fact]
    public async Task Async_Projection_ByName()
    {
        var batch = MakeFiveColumnBatch(25);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        var reader = await new AvroReaderBuilder()
            .WithProjection("col_a", "col_e")
            .BuildAsync(ms);

        Assert.Equal(2, reader.Schema.FieldsList.Count);
        Assert.Equal("col_a", reader.Schema.FieldsList[0].Name);
        Assert.Equal("col_e", reader.Schema.FieldsList[1].Name);

        var result = await reader.ReadNextBatchAsync();
        Assert.NotNull(result);
        Assert.Equal(25, result.Length);

        var ints = (Int32Array)result.Column(0);
        var longs = (Int64Array)result.Column(1);
        for (int i = 0; i < 25; i++)
        {
            Assert.Equal(i, ints.GetValue(i));
            Assert.Equal((long)i * 100, longs.GetValue(i));
        }
    }

    [Fact]
    public void WithProjection_AllColumns_EquivalentToNoProjection()
    {
        var batch = MakeFiveColumnBatch(20);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithProjection(0, 1, 2, 3, 4)
            .Build(ms);

        Assert.Equal(5, reader.Schema.FieldsList.Count);

        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(20, result.Length);
        Assert.Equal(5, result.ColumnCount);

        var ints = (Int32Array)result.Column(0);
        var strings = (StringArray)result.Column(1);
        var doubles = (DoubleArray)result.Column(2);
        var bools = (BooleanArray)result.Column(3);
        var longs = (Int64Array)result.Column(4);
        for (int i = 0; i < 20; i++)
        {
            Assert.Equal(i, ints.GetValue(i));
            Assert.Equal($"str_{i}", strings.GetString(i));
            Assert.Equal(i * 1.5, doubles.GetValue(i));
            Assert.Equal(i % 2 == 0, bools.GetValue(i));
            Assert.Equal((long)i * 100, longs.GetValue(i));
        }
    }

    [Fact]
    public void WithProjection_NonSequentialOrder()
    {
        var batch = MakeFiveColumnBatch(20);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithProjection(4, 1, 0)
            .Build(ms);

        Assert.Equal(3, reader.Schema.FieldsList.Count);
        Assert.Equal("col_e", reader.Schema.FieldsList[0].Name);
        Assert.Equal("col_b", reader.Schema.FieldsList[1].Name);
        Assert.Equal("col_a", reader.Schema.FieldsList[2].Name);

        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(20, result.Length);

        var longs = (Int64Array)result.Column(0);
        var strings = (StringArray)result.Column(1);
        var ints = (Int32Array)result.Column(2);
        for (int i = 0; i < 20; i++)
        {
            Assert.Equal((long)i * 100, longs.GetValue(i));
            Assert.Equal($"str_{i}", strings.GetString(i));
            Assert.Equal(i, ints.GetValue(i));
        }
    }

    [Fact]
    public void WithProjection_ByName_NonSequentialOrder()
    {
        var batch = MakeFiveColumnBatch(20);
        var bytes = WriteToBytes(FiveColumnSchema, batch);

        using var ms = new MemoryStream(bytes);
        var reader = new AvroReaderBuilder()
            .WithProjection("col_e", "col_b", "col_a")
            .Build(ms);

        Assert.Equal(3, reader.Schema.FieldsList.Count);
        Assert.Equal("col_e", reader.Schema.FieldsList[0].Name);
        Assert.Equal("col_b", reader.Schema.FieldsList[1].Name);
        Assert.Equal("col_a", reader.Schema.FieldsList[2].Name);

        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        Assert.Equal(20, result.Length);

        var longs = (Int64Array)result.Column(0);
        var strings = (StringArray)result.Column(1);
        var ints = (Int32Array)result.Column(2);
        for (int i = 0; i < 20; i++)
        {
            Assert.Equal((long)i * 100, longs.GetValue(i));
            Assert.Equal($"str_{i}", strings.GetString(i));
            Assert.Equal(i, ints.GetValue(i));
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
