// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace EngineeredWood.Avro.Tests;

public class AvroAsyncTests
{
    [Fact]
    public async Task AsyncRoundTrip_PrimitiveColumns_Uncompressed()
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
        var result = await WriteAndReadAsync(schema, batch, AvroCodec.Null);
        AssertBatchesEqual(batch, result);
    }

    [Theory]
    [InlineData(AvroCodec.Deflate)]
    [InlineData(AvroCodec.Snappy)]
    [InlineData(AvroCodec.Zstandard)]
    public async Task AsyncRoundTrip_PrimitiveColumns_Compressed(AvroCodec codec)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("int_col", Int32Type.Default, false))
            .Field(new Field("string_col", StringType.Default, false))
            .Build();

        var batch = MakePrimitiveBatch(schema, 50);
        var result = await WriteAndReadAsync(schema, batch, codec);
        AssertBatchesEqual(batch, result);
    }

    [Fact]
    public async Task AsyncRoundTrip_NullableColumn()
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
        var result = await WriteAndReadAsync(schema, batch, AvroCodec.Null);

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
    public async Task AsyncRoundTrip_MultipleBatches()
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

#if NET8_0_OR_GREATER
        await using var ms = new MemoryStream();
#else
        using var ms = new MemoryStream();
#endif
        var writer = await new AvroWriterBuilder(schema).BuildAsync(ms);
        await using (writer)
        {
            foreach (var batch in batches)
                await writer.WriteAsync(batch);
            await writer.FinishAsync();
        }

        ms.Position = 0;
        var reader = await new AvroReaderBuilder().BuildAsync(ms);
        await using (reader)
        {
            var results = new List<RecordBatch>();
            await foreach (var batch in reader)
                results.Add(batch);

            Assert.Equal(3, results.Count);
            for (int b = 0; b < 3; b++)
            {
                Assert.Equal(10, results[b].Length);
                var arr = (Int64Array)results[b].Column(0);
                for (int i = 0; i < 10; i++)
                    Assert.Equal(b * 10 + i, arr.GetValue(i));
            }
        }
    }

    [Fact]
    public async Task AsyncRoundTrip_IAsyncEnumerable()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 5; i++) builder.Append(i);
        var batch = new RecordBatch(schema, [builder.Build()], 5);

#if NET8_0_OR_GREATER
        await using var ms = new MemoryStream();
#else
        using var ms = new MemoryStream();
#endif
        var writer = await new AvroWriterBuilder(schema).BuildAsync(ms);
        await using (writer)
        {
            await writer.WriteAsync(batch);
            await writer.FinishAsync();
        }

        ms.Position = 0;
        var reader = await new AvroReaderBuilder().BuildAsync(ms);
        await using (reader)
        {
            int batchCount = 0;
            await foreach (var b in reader)
            {
                batchCount++;
                Assert.Equal(5, b.Length);
            }
            Assert.Equal(1, batchCount);
        }
    }

    [Fact]
    public async Task AsyncRoundTrip_EmptyBatch()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, false))
            .Build();

        var batch = new RecordBatch(schema, [new Int32Array.Builder().Build()], 0);
        var result = await WriteAndReadAsync(schema, batch, AvroCodec.Null);
        Assert.Equal(0, result.Length);
    }

    [Fact]
    public async Task AsyncReader_ExposesCodecAndMetadata()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, false))
            .Build();

#if NET8_0_OR_GREATER
        await using var ms = new MemoryStream();
#else
        using var ms = new MemoryStream();
#endif
        var writer = await new AvroWriterBuilder(schema)
            .WithCompression(AvroCodec.Deflate)
            .BuildAsync(ms);
        await using (writer)
        {
            var batch = new RecordBatch(schema, [new Int32Array.Builder().Append(1).Build()], 1);
            await writer.WriteAsync(batch);
            await writer.FinishAsync();
        }

        ms.Position = 0;
        var reader = await new AvroReaderBuilder().BuildAsync(ms);
        await using (reader)
        {
            Assert.Equal(AvroCodec.Deflate, reader.Codec);
            Assert.True(reader.Metadata.ContainsKey("avro.schema"));
        }
    }

    [Fact]
    public async Task AsyncRoundTrip_CancellationToken_Respected()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, false))
            .Build();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

#if NET8_0_OR_GREATER
        await using var ms = new MemoryStream();
#else
        using var ms = new MemoryStream();
#endif

        // Building the writer with a cancelled token should throw
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var writer = await new AvroWriterBuilder(schema).BuildAsync(ms, cts.Token);
        });
    }

    [Fact]
    public async Task AsyncWrite_SyncRead_CrossCompatible()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", Int32Type.Default, false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 10; i++) builder.Append(i * 42);
        var batch = new RecordBatch(schema, [builder.Build()], 10);

        // Write async
#if NET8_0_OR_GREATER
        await using var ms = new MemoryStream();
#else
        using var ms = new MemoryStream();
#endif
        var writer = await new AvroWriterBuilder(schema).BuildAsync(ms);
        await using (writer)
        {
            await writer.WriteAsync(batch);
            await writer.FinishAsync();
        }

        // Read sync
        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        AssertBatchesEqual(batch, result);
    }

    [Fact]
    public async Task SyncWrite_AsyncRead_CrossCompatible()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", Int32Type.Default, false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 10; i++) builder.Append(i * 42);
        var batch = new RecordBatch(schema, [builder.Build()], 10);

        // Write sync
#if NET8_0_OR_GREATER
        await using var ms = new MemoryStream();
#else
        using var ms = new MemoryStream();
#endif
        using (var writer = new AvroWriterBuilder(schema).Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        // Read async
        ms.Position = 0;
        var reader = await new AvroReaderBuilder().BuildAsync(ms);
        await using (reader)
        {
            var result = await reader.ReadNextBatchAsync();
            Assert.NotNull(result);
            AssertBatchesEqual(batch, result);
        }
    }

    private static async ValueTask<RecordBatch> WriteAndReadAsync(
        Apache.Arrow.Schema schema, RecordBatch batch, AvroCodec codec)
    {
#if NET8_0_OR_GREATER
        await using var ms = new MemoryStream();
#else
        using var ms = new MemoryStream();
#endif
        var writer = await new AvroWriterBuilder(schema)
            .WithCompression(codec)
            .BuildAsync(ms);
        await using (writer)
        {
            await writer.WriteAsync(batch);
            await writer.FinishAsync();
        }

        ms.Position = 0;
        var reader = await new AvroReaderBuilder().BuildAsync(ms);
        await using (reader)
        {
            var result = await reader.ReadNextBatchAsync();
            Assert.NotNull(result);
            return result;
        }
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
}
