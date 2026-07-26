// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// Regression tests for cross-batch buffer aliasing. The decoder reuses one assembler across
/// batches and hands column buffers to Arrow zero-copy; if a builder mutated a handed-off
/// buffer on the next batch, an earlier retained batch would be silently corrupted. These
/// tests retain every batch and read the earliest ones last.
/// </summary>
public class AvroRetainedBatchTests
{
    private static byte[] Write(Apache.Arrow.Schema schema, RecordBatch batch)
    {
        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(schema).Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }
        return ms.ToArray();
    }

    private static List<RecordBatch> ReadAll(byte[] data, int batchSize)
    {
        using var ms = new MemoryStream(data);
        using var reader = new AvroReaderBuilder().WithBatchSize(batchSize).Build(ms);
        var batches = new List<RecordBatch>();
        foreach (var b in reader)
            batches.Add(b);
        return batches;
    }

    [Fact]
    public void RetainedBatches_TimestampColumn_KeepTheirValues()
    {
        var type = new TimestampType(TimeUnit.Millisecond, (string?)null);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("ts", type, false)).Build();

        var builder = new TimestampArray.Builder(type);
        for (int i = 0; i < 100; i++)
            builder.Append(DateTimeOffset.FromUnixTimeMilliseconds(i));
        var data = Write(schema, new RecordBatch(schema, [builder.Build()], 100));

        // batchSize 30 -> batches of 30,30,30,10, all retained before we read any.
        var batches = ReadAll(data, batchSize: 30);
        Assert.Equal(4, batches.Count);

        long expected = 0;
        foreach (var b in batches)
        {
            var arr = (TimestampArray)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                Assert.Equal(expected++, arr.GetValue(i));
        }
        Assert.Equal(100, expected);
    }

    [Fact]
    public void RetainedBatches_NullableColumn_KeepTheirValidity()
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", Int32Type.Default, true)).Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++)
        {
            if (i % 3 == 0) builder.AppendNull();
            else builder.Append(i);
        }
        var data = Write(schema, new RecordBatch(schema, [builder.Build()], 100));

        var batches = ReadAll(data, batchSize: 30);
        Assert.Equal(4, batches.Count);

        int row = 0;
        foreach (var b in batches)
        {
            var arr = (Int32Array)b.Column(0);
            for (int i = 0; i < b.Length; i++, row++)
            {
                if (row % 3 == 0)
                    Assert.True(arr.IsNull(i));
                else
                    Assert.Equal(row, arr.GetValue(i));
            }
        }
        Assert.Equal(100, row);
    }

    [Fact]
    public void RetainedBatches_BooleanColumn_KeepTheirValues()
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("b", BooleanType.Default, true)).Build();

        var builder = new BooleanArray.Builder();
        for (int i = 0; i < 100; i++)
        {
            if (i % 5 == 0) builder.AppendNull();
            else builder.Append(i % 2 == 0);
        }
        var data = Write(schema, new RecordBatch(schema, [builder.Build()], 100));

        var batches = ReadAll(data, batchSize: 30);
        Assert.Equal(4, batches.Count);

        int row = 0;
        foreach (var b in batches)
        {
            var arr = (BooleanArray)b.Column(0);
            for (int i = 0; i < b.Length; i++, row++)
            {
                if (row % 5 == 0)
                    Assert.True(arr.IsNull(i));
                else
                    Assert.Equal(row % 2 == 0, arr.GetValue(i));
            }
        }
        Assert.Equal(100, row);
    }

    [Fact]
    public void RetainedBatches_StringColumn_KeepTheirValues()
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("s", StringType.Default, true)).Build();

        var builder = new StringArray.Builder();
        for (int i = 0; i < 100; i++)
        {
            if (i % 4 == 0) builder.AppendNull();
            else builder.Append($"row-{i}");
        }
        var data = Write(schema, new RecordBatch(schema, [builder.Build()], 100));

        var batches = ReadAll(data, batchSize: 30);
        Assert.Equal(4, batches.Count);

        int row = 0;
        foreach (var b in batches)
        {
            var arr = (StringArray)b.Column(0);
            for (int i = 0; i < b.Length; i++, row++)
            {
                if (row % 4 == 0)
                    Assert.True(arr.IsNull(i));
                else
                    Assert.Equal($"row-{row}", arr.GetString(i));
            }
        }
        Assert.Equal(100, row);
    }

    [Fact]
    public void RetainedBatches_ListColumn_KeepTheirValues()
    {
        var itemField = new Field("item", Int32Type.Default, false);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("l", new ListType(itemField), false)).Build();

        var valueBuilder = new Int32Array.Builder();
        var offsets = new ArrowBuffer.Builder<int>();
        offsets.Append(0);
        int running = 0;
        for (int i = 0; i < 100; i++)
        {
            // row i has i%3 items: [i*10, i*10+1, ...]
            int len = i % 3;
            for (int j = 0; j < len; j++) valueBuilder.Append(i * 10 + j);
            running += len;
            offsets.Append(running);
        }
        var listArr = new ListArray(new ListType(itemField), 100,
            offsets.Build(), valueBuilder.Build(), ArrowBuffer.Empty, 0);
        var data = Write(schema, new RecordBatch(schema, [listArr], 100));

        var batches = ReadAll(data, batchSize: 30);
        Assert.Equal(4, batches.Count);

        int row = 0;
        foreach (var b in batches)
        {
            var arr = (ListArray)b.Column(0);
            for (int i = 0; i < b.Length; i++, row++)
            {
                var items = (Int32Array)arr.GetSlicedValues(i);
                int len = row % 3;
                Assert.Equal(len, items.Length);
                for (int j = 0; j < len; j++)
                    Assert.Equal(row * 10 + j, items.GetValue(j));
            }
        }
        Assert.Equal(100, row);
    }

    [Fact]
    public void RetainedBatches_DecimalColumn_KeepTheirValues()
    {
        var type = new Decimal128Type(18, 2);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("d", type, false)).Build();

        var builder = new Decimal128Array.Builder(type);
        for (int i = 0; i < 100; i++)
            builder.Append(new decimal(i) + 0.25m);
        var data = Write(schema, new RecordBatch(schema, [builder.Build()], 100));

        var batches = ReadAll(data, batchSize: 30);
        Assert.Equal(4, batches.Count);

        int row = 0;
        foreach (var b in batches)
        {
            var arr = (Decimal128Array)b.Column(0);
            for (int i = 0; i < b.Length; i++, row++)
                Assert.Equal(new decimal(row) + 0.25m, arr.GetValue(i));
        }
        Assert.Equal(100, row);
    }
}
