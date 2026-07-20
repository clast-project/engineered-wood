// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.Avro.Data;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;

namespace EngineeredWood.Avro.Benchmarks;

/// <summary>
/// Isolates the encode path: Arrow RecordBatch → RecordBatchEncoder → Avro binary bytes.
/// No OCF framing, no stream I/O, no compression.
/// </summary>
[MemoryDiagnoser]
public class EncodeBenchmarks
{
    private RecordBatch _primitiveBatch = null!;
    private RecordBatch _mixedBatch = null!;
    private AvroRecordSchema _primitiveAvroSchema = null!;
    private AvroRecordSchema _mixedAvroSchema = null!;

    [Params(10_000, 100_000, 1_000_000)]
    public int RowCount;

    [GlobalSetup]
    public void Setup()
    {
        var primitiveArrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("int_col", Int32Type.Default, false))
            .Field(new Field("long_col", Int64Type.Default, false))
            .Field(new Field("double_col", DoubleType.Default, false))
            .Field(new Field("string_col", StringType.Default, false))
            .Field(new Field("bool_col", BooleanType.Default, false))
            .Build();
        _primitiveAvroSchema = ArrowSchemaConverter.FromArrow(primitiveArrowSchema);
        _primitiveBatch = MakePrimitiveBatch(RowCount);

        var mixedArrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("value", Int64Type.Default, true))
            .Field(new Field("name", StringType.Default, false))
            .Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), false))
            .Field(new Field("amount", new Decimal128Type(18, 2), false))
            .Build();
        _mixedAvroSchema = ArrowSchemaConverter.FromArrow(mixedArrowSchema);
        _mixedBatch = MakeMixedBatch(RowCount);
    }

    [Benchmark(Baseline = true)]
    public int Encode_Primitives()
    {
        var encoder = new RecordBatchEncoder(_primitiveAvroSchema);
        var buffer = new GrowableBuffer(_primitiveBatch.Length * 64);
        return encoder.Encode(_primitiveBatch, buffer);
    }

    [Benchmark]
    public int Encode_Mixed()
    {
        var encoder = new RecordBatchEncoder(_mixedAvroSchema);
        var buffer = new GrowableBuffer(_mixedBatch.Length * 64);
        return encoder.Encode(_mixedBatch, buffer);
    }

    // ─── Helpers ───

    private static RecordBatch MakePrimitiveBatch(int rowCount)
    {
        var rng = new Random(42);
        var intB = new Int32Array.Builder();
        var longB = new Int64Array.Builder();
        var doubleB = new DoubleArray.Builder();
        var stringB = new StringArray.Builder();
        var boolB = new BooleanArray.Builder();

        for (int i = 0; i < rowCount; i++)
        {
            intB.Append(rng.Next());
#if NET8_0_OR_GREATER
            longB.Append(rng.NextInt64());
#else
            longB.Append(((long)rng.Next() << 32) | (long)(uint)rng.Next());
#endif
            doubleB.Append(rng.NextDouble() * 1000);
            stringB.Append($"val_{rng.Next(0, 10000)}");
            boolB.Append(rng.Next(2) == 0);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("int_col", Int32Type.Default, false))
            .Field(new Field("long_col", Int64Type.Default, false))
            .Field(new Field("double_col", DoubleType.Default, false))
            .Field(new Field("string_col", StringType.Default, false))
            .Field(new Field("bool_col", BooleanType.Default, false))
            .Build();

        return new RecordBatch(schema,
            [intB.Build(), longB.Build(), doubleB.Build(), stringB.Build(), boolB.Build()],
            rowCount);
    }

    private static RecordBatch MakeMixedBatch(int rowCount)
    {
        var rng = new Random(42);
        var idB = new Int32Array.Builder();
        var valueB = new Int64Array.Builder();
        var nameB = new StringArray.Builder();
        var tsB = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var decimalB = new Decimal128Array.Builder(new Decimal128Type(18, 2));

        for (int i = 0; i < rowCount; i++)
        {
            idB.Append(i);
            if (i % 5 == 0)
                valueB.AppendNull();
            else
#if NET8_0_OR_GREATER
                valueB.Append(rng.NextInt64());
#else
                valueB.Append(((long)rng.Next() << 32) | (long)(uint)rng.Next());
#endif
            nameB.Append($"name_{rng.Next(0, 10000)}");
#if NET8_0_OR_GREATER
            tsB.Append(DateTimeOffset.UnixEpoch.AddMicroseconds(rng.NextInt64(0, 2_000_000_000_000L)));
            decimalB.Append(rng.NextInt64(0, 100_000_000) / 100m);
#else
            var epoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
            long tsMicros = (((long)rng.Next() << 32) | (long)(uint)rng.Next()) % 2_000_000_000_000L;
            tsB.Append(epoch.AddTicks(tsMicros * 10));
            decimalB.Append((((long)rng.Next() << 32) | (long)(uint)rng.Next()) % 100_000_000 / 100m);
#endif
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("value", Int64Type.Default, true))
            .Field(new Field("name", StringType.Default, false))
            .Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), false))
            .Field(new Field("amount", new Decimal128Type(18, 2), false))
            .Build();

        return new RecordBatch(schema,
            [idB.Build(), valueB.Build(), nameB.Build(), tsB.Build(), decimalB.Build()],
            rowCount);
    }
}
