// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;

namespace EngineeredWood.Avro.Benchmarks;

[MemoryDiagnoser]
public class EncoderBenchmarks
{
    private Apache.Arrow.Schema _schema = null!;
    private RecordBatch _batch = null!;

    [GlobalSetup]
    public void Setup()
    {
        const int rowCount = 10_000;
        var rng = new Random(42);

        var intBuilder = new Int32Array.Builder();
        var longBuilder = new Int64Array.Builder();
        var doubleBuilder = new DoubleArray.Builder();
        var stringBuilder = new StringArray.Builder();
        var boolBuilder = new BooleanArray.Builder();

        for (int i = 0; i < rowCount; i++)
        {
            intBuilder.Append(rng.Next());
#if NET8_0_OR_GREATER
            longBuilder.Append(rng.NextInt64());
#else
            longBuilder.Append(((long)rng.Next() << 32) | (long)(uint)rng.Next());
#endif
            doubleBuilder.Append(rng.NextDouble() * 1000);
            stringBuilder.Append($"value_{rng.Next(0, 10000)}");
            boolBuilder.Append(rng.Next(2) == 0);
        }

        _schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("int_col", Int32Type.Default, false))
            .Field(new Field("long_col", Int64Type.Default, false))
            .Field(new Field("double_col", DoubleType.Default, false))
            .Field(new Field("string_col", StringType.Default, false))
            .Field(new Field("bool_col", BooleanType.Default, false))
            .Build();

        _batch = new RecordBatch(_schema,
            [intBuilder.Build(), longBuilder.Build(), doubleBuilder.Build(),
             stringBuilder.Build(), boolBuilder.Build()],
            rowCount);
    }

    [Benchmark]
    public int EncodeRows_Soe()
    {
        using var encoder = new AvroWriterBuilder(_schema)
            .WithFingerprintStrategy(new FingerprintStrategy.Soe())
            .BuildEncoder();
        encoder.Encode(_batch);
        var rows = encoder.Flush();
        return rows.Count;
    }

    [Benchmark]
    public int EncodeRows_Confluent()
    {
        using var encoder = new AvroWriterBuilder(_schema)
            .WithFingerprintStrategy(new FingerprintStrategy.Confluent(12345))
            .BuildEncoder();
        encoder.Encode(_batch);
        var rows = encoder.Flush();
        return rows.Count;
    }
}
