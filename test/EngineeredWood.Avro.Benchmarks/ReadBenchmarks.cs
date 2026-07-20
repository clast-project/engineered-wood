// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;

namespace EngineeredWood.Avro.Benchmarks;

[MemoryDiagnoser]
public class ReadBenchmarks
{
    private byte[] _primitivesNullBytes = null!;
    private byte[] _primitivesSnappyBytes = null!;
    private byte[] _primitivesDeflateBytes = null!;

    [GlobalSetup]
    public void Setup()
    {
        var schema = PrimitiveSchema();
        var batch = MakePrimitiveBatch(schema, 10_000);

        _primitivesNullBytes = WriteToBytes(schema, batch, AvroCodec.Null);
        _primitivesSnappyBytes = WriteToBytes(schema, batch, AvroCodec.Snappy);
        _primitivesDeflateBytes = WriteToBytes(schema, batch, AvroCodec.Deflate);
    }

    [Benchmark(Baseline = true)]
    public long ReadPrimitives_Uncompressed()
    {
        using var ms = new MemoryStream(_primitivesNullBytes);
        var reader = new AvroReaderBuilder().Build(ms);
        long total = 0;
        foreach (var batch in reader) total += batch.Length;
        return total;
    }

    [Benchmark]
    public long ReadPrimitives_Snappy()
    {
        using var ms = new MemoryStream(_primitivesSnappyBytes);
        var reader = new AvroReaderBuilder().Build(ms);
        long total = 0;
        foreach (var batch in reader) total += batch.Length;
        return total;
    }

    [Benchmark]
    public long ReadPrimitives_Deflate()
    {
        using var ms = new MemoryStream(_primitivesDeflateBytes);
        var reader = new AvroReaderBuilder().Build(ms);
        long total = 0;
        foreach (var batch in reader) total += batch.Length;
        return total;
    }

    [Benchmark]
    public async Task<long> ReadPrimitives_Uncompressed_Async()
    {
        using var ms = new MemoryStream(_primitivesNullBytes);
        var reader = await new AvroReaderBuilder().BuildAsync(ms);
        long total = 0;
        await foreach (var batch in reader) total += batch.Length;
        return total;
    }

    [Benchmark]
    public async Task<long> ReadPrimitives_Snappy_Async()
    {
        using var ms = new MemoryStream(_primitivesSnappyBytes);
        var reader = await new AvroReaderBuilder().BuildAsync(ms);
        long total = 0;
        await foreach (var batch in reader) total += batch.Length;
        return total;
    }

    [Benchmark]
    public async Task<long> ReadPrimitives_Deflate_Async()
    {
        using var ms = new MemoryStream(_primitivesDeflateBytes);
        var reader = await new AvroReaderBuilder().BuildAsync(ms);
        long total = 0;
        await foreach (var batch in reader) total += batch.Length;
        return total;
    }

    [Benchmark]
    public long ReadPrimitives_WithProjection()
    {
        using var ms = new MemoryStream(_primitivesNullBytes);
        var reader = new AvroReaderBuilder()
            .WithProjection(0, 2)
            .Build(ms);
        long total = 0;
        foreach (var batch in reader) total += batch.Length;
        return total;
    }

    [Benchmark]
    public long ReadPrimitives_WithSkipFields()
    {
        using var ms = new MemoryStream(_primitivesNullBytes);
        var reader = new AvroReaderBuilder()
            .WithSkipFields("string_col", "double_col")
            .Build(ms);
        long total = 0;
        foreach (var batch in reader) total += batch.Length;
        return total;
    }

    private static Apache.Arrow.Schema PrimitiveSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("int_col", Int32Type.Default, false))
        .Field(new Field("long_col", Int64Type.Default, false))
        .Field(new Field("double_col", DoubleType.Default, false))
        .Field(new Field("string_col", StringType.Default, false))
        .Field(new Field("bool_col", BooleanType.Default, false))
        .Build();

    private static RecordBatch MakePrimitiveBatch(Apache.Arrow.Schema schema, int rowCount)
    {
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

        return new RecordBatch(schema,
            [intBuilder.Build(), longBuilder.Build(), doubleBuilder.Build(),
             stringBuilder.Build(), boolBuilder.Build()],
            rowCount);
    }

    private static byte[] WriteToBytes(Apache.Arrow.Schema schema, RecordBatch batch, AvroCodec codec)
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
}
