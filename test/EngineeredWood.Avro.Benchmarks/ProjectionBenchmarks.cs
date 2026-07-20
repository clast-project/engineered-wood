// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;

namespace EngineeredWood.Avro.Benchmarks;

/// <summary>
/// Demonstrates the performance benefit of column projection when reading
/// a wide Avro file. The schema has 20 columns (mix of ints, strings, doubles,
/// booleans, and timestamps). Benchmarks compare reading all columns vs
/// projecting 2 columns by index, by name, and via skip-fields.
/// </summary>
[MemoryDiagnoser]
public class ProjectionBenchmarks
{
    private byte[] _wideBytes = null!;

    [Params(10_000, 100_000)]
    public int RowCount;

    [GlobalSetup]
    public void Setup()
    {
        var schema = WideSchema();
        var batch = MakeWideBatch(schema, RowCount);
        _wideBytes = WriteToBytes(schema, batch, AvroCodec.Null);
    }

    [Benchmark(Baseline = true)]
    public long ReadAllColumns()
    {
        using var ms = new MemoryStream(_wideBytes);
        var reader = new AvroReaderBuilder()
            .WithBatchSize(RowCount)
            .Build(ms);
        long total = 0;
        foreach (var batch in reader) total += batch.Length;
        return total;
    }

    [Benchmark]
    public long ProjectTwoByIndex()
    {
        using var ms = new MemoryStream(_wideBytes);
        var reader = new AvroReaderBuilder()
            .WithBatchSize(RowCount)
            .WithProjection(0, 10)
            .Build(ms);
        long total = 0;
        foreach (var batch in reader) total += batch.Length;
        return total;
    }

    [Benchmark]
    public long ProjectTwoByName()
    {
        using var ms = new MemoryStream(_wideBytes);
        var reader = new AvroReaderBuilder()
            .WithBatchSize(RowCount)
            .WithProjection("int_0", "str_2")
            .Build(ms);
        long total = 0;
        foreach (var batch in reader) total += batch.Length;
        return total;
    }

    [Benchmark]
    public long SkipEighteenFields()
    {
        using var ms = new MemoryStream(_wideBytes);
        var reader = new AvroReaderBuilder()
            .WithBatchSize(RowCount)
            .WithSkipFields(
                "int_1", "str_0", "dbl_0", "bool_0", "ts_0",
                "int_2", "str_1", "dbl_1", "bool_1", "ts_1",
                "int_3", "str_3", "dbl_2", "bool_2", "ts_2",
                "int_4", "str_4", "dbl_3", "bool_3")
            .Build(ms);
        long total = 0;
        foreach (var batch in reader) total += batch.Length;
        return total;
    }

    [Benchmark]
    public long ProjectFiveByName()
    {
        using var ms = new MemoryStream(_wideBytes);
        var reader = new AvroReaderBuilder()
            .WithBatchSize(RowCount)
            .WithProjection("int_0", "str_2", "dbl_1", "bool_0", "ts_2")
            .Build(ms);
        long total = 0;
        foreach (var batch in reader) total += batch.Length;
        return total;
    }

    /// <summary>
    /// 20-column schema: 5 ints, 5 strings, 4 doubles, 3 booleans, 3 timestamps.
    /// Strings are the most expensive to skip (variable-length).
    /// </summary>
    private static Apache.Arrow.Schema WideSchema()
    {
        var builder = new Apache.Arrow.Schema.Builder();
        for (int i = 0; i < 5; i++)
            builder.Field(new Field($"int_{i}", Int32Type.Default, false));
        for (int i = 0; i < 5; i++)
            builder.Field(new Field($"str_{i}", StringType.Default, false));
        for (int i = 0; i < 4; i++)
            builder.Field(new Field($"dbl_{i}", DoubleType.Default, false));
        for (int i = 0; i < 3; i++)
            builder.Field(new Field($"bool_{i}", BooleanType.Default, false));
        for (int i = 0; i < 3; i++)
            builder.Field(new Field($"ts_{i}", new TimestampType(TimeUnit.Microsecond, "UTC"), false));
        return builder.Build();
    }

    private static RecordBatch MakeWideBatch(Apache.Arrow.Schema schema, int rowCount)
    {
        var rng = new Random(42);

        var intBuilders = Enumerable.Range(0, 5).Select(_ => new Int32Array.Builder()).ToArray();
        var strBuilders = Enumerable.Range(0, 5).Select(_ => new StringArray.Builder()).ToArray();
        var dblBuilders = Enumerable.Range(0, 4).Select(_ => new DoubleArray.Builder()).ToArray();
        var boolBuilders = Enumerable.Range(0, 3).Select(_ => new BooleanArray.Builder()).ToArray();
        var tsBuilders = Enumerable.Range(0, 3).Select(_ => new TimestampArray.Builder(
            new TimestampType(TimeUnit.Microsecond, "UTC"))).ToArray();

        var baseTimestamp = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

        for (int i = 0; i < rowCount; i++)
        {
            foreach (var b in intBuilders) b.Append(rng.Next());
            foreach (var b in strBuilders) b.Append($"val_{rng.Next(0, 100_000):D6}");
            foreach (var b in dblBuilders) b.Append(rng.NextDouble() * 10_000);
            foreach (var b in boolBuilders) b.Append(rng.Next(2) == 0);
            foreach (var b in tsBuilders) b.Append(baseTimestamp.AddSeconds(rng.Next(0, 31_536_000)));
        }

        var arrays = new IArrowArray[20];
        for (int i = 0; i < 5; i++) arrays[i] = intBuilders[i].Build();
        for (int i = 0; i < 5; i++) arrays[5 + i] = strBuilders[i].Build();
        for (int i = 0; i < 4; i++) arrays[10 + i] = dblBuilders[i].Build();
        for (int i = 0; i < 3; i++) arrays[14 + i] = boolBuilders[i].Build();
        for (int i = 0; i < 3; i++) arrays[17 + i] = tsBuilders[i].Build();

        return new RecordBatch(schema, arrays, rowCount);
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
