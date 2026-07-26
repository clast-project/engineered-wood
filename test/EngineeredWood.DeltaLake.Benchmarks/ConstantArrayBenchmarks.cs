// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.Arrow;

namespace EngineeredWood.DeltaLake.Benchmarks;

/// <summary>
/// Materialising a partition column on read — the hottest loop on the partitioned read path, running once per
/// partition column per batch per file of every partitioned scan.
///
/// <para>The <c>Builder</c> methods replicate what <c>PartitionUtils.BuildConstantArray</c> did before the
/// buffer-based rewrite: append the same already-decoded value <c>length</c> times through a typed array
/// builder. The <c>Repeat</c> methods are the replacement. Both start from the decoded value, so this measures
/// only the construction, not the string parse that happens once either way.</para>
/// </summary>
[MemoryDiagnoser]
public class ConstantArrayBenchmarks
{
    /// <summary>Arrow batch sizes spanning the usual range for a scan.</summary>
    [Params(1_024, 65_536)]
    public int Length { get; set; }

    private const long LongValue = 1_700_000_000_000_123L;
    private const string StringValue = "us-east-1";

    private static readonly byte[] LongBytes = BitConverter.GetBytes(LongValue);
    private static readonly byte[] StringBytes = System.Text.Encoding.UTF8.GetBytes(StringValue);

    private static readonly TimestampType TsType = new(TimeUnit.Microsecond, "UTC");

    [Benchmark(Baseline = true, Description = "Int64 via typed builder")]
    public IArrowArray Int64Builder()
    {
        var builder = new Int64Array.Builder();
        for (int i = 0; i < Length; i++)
            builder.Append(LongValue);
        return builder.Build();
    }

    [Benchmark(Description = "Int64 via ArrowCompute.Repeat")]
    public IArrowArray Int64Repeat() =>
        ArrowCompute.Repeat(Int64Type.Default, LongBytes, Length);

    [Benchmark(Description = "String via typed builder")]
    public IArrowArray StringBuilder_()
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < Length; i++)
            builder.Append(StringValue);
        return builder.Build();
    }

    [Benchmark(Description = "String via ArrowCompute.Repeat")]
    public IArrowArray StringRepeat() =>
        ArrowCompute.Repeat(StringType.Default, StringBytes, Length);

    /// <summary>
    /// The timestamp pair is here because it is the one where the builder is not merely slower: it takes a
    /// <see cref="DateTimeOffset"/>, so the stored value has to be reconstituted from one, and the unit
    /// conversion happens per row.
    /// </summary>
    [Benchmark(Description = "Timestamp via typed builder")]
    public IArrowArray TimestampBuilder()
    {
        var dto = DateTimeOffset.FromUnixTimeMilliseconds(LongValue / 1_000)
            .AddTicks((LongValue % 1_000) * 10);
        var builder = new TimestampArray.Builder(TsType);
        for (int i = 0; i < Length; i++)
            builder.Append(dto);
        return builder.Build();
    }

    [Benchmark(Description = "Timestamp via ArrowCompute.Repeat")]
    public IArrowArray TimestampRepeat() =>
        ArrowCompute.Repeat(TsType, LongBytes, Length);

    /// <summary>The all-null shape, the inverse buffer layout, against its own former builder loop.</summary>
    [Benchmark(Description = "All-null Int64 via typed builder")]
    public IArrowArray NullBuilder()
    {
        var builder = new Int64Array.Builder();
        for (int i = 0; i < Length; i++)
            builder.AppendNull();
        return builder.Build();
    }

    [Benchmark(Description = "All-null Int64 via ArrowCompute.MakeNullArray")]
    public IArrowArray NullMake() =>
        ArrowCompute.MakeNullArray(Int64Type.Default, Length);
}
