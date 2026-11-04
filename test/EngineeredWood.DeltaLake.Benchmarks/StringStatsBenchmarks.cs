// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table.Stats;

namespace EngineeredWood.DeltaLake.Benchmarks;

/// <summary>
/// Per-file Delta stats over string columns (#661). Before the fix every cell was decoded to a .NET string and
/// wrapped in two <c>LiteralValue</c>s per comparison. Now the scan is over UTF-8 bytes and only the min and max
/// are decoded. Run on both commits to A/B: the method itself is unchanged.
/// </summary>
[MemoryDiagnoser]
public class StringStatsBenchmarks
{
    [Params(65_536)]
    public int Length { get; set; }

    /// <summary>ASCII ids, or text mixing two-byte, three-byte and supplementary characters.</summary>
    [Params("ascii", "mixed")]
    public string Text { get; set; } = "ascii";

    private RecordBatch _batch = null!;

    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(661);
        string[] pieces = Text == "ascii"
            ? ["a", "b", "c", "x", "0", "7", "-"]
            : ["a", "é", "中", "\uFFFD", "\U0001F600", " "];
        var builder = new StringArray.Builder();
        var sb = new System.Text.StringBuilder();
        for (int i = 0; i < Length; i++)
        {
            sb.Clear();
            int n = random.Next(8, 24);
            for (int k = 0; k < n; k++) sb.Append(pieces[random.Next(pieces.Length)]);
            builder.Append(sb.ToString());
        }

        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("s", StringType.Default, false)).Build();
        _batch = new RecordBatch(schema, [builder.Build()], Length);
    }

    [Benchmark]
    public string? Collect() => StatsCollector.Collect(_batch);
}
