// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using static EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Benchmarks;

/// <summary>
/// The direct string comparison kernel over each layout a read can return a string column in (#859): 32-bit
/// offsets, 64-bit offsets and views. A view carries its length and first four bytes inline, so a kernel that reads
/// the views can settle most rows without touching the data buffers.
/// </summary>
/// <remarks>
/// <para>Values are one of 16, as in <see cref="RowEvaluatorBenchmarks"/>. "short" values fit inline in a view
/// (<c>cat_03</c>). The two long ones (24 bytes) do not: "long, distinct prefix" differs in its first four bytes
/// (<c>03_category_with_long_name</c>), "long, shared prefix" only after them (<c>category_with_long_name_03</c>), so
/// the view prefix can reject a row in the first and never in the second.</para>
/// </remarks>
[MemoryDiagnoser]
public class StringLayoutComparisonBenchmarks
{
    public enum StringLayout { Offsets, LargeOffsets, Views }

    [Params(1_000_000)]
    public int Rows { get; set; }

    [Params(StringLayout.Offsets, StringLayout.LargeOffsets, StringLayout.Views)]
    public StringLayout Layout { get; set; }

    [Params("short", "long, distinct prefix", "long, shared prefix")]
    public string Values { get; set; } = "short";

    private RecordBatch _batch = null!;
    private readonly ArrowRowEvaluator _evaluator = new();
    private Predicate _equal = null!;
    private Predicate _less = null!;
    private Predicate _startsWith = null!;
    private Predicate _in = null!;

    private string Value(int category) => Values switch
    {
        "short" => $"cat_{category:D2}",
        "long, distinct prefix" => $"{category:D2}_category_with_long_name",
        _ => $"category_with_long_name_{category:D2}",
    };

    [GlobalSetup]
    public void Setup()
    {
        IArrowArray column;
        switch (Layout)
        {
            case StringLayout.Offsets:
            {
                var b = new StringArray.Builder();
                for (int i = 0; i < Rows; i++) b.Append(Value(i % 16));
                column = b.Build();
                break;
            }
            case StringLayout.LargeOffsets:
            {
                var b = new LargeStringArray.Builder();
                for (int i = 0; i < Rows; i++) b.Append(Value(i % 16));
                column = b.Build();
                break;
            }
            default:
            {
                var b = new StringViewArray.Builder();
                for (int i = 0; i < Rows; i++) b.Append(Value(i % 16));
                column = b.Build();
                break;
            }
        }

        _batch = new RecordBatch(
            new Apache.Arrow.Schema([new Field("category", column.Data.DataType, false)], null), [column], Rows);

        _equal = Equal("category", LiteralValue.Of(Value(3)));
        _less = LessThan("category", LiteralValue.Of(Value(8)));
        string value = Value(3);
        _startsWith = new ComparisonPredicate(
            Ref("category"), ComparisonOperator.StartsWith, Lit(LiteralValue.Of(value.Substring(0, value.Length - 1))));
        _in = In("category", LiteralValue.Of(Value(1)), LiteralValue.Of(Value(4)), LiteralValue.Of(Value(7)),
            LiteralValue.Of(Value(10)), LiteralValue.Of(Value(13)));
    }

    [Benchmark(Description = "=")]
    public BooleanArray EqualTo() => _evaluator.EvaluatePredicate(_equal, _batch);

    [Benchmark(Description = "<")]
    public BooleanArray Less() => _evaluator.EvaluatePredicate(_less, _batch);

    [Benchmark(Description = "STARTSWITH")]
    public BooleanArray StartsWith() => _evaluator.EvaluatePredicate(_startsWith, _batch);

    [Benchmark(Description = "IN (5)")]
    public BooleanArray InFive() => _evaluator.EvaluatePredicate(_in, _batch);
}
