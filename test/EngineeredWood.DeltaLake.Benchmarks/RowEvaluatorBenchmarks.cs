// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;
using static EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Benchmarks;

/// <summary>
/// The cost of <see cref="ArrowRowEvaluator.EvaluatePredicate"/> over one batch: the row filter of a
/// scan and the mask of every predicate DELETE / UPDATE (#560).
///
/// <para>The batch matches the delta-dotnet comparison table: an id, a date, one of 16 categories,
/// and an amount. "Plain" is the evaluator with no registry (what <c>DeltaTable.MaskFor</c> used);
/// "Spark" is the one configured with <see cref="SparkFunctionRegistry"/> over the parsed SQL text
/// (what the constraint enforcer uses).</para>
/// </summary>
[MemoryDiagnoser]
public class RowEvaluatorBenchmarks
{
    [Params(1_000_000)]
    public int Rows { get; set; }

    /// <summary>Whether the direct paths (#560, #799) may run; false measures the general path they replace.</summary>
    [Params(true, false)]
    public bool Direct { get; set; }

    private RecordBatch _batch = null!;
    private readonly ArrowRowEvaluator _plain = new();
    private readonly ArrowRowEvaluator _spark = new(new SparkFunctionRegistry());

    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private Predicate _categoryTyped = null!;
    private Predicate _categorySql = null!;
    private Predicate _dateRangeTyped = null!;
    private Predicate _dateRangeSql = null!;
    private Predicate _idTyped = null!;
    private Predicate _amountSql = null!;
    private Predicate _categoryInSql = null!;
    private Predicate _categoryOrSql = null!;
    private Predicate _idInTyped = null!;

    // A typed calendar date where the target has one; .NET Framework has no DateOnly, and a UTC-midnight
    // instant compares the same against a date column.
    private static LiteralValue DateLiteral(int year, int month, int day) =>
#if NET6_0_OR_GREATER
        LiteralValue.Of(new DateOnly(year, month, day));
#else
        LiteralValue.Of(new DateTimeOffset(year, month, day, 0, 0, 0, TimeSpan.Zero));
#endif

    [GlobalSetup]
    public void Setup()
    {
        var ids = new Int64Array.Builder();
        var dates = new Date32Array.Builder();
        var categories = new StringArray.Builder();
        var amounts = new DoubleArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            ids.Append(i);
            dates.Append(Start.AddDays(i % 365));
            categories.Append($"cat_{i % 16:D2}");
            amounts.Append(i * 0.25);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("date").DataType(Date32Type.Default).Nullable(false))
            .Field(f => f.Name("category").DataType(StringType.Default).Nullable(false))
            .Field(f => f.Name("amount").DataType(DoubleType.Default).Nullable(false))
            .Build();
        _batch = new RecordBatch(schema,
            [ids.Build(), dates.Build(), categories.Build(), amounts.Build()], Rows);

        _categoryTyped = Equal("category", LiteralValue.Of("cat_03"));
        _categorySql = SparkSqlParser.ParsePredicate("category = 'cat_03'");
        _dateRangeTyped = And(
            GreaterThanOrEqual("date", DateLiteral(2026, 10, 1)),
            LessThan("date", DateLiteral(2026, 10, 8)));
        _dateRangeSql = SparkSqlParser.ParsePredicate(
            "date >= DATE'2026-10-01' AND date < DATE'2026-10-08'");
        _idTyped = LessThan("id", LiteralValue.Of(500_000L));
        _amountSql = SparkSqlParser.ParsePredicate("amount > 1000.5");

        // #799: five strings, as an IN and as the ORed equalities it means.
        _categoryInSql = SparkSqlParser.ParsePredicate(
            "category IN ('cat_01', 'cat_04', 'cat_07', 'cat_10', 'cat_13')");
        _categoryOrSql = SparkSqlParser.ParsePredicate(
            "category = 'cat_01' OR category = 'cat_04' OR category = 'cat_07' OR category = 'cat_10' OR category = 'cat_13'");
        _idInTyped = In("id", LiteralValue.Of(5L), LiteralValue.Of(50_000L), LiteralValue.Of(500_000L),
            LiteralValue.Of(900_000L), LiteralValue.Of(999_999L));

        _plain.DirectColumnComparison = Direct;
        _spark.DirectColumnComparison = Direct;
    }

    [Benchmark(Description = "category = 'cat_03', plain")]
    public BooleanArray CategoryPlain() => _plain.EvaluatePredicate(_categoryTyped, _batch);

    [Benchmark(Description = "category = 'cat_03', Spark SQL")]
    public BooleanArray CategorySpark() => _spark.EvaluatePredicate(_categorySql, _batch);

    [Benchmark(Description = "date range, plain (typed date literals)")]
    public BooleanArray DateRangePlain() => _plain.EvaluatePredicate(_dateRangeTyped, _batch);

    [Benchmark(Description = "date range, Spark SQL (DATE'...')")]
    public BooleanArray DateRangeSpark() => _spark.EvaluatePredicate(_dateRangeSql, _batch);

    [Benchmark(Description = "id < 500000, plain")]
    public BooleanArray IdPlain() => _plain.EvaluatePredicate(_idTyped, _batch);

    [Benchmark(Description = "amount > 1000.5, Spark SQL")]
    public BooleanArray AmountSpark() => _spark.EvaluatePredicate(_amountSql, _batch);

    [Benchmark(Description = "category IN (5 strings), Spark SQL")]
    public BooleanArray CategoryInSpark() => _spark.EvaluatePredicate(_categoryInSql, _batch);

    [Benchmark(Description = "category = .. OR (5 strings), Spark SQL")]
    public BooleanArray CategoryOrSpark() => _spark.EvaluatePredicate(_categoryOrSql, _batch);

    [Benchmark(Description = "id IN (5 longs), plain")]
    public BooleanArray IdInPlain() => _plain.EvaluatePredicate(_idInTyped, _batch);
}
