// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Typed <see cref="LiteralValue"/> kinds with no Spark type of their own are given the Spark type that
/// holds them losslessly (#589). Before, the Spark-configured evaluator could not type them at all and
/// threw <see cref="NotSupportedException"/> from its analysis check, while the registry-free evaluator
/// compared them fine -- so the same predicate worked for DELETE and failed in a constraint.
/// </summary>
public class TypedLiteralKindTests
{
    private static readonly DateTimeOffset Day = new(2026, 10, 3, 0, 0, 0, TimeSpan.Zero);

    private static RecordBatch Batch() => new(new Apache.Arrow.Schema.Builder()
            .Field(new Field("i32", Int32Type.Default, true))
            .Field(new Field("i64", Int64Type.Default, true))
            .Field(new Field("dec", new Decimal128Type(20, 0), true))
            .Field(new Field("g", DoubleType.Default, true))
            .Field(new Field("dt", Date32Type.Default, true))
            .Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true))
            .Build(),
        [
            new Int32Array.Builder().Append(5).Append(6).Build(),
            new Int64Array.Builder().Append(5).Append(long.MaxValue).Build(),
            new Decimal128Array.Builder(new Decimal128Type(20, 0)).Append(5m).Append(18446744073709551615m).Build(),
            new DoubleArray.Builder().Append(1.5).Append(2.5).Build(),
            new Date32Array.Builder().Append(Day).Append(Day.AddDays(1)).Build(),
            new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"))
                .Append(Day.AddHours(12)).Append(Day.AddDays(1)).Build(),
        ], 2);

    public static TheoryData<string, ComparisonOperator, LiteralValue, bool[]> Cases()
    {
        var data = new TheoryData<string, ComparisonOperator, LiteralValue, bool[]>
        {
            { "i32", ComparisonOperator.Equal, LiteralValue.Of(5u), [true, false] },
            { "i64", ComparisonOperator.Equal, LiteralValue.Of(5ul), [true, false] },
            // Past long.MaxValue a ulong is a decimal(20,0), as Spark types an integer literal that size.
            { "i64", ComparisonOperator.LessThan, LiteralValue.Of(ulong.MaxValue), [true, true] },
            { "dec", ComparisonOperator.Equal, LiteralValue.Of(ulong.MaxValue), [false, true] },
            { "dec", ComparisonOperator.Equal, LiteralValue.Of(5u), [true, false] },
        };
#if NET6_0_OR_GREATER
        data.Add("g", ComparisonOperator.Equal, LiteralValue.Of((Half)1.5), [true, false]);
        data.Add("dt", ComparisonOperator.Equal, LiteralValue.Of(new DateOnly(2026, 10, 3)), [true, false]);
        data.Add("dt", ComparisonOperator.GreaterThan, LiteralValue.Of(new DateOnly(2026, 10, 3)), [false, true]);
        // A DATE against a timestamp compares as midnight, in both dialects of the question.
        data.Add("ts", ComparisonOperator.GreaterThan, LiteralValue.Of(new DateOnly(2026, 10, 3)), [true, true]);
        data.Add("ts", ComparisonOperator.Equal, LiteralValue.Of(new DateOnly(2026, 10, 4)), [false, true]);
#endif
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void BothEvaluatorsAnswerTheSame(string column, ComparisonOperator op, LiteralValue literal, bool[] expected)
    {
        var predicate = new ComparisonPredicate(Expressions.Ref(column), op, Expressions.Lit(literal));
        foreach (var evaluator in new[]
                 {
                     new ArrowRowEvaluator(),
                     new ArrowRowEvaluator(new SparkFunctionRegistry(new SparkDialectOptions { Ansi = true })),
                     new ArrowRowEvaluator(new SparkFunctionRegistry(new SparkDialectOptions { Ansi = false })),
                 })
        {
            var mask = evaluator.EvaluatePredicate(predicate, Batch());
            Assert.Equal(expected, Enumerable.Range(0, mask.Length).Select(i => mask.GetValue(i)!.Value));
        }
    }

#if NET6_0_OR_GREATER
    [Fact]
    public void AHalfInAnInListRoundsTheColumnAsAFloatDoes()
    {
        // Legacy compares an int against a float as a float, and 16777217 is not a float: it rounds to
        // 16777216, which the list holds. A Half is typed as a float, so it must bring the same rounding.
        var batch = new RecordBatch(
            new Apache.Arrow.Schema([new Field("i", Int32Type.Default, false)], null),
            [new Int32Array.Builder().Append(16777217).Build()], 1);
        var predicate = Expressions.In(Expressions.Ref("i"),
            new[] { LiteralValue.Of((Half)1.5), LiteralValue.Of(16777216) });

        var legacy = new ArrowRowEvaluator(new SparkFunctionRegistry(new SparkDialectOptions { Ansi = false }));
        var withFloat = new ArrowRowEvaluator(new SparkFunctionRegistry(new SparkDialectOptions { Ansi = false }))
            .EvaluatePredicate(Expressions.In(Expressions.Ref("i"),
                new[] { LiteralValue.Of(1.5f), LiteralValue.Of(16777216) }), batch);

        Assert.True(withFloat.GetValue(0));
        Assert.True(legacy.EvaluatePredicate(predicate, batch).GetValue(0));
    }

    [Fact]
    public void ATimeOfDayStillHasNoSparkType()
    {
        // Delta and Spark have no TIME type, so there is nothing to compare one against.
        var predicate = Expressions.Equal("dt", LiteralValue.Of(new TimeOnly(1, 0)));

        Assert.Throws<NotSupportedException>(
            () => new ArrowRowEvaluator(new SparkFunctionRegistry()).EvaluatePredicate(predicate, Batch()));
    }
#endif
}
