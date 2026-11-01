// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// The direct column-against-constant comparison (#560) must answer exactly what the general path
/// answers, or decline. Every case here runs both ways -- the direct path on and off -- and the two
/// outcomes, a mask or an exception, must be identical.
/// </summary>
public class DirectColumnComparisonTests
{
    private static readonly Func<ArrowRowEvaluator>[] Evaluators =
    [
        () => new ArrowRowEvaluator(),
        () => new ArrowRowEvaluator(new SparkFunctionRegistry(new SparkDialectOptions { Ansi = true })),
        () => new ArrowRowEvaluator(new SparkFunctionRegistry(new SparkDialectOptions { Ansi = false })),
    ];

    private static readonly string[] EvaluatorNames = ["plain", "ansi", "legacy"];

    private static readonly DateTimeOffset Epoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static readonly ComparisonOperator[] Operators =
    [
        ComparisonOperator.Equal, ComparisonOperator.NotEqual,
        ComparisonOperator.LessThan, ComparisonOperator.LessThanOrEqual,
        ComparisonOperator.GreaterThan, ComparisonOperator.GreaterThanOrEqual,
        ComparisonOperator.NullSafeEqual,
        ComparisonOperator.StartsWith, ComparisonOperator.NotStartsWith,
    ];

    private static RecordBatch Batch()
    {
        var fields = new List<Field>();
        var columns = new List<IArrowArray>();
        void Add(string name, IArrowArray array)
        {
            fields.Add(new Field(name, array.Data.DataType, true));
            columns.Add(array);
        }

        Add("i8", new Int8Array.Builder().AppendRange([sbyte.MinValue, -1, 0, 1, 5, sbyte.MaxValue]).AppendNull().Build());
        Add("i16", new Int16Array.Builder().AppendRange([short.MinValue, -1, 0, 1, 5, short.MaxValue]).AppendNull().Build());
        Add("i32", new Int32Array.Builder().AppendRange([int.MinValue, -1, 0, 1, 5, int.MaxValue]).AppendNull().Build());
        Add("i64", new Int64Array.Builder().AppendRange([long.MinValue, -1, 0, 1, 9007199254740993L, long.MaxValue]).AppendNull().Build());
        Add("u8", new UInt8Array.Builder().AppendRange([0, 1, 5, 127, 200, byte.MaxValue]).AppendNull().Build());
        Add("u16", new UInt16Array.Builder().AppendRange([0, 1, 5, 127, 40000, ushort.MaxValue]).AppendNull().Build());
        Add("u32", new UInt32Array.Builder().AppendRange([0, 1, 5, int.MaxValue, 3_000_000_000u, uint.MaxValue]).AppendNull().Build());
        Add("u64", new UInt64Array.Builder().AppendRange([0, 1, 5, long.MaxValue, 10_000_000_000_000_000_000ul, ulong.MaxValue]).AppendNull().Build());
        Add("f32", new FloatArray.Builder().AppendRange([float.NaN, -0.0f, 0.0f, 1.5f, float.NegativeInfinity, 0.1f]).AppendNull().Build());
        Add("f64", new DoubleArray.Builder().AppendRange([double.NaN, -0.0, 1.5, double.PositiveInfinity, 9007199254740992.0, 0.1]).AppendNull().Build());
        Add("s", new StringArray.Builder().AppendRange(["", "ab", "abc", "é", "😀", "2026-10-01"]).AppendNull().Build());
        Add("b", new BooleanArray.Builder().AppendRange([true, false, true, false, true, false]).AppendNull().Build());
        Add("d32", Date32(-1, 0, 1, 20727, 20728, 2_932_896));
        Add("d64", new Date64Array.Builder()
            .Append(Epoch.AddDays(-1)).Append(Epoch).Append(Epoch.AddDays(1))
            .Append(new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero))
            .Append(new DateTimeOffset(2026, 10, 2, 0, 0, 0, TimeSpan.Zero))
            .Append(new DateTimeOffset(9999, 12, 31, 0, 0, 0, TimeSpan.Zero))
            .AppendNull().Build());
        foreach (var (unit, perSecond) in new[]
                 {
                     (TimeUnit.Second, 1L), (TimeUnit.Millisecond, 1_000L),
                     (TimeUnit.Microsecond, 1_000_000L), (TimeUnit.Nanosecond, 1_000_000_000L),
                 })
        {
            long midnight = 1_790_812_800L * perSecond;   // 2026-10-01T00:00:00Z
            Add($"ts_{unit}", Timestamp(new TimestampType(unit, "UTC"),
                -perSecond, -1, 0, 1, midnight - 1, midnight, midnight + 1));
        }

        Add("ts_ntz", Timestamp(new TimestampType(TimeUnit.Microsecond, (string?)null), -1, 0, 1, 1_790_812_800_000_000L));
        Add("dec", Decimal(new Decimal128Type(10, 2), -150, 0, 150, 100));

        int rows = columns.Max(c => c.Length);
        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i].Length < rows)
                columns[i] = Pad(columns[i], rows);
        }

        return new RecordBatch(new Apache.Arrow.Schema(fields, null), columns, rows);
    }

    private static IArrowArray Date32(params int[] days)
    {
        var b = new Date32Array.Builder();
        foreach (var d in days)
            b.Append(Epoch.AddDays(d));
        return b.AppendNull().Build();
    }

    private static IArrowArray Timestamp(TimestampType type, params long[] values)
    {
        var data = new byte[values.Length * 8 + 8];
        for (int i = 0; i < values.Length; i++)
            BitConverter.GetBytes(values[i]).CopyTo(data, i * 8);
        var validity = new byte[(values.Length + 8) / 8];
        for (int i = 0; i < values.Length; i++)
            validity[i >> 3] |= (byte)(1 << (i & 7));
        return new TimestampArray(type, new ArrowBuffer(data), new ArrowBuffer(validity), values.Length + 1, 1, 0);
    }

    private static IArrowArray Decimal(Decimal128Type type, params long[] unscaled)
    {
        var b = new Decimal128Array.Builder(type);
        foreach (var u in unscaled)
            b.Append((decimal)u / 100m);
        return b.AppendNull().Build();
    }

    /// <summary>Extends a column with nulls, by taking with out-of-range indices replaced by a null row.</summary>
    private static IArrowArray Pad(IArrowArray array, int rows)
    {
        // The last row of every column is null, so repeating it pads with nulls.
        var indices = new int[rows];
        for (int i = 0; i < rows; i++)
            indices[i] = Math.Min(i, array.Length - 1);
        return ArrowCompute.Take(array, indices);
    }

    private static IEnumerable<LiteralValue> Constants()
    {
        yield return LiteralValue.Of(0);
        yield return LiteralValue.Of(-1);
        yield return LiteralValue.Of(5);
        yield return LiteralValue.Of(int.MaxValue);
        yield return LiteralValue.Of(long.MinValue);
        yield return LiteralValue.Of(long.MaxValue);
        yield return LiteralValue.Of(9007199254740992L);
        yield return LiteralValue.Of(3_000_000_000u);
        yield return LiteralValue.Of(5ul);
        yield return LiteralValue.Of(ulong.MaxValue);
        yield return LiteralValue.Of(1.5f);
        yield return LiteralValue.Of(float.NaN);
        yield return LiteralValue.Of(double.NaN);
        yield return LiteralValue.Of(-0.0);
        yield return LiteralValue.Of(0.1);
        yield return LiteralValue.Of(double.PositiveInfinity);
        yield return LiteralValue.Of(1.5m);
        yield return LiteralValue.Of(-0.01m);
        yield return LiteralValue.Of(9007199254740993m);
#if NET6_0_OR_GREATER
        yield return LiteralValue.Of((Half)1.5);
        yield return LiteralValue.Of(new DateOnly(2026, 10, 1));
        yield return LiteralValue.Of(new DateOnly(1969, 12, 31));
#endif
        yield return LiteralValue.Of("");
        yield return LiteralValue.Of("ab");
        yield return LiteralValue.Of("abc");
        yield return LiteralValue.Of("é");
        yield return LiteralValue.Of("😀");
        yield return LiteralValue.Of("￿");
        yield return LiteralValue.Of("\ud800");
        yield return LiteralValue.Of("2026-10-01");
        yield return LiteralValue.Of("1");
        yield return LiteralValue.Of("true");
        yield return LiteralValue.Of(true);
        yield return LiteralValue.Of(false);
        yield return LiteralValue.Of(new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero));
        yield return LiteralValue.Of(new DateTimeOffset(2026, 10, 1, 2, 0, 0, TimeSpan.FromHours(2)));
        yield return LiteralValue.Of(Epoch.AddTicks(1));
        yield return LiteralValue.Of(Epoch.AddTicks(-1));
        yield return LiteralValue.Of(new byte[] { 0x61 });
    }

    private static readonly string[] SqlForms =
    [
        "{0} = DATE'2026-10-01'",
        "{0} >= DATE'2026-10-01'",
        "DATE'2026-10-01' > {0}",
        "{0} < TIMESTAMP'2026-10-01 00:00:00'",
        "{0} >= '2026-10-01'",
        "{0} = 'abc'",
        "'ab' < {0}",
        "{0} > 1",
        "{0} <= 1.5",
        "{0} = CAST('5' AS INT)",
        "{0} = CAST('x' AS INT)",
        "{0} <=> 0",
        "{0} = true",
        "{0} > -1e300",
        "{0} = X'61'",
        "{0} > 4E-38BD",
    ];

    private static object Outcome(Func<ArrowRowEvaluator> make, bool direct, Predicate predicate, RecordBatch batch, out int directCount)
    {
        var evaluator = make();
        evaluator.DirectColumnComparison = direct;
        try
        {
            var mask = evaluator.EvaluatePredicate(predicate, batch);
            directCount = evaluator.DirectComparisons;
            return string.Join(",", Enumerable.Range(0, mask.Length)
                .Select(i => mask.GetValue(i) is { } v ? (v ? "T" : "F") : "n"));
        }
        catch (Exception e)
        {
            directCount = evaluator.DirectComparisons;
            return $"{e.GetType().Name}: {e.Message}";
        }
    }

    private static IEnumerable<(string Name, Predicate Predicate)> Cases(RecordBatch batch)
    {
        foreach (var field in batch.Schema.FieldsList)
        {
            var column = Expressions.Ref(field.Name);
            foreach (var constant in Constants())
            {
                var literal = Expressions.Lit(constant);
                foreach (var op in Operators)
                {
                    yield return ($"{field.Name} {op} {constant}", new ComparisonPredicate(column, op, literal));
                    yield return ($"{constant} {op} {field.Name}", new ComparisonPredicate(literal, op, column));
                }
            }

            foreach (var form in SqlForms)
            {
                var sql = string.Format(form, field.Name);
                yield return (sql, SparkSqlParser.ParsePredicate(sql));
            }
        }
    }

    [Fact]
    public void DirectPathAnswersAsTheGeneralPathDoes()
    {
        var batch = Batch();
        var mismatches = new List<string>();
        int cases = 0, direct = 0;

        foreach (var (name, predicate) in Cases(batch))
        {
            for (int e = 0; e < Evaluators.Length; e++)
            {
                cases++;
                var expected = Outcome(Evaluators[e], false, predicate, batch, out _);
                var actual = Outcome(Evaluators[e], true, predicate, batch, out int used);
                direct += used;
                if (!Equals(expected, actual))
                    mismatches.Add($"[{EvaluatorNames[e]}] {name}\n  general: {expected}\n  direct:  {actual}");
            }
        }

        Assert.True(mismatches.Count == 0,
            $"{mismatches.Count} of {cases} differ:\n" + string.Join("\n", mismatches.Take(25)));

        // Not vacuous: most of the cases are ones the direct path takes.
        Assert.True(direct > cases / 6, $"direct path answered {direct} of {cases}");
    }

    [Fact]
    public void SlicedColumnsAnswerAsTheGeneralPathDoes()
    {
        var full = Batch();
        var columns = full.Arrays.Select(a => ArrowArrayFactory.Slice(a, 2, full.Length - 3)).ToList();
        var batch = new RecordBatch(full.Schema, columns, full.Length - 3);
        var mismatches = new List<string>();
        int direct = 0;

        foreach (var (name, predicate) in Cases(batch))
        {
            for (int e = 0; e < Evaluators.Length; e++)
            {
                var expected = Outcome(Evaluators[e], false, predicate, batch, out _);
                var actual = Outcome(Evaluators[e], true, predicate, batch, out int used);
                direct += used;
                if (!Equals(expected, actual))
                    mismatches.Add($"[{EvaluatorNames[e]}] {name}\n  general: {expected}\n  direct:  {actual}");
            }
        }

        Assert.True(mismatches.Count == 0, string.Join("\n", mismatches.Take(25)));
        Assert.True(direct > 0);
    }

    [Theory]
    [InlineData("i32 > 0 AND i32 < 5")]
    [InlineData("i32 > 0 AND s = 'abc'")]
    [InlineData("i32 > 0 OR s = 'abc'")]
    [InlineData("d32 >= DATE'2026-10-01' AND d32 < DATE'2026-10-02'")]
    [InlineData("i32 <> 0 AND 1 / i32 > 0")]
    [InlineData("i32 = 0 OR 1 / i32 > 0")]
    [InlineData("s = 'abc' AND CAST(s AS INT) > 0")]
    [InlineData("s IS NULL OR s = CAST('x' AS INT)")]
    [InlineData("f64 > 0 AND f64 < 'abc'")]
    [InlineData("NOT (i64 < 0) AND u8 <= 127 AND f32 = 1.5")]
    public void ShortCircuitedOperandsAnswerAsTheGeneralPathDoes(string sql)
    {
        var batch = Batch();
        var predicate = SparkSqlParser.ParsePredicate(sql);
        for (int e = 0; e < Evaluators.Length; e++)
        {
            var expected = Outcome(Evaluators[e], false, predicate, batch, out _);
            var actual = Outcome(Evaluators[e], true, predicate, batch, out _);
            Assert.True(Equals(expected, actual), $"[{EvaluatorNames[e]}]\n  general: {expected}\n  direct:  {actual}");
        }
    }

    [Fact]
    public void ALaterAndOperandIsComparedDirectlyOverTheWholeBatch()
    {
        var evaluator = new ArrowRowEvaluator(new SparkFunctionRegistry());
        var mask = evaluator.EvaluatePredicate(
            SparkSqlParser.ParsePredicate("d32 >= DATE'2026-10-01' AND d32 < DATE'2026-10-02'"), Batch());

        Assert.Equal(2, evaluator.DirectComparisons);
        Assert.Equal([false, false, false, true, false, false, null], Enumerable.Range(0, 7).Select(i => mask.GetValue(i)));
    }

    [Fact]
    public void ABatchWithNoNullsHasNoValidityBitmap()
    {
        var batch = new RecordBatch(
            new Apache.Arrow.Schema([new Field("x", Int32Type.Default, false)], null),
            [new Int32Array.Builder().AppendRange([1, 2, 3]).Build()], 3);

        var mask = new ArrowRowEvaluator().EvaluatePredicate(Expressions.GreaterThan("x", LiteralValue.Of(1)), batch);

        Assert.Equal(0, mask.NullCount);
        Assert.Equal([false, true, true], Enumerable.Range(0, 3).Select(i => mask.GetValue(i)!.Value));
    }
}
