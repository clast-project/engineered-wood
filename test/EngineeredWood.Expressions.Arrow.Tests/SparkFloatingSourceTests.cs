// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// A float or double source takes Spark's double semantics, on every runtime (#911).
/// </summary>
/// <remarks>
/// <para>
/// These paths used to reach their answers through <c>(decimal)double</c>, which .NET 11 changed:
/// through .NET 10 it rounds to 15 significant digits, and on .NET 11 it keeps the binary
/// expansion. Each row below was wrong on one runtime or the other, so the same assertions run
/// unchanged on every target and runtime.
/// </para>
/// <para>
/// Expected values are Spark's formulas evaluated in IEEE doubles, which Java and .NET share:
/// <c>d.toInt</c> and <c>d.toLong</c> truncate the double, <c>Cast.doubleToTimestamp</c> is
/// <c>(d * 1_000_000).toLong</c>, and a double compared with a decimal widens the decimal to
/// double.
/// </para>
/// </remarks>
public sealed class SparkFloatingSourceTests
{
    private static readonly SparkFunctionRegistry Ansi = new();
    private static readonly SparkFunctionRegistry Legacy = new(new SparkDialectOptions { Ansi = false });

    public static TheoryData<bool> Dialects => new() { true, false };

    private static IArrowArray Eval(string sql, bool ansi, double column)
    {
        var batch = new RecordBatch(
            new Schema.Builder().Field(new Field("d", DoubleType.Default, true)).Build(),
            [new DoubleArray.Builder().Append(column).Build()],
            1);

        return new ArrowRowEvaluator(ansi ? Ansi : Legacy)
            .EvaluateExpression(SparkSqlParser.ParseExpression(sql), batch);
    }

    /// <summary>Through .NET 10 the 15-digit rounding carried each of these up to the next integer.</summary>
    [Theory]
    [InlineData(2.9999999999999996, 2L)]
    [InlineData(-2.9999999999999996, -2L)]
    [InlineData(9.999999999999998, 9L)]
    [InlineData(0.9999999999999999, 0L)]
    [InlineData(-0.9999999999999999, 0L)]
    [InlineData(2147483647.9999998, 2147483647L)]
    public void AnIntegralCast_TruncatesTheDoubleItself(double value, long expected)
    {
        foreach (var ansi in new[] { true, false })
        {
            var fromColumn = Assert.IsType<Int64Array>(Eval("CAST(d AS BIGINT)", ansi, value));
            Assert.Equal(expected, fromColumn.GetValue(0));

            if (expected is >= int.MinValue and <= int.MaxValue)
            {
                var asInt = Assert.IsType<Int32Array>(Eval("CAST(d AS INT)", ansi, value));
                Assert.Equal((int)expected, asInt.GetValue(0));
            }
        }
    }

    [Theory]
    [MemberData(nameof(Dialects))]
    public void AnIntegralCast_FromALiteral_TruncatesTheDoubleItself(bool ansi)
    {
        var result = Assert.IsType<Int32Array>(Eval("CAST(2.9999999999999996D AS INT)", ansi, 0));
        Assert.Equal(2, result.GetValue(0));
    }

    /// <summary>
    /// The double just past 2^31 - 1 truncates to 2^31, which is out of range; the one just below
    /// it truncates into range. Through a 15-digit decimal both would have been 2147483648.
    /// </summary>
    [Fact]
    public void AnIntegralCast_JudgesTheRangeOnTheTruncatedDouble()
    {
        // The largest double below 2^31 (Math.BitDecrement, which net472 lacks).
        var below = BitConverter.Int64BitsToDouble(BitConverter.DoubleToInt64Bits(2147483648d) - 1);
        Assert.Equal(2147483647.9999998, below);
        Assert.Equal(int.MaxValue, Assert.IsType<Int32Array>(Eval("CAST(d AS INT)", true, below)).GetValue(0));

        Assert.Throws<SparkEvaluationException>(() => Eval("CAST(d AS INT)", true, 2147483648d));
        Assert.Null(Assert.IsType<Int32Array>(Eval("try_cast(d AS INT)", true, 2147483648d)).GetValue(0));
    }

    [Theory]
    [MemberData(nameof(Dialects))]
    public void AnIntegralCast_StillRefusesWhatALongCannotHold(bool ansi)
    {
        // 2^63 is long.MaxValue as a double, and one past what a long holds.
        if (ansi)
            Assert.Throws<SparkEvaluationException>(() => Eval("CAST(d AS BIGINT)", ansi, 9223372036854775808d));

        Assert.Equal(
            long.MinValue,
            Assert.IsType<Int64Array>(Eval("CAST(d AS BIGINT)", ansi, -9223372036854775808d)).GetValue(0));
    }

    /// <summary>
    /// Spark rounds the product to a double before truncating it: 0.3 * 1e6 is exactly 300000.0,
    /// though the binary 0.3 lies just below 0.3. On .NET 11 the exact expansion gave 299999.
    /// </summary>
    [Theory]
    [InlineData(0.3, 300_000L)]
    [InlineData(1.0000000000000002, 1_000_000L)]
    [InlineData(0.0000005, 0L)]
    [InlineData(-2.5, -2_500_000L)]
    [InlineData(1.1, 1_100_000L)]
    public void ATimestampCast_MultipliesInDoubleBeforeTruncating(double seconds, long micros)
    {
        // Spark's own formula, evaluated here in the same IEEE arithmetic, agrees with the table.
        Assert.Equal(micros, (long)(seconds * 1_000_000d));

        foreach (var ansi in new[] { true, false })
        {
            var result = Assert.IsType<TimestampArray>(Eval("CAST(d AS TIMESTAMP)", ansi, seconds));
            Assert.Equal(TimeUnit.Microsecond, ((TimestampType)result.Data.DataType).Unit);
            Assert.Equal(micros, result.GetValue(0));
        }
    }

    [Fact]
    public void ATimestampCast_StillRefusesSecondsPastTheLastInstant()
    {
        Assert.Throws<SparkEvaluationException>(() => Eval("CAST(d AS TIMESTAMP)", true, 253402300800d));
        Assert.Throws<SparkEvaluationException>(() => Eval("CAST(d AS TIMESTAMP)", true, double.NaN));
    }

    /// <summary>
    /// <c>nullif</c> compares a double with a decimal as doubles. Through .NET 10 the 15-digit
    /// rounding made 0.30000000000000004 equal to 0.3; on .NET 11 the exact expansion made 0.1
    /// unequal to 0.1, and <c>nullif('0.10', 0.1)</c> (a corpus row) came back '0.10'.
    /// </summary>
    [Theory]
    [MemberData(nameof(Dialects))]
    public void NullIf_ComparesADoubleWithADecimalAsDoubles(bool ansi)
    {
        Assert.Null(Assert.IsType<DoubleArray>(Eval("nullif(d, 0.1BD)", ansi, 0.1)).GetValue(0));
        Assert.Null(Assert.IsType<DoubleArray>(Eval("nullif(0.1D, 0.1BD)", ansi, 0)).GetValue(0));

        Assert.Equal(
            0.30000000000000004,
            Assert.IsType<DoubleArray>(Eval("nullif(d, 0.3BD)", ansi, 0.30000000000000004)).GetValue(0));

        // A float widens to double exactly, and 0.1f is not the double 0.1.
        Assert.Equal(
            0.1f,
            Assert.IsType<FloatArray>(Eval("nullif(CAST(0.1 AS FLOAT), 0.1BD)", ansi, 0)).GetValue(0));
    }

    [Fact]
    public void NullIf_OfANumericStringAndADecimal_IsTheCorpusAnswer()
    {
        var result = Assert.IsType<StringArray>(Eval("nullif('0.10', 0.1)", true, 0));
        Assert.True(result.IsNull(0));
    }

    [Theory]
    [MemberData(nameof(Dialects))]
    public void NullIf_KeepsSparksNaNRule(bool ansi)
    {
        Assert.Null(Assert.IsType<DoubleArray>(Eval("nullif(d, CAST('NaN' AS DOUBLE))", ansi, double.NaN)).GetValue(0));
        Assert.Null(Assert.IsType<DoubleArray>(Eval("nullif(d, -0.0D)", ansi, 0.0)).GetValue(0));
    }
}
