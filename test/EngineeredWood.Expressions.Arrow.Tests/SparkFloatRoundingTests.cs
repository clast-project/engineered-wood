// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Reaching a FLOAT from text or a decimal in one rounding step, as Spark does. #372.
/// </summary>
/// <remarks>
/// <para>
/// Spark's <c>Float.parseFloat</c> and <c>BigDecimal.floatValue</c> are correctly rounded --
/// measured on 4.0.3 over 8,400 strings and 2,253 decimals beside a float's rounding ties, with
/// no exceptions. Reading a double and narrowing it rounds twice, and got a third of those wrong.
/// </para>
/// <para>
/// <b>The oracle is built, not borrowed.</b> Every value here sits on, or a hair either side of,
/// the exact midpoint between two adjacent floats, written out in integer arithmetic, so its
/// correctly rounded answer is known from how it was made: the tie goes to the even float, a hair
/// above goes up, a hair below goes down. No platform parser is consulted, which matters because
/// .NET Framework's float parse is one of the things under test, and so is its JSON reader of the
/// harvested answers -- it misreads some of them by an ulp.
/// </para>
/// </remarks>
public class SparkFloatRoundingTests
{
    [Fact]
    public void TextBesideATieRoundsOnce()
    {
        var wrong = new List<string>();

        foreach (var (unscaled, scale, expected) in TieAdjacentValues(new Random(372372), 20000))
        {
            var text = Text(unscaled, scale);

            if (!SparkDoubleText.TryParseExactSingle(text, out var exact) || Bits(exact) != expected)
                wrong.Add($"exact {text}: {Hex(exact)} against {Hex(expected)}");

            if (!SparkDoubleText.TryParseSingle(text, out var platform) || Bits(platform) != expected)
                wrong.Add($"platform {text}: {Hex(platform)} against {Hex(expected)}");

            if (wrong.Count > 10)
                break;
        }

        Assert.Empty(wrong);
    }

    [Fact]
    public void ADecimalBesideATieRoundsOnce()
    {
        var wrong = new List<string>();

        foreach (var (unscaled, scale, expected) in TieAdjacentValues(new Random(372373), 20000))
        {
            var converted = ScaledDecimal.ToSingle(unscaled, scale);
            if (Bits(converted) != expected)
                wrong.Add($"{Text(unscaled, scale)}: {Hex(converted)} against {Hex(expected)}");

            var negated = ScaledDecimal.ToSingle(-unscaled, scale);
            if (Bits(negated) != (expected | unchecked((int)0x80000000)))
                wrong.Add($"-{Text(unscaled, scale)}: {Hex(negated)}");

            if (wrong.Count > 10)
                break;
        }

        Assert.Empty(wrong);
    }

    /// <summary>
    /// Java's fast path -- an unscaled value under 2^24 and a scale up to 10 -- against the exact
    /// route, since the two must agree wherever the fast one is taken.
    /// </summary>
    [Fact]
    public void TheFastPathAgreesWithTheExactOne()
    {
        var random = new Random(372374);
        for (var i = 0; i < 200000; i++)
        {
            var unscaled = random.Next(-(1 << 24) + 1, 1 << 24);
            var scale = random.Next(0, 11);
            var text = Text(unscaled, scale);

            Assert.True(SparkDoubleText.TryParseExactSingle(text, out var exact));
            if (unscaled == 0)
                continue;

            Assert.True(
                Bits(ScaledDecimal.ToSingle(unscaled, scale)) == Bits(exact),
                $"{text}: {Hex(ScaledDecimal.ToSingle(unscaled, scale))} against {Hex(exact)}");
        }
    }

    /// <summary>The shapes a random sweep does not reach.</summary>
    [Theory]
    [InlineData("0", 0x00000000)]
    [InlineData("-0", unchecked((int)0x80000000))]
    [InlineData("-0.0", unchecked((int)0x80000000))]
    [InlineData("1e-400", 0x00000000)]
    [InlineData("-1e-400", unchecked((int)0x80000000))]
    [InlineData("1.4E-45", 0x00000001)]                        // the smallest subnormal
    [InlineData("7.006492321624085354618647916449580656401309709382578858785341419448955413429303e-46", 0x00000000)] // half of it: ties to even
    [InlineData("7.006492321624085354618647916449580656401309709382578858785341419448955413429304e-46", 0x00000001)] // a hair more
    [InlineData("1.17549421E-38", 0x007FFFFF)]                 // the largest subnormal
    [InlineData("1.17549435E-38", 0x00800000)]                 // the smallest normal
    [InlineData("3.4028235E38", 0x7F7FFFFF)]                   // float.MaxValue
    [InlineData("3.40282356e38", 0x7F7FFFFF)]                  // below the midpoint to 2^128: still it
    [InlineData("3.40282357e38", 0x7F800000)]                  // past the midpoint to infinity
    // .NET Framework REFUSES every one of the next five, so these are the rows that hold the
    // netstandard2.0 overflow fallback to the exact answer. The midpoint between float.MaxValue and
    // 2^128 is exactly 340282356779733661637539395458142568448; one below it rounds DOWN, and a
    // double reading lands on the midpoint and then rounds to infinity.
    [InlineData("340282356779733661637539395458142568447", 0x7F7FFFFF)]
    [InlineData("-340282356779733661637539395458142568447", unchecked((int)0xFF7FFFFF))]
    [InlineData("3.4028235677973366e38", 0x7F7FFFFF)]
    [InlineData("340282356779733661637539395458142568448", 0x7F800000)] // the tie: to even, which is infinity
    [InlineData("1e39", 0x7F800000)]
    [InlineData("1e400", 0x7F800000)]
    [InlineData("-1e400", unchecked((int)0xFF800000))]
    [InlineData("16777217", 0x4B800000)]                       // 2^24 + 1: a tie, to even (down)
    [InlineData("16777219", 0x4B800002)]                       // 2^24 + 3: a tie, to even (up)
    [InlineData("1.00000005960464477539062500001", 0x3F800001)] // the issue's row
    [InlineData("1.000000059604644775390625", 0x3F800000)]      // the exact tie beside it
    public void TheExactParseHandlesTheEdges(string text, int expected)
    {
        Assert.True(SparkDoubleText.TryParseExactSingle(text, out var parsed), text);
        Assert.True(Bits(parsed) == expected, $"{text}: {Hex(parsed)} against {Hex(expected)}");

        Assert.True(SparkDoubleText.TryParseSingle(text, out var platform), text);
        Assert.True(Bits(platform) == expected, $"platform {text}: {Hex(platform)} against {Hex(expected)}");
    }

    /// <summary>
    /// Each route into a float, through the evaluator, on the value the issue named.
    /// </summary>
    /// <remarks>
    /// 1 + 2^-24 is 0x3F800001, and one hair above the midpoint below it must reach it. The
    /// double-then-float route answers 1.0, because the double nearest the text IS the midpoint.
    /// </remarks>
    [Theory]
    [InlineData("CAST('1.00000005960464477539062500001' AS FLOAT)")]
    [InlineData("CAST(' 1.00000005960464477539062500001 ' AS FLOAT)")]
    [InlineData("CAST('1.00000005960464477539062500001f' AS FLOAT)")]
    [InlineData("CAST('1.00000005960464477539062500001d' AS FLOAT)")]
    [InlineData("CAST(CAST('1.00000005960464477539062500001' AS DECIMAL(38,29)) AS FLOAT)")]
    [InlineData("CAST(1.00000005960464477539062500001BD AS FLOAT)")]
    [InlineData("1.00000005960464477539062500001F")]
    [InlineData("CAST(s AS FLOAT)")]
    [InlineData("CAST(d AS FLOAT)")]
    public void EveryRouteIntoAFloatRoundsOnce(string sql)
    {
        foreach (var ansi in new[] { true, false })
        {
            var registry = new SparkFunctionRegistry(new SparkDialectOptions { Ansi = ansi });
            var result = new ArrowRowEvaluator(registry)
                .EvaluateExpression(SparkSqlParser.ParseExpression(sql), Batch());

            var value = Assert.IsType<FloatArray>(result).GetValue(0)!.Value;
            Assert.True(Bits(value) == 0x3F800001, $"{sql} (ansi {ansi}): {Hex(value)}");
        }
    }

    /// <remarks>
    /// The decimal column is built by our own exact string cast, not from a <see cref="decimal"/>:
    /// the value has 30 significant digits and <see cref="decimal"/> keeps 28 or 29, so it would
    /// arrive rounded onto the very tie the row is about.
    /// </remarks>
    private static RecordBatch Batch()
    {
        var strings = new StringArray.Builder().Append("1.00000005960464477539062500001").Build();
        var textOnly = new RecordBatch(
            new Schema.Builder().Field(new Field("s", StringType.Default, true)).Build(),
            new IArrowArray[] { strings }, 1);
        var decimals = new ArrowRowEvaluator(new SparkFunctionRegistry())
            .EvaluateExpression(SparkSqlParser.ParseExpression("CAST(s AS DECIMAL(38,29))"), textOnly);

        var schema = new Schema.Builder()
            .Field(new Field("s", StringType.Default, true))
            .Field(new Field("d", decimals.Data.DataType, true))
            .Build();

        return new RecordBatch(schema, new IArrowArray[] { strings, decimals }, 1);
    }

    /// <summary>
    /// Values on and beside the midpoint of two adjacent floats, with the bits each rounds to.
    /// </summary>
    /// <remarks>
    /// A float is <c>m × 2^e</c>; the midpoint above it is <c>(2m + 1) × 2^(e-1)</c>, which is an
    /// integer when <c>e</c> is positive and <c>(2m + 1) × 5^k / 10^k</c> otherwise, with
    /// <c>k = 1 - e</c>. Appending a digit and moving it by one puts the value a hair either side.
    /// Normal floats and subnormals both, and never the largest, whose neighbour is infinity.
    /// </remarks>
    private static IEnumerable<(BigInteger Unscaled, int Scale, int Expected)> TieAdjacentValues(
        Random random, int count)
    {
        for (var i = 0; i < count; i++)
        {
            var bits = random.Next(4) == 0
                ? random.Next(1, 0x007FFFFF)                          // a subnormal
                : random.Next(0x00800000, 0x7F7FFFFF);                // a normal float

            var field = (bits >> 23) & 0xFF;
            var fraction = bits & 0x7FFFFF;
            var mantissa = field == 0 ? fraction : fraction | 0x800000;
            var exponent = field == 0 ? -149 : field - 150;

            var odd = (BigInteger)(2L * mantissa + 1);
            var half = exponent - 1;

            BigInteger unscaled;
            int scale;
            if (half >= 0)
            {
                unscaled = odd << half;
                scale = 0;
            }
            else
            {
                unscaled = odd * BigInteger.Pow(5, -half);
                scale = -half;
            }

            var lowerIsEven = (bits & 1) == 0;
            yield return (unscaled, scale, lowerIsEven ? bits : bits + 1);
            yield return (unscaled * 10 + 1, scale + 1, bits + 1);
            yield return (unscaled * 10 - 1, scale + 1, bits);
        }
    }

    private static string Text(BigInteger unscaled, int scale) =>
        unscaled.ToString(CultureInfo.InvariantCulture) + "E-" + scale.ToString(CultureInfo.InvariantCulture);

    private static int Bits(float value) => BitConverter.ToInt32(BitConverter.GetBytes(value), 0);

    private static string Hex(float value) => Hex(Bits(value));

    private static string Hex(int bits) => bits.ToString("X8", CultureInfo.InvariantCulture);
}
