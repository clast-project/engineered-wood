// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using EngineeredWood.Expressions.Arrow.Spark;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// The exact double parse, held to a fixed oracle rather than to the platform's parser.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="SparkDoubleText.TryParseExact"/> exists because .NET Framework's parser is not
/// correctly rounded — it reads about 1% of ordinary fifteen- and sixteen-digit numbers as the
/// double next door (#350). It is used only there, and compiled everywhere so that these tests can
/// exercise it on every target.
/// </para>
/// <para>
/// <b>Nothing here compares against <c>double.Parse</c>, and that is the point.</b> On net10.0 it
/// would be a sound oracle; on net472 it is the thing under test, and a sweep asserting the two
/// agree would fail there for the very reason this routine exists. So the expectations are BIT
/// PATTERNS, taken once from .NET Core's correctly-rounded parser — which agrees with Java's
/// <c>Double.parseDouble</c> — and they assert the same answer on every framework.
/// </para>
/// <para>
/// The sweep needs no table at all, because rendering and parsing are inverses: the shortest form
/// of a double reads back as that double, by definition, and <see cref="SparkFloatText"/> produces
/// the same shortest form on every target (#337, #338). Neither half borrows the platform's.
/// </para>
/// </remarks>
public class SparkDoubleTextTests
{
    /// <summary>
    /// Parsing inverts our own rendering, which is a platform-independent oracle for both.
    /// </summary>
    /// <remarks>
    /// <c>ToString</c> could not stand in for the renderer here: .NET Framework's formatter is a
    /// digit out past fifteen significant figures (#338), so the text itself would differ by
    /// target and the sweep would be comparing different questions on different frameworks.
    /// </remarks>
    [Fact]
    public void ParsingInvertsRendering()
    {
        var random = new Random(350350);
        var wrong = new List<string>();
        var swept = 0;

        for (var i = 0; i < 60000; i++)
        {
            var value = BitConverter.Int64BitsToDouble(((long)random.Next() << 32) | (uint)random.Next());
            if (double.IsNaN(value) || double.IsInfinity(value))
                continue;

            var text = SparkFloatText.Render(value);
            swept++;

            if (!SparkDoubleText.TryParseExact(text, out var parsed))
            {
                wrong.Add($"{text}: declined");
                continue;
            }

            if (!Same(parsed, value))
                wrong.Add($"{text}: {Bits(parsed)} against {Bits(value)}");
        }

        Assert.Empty(wrong);
        Assert.True(swept > 55000, $"only {swept} values were swept");
    }

    /// <summary>The same, over the subnormals, where the step stops shrinking with the value.</summary>
    [Fact]
    public void ParsingInvertsRenderingForSubnormals()
    {
        var random = new Random(324324);
        var wrong = new List<string>();

        for (var i = 0; i < 20000; i++)
        {
            var value = BitConverter.Int64BitsToDouble((long)(random.NextDouble() * 4503599627370495L) + 1);
            var text = SparkFloatText.Render(value);

            if (!SparkDoubleText.TryParseExact(text, out var parsed) || !Same(parsed, value))
                wrong.Add($"{text}: {Bits(value)}");
        }

        Assert.Empty(wrong);
    }

    /// <summary>
    /// Text that .NET Framework reads as the double next door, and the double it really is.
    /// </summary>
    /// <remarks>
    /// <b>Every row here is a MEASURED divergence</b>, not merely a long number: each was found by
    /// rendering random doubles on .NET Core and re-reading the text on net472, and keeping the
    /// ones whose bits came back different. The first table written for this test was chosen by
    /// text LENGTH instead and only one of its eighteen rows had any teeth.
    /// <para>
    /// The expectations are the bit patterns .NET Core's parser produces, spot-checked against
    /// <c>Double.parseDouble</c> on JDK 21 — so they are Spark's answers and not merely .NET
    /// Core's. Note <c>49.0793458194787</c>: this is not confined to exotic magnitudes.
    /// <b>Only a run on net472 can fail these.</b>
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("49.0793458194787", 4632104121170255391L)]
    [InlineData("1.20823154016232E-221", 1301953993310182709L)]
    [InlineData("1.55282762265901E-206", 1527829738860004276L)]
    [InlineData("2.17221101871326E+109", 6242692313457557362L)]
    [InlineData("2.99735601060275E-248", 903766979248427998L)]
    [InlineData("5.596579825486209E-132", 2643550992869693082L)]
    [InlineData("6.307966394977904E+273", 8703015099832087973L)]
    [InlineData("1.643713829786471E-147", 2410838142474331089L)]
    [InlineData("2.72048061272348E-269", 589055153271405612L)]
    [InlineData("1.989756826399447E-278", 452210342118158934L)]
    [InlineData("3.02013378000196E-84", 3357283912562899968L)]
    [InlineData("3.885414067627461E-282", 396895265501088413L)]
    [InlineData("2.05008657452729E-292", 243297563451762676L)]
    [InlineData("1.27943596179521E-163", 2170023195267299722L)]
    [InlineData("5.7205163987209E-202", 1596081144305928480L)]
    [InlineData("1.56378397455836E-185", 1842256644976185715L)]
    [InlineData("3.465494185217035E-271", 560538912182121944L)]
    public void TheExactParseReadsWhatNetFrameworkMisreads(string text, long expected) =>
        AssertParses(text, expected);

    /// <summary>
    /// The shapes the ratio has to get right, which random doubles do not reach.
    /// </summary>
    /// <remarks>
    /// Subnormals and the boundary into them, the exact ties round-half-to-even decides, both
    /// infinities, and exponents far enough out that the answer is settled before any power is
    /// built.
    /// </remarks>
    [Theory]
    [InlineData("0", 0L)]
    [InlineData("-0", -9223372036854775808L)]
    [InlineData("0.0", 0L)]
    [InlineData("1", 4607182418800017408L)]
    [InlineData("0.1", 4591870180066957722L)]
    [InlineData("4.9406564584124654E-324", 1L)]                 // the smallest subnormal
    [InlineData("2.4703282292062327E-324", 0L)]                 // half of it, which rounds to even
    [InlineData("2.4703282292062328E-324", 1L)]                 // a hair more, which rounds up
    [InlineData("2.2250738585072011E-308", 4503599627370495L)]  // the largest subnormal
    [InlineData("2.2250738585072014E-308", 4503599627370496L)]  // the smallest normal
    [InlineData("1.7976931348623157E308", 9218868437227405311L)] // double.MaxValue
    [InlineData("1.7976931348623159E308", 9218868437227405312L)] // past it, saturating
    [InlineData("9007199254740993", 4845873199050653696L)]      // 2^53 + 1, which cannot be held
    [InlineData("1e400", 9218868437227405312L)]
    [InlineData("-1e400", -4503599627370496L)]
    [InlineData("1e-400", 0L)]
    [InlineData("1e-4000000", 0L)]
    [InlineData("1e4000000", 9218868437227405312L)]
    [InlineData("0.000000000000000000000000000000001", 4113128866711215964L)]
    [InlineData("123456789012345678901234567890", 5042042089369253694L)]
    [InlineData(" 1.5 ", 4609434218613702656L)]
    [InlineData("+1.5", 4609434218613702656L)]
    [InlineData("+0.0", 0L)]
    [InlineData("1.", 4607182418800017408L)]
    [InlineData(".5", 4602678819172646912L)]
    public void TheExactParseHandlesTheEdges(string text, long expected) => AssertParses(text, expected);

    /// <summary>
    /// A shape outside its grammar is declined, and the caller keeps the platform's answer.
    /// </summary>
    /// <remarks>
    /// Deliberately narrow: every one of these has no rounding in it, so the platform reads it
    /// exactly on every framework and there is nothing to correct. Widening the grammar would put
    /// the ACCEPTANCE question on this routine too, which is a far larger surface than the defect.
    /// </remarks>
    [Theory]
    [InlineData("Infinity")]
    [InlineData("-Infinity")]
    [InlineData("NaN")]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("1,000")]
    [InlineData("1e")]
    [InlineData("1.2.3")]
    [InlineData("0x10")]
    [InlineData("1d")]
    public void AShapeOutsideItsGrammarIsDeclined(string text) =>
        Assert.False(SparkDoubleText.TryParseExact(text, out _));

    private static void AssertParses(string text, long expected)
    {
        Assert.True(SparkDoubleText.TryParseExact(text, out var parsed), text);

        Assert.True(
            BitConverter.DoubleToInt64Bits(parsed) == expected,
            $"{text}: {Bits(parsed)} against {expected.ToString("X16", CultureInfo.InvariantCulture)}");
    }

    /// <summary>Bit equality, so that a negative zero is not mistaken for a positive one.</summary>
    private static bool Same(double left, double right) =>
        BitConverter.DoubleToInt64Bits(left) == BitConverter.DoubleToInt64Bits(right);

    private static string Bits(double value) =>
        BitConverter.DoubleToInt64Bits(value).ToString("X16", CultureInfo.InvariantCulture);
}
