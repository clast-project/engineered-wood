// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// Reading a double out of text the way Java's <c>Double.parseDouble</c> does, which is not what
/// every target's own parser does.
/// </summary>
/// <remarks>
/// <para>
/// The mirror of <see cref="SparkFloatText"/>, and it has the same problem from the other
/// direction: the answer must not depend on which target framework loaded the library, and the
/// platform parse makes it depend on exactly that.
/// </para>
/// <para>
/// <b>.NET Framework REFUSES a value too large to represent</b> where .NET Core returns an
/// infinity and Java returns one too. So <c>CAST('1e400' AS DOUBLE)</c> was <c>Infinity</c> on
/// net10.0 and CAST_INVALID_INPUT on net472 — not a wrong value but a wrong ANSWER CLASS, and it
/// reached further than the double cast: the refusal goes through <c>IsNumeric</c>, so
/// <c>CAST('1e400' AS INT)</c> raised CAST_INVALID_INPUT on one framework and CAST_OVERFLOW on the
/// other, and a string compared against a number took the same branch. #326.
/// </para>
/// <para>
/// The underflow side needs nothing: <c>'1e-400'</c> is zero on both, measured.
/// </para>
/// </remarks>
internal static class SparkDoubleText
{
    /// <summary>
    /// Reads a double, saturating to an infinity where the magnitude is out of range.
    /// </summary>
    /// <remarks>
    /// The overflow branch costs nothing on the path that already works: it is reached only when
    /// the platform parse has already refused the text, which on .NET Core it never does for a
    /// number, and on .NET Framework only for one too large.
    /// </remarks>
    internal static bool TryParse(string text, out double value)
    {
#if NETSTANDARD2_0
        if (double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
            return true;
#else
        if (double.TryParse(text.AsSpan(), NumberStyles.Float, CultureInfo.InvariantCulture, out value))
            return true;
#endif

        return TryOverflow(text, out value);
    }

#if !NETSTANDARD2_0
    /// <inheritdoc cref="TryParse(string, out double)"/>
    internal static bool TryParse(ReadOnlySpan<char> text, out double value)
    {
        if (double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
            return true;

        return TryOverflow(text.ToString(), out value);
    }
#endif

    /// <summary>
    /// Whether the text is a well-formed number whose magnitude exceeds a double.
    /// </summary>
    /// <remarks>
    /// Asked of the PARSER rather than of the digits, because "too large" and "not a number" are
    /// the two ways a parse can fail and only the parser can tell them apart without re-reading
    /// the grammar. .NET Framework raises <see cref="OverflowException"/> for the first and
    /// <see cref="FormatException"/> for the second; .NET Core raises neither, which is why it
    /// never reaches here.
    /// <para>
    /// The sign comes from the text, because there is no value to take it from.
    /// </para>
    /// </remarks>
    private static bool TryOverflow(string text, out double value)
    {
        try
        {
            value = double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
            return true;
        }
        catch (OverflowException)
        {
            value = text.TrimStart().StartsWith("-", StringComparison.Ordinal)
                ? double.NegativeInfinity
                : double.PositiveInfinity;

            return true;
        }
        catch (FormatException)
        {
            value = 0d;
            return false;
        }
    }
}
