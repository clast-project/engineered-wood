// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <see cref="ColumnDefaultLiteral"/> against the Java parsers delta-spark's <c>DeltaColumnDefaults.isLiteral</c>
/// uses (javap on the 4.3.1 jar). Each case states what the Java call does with the text.
/// </summary>
public class ColumnDefaultLiteralTests
{
    private static bool IsLiteral(string text, string type) =>
        ColumnDefaultLiteral.IsLiteral(text, new PrimitiveType { TypeName = type });

    [Theory]
    // Integer.valueOf: sign and digits, within int, for byte and short as well (no narrower range check).
    [InlineData("5", "integer", true)]
    [InlineData("-5", "integer", true)]
    [InlineData("+5", "short", true)]
    [InlineData("300", "byte", true)]
    [InlineData("2147483648", "integer", false)]
    [InlineData("2 + 3", "integer", false)]
    [InlineData(" 5", "integer", false)]
    [InlineData("5L", "integer", false)]
    // Long.valueOf after one trailing l/L is removed.
    [InlineData("5L", "long", true)]
    [InlineData("5l", "long", true)]
    [InlineData("5LL", "long", false)]
    [InlineData("9223372036854775807", "long", true)]
    // Float/Double.valueOf: trimmed, suffixes, NaN, Infinity, exponents, hex.
    [InlineData("1.5", "double", true)]
    [InlineData(" 1.5f ", "float", true)]
    [InlineData("1e10", "double", true)]
    [InlineData(".5", "double", true)]
    [InlineData("5.", "double", true)]
    [InlineData("NaN", "double", true)]
    [InlineData("-Infinity", "float", true)]
    [InlineData("0x1.8p1", "double", true)]
    [InlineData("1.5.0", "double", false)]
    [InlineData("rand()", "double", false)]
    // Boolean.valueOf never fails.
    [InlineData("true", "boolean", true)]
    [InlineData("anything", "boolean", true)]
    // Quoted strings.
    [InlineData("'abc'", "string", true)]
    [InlineData("\"abc\"", "string", true)]
    [InlineData("''", "string", true)]
    [InlineData("'", "string", false)]
    [InlineData("abc", "string", false)]
    [InlineData("'abc\"", "string", false)]
    // Binary: x'..' must be hex; any other text passes.
    [InlineData("x'0aFF'", "binary", true)]
    [InlineData("X''", "binary", true)]
    [InlineData("x'0g'", "binary", false)]
    [InlineData("'whatever'", "binary", true)]
    // Dates: quoted ISO_LOCAL_DATE, a day that exists.
    [InlineData("'2024-02-29'", "date", true)]
    [InlineData("'2023-02-29'", "date", false)]
    [InlineData("2024-02-29", "date", false)]
    [InlineData("DATE'2024-02-29'", "date", false)]
    [InlineData("current_date()", "date", false)]
    // ISO_LOCAL_DATE's proleptic years, measured with jshell: year 0 and negative years, signs past four digits.
    [InlineData("'0000-01-01'", "date", true)]
    [InlineData("'0000-02-29'", "date", true)]
    [InlineData("'0001-02-29'", "date", false)]
    [InlineData("'-0001-01-01'", "date", true)]
    [InlineData("'-0004-02-29'", "date", true)]
    [InlineData("'+2024-01-01'", "date", false)]
    [InlineData("'+10000-01-01'", "date", true)]
    [InlineData("'10000-01-01'", "date", false)]
    [InlineData("'+999999999-12-31'", "date", true)]
    [InlineData("'+1000000000-01-01'", "date", false)]
    [InlineData("'-999999999-01-01'", "date", true)]
    [InlineData("'00001-01-01'", "date", false)]
    [InlineData("'1900-02-29'", "date", false)]
    [InlineData("'2000-02-29'", "date", true)]
    [InlineData("'0000-01-01T00:00:00Z'", "timestamp", true)]
    [InlineData("'-0001-01-01T00:00:00Z'", "timestamp", true)]
    [InlineData("'+10000-01-01T00:00:00Z'", "timestamp", true)]
    [InlineData("'0000-01-01T00:00:00'", "timestamp_ntz", true)]
    [InlineData("'+10000-01-01T00:00:00'", "timestamp_ntz", true)]
    // "yyyy-MM-dd HH:mm:ss" reads a year of the era: 0000 and a negative year are refused there.
    [InlineData("'0000-01-01 00:00:00'", "timestamp_ntz", false)]
    [InlineData("'-0001-01-01 00:00:00'", "timestamp_ntz", false)]
    // Timestamps: an ISO date-time with an offset, else as timestamp_ntz.
    [InlineData("'2024-01-02T03:04:05Z'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05.123+01:00'", "timestamp", true)]
    [InlineData("'2024-01-02 03:04:05'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04'", "timestamp", true)]
    [InlineData("'2024-01-02'", "timestamp", false)]
    [InlineData("current_timestamp()", "timestamp", false)]
    [InlineData("'2024-01-02 03:04:05'", "timestamp_ntz", true)]
    [InlineData("'2024-01-02T03:04:05.123456'", "timestamp_ntz", true)]
    [InlineData("'2024-01-02T03:04:05Z'", "timestamp_ntz", false)]
    [InlineData("'2024-01-02 03:04'", "timestamp_ntz", false)]
    // OffsetDateTime.parse with ISO_DATE_TIME, each case measured with jshell (JDK 17): the offset's form and range,
    // and the [zone] after it.
    [InlineData("'2024-01-02T03:04:05z'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05+01:00:30'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05+18:00'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05-18:00'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05+18:01'", "timestamp", false)]
    [InlineData("'2024-01-02T03:04:05+19:00'", "timestamp", false)]
    [InlineData("'2024-01-02T03:04:05+99:99'", "timestamp", false)]
    [InlineData("'2024-01-02T03:04:05+01:60'", "timestamp", false)]
    [InlineData("'2024-01-02T03:04:05+01'", "timestamp", false)]
    [InlineData("'2024-01-02T03:04:05+0100'", "timestamp", false)]
    [InlineData("'2024-01-02T03:04:05+010030'", "timestamp", false)]
    [InlineData("'2024-01-02T03:04:05+01:00[Europe/Paris]'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05Z[UTC]'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05Z[GMT]'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05Z[Z]'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05+01:00[UT]'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05+01:00[+01:00]'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05+01:00[UTC+01:00]'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05+01:00[GMT+1]'", "timestamp", false)]
    [InlineData("'2024-01-02T03:04:05+01:00[UTC+19:00]'", "timestamp", false)]
    [InlineData("'2024-01-02T03:04:05+01:00[America/New_York]'", "timestamp", true)]
    [InlineData("'2024-01-02T03:04:05Z[z]'", "timestamp", false)]
    [InlineData("'2024-01-02T03:04:05+01:00[]'", "timestamp", false)]
    [InlineData("'2024-01-02T03:04:05+01:00[1abc]'", "timestamp", false)]
    // new BigDecimal(text, MathContext(p)).setScale(s): rounds to p digits, then refuses a lossy scale change.
    [InlineData("1.2", "decimal(10,2)", true)]
    [InlineData("1.20", "decimal(10,2)", true)]
    [InlineData("1.234", "decimal(10,2)", false)]
    [InlineData("1.2300", "decimal(10,2)", true)]
    [InlineData("-3", "decimal(5,0)", true)]
    [InlineData("1e2", "decimal(5,0)", true)]
    [InlineData("1.5e-1", "decimal(5,2)", true)]
    [InlineData("1.234", "decimal(3,2)", true)]
    // HALF_UP rounds a half away from zero, for a negative value too (measured: -1.205 rounds to -1.21). The
    // magnitude is rounded, so these pass with the sign ignored.
    [InlineData("-1.205", "decimal(3,1)", false)]
    [InlineData("-1.205", "decimal(3,2)", true)]
    [InlineData("-1.215", "decimal(3,1)", false)]
    [InlineData("-1.204", "decimal(3,1)", true)]
    [InlineData("1.205", "decimal(3,1)", false)]
    [InlineData("-9.995", "decimal(3,1)", true)]
    [InlineData("-0.0005", "decimal(3,3)", false)]
    [InlineData("12345678901", "decimal(10,2)", true)]
    [InlineData("0.001", "decimal(10,2)", false)]
    [InlineData("1.", "decimal(10,2)", true)]
    [InlineData(".", "decimal(10,2)", false)]
    [InlineData("1 ", "decimal(10,2)", false)]
    // Exponents at the int limits, measured with jshell. 1e-1000000000 is refused without building 10^999999998.
    [InlineData("1e-1000000000", "decimal(10,2)", false)]
    [InlineData("1e2147483647", "decimal(10,2)", false)]
    [InlineData("1e2147483648", "decimal(10,2)", false)]
    [InlineData("1e-2147483648", "decimal(10,2)", false)]
    [InlineData("1e-2147483649", "decimal(10,2)", false)]
    [InlineData("1e100000000", "decimal(10,2)", true)]
    [InlineData("100e-2", "decimal(10,0)", true)]
    [InlineData("0e-1000000000", "decimal(10,2)", true)]
    [InlineData("0e2147483647", "decimal(10,2)", true)]
    // Other types never have a literal default.
    [InlineData("'x'", "variant", false)]
    public void IsLiteral_MatchesTheJavaParse(string text, string type, bool expected)
    {
        Assert.Equal(expected, IsLiteral(text, type));
    }

    /// <summary>
    /// The one known difference from Java: a region id is checked for its syntax, not looked up in a tz database, so
    /// a well-formed id that names no zone passes here though <c>OffsetDateTime.parse</c> refuses it (both measured
    /// with jshell). .NET's zone lookup depends on the platform and on globalization mode.
    /// </summary>
    [Theory]
    [InlineData("'2024-01-02T03:04:05+01:00[Foo/Bar]'")]
    [InlineData("'2024-01-02T03:04:05+01:00[EST]'")]
    [InlineData("'2024-01-02T03:04:05+01:00[europe/paris]'")]
    public void AWellFormedRegionIdNamingNoZone_PassesThoughJavaRefusesIt(string text)
    {
        Assert.True(IsLiteral(text, "timestamp"));
    }

    [Fact]
    public void ComplexTypes_HaveNoLiteralDefault()
    {
        var array = new ArrayType { ElementType = new PrimitiveType { TypeName = "integer" }, ContainsNull = true };
        Assert.False(ColumnDefaultLiteral.IsLiteral("array(1)", array));
    }
}
