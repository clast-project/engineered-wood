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
    // new BigDecimal(text, MathContext(p)).setScale(s): rounds to p digits, then refuses a lossy scale change.
    [InlineData("1.2", "decimal(10,2)", true)]
    [InlineData("1.20", "decimal(10,2)", true)]
    [InlineData("1.234", "decimal(10,2)", false)]
    [InlineData("1.2300", "decimal(10,2)", true)]
    [InlineData("-3", "decimal(5,0)", true)]
    [InlineData("1e2", "decimal(5,0)", true)]
    [InlineData("1.5e-1", "decimal(5,2)", true)]
    [InlineData("1.234", "decimal(3,2)", true)]
    [InlineData("12345678901", "decimal(10,2)", true)]
    [InlineData("0.001", "decimal(10,2)", false)]
    [InlineData("1.", "decimal(10,2)", true)]
    [InlineData(".", "decimal(10,2)", false)]
    [InlineData("1 ", "decimal(10,2)", false)]
    // Other types never have a literal default.
    [InlineData("'x'", "variant", false)]
    public void IsLiteral_MatchesTheJavaParse(string text, string type, bool expected)
    {
        Assert.Equal(expected, IsLiteral(text, type));
    }

    [Fact]
    public void ComplexTypes_HaveNoLiteralDefault()
    {
        var array = new ArrayType { ElementType = new PrimitiveType { TypeName = "integer" }, ContainsNull = true };
        Assert.False(ColumnDefaultLiteral.IsLiteral("array(1)", array));
    }
}
