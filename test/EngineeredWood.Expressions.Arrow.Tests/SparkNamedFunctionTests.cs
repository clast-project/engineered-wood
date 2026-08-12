// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// The named string, pattern, date-part and conditional functions.
/// </summary>
/// <remarks>
/// Expected values come from Spark — most from the harvested corpus, the rest from probing the
/// edges the corpus does not cover (<c>substring</c> positions, <c>LIKE</c> escapes, the trim
/// variants). Nothing here is asserted from reading Spark's documentation.
/// </remarks>
public sealed class SparkNamedFunctionTests
{
    private static readonly SparkFunctionRegistry Registry = new();

    private static RecordBatch Batch(params (string Name, IArrowArray Array)[] columns)
    {
        var schema = new Schema.Builder();
        foreach (var (name, array) in columns)
            schema.Field(new Field(name, array.Data.DataType, true));

        return new RecordBatch(schema.Build(), columns.Select(c => c.Array), columns[0].Array.Length);
    }

    private static IArrowArray Strings(params string?[] values)
    {
        var b = new StringArray.Builder();
        foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(v); }
        return b.Build();
    }

    private static IArrowArray Ints(params int?[] values)
    {
        var b = new Int32Array.Builder();
        foreach (var v in values) { if (v is { } x) b.Append(x); else b.AppendNull(); }
        return b.Build();
    }

    private static IArrowArray Doubles(params double[] values)
    {
        var b = new DoubleArray.Builder();
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    private static IArrowArray Timestamps(params DateTimeOffset[] values)
    {
        var b = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    private static IArrowArray Dates(params DateTimeOffset[] values)
    {
        var b = new Date32Array.Builder();
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    private static IArrowArray Eval(string sql, RecordBatch batch) =>
        new ArrowRowEvaluator(Registry).EvaluateExpression(SparkSqlParser.ParseExpression(sql), batch);

    private static string? Str(string sql, RecordBatch batch) =>
        ((StringArray)Eval(sql, batch)).GetString(0);

    private static bool? Bool(string sql, RecordBatch batch) =>
        ((BooleanArray)Eval(sql, batch)).GetValue(0);

    private static RecordBatch Abc => Batch(("s", Strings("abc")), ("t", Strings("xyz")));

    // ── Strings ────────────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("substring(s, 1, 2)", "ab")]
    [InlineData("substring(s, 0, 2)", "ab")]      // position 0 behaves as 1
    [InlineData("substring(s, -2, 2)", "bc")]     // negative counts back from the end
    [InlineData("substring(s, 2)", "bc")]         // no length means the rest
    [InlineData("substring(s, 1, 99)", "abc")]    // a length past the end clamps
    [InlineData("substring(s, 4, 2)", "")]        // a start past the end is empty, not null
    [InlineData("substring(s, 1, 0)", "")]
    public void SubstringFollowsSparksPositionRules(string sql, string expected)
    {
        // None of these are the obvious reading of a 1-based substring, and all were measured.
        Assert.Equal(expected, Str(sql, Abc));
    }

    [Fact]
    public void TheStringFunctionsMatchTheirRecordedValues()
    {
        Assert.Equal(3, ((Int32Array)Eval("length(s)", Abc)).GetValue(0));
        Assert.Equal("ABC", Str("upper(s)", Abc));
        Assert.Equal("abc", Str("lower(s)", Abc));
        Assert.Equal("abcxyz", Str("concat(s, t)", Abc));
        Assert.Equal("abcxyz", Str("s || t", Abc));
    }

    [Fact]
    public void TheTrimVariantsTrimTheEndsTheyName()
    {
        var padded = Batch(("s", Strings("  ab  ")));

        Assert.Equal("ab", Str("trim(s)", padded));
        Assert.Equal("ab  ", Str("ltrim(s)", padded));
        Assert.Equal("  ab", Str("rtrim(s)", padded));
    }

    [Fact]
    public void ConcatPropagatesNullRatherThanSkippingIt()
    {
        // `concat('abc', NULL)` is null, not 'abc'. Measured, and the opposite of what a
        // string-builder implementation falls into by default.
        Assert.Null(Str("concat(s, t)", Batch(("s", Strings("abc")), ("t", Strings([null])))));
        Assert.Null(Str("s || t", Batch(("s", Strings("abc")), ("t", Strings([null])))));
    }

    [Fact]
    public void ConcatRendersANonStringArgument()
    {
        Assert.Equal("abc1", Str("concat(s, a)", Batch(("s", Strings("abc")), ("a", Ints(1)))));
    }

    // ── Pattern matching ───────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("s LIKE 'a_c'", true)]     // _ matches exactly one character
    [InlineData("s LIKE '%b%'", true)]
    [InlineData("s LIKE 'A%'", false)]     // LIKE is case-sensitive
    [InlineData("s ILIKE 'A%'", true)]     // ILIKE is not
    [InlineData("s RLIKE '^a'", true)]     // RLIKE is a regular expression
    [InlineData("s LIKE 'ab'", false)]     // anchored: a prefix does not match
    public void PatternMatchingBehavesAsMeasured(string sql, bool expected)
    {
        Assert.Equal(expected, Bool(sql, Abc));
    }

    [Fact]
    public void ABackslashEscapesTheWildcardItPrecedes()
    {
        // Measured: `'100%' LIKE '100\%'` is true, so the escape has to be honoured rather than
        // treated as a literal backslash followed by "match anything".
        var batch = Batch(("s", Strings("100%")));
        Assert.True(Bool(@"s LIKE '100\%'", batch));

        Assert.False(Bool(@"s LIKE '100\%'", Batch(("s", Strings("100abc")))));
    }

    [Fact]
    public void ANullOperandMakesTheMatchNull()
    {
        Assert.Null(Bool("s LIKE 'a%'", Batch(("s", Strings([null])))));
    }

    // ── Date parts ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void DatePartsReadTheInstantInTheResolvedZone()
    {
        var batch = Batch(("ts", Timestamps(new DateTimeOffset(2026, 8, 11, 12, 30, 45, TimeSpan.Zero))));

        Assert.Equal(2026, ((Int32Array)Eval("year(ts)", batch)).GetValue(0));
        Assert.Equal(8, ((Int32Array)Eval("month(ts)", batch)).GetValue(0));
        Assert.Equal(11, ((Int32Array)Eval("day(ts)", batch)).GetValue(0));
        Assert.Equal(12, ((Int32Array)Eval("hour(ts)", batch)).GetValue(0));
    }

    [Fact]
    public void DatePartsAlsoReadACalendarDate()
    {
        var batch = Batch(("dt", Dates(new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero))));

        Assert.Equal(2026, ((Int32Array)Eval("year(dt)", batch)).GetValue(0));
        Assert.Equal(11, ((Int32Array)Eval("day(dt)", batch)).GetValue(0));
    }

    [Fact]
    public void DateFormatSupportsThePatternLettersThatMeanTheSameInBothDialects()
    {
        var batch = Batch(("ts", Timestamps(new DateTimeOffset(2026, 8, 11, 12, 30, 45, TimeSpan.Zero))));

        Assert.Equal("2026-08", Str("date_format(ts, 'yyyy-MM')", batch));
        Assert.Equal("2026-08-11 12:30:45", Str("date_format(ts, 'yyyy-MM-dd HH:mm:ss')", batch));
    }

    [Fact]
    public void AnUnknownPatternLetterIsRefusedRatherThanReinterpreted()
    {
        // Java and .NET pattern languages overlap for y M d H m s and diverge elsewhere, so a
        // letter outside that set would be silently reinterpreted rather than rejected.
        var batch = Batch(("ts", Timestamps(new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero))));

        var ex = Assert.Throws<NotSupportedException>(
            () => Eval("date_format(ts, 'yyyy-DDD')", batch));
        Assert.Contains("'D'", ex.Message, StringComparison.Ordinal);
    }

    // ── Conditionals ───────────────────────────────────────────────────────────────────────

    [Fact]
    public void CoalesceTakesTheFirstNonNullAndUnifiesTheType()
    {
        var batch = Batch(("a", Ints(null, 5)), ("g", Doubles(2.5, 9.5)));

        var result = Assert.IsType<DoubleArray>(Eval("coalesce(a, g)", batch));
        Assert.Equal(2.5, result.GetValue(0));
        Assert.Equal(5.0, result.GetValue(1));
    }

    [Fact]
    public void CoalesceOfADecimalAndAnIntWidensWithoutTheCarryDigitAdditionNeeds()
    {
        // Measured: coalesce(decimal(10,2), int) is decimal(12,2), where the same pair added is
        // decimal(13,2). Unification holds either operand; arithmetic holds a result.
        Assert.Equal("decimal(12,2)", Describe(
            SparkNumericTypes.CommonType(new Decimal128Type(10, 2), Int32Type.Default)));
        Assert.Equal("decimal(13,2)", Describe(
            SparkNumericTypes.ArithmeticResult("+", new Decimal128Type(10, 2), Int32Type.Default)));
    }

    private static string Describe(IArrowType type) =>
        type is Decimal128Type d ? $"decimal({d.Precision},{d.Scale})" : type.Name;

    [Fact]
    public void IfAndCaseChooseTheirBranch()
    {
        var batch = Batch(("a", Ints(1, -1)));

        var chosen = Assert.IsType<Int32Array>(Eval("if(a > 0, 10, 20)", batch));
        Assert.Equal(10, chosen.GetValue(0));
        Assert.Equal(20, chosen.GetValue(1));

        Assert.Equal("pos", ((StringArray)Eval(
            "CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END", batch)).GetString(0));
        Assert.Equal("neg", ((StringArray)Eval(
            "CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END", batch)).GetString(1));
    }

    [Fact]
    public void ACaseWithNoMatchingBranchAndNoElseIsNull()
    {
        // Measured: `CASE WHEN a > 0 THEN 1 END` is null where the condition fails.
        var batch = Batch(("a", Ints(-1)));

        Assert.Null(Assert.IsType<Int32Array>(
            Eval("CASE WHEN a > 0 THEN 1 END", batch)).GetValue(0));
    }

    [Fact]
    public void ANullConditionIsNotTaken()
    {
        // A condition has to be true, not merely non-false — SQL's three-valued logic means a
        // null condition falls through to ELSE.
        var batch = Batch(("a", Ints([null])));

        Assert.Equal(20, Assert.IsType<Int32Array>(
            Eval("if(a > 0, 10, 20)", batch)).GetValue(0));
    }

    [Fact]
    public void NullIfIsNullWhenTheOperandsAreEqual()
    {
        var batch = Batch(("a", Ints(0, 5)));

        var result = Assert.IsType<Int32Array>(Eval("nullif(a, 0)", batch));
        Assert.Null(result.GetValue(0));
        Assert.Equal(5, result.GetValue(1));
    }

    [Fact]
    public void IfnullAndNvlAreCoalesce()
    {
        var batch = Batch(("a", Ints([null])));

        Assert.Equal(7, Assert.IsType<Int32Array>(Eval("ifnull(a, 7)", batch)).GetValue(0));
        Assert.Equal(7, Assert.IsType<Int32Array>(Eval("nvl(a, 7)", batch)).GetValue(0));
    }

    [Fact]
    public void TheRegistryClaimsOnlyWhatItImplements()
    {
        Assert.True(Registry.IsRegistered("substring"));
        Assert.True(Registry.IsRegistered("date_format"));
        Assert.True(Registry.IsRegistered("case"));
        Assert.False(Registry.IsRegistered("current_timestamp"));
        Assert.False(Registry.IsRegistered("to_date"));
    }

    // ── From review of #134 ────────────────────────────────────────────────────────────────

    private static IArrowArray Decimals(int precision, int scale, params decimal[] values)
    {
        var b = new Decimal128Array.Builder(new Decimal128Type(precision, scale));
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    [Fact]
    public void NullIfComparesValuesRatherThanTheirRenderings()
    {
        // decimal(10,2) holding 1.00 renders as "1.00" and the int as "1", so comparing text
        // would call them different and return the value. Measured: Spark says null.
        var batch = Batch(("d", Decimals(10, 2, 1.00m)));

        Assert.Null(Assert.IsType<Decimal128Array>(Eval("nullif(d, 1)", batch)).GetValue(0));
        Assert.Equal(1.00m, Assert.IsType<Decimal128Array>(Eval("nullif(d, 2)", batch)).GetValue(0));
    }

    [Fact]
    public void ANonBooleanConditionFailsRatherThanQuietlyTakingTheElseBranch()
    {
        // Spark rejects it outright as a DATATYPE_MISMATCH. Reading it as false would take ELSE
        // on every row — a wrong answer indistinguishable from a deliberate one.
        var batch = Batch(("a", Ints(1)));

        Assert.Throws<NotSupportedException>(() => Eval("if(a, 10, 20)", batch));
        Assert.Throws<NotSupportedException>(() => Eval("CASE WHEN a THEN 1 ELSE 2 END", batch));
    }

    [Fact]
    public void AQuotedSectionOfAPatternIsALiteralAndNotValidated()
    {
        // `yyyy-MM-dd'T'HH:mm:ss` is the ordinary ISO 8601 spelling, and both dialects treat a
        // quoted section as a literal. Validating its letters would refuse it.
        var batch = Batch(("ts", Timestamps(new DateTimeOffset(2026, 8, 11, 12, 30, 45, TimeSpan.Zero))));

        Assert.Equal("2026-08-11T12:30:45", Str(@"date_format(ts, 'yyyy-MM-dd\'T\'HH:mm:ss')", batch));
    }

    [Fact]
    public void CaseFoldingDoesNotDependOnTheProcessCulture()
    {
        // Without RegexOptions.CultureInvariant a Turkish locale folds 'I' to a dotless
        // lowercase, so ILIKE would match different rows on different machines.
        var original = System.Globalization.CultureInfo.CurrentCulture;
        try
        {
            System.Globalization.CultureInfo.CurrentCulture =
                new System.Globalization.CultureInfo("tr-TR");

            Assert.True(Bool("s ILIKE 'I%'", Batch(("s", Strings("index")))));
        }
        finally
        {
            System.Globalization.CultureInfo.CurrentCulture = original;
        }
    }
}
