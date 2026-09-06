// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Comparing a string against a non-string, which Spark resolves by casting the string.
/// </summary>
/// <remarks>
/// The corpus pins these against Spark's own answers; this file states the rules directly, and
/// covers the two cases the corpus cannot reach: a registry that declines to coerce, and no
/// registry at all. See <c>string-coercion</c> in
/// <c>Fixtures/spark-expression-corpus.json</c> and #180.
/// </remarks>
public class StringComparisonCoercionTests
{
    private static readonly SparkFunctionRegistry Ansi =
        new(new SparkDialectOptions { Ansi = true });

    private static readonly SparkFunctionRegistry Legacy =
        new(new SparkDialectOptions { Ansi = false });

    /// <summary>
    /// One row of every type a coerced comparison can land on, so a test names a column rather
    /// than building a batch of its own.
    /// </summary>
    private static RecordBatch Row()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, true))
            .Field(new Field("sh", Int16Type.Default, true))
            .Field(new Field("f", FloatType.Default, true))
            .Field(new Field("g", DoubleType.Default, true))
            .Field(new Field("d", new Decimal128Type(38, 0), true))
            .Field(new Field("s", StringType.Default, true))
            .Field(new Field("bl", BooleanType.Default, true))
            .Field(new Field("dt", Date32Type.Default, true))
            .Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true))
            .Build();

        return new RecordBatch(schema, new IArrowArray[]
        {
            new Int32Array.Builder().Append(1).Build(),
            new Int16Array.Builder().Append(2).Build(),
            new FloatArray.Builder().Append(0.1f).Build(),
            new DoubleArray.Builder().Append(2.5).Build(),
            Decimal(new Decimal128Type(38, 0), BigInteger.Pow(10, 30)),
            new StringArray.Builder().Append("abc").Build(),
            new BooleanArray.Builder().Append(true).Build(),
            new Date32Array.Builder()
                .Append(new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero)).Build(),
            new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"))
                .Append(new DateTimeOffset(2026, 8, 11, 12, 30, 0, TimeSpan.Zero)).Build(),
        }, 1);
    }

    private static Decimal128Array Decimal(Decimal128Type type, BigInteger unscaled)
    {
        var bytes = new byte[16];
        var raw = unscaled.ToByteArray();
        System.Array.Copy(raw, bytes, raw.Length);

        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(true);
        return new Decimal128Array(new ArrayData(
            type, 1, 0, 0, new[] { validity.Build(), new ArrowBuffer(bytes) }));
    }

    private static bool? Evaluate(
        SparkFunctionRegistry registry, string expression, RecordBatch? batch = null)
    {
        var result = (BooleanArray)new ArrowRowEvaluator(registry)
            .EvaluateExpression(SparkSqlParser.ParseExpression(expression), batch ?? Row());
        return result.GetValue(0);
    }

    // ── The larger half: a VALID string compared, rather than answered null ──

    [Theory]
    [InlineData("'1' = a", true)]
    [InlineData("a = '1'", true)]        // the string is cast whichever side it is on
    [InlineData("'1' <> a", false)]
    [InlineData("'2' > a", true)]
    [InlineData("'0' < a", true)]
    [InlineData("'1' <=> a", true)]
    [InlineData("'  1  ' = a", true)]    // the cast trims
    [InlineData("'1' BETWEEN a AND '5'", true)]
    public void AValidNumericStringComparesAsANumberUnderBothDialects(
        string expression, bool expected)
    {
        // Before #180 every one of these answered null: the comparison had no cross-kind branch
        // for a string and fell through to "incomparable". The missing error was the smaller
        // half of that defect; these are the larger half.
        Assert.Equal(expected, Evaluate(Ansi, expression));
        Assert.Equal(expected, Evaluate(Legacy, expression));
    }

    // ── The dialects choose DIFFERENT targets, which changes the answer ──

    [Fact]
    public void AnsiWidensAnIntegralTargetToBigintAndLegacyKeepsTheColumnsOwnWidth()
    {
        // '32768' does not fit the smallint it is compared against. ANSI never casts to that
        // width -- it goes to bigint -- so it answers; the legacy dialect overflows to null.
        Assert.False(Evaluate(Ansi, "'32768' = sh"));
        Assert.Null(Evaluate(Legacy, "'32768' = sh"));
    }

    [Fact]
    public void AnsiWidensAFloatTargetToDoubleAndLegacyComparesAsAFloat()
    {
        // 0.1f widened to double is 0.100000001490116…, which the double 0.1 is not.
        Assert.False(Evaluate(Ansi, "'0.1' = f"));
        Assert.True(Evaluate(Legacy, "'0.1' = f"));
    }

    [Fact]
    public void AnsiWidensADecimalTargetToDoubleAndLegacyKeepsItExact()
    {
        // 10^30 and 10^30+1 are the same double and two different decimal(38,0)s.
        Assert.True(Evaluate(Ansi, "'1000000000000000000000000000001' = d"));
        Assert.False(Evaluate(Legacy, "'1000000000000000000000000000001' = d"));
    }

    [Fact]
    public void ACastIsTypedByItsTargetAndNotByTheValuesItProduced()
    {
        // Not a column, so there is no schema entry to read -- but a cast still has a declared
        // type, and it is the one the coercion has to use. Typing it from its values instead
        // loses exactly what a LiteralValue cannot carry, and each of these three shows one
        // face of that. All measured; the corpus records them.

        // A float, where the dialects then disagree about widening it.
        Assert.False(Evaluate(Ansi, "'0.1' = CAST(0.1 AS FLOAT)"));
        Assert.True(Evaluate(Legacy, "'0.1' = CAST(0.1 AS FLOAT)"));

        // A DATE, whose values look like instants. Spark truncates the string to a date, so this
        // is true; read as a timestamp it would compare 12:30 against midnight and answer false.
        Assert.True(Evaluate(Ansi, "'2026-08-11 12:30:00' = CAST(ts AS DATE)"));
        Assert.True(Evaluate(Legacy, "'2026-08-11 12:30:00' = CAST(ts AS DATE)"));

        // A decimal(38,0) holding 10^30, which needs only 31 digits. Under the legacy dialect the
        // string is cast to the DECLARED type, so 38 digits fit; typed from the value they would
        // overflow a decimal(31,0) and answer null instead of false.
        Assert.False(Evaluate(Legacy, "'99999999999999999999999999999999999999' = CAST(d AS DECIMAL(38,0))"));
    }

    [Fact]
    public void AnAllNullCastStillTypesTheComparison()
    {
        // The values carry no type at all here, and `<=>` reads both sides regardless -- so the
        // cast runs, and refuses. Measured: `s <=> CAST(NULL AS INT)` raises under ANSI and is
        // true under the legacy dialect, where the failed cast makes it null <=> null.
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, "s <=> CAST(NULL AS INT)"));
        Assert.True(Evaluate(Legacy, "s <=> CAST(NULL AS INT)"));

        // `=` short-circuits on the null instead, under both.
        Assert.Null(Evaluate(Ansi, "s = CAST(NULL AS INT)"));
        Assert.Null(Evaluate(Legacy, "s = CAST(NULL AS INT)"));
    }

    // ── A string the target cast refuses: the ordinary raise-or-null split ──

    [Theory]
    [InlineData("s = a")]
    [InlineData("s < a")]
    [InlineData("s = g")]
    [InlineData("s = d")]
    [InlineData("s = bl")]
    [InlineData("s = dt")]
    [InlineData("'1.5' = a")]   // a valid number, invalid for the integral target it takes
    public void AnsiRefusesAStringTheTargetCastRefuses(string expression)
    {
        var thrown = Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, expression));
        Assert.Equal("CAST_INVALID_INPUT", thrown.ErrorClass);
    }

    [Theory]
    [InlineData("s = a")]
    [InlineData("s < a")]
    [InlineData("s = bl")]
    [InlineData("s = dt")]
    public void TheLegacyDialectNullsWhatAnsiRefuses(string expression) =>
        Assert.Null(Evaluate(Legacy, expression));

    [Fact]
    public void NullSafeEqualityIsFalseAgainstAStringTheCastCannotRead()
    {
        // <=> never answers null, so the legacy dialect's null cast makes it false rather than
        // unknown -- and a raising dialect still raises, because the cast runs either way.
        Assert.False(Evaluate(Legacy, "s <=> a"));
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, "s <=> a"));
    }

    // ── Targets that need no widening: the same answer under both dialects ──

    [Theory]
    [InlineData("'true' = bl", true)]
    [InlineData("'1' = bl", true)]
    [InlineData("dt = '2026-08-11'", true)]
    [InlineData("dt = '2026-08-11 12:30:00'", true)]   // Spark truncates to the date
    [InlineData("dt > '1970-01-01'", true)]
    [InlineData("ts = '2026-08-11 12:30:00'", true)]
    [InlineData("ts = '2026-08-11'", false)]           // midnight, not 12:30
    public void ABooleanOrTemporalTargetTakesTheOtherSidesOwnTypeUnderBothDialects(
        string expression, bool expected)
    {
        Assert.Equal(expected, Evaluate(Ansi, expression));
        Assert.Equal(expected, Evaluate(Legacy, expression));
    }

    // ── A binary is the pair where the OTHER operand moves ──

    [Fact]
    public void ABinaryComparedAgainstAStringIsRenderedAsText()
    {
        // THE discriminator, and the reason this is not a guess. X'FF' is not valid UTF-8, so
        // the two candidate directions disagree about it: rendered as text it is U+FFFD, and
        // that is what the left side already is -- true. Cast the other way, U+FFFD's three
        // UTF-8 bytes (EF BF BD) are not FF, and it would be false. Spark says true.
        Assert.True(Evaluate(Ansi, "CAST(X'FF' AS STRING) = X'FF'"));
        Assert.True(Evaluate(Legacy, "CAST(X'FF' AS STRING) = X'FF'"));

        // The same rule on values where both directions would agree, so the ordering is covered.
        Assert.True(Evaluate(Ansi, "'A' = X'41'"));
        Assert.True(Evaluate(Ansi, "X'41' < 'B'"));
        Assert.False(Evaluate(Ansi, "s = X'41'"));
    }

    [Fact]
    public void TwoBinariesAreComparedAsBinaries()
    {
        // No string on either side, so nothing moves and the bytes compare as bytes.
        Assert.True(Evaluate(Ansi, "X'41' = X'41'"));
        Assert.False(Evaluate(Ansi, "X'41' = X'42'"));
    }

    // ── IN resolves ONE type over the operand and the whole list ──

    [Theory]
    [InlineData("'1' IN (1, 2)", true)]
    [InlineData("a IN ('1', '2')", true)]
    [InlineData("a IN (1, 2)", true)]            // no string anywhere: untouched
    [InlineData("'1' NOT IN (1, 2)", false)]
    [InlineData("dt IN ('2026-08-11')", true)]
    public void AMixedSetIsComparedThroughOneTypeUnderBothDialects(string expression, bool expected)
    {
        // Every one of these answered FALSE before #259: the set compared through
        // LiteralValue.CompareTo, which has no cross-kind branch for a string, so a string never
        // matched a number however equal the two were.
        Assert.Equal(expected, Evaluate(Ansi, expression));
        Assert.Equal(expected, Evaluate(Legacy, expression));
    }

    [Fact]
    public void TheLegacyDialectResolvesASetThroughTextAndAnsiThroughTheNumber()
    {
        // THE discriminator, and the reason IN cannot borrow the comparison rule: 1 and 01 are
        // the same number and different text. `a = '01'` is true under both dialects; the set is
        // false under the legacy one, because the list resolves to STRING.
        Assert.True(Evaluate(Ansi, "a IN ('01')"));
        Assert.False(Evaluate(Legacy, "a IN ('01')"));
        Assert.True(Evaluate(Ansi, "a = '01'"));
        Assert.True(Evaluate(Legacy, "a = '01'"));

        // The same split on a floating operand, where the trailing zero is what differs.
        Assert.True(Evaluate(Ansi, "g IN ('2.50')"));
        Assert.False(Evaluate(Legacy, "g IN ('2.50')"));

        // ...and on a date, which the legacy dialect renders rather than parses.
        Assert.True(Evaluate(Ansi, "dt IN ('2026-08-11 12:30:00')"));
        Assert.False(Evaluate(Legacy, "dt IN ('2026-08-11 12:30:00')"));
    }

    [Fact]
    public void AnsiRefusesASetMemberTheResolvedTypeCannotRead()
    {
        // The set resolves through bigint, so a string that is not an integer refuses -- exactly
        // as the same value would in a comparison.
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, "s IN (1, 2)"));
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, "a IN ('1.5')"));

        // The legacy dialect resolves through text instead, where nothing fails to read.
        Assert.False(Evaluate(Legacy, "s IN (1, 2)"));
        Assert.False(Evaluate(Legacy, "a IN ('1.5')"));
    }

    [Fact]
    public void ANullInTheListKeepsItsThreeValuedAnswerThroughTheCoercion()
    {
        // No match plus a null in the list is unknown, not false -- and the coercion must not
        // turn the null into a value on the way through.
        Assert.Null(Evaluate(Legacy, "s IN (1, NULL)"));
        Assert.True(Evaluate(Legacy, "'1' IN (1, NULL)"));
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, "s IN (1, NULL)"));
    }

    // ── What is deliberately left alone ──

    [Fact]
    public void TwoStringsAreNotCoerced() => Assert.False(Evaluate(Ansi, "s = 'other'"));

    [Fact]
    public void ADecimalWiderThanSparkCanDeclareTakesNoLegacyCoercion()
    {
        // Parquet's decimal runs wider than Spark's: `ArrowSchemaConverter` builds a
        // Decimal256Type for precision > 38, which no Spark expression can name. The legacy
        // dialect casts to the operand's OWN type, and there is no cast to a decimal that wide --
        // so the pair keeps the answer it had rather than failing on the way to one.
        var type = new Decimal256Type(50, 0);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("wide", type, true))
            .Field(new Field("s", StringType.Default, true))
            .Build();

        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(true);
        var batch = new RecordBatch(schema, new IArrowArray[]
        {
            new Decimal256Array(new ArrayData(
                type, 1, 0, 0, new[] { validity.Build(), new ArrowBuffer(new byte[32]) })),
            new StringArray.Builder().Append("0").Build(),
        }, 1);

        Assert.Null(Evaluate(Legacy, "s = wide", batch));

        // ANSI never reaches that cast -- every decimal goes to double, whatever its precision.
        Assert.True(Evaluate(Ansi, "s = wide", batch));
    }

    [Fact]
    public void AnAllNullStringOperandIsNeverCastAndSoNeverRaises()
    {
        // No row has two values to compare, so there is nothing to cast and ANSI has nothing to
        // refuse. Casting anyway would turn a null answer into an error.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, true))
            .Field(new Field("s", StringType.Default, true))
            .Build();
        var batch = new RecordBatch(schema, new IArrowArray[]
        {
            new Int32Array.Builder().Append(1).Build(),
            new StringArray.Builder().AppendNull().Build(),
        }, 1);

        Assert.Null(Evaluate(Ansi, "s = a", batch));
    }

    [Fact]
    public void ARowWhoseOtherOperandIsNullIsNotCast()
    {
        // Row 2 pairs a malformed string with a null number. Spark's relational operators
        // evaluate nothing once an operand is null, so the cast never runs and the row is null
        // rather than an error -- measured over exactly this pair, in both operand orders.
        // Casting the column regardless would refuse the whole batch, including row 1.
        var batch = NullOpposite();

        var result = (BooleanArray)new ArrowRowEvaluator(Ansi)
            .EvaluateExpression(SparkSqlParser.ParseExpression("s = a"), batch);

        Assert.True(result.GetValue(0));
        Assert.Null(result.GetValue(1));
    }

    [Fact]
    public void NullSafeEqualityHasNoSuchShortCircuitAndStillRefuses()
    {
        // The exception that makes the mask a rule rather than a convenience: `<=>` reads both
        // sides whatever their nullness, so the same row Spark answers null for under `=` it
        // refuses under `<=>`. Measured.
        Assert.Throws<SparkEvaluationException>(() => new ArrowRowEvaluator(Ansi)
            .EvaluateExpression(SparkSqlParser.ParseExpression("s <=> a"), NullOpposite()));
    }

    [Fact]
    public void NullSafeEqualityRefusesEvenWhenTheWholeOtherColumnIsNull()
    {
        // `<=>` reads both sides, so an all-null column opposite a malformed string is still a
        // refused cast rather than a false. Measured: over a single row of (a = NULL, s = 'abc'),
        // Spark answers null for `s = a` and raises for `s <=> a`. The column still types the
        // cast even though it holds no value to type it from.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, true))
            .Field(new Field("s", StringType.Default, true))
            .Build();
        var batch = new RecordBatch(schema, new IArrowArray[]
        {
            new Int32Array.Builder().AppendNull().Build(),
            new StringArray.Builder().Append("abc").Build(),
        }, 1);

        Assert.Null(Evaluate(Ansi, "s = a", batch));
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, "s <=> a", batch));
    }

    /// <summary>A valid row, then a malformed string opposite a null number.</summary>
    private static RecordBatch NullOpposite()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, true))
            .Field(new Field("s", StringType.Default, true))
            .Build();

        return new RecordBatch(schema, new IArrowArray[]
        {
            new Int32Array.Builder().Append(1).AppendNull().Build(),
            new StringArray.Builder().Append("1").Append("abc").Build(),
        }, 2);
    }

    [Fact]
    public void ARegistryThatDoesNotCoerceLeavesTheComparisonAsItWas()
    {
        // The interface is optional: a host with its own registry keeps the previous behaviour
        // rather than losing comparison altogether.
        var result = (BooleanArray)new ArrowRowEvaluator(new PlainRegistry())
            .EvaluateExpression(SparkSqlParser.ParseExpression("'1' = a"), Row());

        Assert.Null(result.GetValue(0));
    }

    [Fact]
    public void NoRegistryAtAllLeavesTheComparisonAsItWas()
    {
        var result = (BooleanArray)new ArrowRowEvaluator()
            .EvaluateExpression(SparkSqlParser.ParseExpression("'1' = a"), Row());

        Assert.Null(result.GetValue(0));
    }

    /// <summary>A registry with functions and no opinion about comparison.</summary>
    private sealed class PlainRegistry : IFunctionRegistry
    {
        public bool IsRegistered(string name) => false;

        public IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount) =>
            throw new NotSupportedException(name);
    }
}
