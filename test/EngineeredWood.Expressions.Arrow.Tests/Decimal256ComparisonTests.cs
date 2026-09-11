// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// A <c>decimal256</c> operand keeps the exact comparison rather than reaching the rounding rule.
/// </summary>
/// <remarks>
/// <para>
/// #280's comparison rule rounds both operands to their least common type, and it is implemented
/// over <see cref="SparkWideDecimals"/>, which reads a <c>Decimal128Array</c> and nothing else.
/// A <c>decimal256</c> is therefore OUT OF SCOPE for it, and the rule has to decline rather than
/// half-apply: <c>SparkNumericTypes.CommonType</c> reads a decimal256's precision and scale
/// happily and hands back a decimal128 common type, so nothing upstream stops the cast on its own.
/// </para>
/// <para>
/// The set path is the one that needed a guard, and it is the asymmetry that made it easy to
/// miss: the comparison path asks about each operand and a decimal256 declines on its own, while
/// the set path returns ONE target for the whole list and then casts every member with it. So a
/// list whose decimal128 member loses scale would return a target that its decimal256 member
/// cannot be cast to, turning an answer into a <see cref="NotSupportedException"/>. Caught by the
/// Copilot reviewer on #305.
/// </para>
/// <para>
/// These cannot be corpus rows: Spark has no decimal256, so there is no Spark answer to harvest.
/// What they pin is that the pair behaves as it did BEFORE #280 — an exact comparison, unrounded
/// — rather than throwing. Making decimal256 a first-class exact decimal is a separate change.
/// </para>
/// </remarks>
public sealed class Decimal256ComparisonTests
{
    /// <summary>
    /// decimal(38,38) against decimal256(38,0) — a pair whose common decimal(38,0) WOULD round
    /// the first operand away, so the rule is live and only the guard declines it.
    /// </summary>
    private static RecordBatch WideBatch()
    {
        var highScale = new Decimal128Type(38, 38);
        var highScaleValues = new Decimal128Array.Builder(highScale);
        highScaleValues.Append(0.4m);

        var wide = new Decimal256Type(38, 0);
        var wideValues = new Decimal256Array.Builder(wide);
        wideValues.Append(0m);

        var schema = new Schema(
            new[] { new Field("hi", highScale, true), new Field("wide256", wide, true) }, null);
        return new RecordBatch(
            schema, new IArrowArray[] { highScaleValues.Build(), wideValues.Build() }, 1);
    }

    [Theory]
    // 0.4 against 0. Rounded at the common decimal(38,0) these would be equal, which is what
    // Spark answers for the all-decimal128 version of the same pair; unrounded they are not.
    // The unrounded answer is the one to expect here.
    [InlineData("hi = wide256", false)]
    [InlineData("hi > wide256", true)]
    [InlineData("hi IN (wide256)", false)]
    public void ADecimal256OperandAnswersExactlyRatherThanThrowing(string expression, bool expected)
    {
        var actual = new ArrowRowEvaluator(new SparkFunctionRegistry())
            .EvaluateExpression(SparkSqlParser.ParseExpression(expression), WideBatch());

        var values = Assert.IsType<BooleanArray>(actual);
        Assert.Equal(expected, values.GetValue(0));
    }

    /// <summary>
    /// The registry declines directly, which is where the guard lives.
    /// </summary>
    [Fact]
    public void TheSetTargetDeclinesAListItCannotCastEveryMemberOf()
    {
        var registry = new SparkFunctionRegistry();

        // decimal(38,38) with decimal(38,0) DOES round, so the target is returned...
        Assert.NotNull(registry.SetComparisonTarget(
            new IArrowType[] { new Decimal128Type(38, 38), new Decimal128Type(38, 0) }));

        // ...and the same list with the second member held as a decimal256 does not, even though
        // the first member still loses scale and the common type is unchanged.
        Assert.Null(registry.SetComparisonTarget(
            new IArrowType[] { new Decimal128Type(38, 38), new Decimal256Type(38, 0) }));
    }
}
