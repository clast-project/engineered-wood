// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Pins what a registry function is guaranteed to receive.
/// </summary>
/// <remarks>
/// A <see cref="LiteralValue"/> cannot carry a declared type, so anything routed through one
/// arrives as a bare CLR value with its precision and scale gone. Spark's promotion rules are
/// computed from exactly those — <c>decimal(10,2) + decimal(6,4)</c> is <c>decimal(13,4)</c> —
/// so a function that cannot see them cannot implement them. These tests exist so that
/// guarantee cannot regress quietly: a change that reintroduces the round-trip would leave the
/// registry unable to type its own results, and would show up as a plausible wrong answer
/// rather than a crash.
/// </remarks>
public sealed class FunctionArgumentTypeTests
{
    /// <summary>Records the Arrow types its arguments arrived with.</summary>
    private sealed class TypeCapturingRegistry : IFunctionRegistry
    {
        public List<IArrowType> ArgumentTypes { get; } = [];

        public IArrowArray? Result { get; set; }

        public bool IsRegistered(string name) => true;

        public IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount)
        {
            foreach (var arg in args)
                ArgumentTypes.Add(arg.Data.DataType);

            return Result ?? args[0];
        }
    }

    private static RecordBatch Batch()
    {
        var money = new Decimal128Type(10, 2);
        var moneyBuilder = new Decimal128Array.Builder(money);
        moneyBuilder.Append(12.34m);
        moneyBuilder.Append(56.78m);

        var counts = new Int32Array.Builder();
        counts.Append(1);
        counts.Append(2);

        var schema = new Schema.Builder()
            .Field(new Field("money", money, true))
            .Field(new Field("count", Int32Type.Default, true))
            .Build();

        return new RecordBatch(schema, [moneyBuilder.Build(), counts.Build()], 2);
    }

    private static (TypeCapturingRegistry Registry, ArrowRowEvaluator Evaluator) Rig()
    {
        var registry = new TypeCapturingRegistry();
        return (registry, new ArrowRowEvaluator(registry));
    }

    [Fact]
    public void ADecimalColumnReachesTheFunctionWithItsPrecisionAndScale()
    {
        var (registry, evaluator) = Rig();

        evaluator.EvaluateExpression(
            new FunctionCall("+", [new UnboundReference("money"), new UnboundReference("money")]),
            Batch());

        Assert.Equal(2, registry.ArgumentTypes.Count);
        Assert.All(registry.ArgumentTypes, type =>
        {
            var actual = Assert.IsType<Decimal128Type>(type);
            Assert.Equal(10, actual.Precision);
            Assert.Equal(2, actual.Scale);
        });
    }

    [Fact]
    public void AnIntColumnArrivesAsInt32RatherThanWidened()
    {
        var (registry, evaluator) = Rig();

        evaluator.EvaluateExpression(
            new FunctionCall("abs", [new UnboundReference("count")]), Batch());

        Assert.IsType<Int32Type>(Assert.Single(registry.ArgumentTypes));
    }

    [Theory]
    [InlineData("1.5", 2, 1)]
    [InlineData("0.5", 1, 1)]
    [InlineData("1", 1, 0)]
    [InlineData("0.05", 2, 2)]
    [InlineData("12.34", 4, 2)]
    public void ADecimalLiteralIsTypedFromItsOwnValue(string literal, int precision, int scale)
    {
        // Spark types a decimal literal by what it takes to represent it: `1.5` is decimal(2,1),
        // `.5` is decimal(1,1), `1.` is decimal(1,0). Measured into the corpus.
        var (registry, evaluator) = Rig();
        var value = decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture);

        evaluator.EvaluateExpression(
            new FunctionCall("+", [new LiteralExpression(LiteralValue.Of(value))]), Batch());

        var type = Assert.IsType<Decimal128Type>(Assert.Single(registry.ArgumentTypes));
        Assert.Equal(precision, type.Precision);
        Assert.Equal(scale, type.Scale);
    }

    [Fact]
    public void ANestedCallPassesOnWhateverTheInnerCallReturned()
    {
        // The inner result must not be re-inferred from its values on the way out, or a function
        // returning decimal(13,4) would be handed to its caller as something else.
        var (registry, evaluator) = Rig();

        var wide = new Decimal128Type(13, 4);
        var builder = new Decimal128Array.Builder(wide);
        builder.Append(1.2345m);
        builder.Append(6.7890m);
        registry.Result = builder.Build();

        evaluator.EvaluateExpression(
            new FunctionCall("outer", [
                new FunctionCall("inner", [new UnboundReference("money")]),
            ]),
            Batch());

        // inner's argument, then outer's argument (which is inner's result).
        Assert.Equal(2, registry.ArgumentTypes.Count);
        var outerArgument = Assert.IsType<Decimal128Type>(registry.ArgumentTypes[1]);
        Assert.Equal(13, outerArgument.Precision);
        Assert.Equal(4, outerArgument.Scale);
    }

    [Fact]
    public void APredicateArgumentArrivesAsBoolean()
    {
        var (registry, evaluator) = Rig();

        evaluator.EvaluateExpression(
            new FunctionCall("if", [
                new ComparisonPredicate(
                    new UnboundReference("count"), ComparisonOperator.GreaterThan,
                    new LiteralExpression(LiteralValue.Of(1))),
            ]),
            Batch());

        Assert.IsType<BooleanType>(Assert.Single(registry.ArgumentTypes));
    }

    [Fact]
    public void EvaluatingADecimalColumnDirectlyKeepsItsType()
    {
        // The same round-trip used to make this throw outright:
        // "Cannot materialize LiteralValue kind Decimal as Arrow array".
        var evaluator = new ArrowRowEvaluator();

        var result = evaluator.EvaluateExpression(new UnboundReference("money"), Batch());

        var type = Assert.IsType<Decimal128Type>(result.Data.DataType);
        Assert.Equal(10, type.Precision);
        Assert.Equal(2, type.Scale);
    }
}
