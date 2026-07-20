// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;
using Ex = EngineeredWood.Iceberg.Expressions.Expressions;

namespace EngineeredWood.Iceberg.Tests.Expressions;

public class ExpressionTests
{
    [Fact]
    public void And_WithFalse_ReturnsFalse()
    {
        var result = Ex.And(Ex.AlwaysTrue(), Ex.AlwaysFalse());
        Assert.IsType<FalsePredicate>(result);
    }

    [Fact]
    public void And_WithTrue_ReturnsOther()
    {
        var pred = Ex.Equal("x", 1);
        var result = Ex.And(Ex.AlwaysTrue(), pred);
        Assert.Same(pred, result);
    }

    [Fact]
    public void Or_WithTrue_ReturnsTrue()
    {
        var result = Ex.Or(Ex.AlwaysFalse(), Ex.AlwaysTrue());
        Assert.IsType<TruePredicate>(result);
    }

    [Fact]
    public void Or_WithFalse_ReturnsOther()
    {
        var pred = Ex.Equal("x", 1);
        var result = Ex.Or(Ex.AlwaysFalse(), pred);
        Assert.Same(pred, result);
    }

    [Fact]
    public void Not_True_ReturnsFalse()
    {
        Assert.IsType<FalsePredicate>(Ex.Not(Ex.AlwaysTrue()));
    }

    [Fact]
    public void Not_Not_Unwraps()
    {
        var pred = Ex.Equal("x", 1);
        var result = Ex.Not(Ex.Not(pred));
        Assert.Same(pred, result);
    }

    [Fact]
    public void LiteralValue_Comparison()
    {
        Assert.True(LiteralValue.Of(1) < LiteralValue.Of(2));
        Assert.True(LiteralValue.Of(10L) > LiteralValue.Of(5L));
        Assert.True(LiteralValue.Of("abc") < LiteralValue.Of("xyz"));
        Assert.True(LiteralValue.Of(1.0) <= LiteralValue.Of(1.0));
    }

    [Fact]
    public void LiteralValue_CrossTypeNumericComparison()
    {
        Assert.True(LiteralValue.Of(5) < LiteralValue.Of(10L));
        Assert.True(LiteralValue.Of(1.0f) < LiteralValue.Of(2.0));
    }

    [Fact]
    public void Equal_ProducesComparisonPredicate()
    {
        var expr = Ex.Equal("id", 42L);
        var cmp = Assert.IsType<ComparisonPredicate>(expr);
        Assert.IsType<UnboundReference>(cmp.Left);
        Assert.IsType<LiteralExpression>(cmp.Right);
        Assert.Equal(ComparisonOperator.Equal, cmp.Op);
    }

    [Fact]
    public void IsNull_ProducesUnaryPredicate()
    {
        var expr = Ex.IsNull("name");
        var u = Assert.IsType<UnaryPredicate>(expr);
        Assert.IsType<UnboundReference>(u.Operand);
        Assert.Equal(UnaryOperator.IsNull, u.Op);
    }

    [Fact]
    public void In_ProducesSetPredicate()
    {
        var expr = Ex.In("id", 1L, 2L, 3L);
        var s = Assert.IsType<SetPredicate>(expr);
        Assert.IsType<UnboundReference>(s.Operand);
        Assert.Equal(3, s.Values.Count);
    }

    [Fact]
    public void Ref_ProducesUnboundReference()
    {
        var expr = Ex.Ref("col");
        Assert.IsType<UnboundReference>(expr);
        Assert.Equal("col", ((UnboundReference)expr).Name);
    }

    [Fact]
    public void Apply_ProducesFunctionCall()
    {
        var expr = Ex.Apply("day", Ex.Ref("ts"));
        var a = Assert.IsType<FunctionCall>(expr);
        Assert.Equal("day", a.Name);
        Assert.Single(a.Arguments);
    }

    [Fact]
    public void GenericEqual_TakesExpressions()
    {
        var expr = Ex.Equal(Ex.Ref("a"), Ex.Ref("b"));
        var cmp = Assert.IsType<ComparisonPredicate>(expr);
        Assert.IsType<UnboundReference>(cmp.Left);
        Assert.IsType<UnboundReference>(cmp.Right);
    }
}
