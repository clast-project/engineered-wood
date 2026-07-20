// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Tests;

public class ExpressionTests
{
    [Fact]
    public void Equal_BuildsComparisonPredicate()
    {
        var p = Expressions.Equal("id", 42);
        var cmp = Assert.IsType<ComparisonPredicate>(p);
        Assert.Equal(ComparisonOperator.Equal, cmp.Op);
        Assert.Equal(new UnboundReference("id"), cmp.Left);
        Assert.Equal(new LiteralExpression(42), cmp.Right);
    }

    [Fact]
    public void GreaterThan_BuildsComparisonPredicate()
    {
        var p = Expressions.GreaterThan("age", 18);
        Assert.Equal(ComparisonOperator.GreaterThan, ((ComparisonPredicate)p).Op);
    }

    [Fact]
    public void IsNull_BuildsUnaryPredicate()
    {
        var p = Expressions.IsNull("name");
        Assert.Equal(UnaryOperator.IsNull, p.Op);
        Assert.Equal(new UnboundReference("name"), p.Operand);
    }

    [Fact]
    public void In_BuildsSetPredicate()
    {
        var p = Expressions.In("status", "active", "pending");
        Assert.Equal(SetOperator.In, p.Op);
        Assert.Equal(2, p.Values.Count);
        Assert.Equal((LiteralValue)"active", p.Values[0]);
    }

    [Fact]
    public void And_Empty_ReturnsTrue()
    {
        Assert.IsType<TruePredicate>(Expressions.And());
    }

    [Fact]
    public void And_Single_ReturnsChild()
    {
        var p = Expressions.Equal("x", 1);
        Assert.Same(p, Expressions.And(p));
    }

    [Fact]
    public void And_Multiple_BuildsAndPredicate()
    {
        var a = Expressions.Equal("x", 1);
        var b = Expressions.Equal("y", 2);
        var p = Expressions.And(a, b);
        var and = Assert.IsType<AndPredicate>(p);
        Assert.Equal(2, and.Children.Count);
    }

    [Fact]
    public void And_Flattens_NestedAnd()
    {
        var a = Expressions.Equal("x", 1);
        var b = Expressions.Equal("y", 2);
        var c = Expressions.Equal("z", 3);
        var p = Expressions.And(Expressions.And(a, b), c);
        var and = Assert.IsType<AndPredicate>(p);
        Assert.Equal(3, and.Children.Count); // flattened
    }

    [Fact]
    public void Or_Empty_ReturnsFalse()
    {
        Assert.IsType<FalsePredicate>(Expressions.Or());
    }

    [Fact]
    public void Or_Flattens_NestedOr()
    {
        var a = Expressions.Equal("x", 1);
        var b = Expressions.Equal("y", 2);
        var c = Expressions.Equal("z", 3);
        var p = Expressions.Or(a, Expressions.Or(b, c));
        var or = Assert.IsType<OrPredicate>(p);
        Assert.Equal(3, or.Children.Count);
    }

    [Fact]
    public void Not_True_ReturnsFalse()
    {
        Assert.IsType<FalsePredicate>(Expressions.Not(Expressions.True));
    }

    [Fact]
    public void Not_False_ReturnsTrue()
    {
        Assert.IsType<TruePredicate>(Expressions.Not(Expressions.False));
    }

    [Fact]
    public void Not_NotX_ReturnsX()
    {
        var p = Expressions.Equal("x", 1);
        Assert.Same(p, Expressions.Not(Expressions.Not(p)));
    }

    [Fact]
    public void True_False_AreSingletons()
    {
        Assert.Same(Expressions.True, Expressions.True);
        Assert.Same(Expressions.False, Expressions.False);
    }

    [Fact]
    public void Call_BuildsFunctionCall()
    {
        var f = Expressions.Call("YEAR", Expressions.Ref("ts"));
        Assert.Equal("YEAR", f.Name);
        Assert.Single(f.Arguments);
    }

    // ── ToString ──

    [Fact]
    public void ComparisonPredicate_ToString()
    {
        var p = Expressions.Equal("id", 42);
        Assert.Equal("id = 42", p.ToString());
    }

    [Fact]
    public void And_ToString()
    {
        var p = Expressions.And(
            Expressions.Equal("x", 1),
            Expressions.Equal("y", 2));
        Assert.Equal("(x = 1 AND y = 2)", p.ToString());
    }

    [Fact]
    public void IsNull_ToString()
    {
        Assert.Equal("name IS NULL", Expressions.IsNull("name").ToString());
    }

    [Fact]
    public void In_ToString()
    {
        var p = Expressions.In("status", "a", "b");
        Assert.Equal("status IN (a, b)", p.ToString());
    }

    // ── Records compare structurally ──

    [Fact]
    public void Equal_StructurallyEqualPredicates()
    {
        var a = Expressions.Equal("x", 1);
        var b = Expressions.Equal("x", 1);
        Assert.Equal(a, b);
    }

    [Fact]
    public void Equal_DifferentColumn_NotEqual()
    {
        var a = Expressions.Equal("x", 1);
        var b = Expressions.Equal("y", 1);
        Assert.NotEqual(a, b);
    }
}
