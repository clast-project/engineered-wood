// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Tests;

/// <summary>
/// Structural equality for the nodes that hold a list.
/// </summary>
/// <remarks>
/// <c>Expression</c> and <c>Predicate</c> are records, so equality reads as structural — and for
/// four nodes it was not. A compiler-generated <c>Equals</c> compares an <c>IReadOnlyList</c>
/// member with <c>EqualityComparer&lt;T&gt;.Default</c>, which for an interface type is reference
/// equality, so two trees built separately from the same text were unequal. The other nodes
/// compared structurally all along, which is what made it easy to miss: equality appeared to work
/// until an expression happened to contain a call, a junction, or an IN.
/// </remarks>
public sealed class ExpressionEqualityTests
{
    private static Expression Ref(string name) => new UnboundReference(name);

    private static Predicate Cmp(string name) =>
        new ComparisonPredicate(Ref(name), ComparisonOperator.GreaterThan,
            new LiteralExpression(LiteralValue.Of(0)));

    [Fact]
    public void TwoIdenticalFunctionCallsAreEqual()
    {
        var a = new FunctionCall("+", new[] { Ref("x"), Ref("y") });
        var b = new FunctionCall("+", new[] { Ref("x"), Ref("y") });

        Assert.Equal(a, b);
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void TwoIdenticalJunctionsAreEqual()
    {
        var a = new AndPredicate(new[] { Cmp("x"), Cmp("y") });
        var b = new AndPredicate(new[] { Cmp("x"), Cmp("y") });
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());

        var c = new OrPredicate(new[] { Cmp("x"), Cmp("y") });
        var d = new OrPredicate(new[] { Cmp("x"), Cmp("y") });
        Assert.Equal(c, d);
        Assert.Equal(c.GetHashCode(), d.GetHashCode());
    }

    [Fact]
    public void TwoIdenticalSetPredicatesAreEqual()
    {
        var a = new SetPredicate(Ref("x"), new LiteralValue[] { 1, 2 }, SetOperator.In);
        var b = new SetPredicate(Ref("x"), new LiteralValue[] { 1, 2 }, SetOperator.In);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void NestedTreesCompareAllTheWayDown()
    {
        var a = new AndPredicate(new Predicate[]
        {
            Cmp("x"),
            new NotPredicate(new ComparisonPredicate(
                new FunctionCall("+", new[] { Ref("a"), Ref("b") }),
                ComparisonOperator.Equal,
                new LiteralExpression(LiteralValue.Of(1)))),
        });
        var b = new AndPredicate(new Predicate[]
        {
            Cmp("x"),
            new NotPredicate(new ComparisonPredicate(
                new FunctionCall("+", new[] { Ref("a"), Ref("b") }),
                ComparisonOperator.Equal,
                new LiteralExpression(LiteralValue.Of(1)))),
        });

        Assert.Equal(a, b);
    }

    [Fact]
    public void OrderIsPartOfTheValue()
    {
        // AND(a, b) and AND(b, a) are logically equivalent but not the same tree. Making equality
        // order-insensitive would be a claim about commutativity and evaluation order that this
        // type is not the place to make.
        Assert.NotEqual(
            new AndPredicate(new[] { Cmp("x"), Cmp("y") }),
            new AndPredicate(new[] { Cmp("y"), Cmp("x") }));

        Assert.NotEqual(
            new FunctionCall("-", new[] { Ref("x"), Ref("y") }),
            new FunctionCall("-", new[] { Ref("y"), Ref("x") }));
    }

    [Fact]
    public void DifferencesStillSeparate()
    {
        Assert.NotEqual(
            new FunctionCall("+", new[] { Ref("x") }),
            new FunctionCall("-", new[] { Ref("x") }));

        Assert.NotEqual(
            new FunctionCall("+", new[] { Ref("x") }),
            new FunctionCall("+", new[] { Ref("x"), Ref("y") }));

        Assert.NotEqual(
            new SetPredicate(Ref("x"), new LiteralValue[] { 1 }, SetOperator.In),
            new SetPredicate(Ref("x"), new LiteralValue[] { 1 }, SetOperator.NotIn));
    }

    [Fact]
    public void UsableAsADictionaryKey()
    {
        // The property the compiler-generated version could not support, and the one every
        // natural next step needs: caching a parsed constraint, de-duplicating predicates,
        // checking whether a rewrite changed anything.
        var seen = new HashSet<Expression>
        {
            new FunctionCall("+", new[] { Ref("x"), Ref("y") }),
        };

        Assert.Contains(new FunctionCall("+", new[] { Ref("x"), Ref("y") }), seen);
    }
}
