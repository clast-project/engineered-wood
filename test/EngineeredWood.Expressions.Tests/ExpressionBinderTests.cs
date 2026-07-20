// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Tests;

public class ExpressionBinderTests
{
    private static readonly Dictionary<string, int> Schema = new()
    {
        { "id", 1 },
        { "name", 2 },
        { "age", 3 },
        { "email", 4 },
    };

    private static ExpressionBinder Binder() => new(Schema);
    private static ExpressionBinder LenientBinder() => new(Schema, allowUnresolved: true);

    // ── Basic binding ──

    [Fact]
    public void Bind_UnboundReference_Resolves()
    {
        var bound = Binder().Bind(Expressions.Ref("id"));
        var b = Assert.IsType<BoundReference>(bound);
        Assert.Equal(1, b.FieldId);
        Assert.Equal("id", b.Name);
    }

    [Fact]
    public void Bind_BoundReference_Unchanged()
    {
        var input = new BoundReference(99, "preexisting");
        var output = Binder().Bind(input);
        Assert.Same(input, output);
    }

    [Fact]
    public void Bind_Literal_Unchanged()
    {
        var input = new LiteralExpression(42);
        var output = Binder().Bind(input);
        Assert.Same(input, output);
    }

    [Fact]
    public void Bind_Unresolvable_Throws()
    {
        var ex = Assert.Throws<UnboundReferenceException>(
            () => Binder().Bind(Expressions.Ref("nonexistent")));
        Assert.Equal("nonexistent", ex.Name);
    }

    [Fact]
    public void Bind_Unresolvable_LenientLeavesUnbound()
    {
        var output = LenientBinder().Bind(Expressions.Ref("nonexistent"));
        var u = Assert.IsType<UnboundReference>(output);
        Assert.Equal("nonexistent", u.Name);
    }

    // ── Comparison predicates ──

    [Fact]
    public void Bind_ComparisonPredicate_BindsBothSides()
    {
        var p = Expressions.Equal("id", 42);
        var bound = (ComparisonPredicate)Binder().Bind(p);
        Assert.IsType<BoundReference>(bound.Left);
        Assert.IsType<LiteralExpression>(bound.Right);
        Assert.Equal(1, ((BoundReference)bound.Left).FieldId);
    }

    [Fact]
    public void Bind_TwoColumnComparison_BindsBoth()
    {
        var p = new ComparisonPredicate(
            Expressions.Ref("id"), ComparisonOperator.Equal, Expressions.Ref("age"));
        var bound = (ComparisonPredicate)Binder().Bind(p);
        Assert.Equal(1, ((BoundReference)bound.Left).FieldId);
        Assert.Equal(3, ((BoundReference)bound.Right).FieldId);
    }

    // ── Unary / Set ──

    [Fact]
    public void Bind_UnaryPredicate_BindsOperand()
    {
        var p = Expressions.IsNull("name");
        var bound = (UnaryPredicate)Binder().Bind(p);
        Assert.Equal(2, ((BoundReference)bound.Operand).FieldId);
    }

    [Fact]
    public void Bind_SetPredicate_BindsOperand()
    {
        var p = Expressions.In("age", 18, 21, 30);
        var bound = (SetPredicate)Binder().Bind(p);
        Assert.Equal(3, ((BoundReference)bound.Operand).FieldId);
        Assert.Equal(3, bound.Values.Count); // values unchanged
    }

    // ── And / Or / Not ──

    [Fact]
    public void Bind_And_BindsAllChildren()
    {
        var p = Expressions.And(
            Expressions.Equal("id", 1),
            Expressions.GreaterThan("age", 18));
        var bound = (AndPredicate)Binder().Bind(p);
        Assert.Equal(2, bound.Children.Count);
        foreach (var c in bound.Children)
        {
            var cmp = Assert.IsType<ComparisonPredicate>(c);
            Assert.IsType<BoundReference>(cmp.Left);
        }
    }

    [Fact]
    public void Bind_Or_BindsAllChildren()
    {
        var p = Expressions.Or(
            Expressions.Equal("id", 1),
            Expressions.Equal("id", 2));
        var bound = (OrPredicate)Binder().Bind(p);
        Assert.Equal(2, bound.Children.Count);
    }

    [Fact]
    public void Bind_Not_BindsChild()
    {
        var p = Expressions.Not(Expressions.Equal("id", 1));
        var bound = (NotPredicate)Binder().Bind(p);
        var cmp = Assert.IsType<ComparisonPredicate>(bound.Child);
        Assert.IsType<BoundReference>(cmp.Left);
    }

    [Fact]
    public void Bind_Constants_PassThrough()
    {
        Assert.Same(Expressions.True, Binder().Bind(Expressions.True));
        Assert.Same(Expressions.False, Binder().Bind(Expressions.False));
    }

    // ── Function calls ──

    [Fact]
    public void Bind_FunctionCall_BindsArguments()
    {
        var fc = Expressions.Call("UPPER", Expressions.Ref("name"));
        var bound = (FunctionCall)Binder().Bind(fc);
        Assert.Equal("UPPER", bound.Name);
        Assert.IsType<BoundReference>(bound.Arguments[0]);
    }

    [Fact]
    public void Bind_FunctionCallInComparison_BindsRecursively()
    {
        var p = new ComparisonPredicate(
            Expressions.Call("YEAR", Expressions.Ref("created_at")),
            ComparisonOperator.Equal,
            new LiteralExpression(2024));

        var binder = new ExpressionBinder(new Dictionary<string, int> { { "created_at", 5 } });
        var bound = (ComparisonPredicate)binder.Bind(p);
        var fc = Assert.IsType<FunctionCall>(bound.Left);
        Assert.Equal(5, ((BoundReference)fc.Arguments[0]).FieldId);
    }

    // ── Identity (no allocations when nothing changes) ──

    [Fact]
    public void Bind_AlreadyBound_ReturnsSameInstance()
    {
        var p = new ComparisonPredicate(
            new BoundReference(1, "id"), ComparisonOperator.Equal,
            new LiteralExpression(42));
        var output = Binder().Bind(p);
        Assert.Same(p, output);
    }

    [Fact]
    public void Bind_AndWithBoundChildren_ReturnsSameInstance()
    {
        var p = new AndPredicate(new Predicate[]
        {
            new ComparisonPredicate(
                new BoundReference(1, "id"), ComparisonOperator.Equal,
                new LiteralExpression(1)),
            new ComparisonPredicate(
                new BoundReference(2, "name"), ComparisonOperator.Equal,
                new LiteralExpression("alice")),
        });
        var output = Binder().Bind(p);
        Assert.Same(p, output);
    }

    // ── Resolver overload ──

    [Fact]
    public void Bind_ResolverFunc_Works()
    {
        var binder = new ExpressionBinder(name => name == "x" ? 42 : null);
        var bound = (BoundReference)binder.Bind(Expressions.Ref("x"));
        Assert.Equal(42, bound.FieldId);
    }

    [Fact]
    public void Bind_NullResolver_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new ExpressionBinder((Func<string, int?>)null!));
    }
}
