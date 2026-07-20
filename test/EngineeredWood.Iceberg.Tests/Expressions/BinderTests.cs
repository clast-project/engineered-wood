// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;
using Ex = EngineeredWood.Iceberg.Expressions.Expressions;

namespace EngineeredWood.Iceberg.Tests.Expressions;

/// <summary>
/// Iceberg uses the shared <see cref="ExpressionBinder"/> with a name→id
/// resolver derived from its <see cref="Schema"/>. These tests exercise that
/// integration via a binder constructed the same way <see cref="Iceberg.Expressions.TableScan"/>
/// constructs it internally.
/// </summary>
public class BinderTests
{
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "name", IcebergType.String, false),
        new NestedField(3, "score", IcebergType.Double, false),
    ]);

    private ExpressionBinder MakeBinder() =>
        new(_schema.Fields.ToDictionary(f => f.Name, f => f.Id), allowUnresolved: true);

    [Fact]
    public void Bind_ResolvesColumnNameToFieldId()
    {
        var expr = Ex.Equal("id", 42L);
        var bound = MakeBinder().Bind(expr);

        var pred = Assert.IsType<ComparisonPredicate>(bound);
        var b = Assert.IsType<BoundReference>(pred.Left);
        Assert.Equal(1, b.FieldId);
        Assert.Equal("id", b.Name);
        Assert.Equal(ComparisonOperator.Equal, pred.Op);
    }

    [Fact]
    public void Bind_UnknownColumn_LeavesUnbound()
    {
        var expr = Ex.Equal("nonexistent", 1);
        var bound = MakeBinder().Bind(expr);
        var pred = Assert.IsType<ComparisonPredicate>(bound);
        Assert.IsType<UnboundReference>(pred.Left);
    }

    [Fact]
    public void Bind_And_BindsBothSides()
    {
        var expr = Ex.And(
            Ex.Equal("id", 1L),
            Ex.GreaterThan("score", 90.0));

        var bound = MakeBinder().Bind(expr);
        var and = Assert.IsType<AndPredicate>(bound);
        Assert.Equal(2, and.Children.Count);

        var left = Assert.IsType<ComparisonPredicate>(and.Children[0]);
        var right = Assert.IsType<ComparisonPredicate>(and.Children[1]);
        Assert.Equal(1, ((BoundReference)left.Left).FieldId);
        Assert.Equal(3, ((BoundReference)right.Left).FieldId);
    }

    [Fact]
    public void Bind_Or_BindsBothSides()
    {
        var expr = Ex.Or(
            Ex.Equal("name", "alice"),
            Ex.Equal("name", "bob"));

        var bound = MakeBinder().Bind(expr);
        Assert.IsType<OrPredicate>(bound);
    }

    [Fact]
    public void Bind_Not_BindsChild()
    {
        var expr = Ex.Not(Ex.IsNull("name"));
        var bound = MakeBinder().Bind(expr);
        var not = Assert.IsType<NotPredicate>(bound);

        var unary = Assert.IsType<UnaryPredicate>(not.Child);
        Assert.IsType<BoundReference>(unary.Operand);
    }

    [Fact]
    public void Bind_FunctionCall_BindsArguments()
    {
        var expr = Ex.Equal(Ex.Apply("day", Ex.Ref("id")), Ex.Lit(1L));
        var bound = MakeBinder().Bind(expr);

        var cmp = Assert.IsType<ComparisonPredicate>(bound);
        var fc = Assert.IsType<FunctionCall>(cmp.Left);
        Assert.IsType<BoundReference>(fc.Arguments[0]);
    }
}
