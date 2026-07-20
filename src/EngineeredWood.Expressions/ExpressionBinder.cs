// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Walks an <see cref="Expression"/> tree and resolves
/// <see cref="UnboundReference"/> nodes to <see cref="BoundReference"/> using
/// a caller-provided name-to-field-id resolver.
/// </summary>
/// <remarks>
/// Format libraries provide the resolver from their own schema representation
/// (Iceberg field IDs, Parquet column indexes, Delta column mapping IDs,
/// etc.). The binder itself contains no schema-specific logic.
///
/// Binding is opt-in: formats whose evaluators work directly on column names
/// (Parquet, Delta, ORC) need not bind. Iceberg requires bound references for
/// manifest evaluation because stats are keyed by field ID.
/// </remarks>
public sealed class ExpressionBinder
{
    private readonly Func<string, int?> _resolver;
    private readonly bool _allowUnresolved;

    /// <summary>
    /// Creates a binder backed by a resolver function. The resolver returns
    /// null for names not present in the schema.
    /// </summary>
    /// <param name="resolver">Maps column name to field ID.</param>
    /// <param name="allowUnresolved">
    /// When true, references that the resolver returns null for are left as
    /// <see cref="UnboundReference"/> rather than throwing. Default: false.
    /// </param>
    public ExpressionBinder(Func<string, int?> resolver, bool allowUnresolved = false)
    {
        _resolver = resolver ?? throw new ArgumentNullException(nameof(resolver));
        _allowUnresolved = allowUnresolved;
    }

    /// <summary>
    /// Creates a binder backed by a name-to-field-id dictionary.
    /// </summary>
    public ExpressionBinder(
        IReadOnlyDictionary<string, int> nameToFieldId,
        bool allowUnresolved = false)
        : this(name => nameToFieldId.TryGetValue(name, out int id) ? id : null,
               allowUnresolved)
    {
    }

    /// <summary>
    /// Binds an expression, recursively replacing <see cref="UnboundReference"/>
    /// nodes with <see cref="BoundReference"/>.
    /// </summary>
    /// <exception cref="UnboundReferenceException">
    /// When a reference cannot be resolved and <c>allowUnresolved</c> is false.
    /// </exception>
    public Expression Bind(Expression expression)
    {
        return BindExpression(expression);
    }

    /// <summary>
    /// Binds a predicate. Same semantics as <see cref="Bind(Expression)"/>.
    /// </summary>
    /// <exception cref="UnboundReferenceException">
    /// When a reference cannot be resolved and <c>allowUnresolved</c> is false.
    /// </exception>
    public Predicate Bind(Predicate predicate)
    {
        return BindPredicate(predicate);
    }

    private Expression BindExpression(Expression expression)
    {
        switch (expression)
        {
            case UnboundReference u:
                int? id = _resolver(u.Name);
                if (id.HasValue)
                    return new BoundReference(id.Value, u.Name);
                if (_allowUnresolved)
                    return u;
                throw new UnboundReferenceException(u.Name);

            case BoundReference:
            case LiteralExpression:
                return expression;

            case FunctionCall fc:
                return BindFunctionCall(fc);

            case Predicate p:
                return BindPredicate(p);

            default:
                return expression;
        }
    }

    private Predicate BindPredicate(Predicate predicate)
    {
        switch (predicate)
        {
            case TruePredicate:
            case FalsePredicate:
                return predicate;

            case AndPredicate and:
                return BindAnd(and);

            case OrPredicate or:
                return BindOr(or);

            case NotPredicate not:
                return new NotPredicate(BindPredicate(not.Child));

            case ComparisonPredicate cmp:
                var left = BindExpression(cmp.Left);
                var right = BindExpression(cmp.Right);
                return ReferenceEquals(left, cmp.Left) && ReferenceEquals(right, cmp.Right)
                    ? cmp
                    : new ComparisonPredicate(left, cmp.Op, right);

            case UnaryPredicate unary:
                var operand = BindExpression(unary.Operand);
                return ReferenceEquals(operand, unary.Operand)
                    ? unary
                    : new UnaryPredicate(operand, unary.Op);

            case SetPredicate set:
                var setOperand = BindExpression(set.Operand);
                return ReferenceEquals(setOperand, set.Operand)
                    ? set
                    : new SetPredicate(setOperand, set.Values, set.Op);

            default:
                return predicate;
        }
    }

    private FunctionCall BindFunctionCall(FunctionCall fc)
    {
        Expression[]? bound = null;
        for (int i = 0; i < fc.Arguments.Count; i++)
        {
            var b = BindExpression(fc.Arguments[i]);
            if (bound is null)
            {
                if (!ReferenceEquals(b, fc.Arguments[i]))
                {
                    bound = new Expression[fc.Arguments.Count];
                    for (int j = 0; j < i; j++)
                        bound[j] = fc.Arguments[j];
                    bound[i] = b;
                }
            }
            else
            {
                bound[i] = b;
            }
        }

        return bound is null ? fc : new FunctionCall(fc.Name, bound);
    }

    private Predicate BindAnd(AndPredicate and)
    {
        Predicate[]? bound = null;
        for (int i = 0; i < and.Children.Count; i++)
        {
            var b = BindPredicate(and.Children[i]);
            if (bound is null)
            {
                if (!ReferenceEquals(b, and.Children[i]))
                {
                    bound = new Predicate[and.Children.Count];
                    for (int j = 0; j < i; j++)
                        bound[j] = and.Children[j];
                    bound[i] = b;
                }
            }
            else
            {
                bound[i] = b;
            }
        }

        return bound is null ? and : new AndPredicate(bound);
    }

    private Predicate BindOr(OrPredicate or)
    {
        Predicate[]? bound = null;
        for (int i = 0; i < or.Children.Count; i++)
        {
            var b = BindPredicate(or.Children[i]);
            if (bound is null)
            {
                if (!ReferenceEquals(b, or.Children[i]))
                {
                    bound = new Predicate[or.Children.Count];
                    for (int j = 0; j < i; j++)
                        bound[j] = or.Children[j];
                    bound[i] = b;
                }
            }
            else
            {
                bound[i] = b;
            }
        }

        return bound is null ? or : new OrPredicate(bound);
    }
}

/// <summary>
/// Thrown when an <see cref="ExpressionBinder"/> cannot resolve a reference
/// against the supplied schema.
/// </summary>
public sealed class UnboundReferenceException : Exception
{
    public string Name { get; }

    public UnboundReferenceException(string name)
        : base($"Cannot resolve column reference '{name}'.")
    {
        Name = name;
    }
}
