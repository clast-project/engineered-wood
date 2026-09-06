// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow;

/// <summary>
/// Evaluates expressions and predicates against an Arrow
/// <see cref="RecordBatch"/>, producing typed Arrow arrays.
/// </summary>
public interface IRowEvaluator
{
    /// <summary>
    /// Evaluates a predicate against every row in the batch. Returns a
    /// <see cref="BooleanArray"/> of the same length: each element is
    /// <c>true</c>/<c>false</c> per SQL semantics, or <c>null</c> when the
    /// predicate produced an unknown result for that row (e.g. comparison
    /// with a NULL operand).
    /// </summary>
    BooleanArray EvaluatePredicate(Predicate predicate, RecordBatch batch);

    /// <summary>
    /// Evaluates a value expression against every row. The returned array's
    /// type is inferred from the values: column references return the
    /// underlying column, literals return a constant array, function calls
    /// return whatever the registered function produces. Value kinds whose
    /// Arrow type cannot be reconstructed from the value alone (decimal,
    /// timestamp, date, unsigned integers) are not supported by this overload —
    /// use <see cref="EvaluateExpression(Expression, RecordBatch, IArrowType)"/>.
    /// </summary>
    IArrowArray EvaluateExpression(Expression expression, RecordBatch batch);

    /// <summary>
    /// Evaluates a value expression against every row and materializes it as
    /// <paramref name="targetType"/>. Supplying the target Arrow type resolves
    /// the metadata a bare value cannot carry — a decimal's precision/scale and
    /// physical width, a timestamp's unit and timezone, date vs timestamp — so
    /// decimal, timestamp and date results (in addition to the inferrable
    /// primitives) can be produced faithfully.
    /// </summary>
    IArrowArray EvaluateExpression(Expression expression, RecordBatch batch, IArrowType targetType);
}

/// <summary>
/// Pluggable registry for function calls invoked during row evaluation.
/// Format-specific function libraries (e.g. Spark SQL functions) implement
/// this interface to provide their own functions to the evaluator.
/// </summary>
public interface IFunctionRegistry
{
    /// <summary>Returns true if a function with the given name is registered.</summary>
    bool IsRegistered(string name);

    /// <summary>
    /// Invokes a function. Implementations must return an array of length
    /// <paramref name="rowCount"/>.
    /// </summary>
    IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount);
}

/// <summary>
/// A function registry that also knows how a comparison coerces a string operand.
/// </summary>
/// <remarks>
/// <para>
/// Optional, and separate from <see cref="IFunctionRegistry"/> because it answers a different
/// question. A registry says what <c>cast</c> does; this says when a comparison inserts one.
/// <see cref="ArrowRowEvaluator"/> asks for it with an <c>as</c> cast, so a registry that does
/// not implement it leaves comparison exactly as it was.
/// </para>
/// <para>
/// It exists because the rule is dialect-dependent and the evaluator has no dialect. Measured
/// against Spark 4.0, ANSI compares <c>'0.1'</c> against a <c>float</c> column through
/// <c>double</c> and the legacy dialect compares it as a <c>float</c> — the same expression,
/// two answers, and only the registry knows which. See the <c>string-coercion</c> group of
/// <c>Fixtures/spark-expression-corpus.json</c>.
/// </para>
/// </remarks>
public interface IComparisonCoercion
{
    /// <summary>
    /// Casts a string operand to the type it must take to be compared against a value of
    /// <paramref name="otherType"/>, or returns null when this pair takes no coercion and
    /// should be compared as it stands.
    /// </summary>
    /// <remarks>
    /// Returning null is not a failure: a string against a binary is a pair Spark does not
    /// resolve this way, and a string against a string needs nothing. Under a raising dialect
    /// this may throw rather than return, because a comparison against a value the cast refuses
    /// is a refused comparison.
    /// </remarks>
    IArrowArray? CoerceStringForComparison(IArrowArray strings, IArrowType otherType, int rowCount);
}
