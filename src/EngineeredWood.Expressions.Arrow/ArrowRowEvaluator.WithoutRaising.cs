// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Expressions.Arrow;

/// <summary>
/// Answering a predicate over rows it may never have been asked about (#502 phase 5a).
/// </summary>
public sealed partial class ArrowRowEvaluator
{
    /// <summary>
    /// <paramref name="predicate"/>'s answer for every row of <paramref name="batch"/>, when it can be computed by a
    /// path on which no row's value can raise; null otherwise, having read no value.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A Parquet row filter hands each conjunct after the first only the rows the ones before it left live, which
    /// costs a gather of its columns, because a conjunct asked about a row an earlier one rejected could raise there
    /// (an ANSI cast), where the whole expression would not. A conjunct that cannot raise on any value may instead be
    /// asked about every row, with no gather: its answers for rows no longer live are ignored.
    /// </para>
    /// <para>
    /// Covered: a comparison of a column with a constant that the direct kernels answer (<c>TryCompareColumnToConstant</c>,
    /// which reads the column's buffers and never converts a row's value), <c>IS [NOT] NULL</c> of a column, and AND, OR
    /// and NOT of these. Anything else declines, and so does a covered form the direct path declines for this batch (a
    /// column type with no kernel, a coercion that would cast the column).
    /// </para>
    /// <para>
    /// Every operand is evaluated over every row, so AND and OR are Kleene's, as the short-circuiting evaluation
    /// answers too. An empty batch is answered by <see cref="EvaluatePredicate"/> itself, which over no rows reads no
    /// row's value.
    /// </para>
    /// <para>What it promises:</para>
    /// <list type="bullet">
    /// <item><description>It never raises an error a row's value causes.</description></item>
    /// <item><description>An answer is exactly <see cref="EvaluatePredicate"/>'s answer for the same batch.</description></item>
    /// <item><description>An error it does raise is one <see cref="EvaluatePredicate"/> raises for the same batch (an
    /// analysis error met on the way).</description></item>
    /// <item><description>A decline is not a verdict on the predicate. It may decline one that
    /// <see cref="EvaluatePredicate"/> would refuse (an unresolvable column, a pair of types with no comparison, a form it
    /// does not cover) without checking it. A caller that is declined must ask <see cref="EvaluatePredicate"/> instead,
    /// over the rows it needs answered (over no rows if none), and that call raises any such error (review rounds 2 and
    /// 3).</description></item>
    /// </list>
    /// </remarks>
    public BooleanArray? TryEvaluateWithoutRaising(Predicate predicate, RecordBatch batch)
    {
        if (!MayEvaluateWithoutRaising(predicate))
            return null;
        if (batch.Length == 0)
            return EvaluatePredicate(predicate, batch);

        // The commonest conjunct: the kernel writes the answer's bitmaps itself.
        if (predicate is ComparisonPredicate cmp)
            return TryCompareColumnToConstant(cmp, batch, CheckComparableFromTree(cmp, batch))?.ToBooleanArray();

        return TryAnswer(predicate, batch) is { } answer ? ToArray(answer, batch.Length) : null;
    }

    /// <summary>
    /// Whether <see cref="TryEvaluateWithoutRaising"/> may answer <paramref name="predicate"/>, from its form alone: false
    /// means it always declines, so a caller need not prepare a batch for it. True does not promise an answer; the column
    /// types of a batch can still make it decline.
    /// </summary>
    public static bool MayEvaluateWithoutRaising(Predicate predicate) => predicate switch
    {
        // A column against a constant, as the direct path takes it; anything else would be shaped only to decline
        // (review round 1). Whether a constant call's functions are foldable depends on the registry, and is left to
        // the direct path.
        ComparisonPredicate cmp =>
            (IsReference(cmp.Left) && IsStructuralConstant(cmp.Right))
            || (IsStructuralConstant(cmp.Left) && IsReference(cmp.Right)
                && cmp.Op is not (ComparisonOperator.StartsWith or ComparisonOperator.NotStartsWith)),
        UnaryPredicate { Op: UnaryOperator.IsNull or UnaryOperator.IsNotNull } unary => IsReference(unary.Operand),
        NotPredicate not => MayEvaluateWithoutRaising(not.Child),
        AndPredicate and => and.Children.Count > 0 && and.Children.All(MayEvaluateWithoutRaising),
        OrPredicate or => or.Children.Count > 0 && or.Children.All(MayEvaluateWithoutRaising),
        _ => false,
    };

    // A constant operand by its structure: a non-null literal, or a call reading no column.
    private static bool IsStructuralConstant(Expression expression) => expression switch
    {
        LiteralExpression literal => !literal.Value.IsNull,
        FunctionCall call => IsConstant(call),
        _ => false,
    };

    // Answers as (value, known) bit pairs: one byte per row each, value meaningful only where known.
    private (byte[] Values, byte[] Known)? TryAnswer(Predicate predicate, RecordBatch batch)
    {
        int rows = batch.Length;
        switch (predicate)
        {
            case ComparisonPredicate cmp:
            {
                bool asked = CheckComparableFromTree(cmp, batch);
                if (TryCompareColumnToConstant(cmp, batch, asked) is not { } direct)
                    return null;
                return FromNullable(direct.ToNullable());
            }

            case UnaryPredicate unary:
                return FromNullable(EvalUnary(unary, batch));

            case NotPredicate not:
            {
                if (TryAnswer(not.Child, batch) is not { } child)
                    return null;
                for (int i = 0; i < rows; i++)
                    child.Values[i] ^= 1;
                return child;
            }

            case AndPredicate and:
                return Combine(and.Children, batch, isAnd: true);

            case OrPredicate or:
                return Combine(or.Children, batch, isAnd: false);

            default:
                return null;
        }
    }

    // Kleene AND / OR: the dominant answer (FALSE for AND, TRUE for OR) wins over NULL.
    private (byte[] Values, byte[] Known)? Combine(IReadOnlyList<Predicate> children, RecordBatch batch, bool isAnd)
    {
        int rows = batch.Length;
        byte dominant = isAnd ? (byte)0 : (byte)1;
        var values = new byte[rows];
        var known = new byte[rows];
        for (int i = 0; i < rows; i++)
        {
            values[i] = (byte)(1 - dominant);
            known[i] = 1;
        }

        foreach (var child in children)
        {
            if (TryAnswer(child, batch) is not { } answer)
                return null;
            for (int i = 0; i < rows; i++)
            {
                if (known[i] == 1 && values[i] == dominant)
                    continue;
                if (answer.Known[i] == 1 && answer.Values[i] == dominant)
                {
                    values[i] = dominant;
                    known[i] = 1;
                }
                else if (answer.Known[i] == 0)
                {
                    known[i] = 0;
                }
            }
        }

        return (values, known);
    }

    private static (byte[] Values, byte[] Known) FromNullable(bool?[] answers)
    {
        var values = new byte[answers.Length];
        var known = new byte[answers.Length];
        for (int i = 0; i < answers.Length; i++)
        {
            if (answers[i] is bool b)
            {
                known[i] = 1;
                values[i] = b ? (byte)1 : (byte)0;
            }
        }

        return (values, known);
    }

    private static BooleanArray ToArray((byte[] Values, byte[] Known) answer, int rows)
    {
        var builder = new BooleanArray.Builder().Reserve(rows);
        for (int i = 0; i < rows; i++)
        {
            if (answer.Known[i] == 0)
                builder.AppendNull();
            else
                builder.Append(answer.Values[i] == 1);
        }

        return builder.Build();
    }
}
