// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Proofs that a predicate is TRUE for every row of a unit, which pruning never needs and the evaluator does not
/// promise. Used where statistics stand in for reading the rows: a DELETE removing a file unread, ADD CONSTRAINT
/// skipping one, a scan dropping a predicate for a row group (#502 phase 5c).
/// </summary>
public static partial class StatisticsEvaluator
{
    /// <summary>
    /// Whether <paramref name="predicate"/> is TRUE for every row of the unit, as far as its statistics prove: AND needs
    /// every child proven, OR one, and any other part must be a leaf <see cref="ProvesLeafEveryRowTrue"/> proves. NOT
    /// and everything else is not proven.
    /// </summary>
    public static bool ProvesEveryRowTrue<TStats>(
        Predicate predicate, TStats stats, IStatisticsAccessor<TStats> accessor) => predicate switch
    {
        TruePredicate => true,
        AndPredicate and => and.Children.Count > 0 && and.Children.All(c => ProvesEveryRowTrue(c, stats, accessor)),
        OrPredicate or => or.Children.Any(c => ProvesEveryRowTrue(c, stats, accessor)),
        _ => ProvesLeafEveryRowTrue(predicate, stats, accessor),
    };

    /// <summary>
    /// Whether a leaf (a comparison of a top-level column with a literal of the column's own type, or
    /// <c>IS [NOT] NULL</c> of one) is TRUE for every row of the unit. <c>IN</c> is never proven: for a column whose
    /// bounds meet, write <c>=</c>.
    /// </summary>
    /// <remarks>
    /// <para><see cref="Evaluate{TStats}"/> is built for pruning, where AlwaysFalse means "no row is TRUE", NULL rows
    /// included. That makes its AlwaysTrue unsound in two places a proof must not follow: under NOT, which turns such
    /// an AlwaysFalse into AlwaysTrue, and for a prefix test, which it answers without the null count. So a caller
    /// combines leaves itself (AND and OR only, <see cref="ProvesEveryRowTrue"/>), and every leaf but <c>IS NULL</c>
    /// also needs a null count of 0: a NULL row answers NULL, which is not TRUE.</para>
    /// <para>A literal of another type would hand the decision to cross-type comparison rules a row evaluator need not
    /// share, so it is not answered. A NaN outside the bounds is the evaluator's to rule on: it answers Unknown when
    /// one may hide there and would answer otherwise.</para>
    /// </remarks>
    public static bool ProvesLeafEveryRowTrue<TStats>(
        Predicate leaf, TStats stats, IStatisticsAccessor<TStats> accessor)
    {
        Expression operand;
        IReadOnlyList<Expression> literals;
        bool isNull = false;
        switch (leaf)
        {
            case ComparisonPredicate cmp when cmp.Right is LiteralExpression:
                operand = cmp.Left;
                literals = [cmp.Right];
                break;
            // A prefix test does not turn round (see TryExtractColumnAndLiteral).
            case ComparisonPredicate cmp when cmp.Left is LiteralExpression
                && cmp.Op is not (ComparisonOperator.StartsWith or ComparisonOperator.NotStartsWith):
                operand = cmp.Right;
                literals = [cmp.Left];
                break;
            case UnaryPredicate { Op: UnaryOperator.IsNull or UnaryOperator.IsNotNull } unary:
                operand = unary.Operand;
                literals = [];
                isNull = unary.Op == UnaryOperator.IsNull;
                break;
            default:
                return false;
        }

        if (operand is not UnboundReference { NameParts.Count: 1 } reference)
            return false;
        var column = reference.NameParts;

        if (literals.Count > 0)
        {
            if (accessor.GetMinValue(stats, column) is not { } min)
                return false;
            foreach (var literal in literals)
            {
                if (literal is not LiteralExpression { Value: var value } || value.Type != min.Type)
                    return false;
            }
        }

        if (Evaluate(leaf, stats, accessor) != FilterResult.AlwaysTrue)
            return false;
        return isNull || accessor.GetNullCount(stats, column) == 0;
    }
}
