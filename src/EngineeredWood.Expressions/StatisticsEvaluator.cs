// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Evaluates <see cref="Predicate"/> trees against aggregated column
/// statistics, producing three-valued <see cref="FilterResult"/> answers
/// suitable for skipping files / row groups / stripes that provably contain
/// no matching rows.
/// </summary>
/// <remarks>
/// The evaluator is generic over the statistics carrier — each format plugs
/// in an <see cref="IStatisticsAccessor{TStats}"/> for its own metadata
/// representation. The evaluator itself contains no format-specific logic.
///
/// Conservative: when statistics are missing or the predicate references
/// expressions that statistics can't evaluate (e.g. function calls, two-column
/// comparisons), the result is <see cref="FilterResult.Unknown"/>. Callers
/// should not skip data on Unknown.
/// </remarks>
public static class StatisticsEvaluator
{
    /// <summary>
    /// Evaluates a predicate against statistics for a unit of data.
    /// </summary>
    public static FilterResult Evaluate<TStats>(
        Predicate predicate,
        TStats stats,
        IStatisticsAccessor<TStats> accessor)
    {
        return EvaluatePredicate(predicate, stats, accessor);
    }

    private static FilterResult EvaluatePredicate<TStats>(
        Predicate predicate,
        TStats stats,
        IStatisticsAccessor<TStats> accessor)
    {
        return predicate switch
        {
            TruePredicate => FilterResult.AlwaysTrue,
            FalsePredicate => FilterResult.AlwaysFalse,
            AndPredicate and => EvaluateAnd(and, stats, accessor),
            OrPredicate or => EvaluateOr(or, stats, accessor),
            NotPredicate not => EvaluateNot(not, stats, accessor),
            ComparisonPredicate cmp => EvaluateComparison(cmp, stats, accessor),
            UnaryPredicate unary => EvaluateUnary(unary, stats, accessor),
            SetPredicate set => EvaluateSet(set, stats, accessor),
            _ => FilterResult.Unknown,
        };
    }

    // ── And / Or / Not ──

    private static FilterResult EvaluateAnd<TStats>(
        AndPredicate and, TStats stats, IStatisticsAccessor<TStats> accessor)
    {
        bool allTrue = true;
        foreach (var child in and.Children)
        {
            var r = EvaluatePredicate(child, stats, accessor);
            if (r == FilterResult.AlwaysFalse) return FilterResult.AlwaysFalse;
            if (r != FilterResult.AlwaysTrue) allTrue = false;
        }
        return allTrue ? FilterResult.AlwaysTrue : FilterResult.Unknown;
    }

    private static FilterResult EvaluateOr<TStats>(
        OrPredicate or, TStats stats, IStatisticsAccessor<TStats> accessor)
    {
        bool allFalse = true;
        foreach (var child in or.Children)
        {
            var r = EvaluatePredicate(child, stats, accessor);
            if (r == FilterResult.AlwaysTrue) return FilterResult.AlwaysTrue;
            if (r != FilterResult.AlwaysFalse) allFalse = false;
        }
        return allFalse ? FilterResult.AlwaysFalse : FilterResult.Unknown;
    }

    private static FilterResult EvaluateNot<TStats>(
        NotPredicate not, TStats stats, IStatisticsAccessor<TStats> accessor)
    {
        return EvaluatePredicate(not.Child, stats, accessor) switch
        {
            FilterResult.AlwaysTrue => FilterResult.AlwaysFalse,
            FilterResult.AlwaysFalse => FilterResult.AlwaysTrue,
            _ => FilterResult.Unknown,
        };
    }

    // ── Comparison predicates ──

    private static FilterResult EvaluateComparison<TStats>(
        ComparisonPredicate cmp, TStats stats, IStatisticsAccessor<TStats> accessor)
    {
        // Try to extract (column, op, value). The value may be on either side.
        if (TryExtractColumnAndLiteral(cmp.Left, cmp.Right, cmp.Op,
                out string? column, out LiteralValue value, out var op))
        {
            return EvaluateColumnComparison(column!, op, value, stats, accessor);
        }

        // Two literals — evaluate directly.
        if (cmp.Left is LiteralExpression la && cmp.Right is LiteralExpression lb)
        {
            return CompareLiterals(la.Value, cmp.Op, lb.Value)
                ? FilterResult.AlwaysTrue
                : FilterResult.AlwaysFalse;
        }

        // Anything else (function call, column op column, etc.) — can't decide.
        return FilterResult.Unknown;
    }

    private static FilterResult EvaluateColumnComparison<TStats>(
        string column,
        ComparisonOperator op,
        LiteralValue value,
        TStats stats,
        IStatisticsAccessor<TStats> accessor)
    {
        // Comparing against null? With Spark semantics, normal comparisons against
        // null produce null (treated as Unknown). Only NullSafeEqual handles null.
        if (value.IsNull)
        {
            if (op != ComparisonOperator.NullSafeEqual)
                return FilterResult.Unknown;
        }

        var min = accessor.GetMinValue(stats, column);
        var max = accessor.GetMaxValue(stats, column);
        long? nullCount = accessor.GetNullCount(stats, column);
        long? valueCount = accessor.GetValueCount(stats, column);

        // All-null column: any non-null comparison is Unknown (Spark) or AlwaysFalse
        // for NullSafeEqual unless the value is also null.
        bool allNull = nullCount.HasValue && valueCount.HasValue && nullCount == valueCount;
        if (allNull)
        {
            return op == ComparisonOperator.NullSafeEqual && value.IsNull
                ? FilterResult.AlwaysTrue
                : FilterResult.AlwaysFalse;
        }

        if (min is null || max is null)
            return FilterResult.Unknown;

        bool minExact = accessor.IsMinExact(stats, column);
        bool maxExact = accessor.IsMaxExact(stats, column);

        return op switch
        {
            ComparisonOperator.Equal or ComparisonOperator.NullSafeEqual =>
                EvaluateEqual(min.Value, max.Value, value, minExact, maxExact, nullCount),
            ComparisonOperator.NotEqual =>
                EvaluateNotEqual(min.Value, max.Value, value, minExact, maxExact, nullCount),
            ComparisonOperator.LessThan =>
                EvaluateLessThan(min.Value, max.Value, value, nullCount),
            ComparisonOperator.LessThanOrEqual =>
                EvaluateLessThanOrEqual(min.Value, max.Value, value, nullCount),
            ComparisonOperator.GreaterThan =>
                EvaluateGreaterThan(min.Value, max.Value, value, nullCount),
            ComparisonOperator.GreaterThanOrEqual =>
                EvaluateGreaterThanOrEqual(min.Value, max.Value, value, nullCount),
            ComparisonOperator.StartsWith =>
                EvaluateStartsWith(min.Value, max.Value, value, minExact, maxExact),
            ComparisonOperator.NotStartsWith => FilterResult.Unknown,
            _ => FilterResult.Unknown,
        };
    }

    private static FilterResult EvaluateEqual(
        LiteralValue min, LiteralValue max, LiteralValue v,
        bool minExact, bool maxExact, long? nullCount)
    {
        // AlwaysFalse: v outside [min, max]. Truncated bounds are still safe
        // (stored_min ≤ actual_min, stored_max ≥ actual_max), so v < stored_min
        // implies v < actual_min.
        int cmpVMin = SafeCompare(v, min);
        int cmpVMax = SafeCompare(v, max);
        if (cmpVMin == int.MinValue || cmpVMax == int.MinValue)
            return FilterResult.Unknown;

        if (cmpVMin < 0 || cmpVMax > 0)
            return FilterResult.AlwaysFalse;

        // AlwaysTrue requires: every value equals v. That means min == max == v
        // AND no nulls AND bounds are exact.
        if (minExact && maxExact && cmpVMin == 0 && cmpVMax == 0
            && nullCount.HasValue && nullCount.Value == 0)
        {
            return FilterResult.AlwaysTrue;
        }

        return FilterResult.Unknown;
    }

    private static FilterResult EvaluateNotEqual(
        LiteralValue min, LiteralValue max, LiteralValue v,
        bool minExact, bool maxExact, long? nullCount)
    {
        // AlwaysFalse: every value equals v (min == max == v exact, no nulls).
        int cmpVMin = SafeCompare(v, min);
        int cmpVMax = SafeCompare(v, max);
        if (cmpVMin == int.MinValue || cmpVMax == int.MinValue)
            return FilterResult.Unknown;

        if (minExact && maxExact && cmpVMin == 0 && cmpVMax == 0
            && nullCount.HasValue && nullCount.Value == 0)
        {
            return FilterResult.AlwaysFalse;
        }

        // AlwaysTrue: no value equals v. v must be strictly outside [min, max]
        // AND no nulls (because v != null is null in SQL, treated as Unknown).
        if (nullCount.HasValue && nullCount.Value == 0
            && (cmpVMin < 0 || cmpVMax > 0))
        {
            return FilterResult.AlwaysTrue;
        }

        return FilterResult.Unknown;
    }

    private static FilterResult EvaluateLessThan(
        LiteralValue min, LiteralValue max, LiteralValue v, long? nullCount)
    {
        int cmpVMin = SafeCompare(v, min);
        int cmpVMax = SafeCompare(v, max);
        if (cmpVMin == int.MinValue || cmpVMax == int.MinValue)
            return FilterResult.Unknown;

        // AlwaysFalse: min >= v (no value can be < v).
        if (cmpVMin <= 0)
            return FilterResult.AlwaysFalse;

        // AlwaysTrue: max < v AND no nulls.
        if (cmpVMax > 0 && nullCount.HasValue && nullCount.Value == 0)
            return FilterResult.AlwaysTrue;

        return FilterResult.Unknown;
    }

    private static FilterResult EvaluateLessThanOrEqual(
        LiteralValue min, LiteralValue max, LiteralValue v, long? nullCount)
    {
        int cmpVMin = SafeCompare(v, min);
        int cmpVMax = SafeCompare(v, max);
        if (cmpVMin == int.MinValue || cmpVMax == int.MinValue)
            return FilterResult.Unknown;

        // AlwaysFalse: min > v.
        if (cmpVMin < 0)
            return FilterResult.AlwaysFalse;

        // AlwaysTrue: max <= v AND no nulls.
        if (cmpVMax >= 0 && nullCount.HasValue && nullCount.Value == 0)
            return FilterResult.AlwaysTrue;

        return FilterResult.Unknown;
    }

    private static FilterResult EvaluateGreaterThan(
        LiteralValue min, LiteralValue max, LiteralValue v, long? nullCount)
    {
        int cmpVMin = SafeCompare(v, min);
        int cmpVMax = SafeCompare(v, max);
        if (cmpVMin == int.MinValue || cmpVMax == int.MinValue)
            return FilterResult.Unknown;

        // AlwaysFalse: max <= v.
        if (cmpVMax >= 0)
            return FilterResult.AlwaysFalse;

        // AlwaysTrue: min > v AND no nulls.
        if (cmpVMin < 0 && nullCount.HasValue && nullCount.Value == 0)
            return FilterResult.AlwaysTrue;

        return FilterResult.Unknown;
    }

    private static FilterResult EvaluateGreaterThanOrEqual(
        LiteralValue min, LiteralValue max, LiteralValue v, long? nullCount)
    {
        int cmpVMin = SafeCompare(v, min);
        int cmpVMax = SafeCompare(v, max);
        if (cmpVMin == int.MinValue || cmpVMax == int.MinValue)
            return FilterResult.Unknown;

        // AlwaysFalse: max < v.
        if (cmpVMax > 0)
            return FilterResult.AlwaysFalse;

        // AlwaysTrue: min >= v AND no nulls.
        if (cmpVMin <= 0 && nullCount.HasValue && nullCount.Value == 0)
            return FilterResult.AlwaysTrue;

        return FilterResult.Unknown;
    }

    private static FilterResult EvaluateStartsWith(
        LiteralValue min, LiteralValue max, LiteralValue prefix,
        bool minExact, bool maxExact)
    {
        // Only meaningful for strings. For STARTS WITH 'foo', we can prove
        // AlwaysFalse only when all values are lexicographically outside the
        // range [prefix, prefix+1) — i.e. max < prefix or min >= prefix+1.
        // Truncation makes this hard to do safely, so we keep it conservative.
        if (prefix.Type != LiteralValue.Kind.String
            || min.Type != LiteralValue.Kind.String
            || max.Type != LiteralValue.Kind.String)
        {
            return FilterResult.Unknown;
        }

        string p = prefix.AsString;
        string mn = min.AsString;
        string mx = max.AsString;

        // If max is strictly less than prefix and exact: AlwaysFalse.
        if (maxExact && string.CompareOrdinal(mx, p) < 0)
            return FilterResult.AlwaysFalse;

        // If min sorts strictly after the prefix range: AlwaysFalse.
        // The "after prefix" boundary is the smallest string that doesn't start
        // with the prefix. Computing it correctly across all Unicode code units
        // is non-trivial; conservatively skip this case.

        // AlwaysTrue: every value starts with the prefix. Requires both bounds
        // exact and both starting with the prefix.
        if (minExact && maxExact
            && mn.StartsWith(p, StringComparison.Ordinal)
            && mx.StartsWith(p, StringComparison.Ordinal))
        {
            return FilterResult.AlwaysTrue;
        }

        return FilterResult.Unknown;
    }

    // ── Unary predicates ──

    private static FilterResult EvaluateUnary<TStats>(
        UnaryPredicate unary, TStats stats, IStatisticsAccessor<TStats> accessor)
    {
        if (!TryGetColumnName(unary.Operand, out string? column))
            return FilterResult.Unknown;

        switch (unary.Op)
        {
            case UnaryOperator.IsNull:
            {
                long? nullCount = accessor.GetNullCount(stats, column!);
                long? valueCount = accessor.GetValueCount(stats, column!);
                if (nullCount.HasValue && nullCount.Value == 0)
                    return FilterResult.AlwaysFalse;
                if (nullCount.HasValue && valueCount.HasValue
                    && nullCount.Value == valueCount.Value)
                    return FilterResult.AlwaysTrue;
                return FilterResult.Unknown;
            }
            case UnaryOperator.IsNotNull:
            {
                long? nullCount = accessor.GetNullCount(stats, column!);
                long? valueCount = accessor.GetValueCount(stats, column!);
                if (nullCount.HasValue && nullCount.Value == 0)
                    return FilterResult.AlwaysTrue;
                if (nullCount.HasValue && valueCount.HasValue
                    && nullCount.Value == valueCount.Value)
                    return FilterResult.AlwaysFalse;
                return FilterResult.Unknown;
            }
            case UnaryOperator.IsNaN:
            case UnaryOperator.IsNotNaN:
                return EvaluateNaN(unary.Op, column!, stats, accessor);
            default:
                return FilterResult.Unknown;
        }
    }

    /// <summary>
    /// Resolves IsNaN / IsNotNaN using a NaN count when the accessor exposes one
    /// (<see cref="INanCountAccessor{TStats}"/>). NaN values are non-null, so a
    /// definitive AlwaysTrue/AlwaysFalse also requires the absence of nulls.
    /// </summary>
    private static FilterResult EvaluateNaN<TStats>(
        UnaryOperator op, string column, TStats stats, IStatisticsAccessor<TStats> accessor)
    {
        if (accessor is not INanCountAccessor<TStats> nanAccessor)
            return FilterResult.Unknown;

        long? nanCount = nanAccessor.GetNanCount(stats, column);
        if (!nanCount.HasValue)
            return FilterResult.Unknown; // unknown ⇒ NaNs may be present

        long? nullCount = accessor.GetNullCount(stats, column);
        long? valueCount = accessor.GetValueCount(stats, column);

        bool noNaN = nanCount.Value == 0;
        bool noNull = nullCount is 0;
        // Every non-null value is NaN (only decidable when both counts are known).
        bool allNaN = nullCount.HasValue && valueCount.HasValue && nanCount.Value > 0
            && nanCount.Value == valueCount.Value - nullCount.Value;

        if (op == UnaryOperator.IsNaN)
        {
            if (noNaN) return FilterResult.AlwaysFalse;
            if (allNaN && noNull) return FilterResult.AlwaysTrue;
            return FilterResult.Unknown;
        }
        else // IsNotNaN
        {
            if (noNaN && noNull) return FilterResult.AlwaysTrue;
            if (allNaN && noNull) return FilterResult.AlwaysFalse;
            return FilterResult.Unknown;
        }
    }

    // ── Set predicates ──

    private static FilterResult EvaluateSet<TStats>(
        SetPredicate set, TStats stats, IStatisticsAccessor<TStats> accessor)
    {
        if (!TryGetColumnName(set.Operand, out string? column))
            return FilterResult.Unknown;

        if (set.Values.Count == 0)
        {
            return set.Op == SetOperator.In
                ? FilterResult.AlwaysFalse
                : FilterResult.AlwaysTrue;
        }

        var min = accessor.GetMinValue(stats, column!);
        var max = accessor.GetMaxValue(stats, column!);
        if (min is null || max is null)
            return FilterResult.Unknown;

        // For IN: AlwaysFalse if every value in the set is outside [min, max].
        // For NOT IN: complement.
        bool allOutside = true;
        foreach (var v in set.Values)
        {
            int cmpVMin = SafeCompare(v, min.Value);
            int cmpVMax = SafeCompare(v, max.Value);
            if (cmpVMin == int.MinValue || cmpVMax == int.MinValue)
                return FilterResult.Unknown;

            if (cmpVMin >= 0 && cmpVMax <= 0)
            {
                allOutside = false;
                break;
            }
        }

        if (allOutside)
        {
            return set.Op == SetOperator.In
                ? FilterResult.AlwaysFalse
                : FilterResult.Unknown; // NOT IN being true requires no nulls
        }

        return FilterResult.Unknown;
    }

    // ── Helpers ──

    /// <summary>
    /// Returns true if the comparison can be reduced to (column, op, value),
    /// flipping the operator if the value is on the left.
    /// </summary>
    private static bool TryExtractColumnAndLiteral(
        Expression left, Expression right, ComparisonOperator op,
        out string? column, out LiteralValue value, out ComparisonOperator effectiveOp)
    {
        if (TryGetColumnName(left, out column) && right is LiteralExpression rl)
        {
            value = rl.Value;
            effectiveOp = op;
            return true;
        }

        if (left is LiteralExpression ll && TryGetColumnName(right, out column))
        {
            value = ll.Value;
            effectiveOp = FlipOperator(op);
            return true;
        }

        column = null;
        value = LiteralValue.Null;
        effectiveOp = op;
        return false;
    }

    private static bool TryGetColumnName(Expression expr, out string? name)
    {
        switch (expr)
        {
            case UnboundReference u:
                name = u.Name;
                return true;
            case BoundReference b:
                name = b.Name;
                return true;
            default:
                name = null;
                return false;
        }
    }

    private static ComparisonOperator FlipOperator(ComparisonOperator op) => op switch
    {
        ComparisonOperator.LessThan => ComparisonOperator.GreaterThan,
        ComparisonOperator.LessThanOrEqual => ComparisonOperator.GreaterThanOrEqual,
        ComparisonOperator.GreaterThan => ComparisonOperator.LessThan,
        ComparisonOperator.GreaterThanOrEqual => ComparisonOperator.LessThanOrEqual,
        // Symmetric ops are unchanged.
        _ => op,
    };

    /// <summary>
    /// Returns the comparison result, or <c>int.MinValue</c> on incompatible
    /// types — sentinel for "can't compare, treat as Unknown."
    /// </summary>
    private static int SafeCompare(LiteralValue a, LiteralValue b)
    {
        try
        {
            return a.CompareTo(b);
        }
        catch (InvalidOperationException)
        {
            return int.MinValue;
        }
    }

    private static bool CompareLiterals(LiteralValue a, ComparisonOperator op, LiteralValue b)
    {
        if (op == ComparisonOperator.NullSafeEqual)
            return a.Equals(b);

        if (a.IsNull || b.IsNull)
            return false; // SQL: comparison with NULL is NULL, treat as false

        int c = SafeCompare(a, b);
        if (c == int.MinValue) return false;

        return op switch
        {
            ComparisonOperator.Equal => c == 0,
            ComparisonOperator.NotEqual => c != 0,
            ComparisonOperator.LessThan => c < 0,
            ComparisonOperator.LessThanOrEqual => c <= 0,
            ComparisonOperator.GreaterThan => c > 0,
            ComparisonOperator.GreaterThanOrEqual => c >= 0,
            ComparisonOperator.StartsWith =>
                a.Type == LiteralValue.Kind.String && b.Type == LiteralValue.Kind.String
                && a.AsString.StartsWith(b.AsString, StringComparison.Ordinal),
            ComparisonOperator.NotStartsWith =>
                a.Type == LiteralValue.Kind.String && b.Type == LiteralValue.Kind.String
                && !a.AsString.StartsWith(b.AsString, StringComparison.Ordinal),
            _ => false,
        };
    }
}
