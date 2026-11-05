// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Optional extension of <see cref="IStatisticsAccessor{TStats}"/> for a format that keeps statistics per path
/// inside a variant column, so <see cref="StatisticsEvaluator"/> can prune on <c>variant_get(v, '$.a', 'int')</c>
/// and its forms (see <see cref="VariantGetCall"/>).
/// </summary>
public interface IVariantStatisticsAccessor<TStats>
{
    /// <summary>
    /// An accessor whose statistics for <see cref="VariantGetCall.Column"/> are those of
    /// <paramref name="call"/>'s result, or null when none can be given for that path and type.
    /// </summary>
    /// <remarks>
    /// <para>Its bounds must hold for every row whose result is not NULL, as literals of the type the result
    /// is, so that the evaluator compares them as it would a column of that type. A row the bounds do not
    /// describe has to be one whose result is NULL. And no row may be one whose conversion fails, since
    /// skipping it would turn the error the query raises into an answer.</para>
    /// <para>Its null count, if known, counts the rows whose RESULT is NULL, which include the rows where the
    /// path is missing.</para>
    /// </remarks>
    IStatisticsAccessor<TStats>? ForVariantGet(VariantGetCall call);
}
