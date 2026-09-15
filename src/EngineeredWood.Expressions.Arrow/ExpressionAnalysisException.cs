// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Arrow;

/// <summary>
/// Thrown when an expression is refused on type grounds by the registry's
/// <see cref="IAnalysisRules"/>, before any row of the batch is read.
/// </summary>
/// <remarks>
/// <para>
/// Separate from the registry's own evaluation failures, and deliberately so. A refusal here is a
/// property of the expression and its operand TYPES: it does not depend on a value, it is the same
/// for an empty batch as for a full one, and it would be reported identically by a
/// definition-time pass that has no batch at all. <c>SparkEvaluationException</c> reports the
/// other kind — a value this row could not be converted, added or divided — and those two are
/// worth telling apart at a catch site that decides whether a write can be retried with different
/// data.
/// </para>
/// <para>
/// Dialect-neutral, because <see cref="ArrowRowEvaluator"/> is. The registry has already decided,
/// under whatever dialect it implements, that the expression is refused; the evaluator only
/// carries the answer out, and names the condition with the
/// <see cref="AnalysisDiagnostic.ErrorClass"/> the registry supplied.
/// </para>
/// </remarks>
public sealed class ExpressionAnalysisException : Exception
{
    internal ExpressionAnalysisException(AnalysisDiagnostic diagnostic)
        : base($"[{diagnostic.ErrorClass}] {diagnostic.Message}")
    {
        ErrorClass = diagnostic.ErrorClass;
    }

    /// <summary>The refusing engine's error class, without the surrounding brackets.</summary>
    public string ErrorClass { get; }
}
