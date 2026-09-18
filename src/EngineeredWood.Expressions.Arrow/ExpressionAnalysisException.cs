// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Arrow;

/// <summary>
/// Thrown when an expression is refused on type grounds by the registry's
/// <see cref="IAnalysisRules"/>, before any row of the batch is read.
/// </summary>
/// <remarks>
/// <para>
/// A refusal here is a property of the expression and its operand types: it does not depend on a
/// value, is the same for an empty batch as for a full one, and would be reported identically by
/// a definition-time pass with no batch at all. A registry's evaluation failures (such as
/// <c>SparkEvaluationException</c>) are the other kind — a value this row could not be
/// converted, added or divided — and a catch site deciding whether a write can be retried with
/// different data needs to tell the two apart.
/// </para>
/// <para>
/// Dialect-neutral, because <see cref="ArrowRowEvaluator"/> is: the registry decided the refusal
/// under its own dialect, and the evaluator only raises it with the
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
