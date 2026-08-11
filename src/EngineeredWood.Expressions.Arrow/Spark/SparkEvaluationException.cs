// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// Thrown when evaluating a Spark function fails under ANSI semantics.
/// </summary>
/// <remarks>
/// <see cref="ErrorClass"/> carries Spark's own name for the failure —
/// <c>ARITHMETIC_OVERFLOW</c>, <c>DIVIDE_BY_ZERO</c>, <c>CAST_INVALID_INPUT</c> — so a caller
/// reporting a refused write can name the same condition the user would have seen from Spark,
/// and so the corpus's recorded failures can be matched against ours by class rather than by
/// message text.
///
/// With <see cref="SparkDialectOptions.Ansi"/> false, these conditions produce null instead and
/// nothing is thrown.
/// </remarks>
public sealed class SparkEvaluationException : Exception
{
    internal SparkEvaluationException(string errorClass, string message)
        : base($"[{errorClass}] {message}")
    {
        ErrorClass = errorClass;
    }

    /// <summary>Spark's error class for this failure, without the surrounding brackets.</summary>
    public string ErrorClass { get; }

    /// <summary>
    /// Arithmetic overflow, in Spark's two flavours.
    /// </summary>
    /// <remarks>
    /// Spark reports a result too wide for <c>int</c> or <c>bigint</c> as
    /// <c>ARITHMETIC_OVERFLOW</c>, but one too wide for <c>tinyint</c> or <c>smallint</c> as
    /// <c>BINARY_ARITHMETIC_OVERFLOW</c> — measured, <c>bigint * bigint</c> gives the first and
    /// <c>smallint * smallint</c> the second. The split is preserved so a reported failure names
    /// the same condition Spark would have named.
    /// </remarks>
    internal static SparkEvaluationException Overflow(bool narrowerThanInt, string detail) =>
        new(narrowerThanInt ? "BINARY_ARITHMETIC_OVERFLOW" : "ARITHMETIC_OVERFLOW", detail);

    internal static SparkEvaluationException DivideByZero() =>
        new("DIVIDE_BY_ZERO", "Division by zero. Use `try_divide` to tolerate a zero divisor.");

    internal static SparkEvaluationException InvalidCast(string value, string targetType) =>
        new("CAST_INVALID_INPUT", $"The value '{value}' cannot be cast to \"{targetType}\".");

    /// <summary>A cast whose input is well-formed but does not fit the target.</summary>
    /// <remarks>
    /// Spark separates this from <c>ARITHMETIC_OVERFLOW</c>: <c>CAST(1e30 AS INT)</c> and
    /// <c>CAST(300 AS TINYINT)</c> both report <c>CAST_OVERFLOW</c>.
    /// </remarks>
    internal static SparkEvaluationException CastOverflow(string value, string targetType) =>
        new("CAST_OVERFLOW", $"The value '{value}' does not fit \"{targetType}\".");
}
