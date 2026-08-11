// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// The Spark evaluation semantics a <see cref="SparkFunctionRegistry"/> implements.
/// </summary>
/// <remarks>
/// Bound when the registry is constructed rather than chosen by a parser or threaded through
/// evaluation — see "Where the dialect configuration lives" in
/// <c>doc/predicate-pushdown-design.md</c>.
///
/// There is no upstream contract to match here. Delta pins no configuration when it validates a
/// CHECK constraint: <c>CheckDeltaInvariant</c> carries no <c>SQLConf</c> reference, and the same
/// constraint accepts or rejects the same row depending on the writing session — measured,
/// <c>a + b &lt; 0</c> over <c>(2147483647, 1)</c> is accepted with ANSI off and raises
/// <c>ARITHMETIC_OVERFLOW</c> with it on. EngineeredWood has no session to inherit from, so it
/// chooses and documents one instead.
/// </remarks>
public sealed record SparkDialectOptions
{
    /// <summary>The default: ANSI semantics, matching what Spark 4 ships.</summary>
    public static SparkDialectOptions Default { get; } = new();

    /// <summary>
    /// Whether arithmetic overflow, division by zero, and invalid casts raise rather than
    /// producing null.
    /// </summary>
    /// <remarks>
    /// True by default. Spark 4.0 defaults <c>spark.sql.ansi.enabled</c> to true, so it is what a
    /// current-generation writer produces, and the harvested corpus is pinned to it. Setting this
    /// false selects the legacy behaviour, where the same expressions yield null instead.
    /// </remarks>
    public bool Ansi { get; init; } = true;
}
