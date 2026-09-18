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
/// There is no upstream contract to match. Delta pins no configuration when it validates a CHECK
/// constraint (<c>CheckDeltaInvariant</c> carries no <c>SQLConf</c>), so the same constraint
/// accepts or rejects the same row depending on the writing session: <c>a + b &lt; 0</c> over
/// <c>(2147483647, 1)</c> is accepted with ANSI off and raises <c>ARITHMETIC_OVERFLOW</c> with it
/// on. EngineeredWood has no session to inherit from, so it chooses and documents one.
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

    /// <summary>
    /// The timezone temporal conversions resolve against. Always UTC.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark resolves a timestamp against <c>spark.sql.session.timeZone</c>, and it changes
    /// answers: the instant 2026-08-11T03:00Z casts to the date <c>2026-08-11</c> under UTC and
    /// <c>2026-08-10</c> under <c>America/Los_Angeles</c>. A generated column defined as
    /// <c>CAST(ts AS DATE)</c> therefore stores a value that depends on the writing session, and
    /// Delta pins the zone no more than it pins ANSI. EngineeredWood has no session, so it fixes
    /// UTC: the harvested corpus is pinned to it, and <c>ArrowRowEvaluator</c> already reads a
    /// Date32 as UTC midnight of that day.
    /// </para>
    /// <para>
    /// It is not settable because honouring another zone needs more than this option. The parser
    /// resolves a zone-less <c>TIMESTAMP'…'</c> literal to an instant in
    /// <c>EngineeredWood.Expressions</c>, which cannot see these options, so the zone is defined
    /// there and this property only reports it. A configurable zone (#133) means carrying a
    /// setting to the parser as well.
    /// </para>
    /// </remarks>
    public static TimeZoneInfo TimeZone => SparkTemporalText.SessionTimeZone;
}
