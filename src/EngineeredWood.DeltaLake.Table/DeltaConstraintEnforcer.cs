// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Evaluates a table's CHECK constraints and column invariants against the rows being written.
/// </summary>
/// <remarks>
/// <para>
/// Delta enforces these at write time only, so a single unvalidated commit poisons the table for
/// every later reader. That is why the rule here is refuse-rather-than-guess throughout: an
/// expression that cannot be parsed refuses the write exactly as it did before any of this
/// existed, and only an expression we can actually evaluate lets a write proceed.
/// </para>
/// <para>
/// A row satisfies a rule only when it evaluates to <c>true</c>. Null violates. That is the
/// protocol's wording — "evaluating the SQL expressions of CHECK constraints must return
/// <c>true</c> for each row in a table" — and delta-spark's own <c>CheckConstraintsSuite</c>
/// asserts a violation for a null-valued expression across three separate null origins.
/// </para>
/// </remarks>
internal sealed class DeltaConstraintEnforcer
{
    /// <summary>One rule to check, with enough context to name it in a failure.</summary>
    private readonly record struct Rule(string Description, string Sql, Predicate Predicate);

    private static readonly IReadOnlyDictionary<string, string> EmptyConfiguration =
        new Dictionary<string, string>();

    private readonly IReadOnlyList<Rule> _rules;
    private readonly ArrowRowEvaluator _evaluator;

    private DeltaConstraintEnforcer(IReadOnlyList<Rule> rules)
    {
        _rules = rules;
        _evaluator = new ArrowRowEvaluator(new SparkFunctionRegistry());
    }

    /// <summary>
    /// Builds an enforcer for a snapshot, or null when the table declares nothing to enforce.
    /// </summary>
    /// <exception cref="DeltaFormatException">
    /// A constraint or invariant is declared that this writer cannot parse. The write is refused
    /// rather than committed unvalidated.
    /// </exception>
    public static DeltaConstraintEnforcer? Create(Snapshot.Snapshot snapshot)
    {
        var rules = new List<Rule>();

        foreach (var pair in snapshot.Metadata.Configuration ?? EmptyConfiguration)
        {
            if (!pair.Key.StartsWith("delta.constraints.", StringComparison.Ordinal))
                continue;

            rules.Add(Parse($"CHECK constraint '{pair.Key}'", pair.Value));
        }

        foreach (var field in snapshot.ArrowSchema.FieldsList)
        {
            if (field.Metadata is null
                || !field.Metadata.TryGetValue("delta.invariants", out var invariant))
            {
                continue;
            }

            rules.Add(Parse(
                $"invariant on column '{field.Name}'", UnwrapInvariant(field.Name, invariant)));
        }

        return rules.Count == 0 ? null : new DeltaConstraintEnforcer(rules);
    }

    /// <summary>
    /// Whether a snapshot declares anything this class would enforce.
    /// </summary>
    /// <remarks>
    /// Deliberately does not parse. It answers "is there a rule here", which the write gate needs
    /// before it knows whether the caller intends to validate; parsing failures belong to
    /// <see cref="Create"/>, where refusing them is the point.
    /// </remarks>
    public static bool Declares(Snapshot.Snapshot snapshot)
    {
        foreach (var key in (snapshot.Metadata.Configuration ?? EmptyConfiguration).Keys)
        {
            if (key.StartsWith("delta.constraints.", StringComparison.Ordinal))
                return true;
        }

        foreach (var field in snapshot.ArrowSchema.FieldsList)
        {
            if (field.Metadata is not null && field.Metadata.ContainsKey("delta.invariants"))
                return true;
        }

        return false;
    }

    /// <summary>Refuses the write if any row of <paramref name="batch"/> fails a rule.</summary>
    /// <exception cref="DeltaFormatException">A row does not satisfy a declared rule.</exception>
    public void Validate(RecordBatch batch)
    {
        foreach (var rule in _rules)
        {
            BooleanArray result;
            try
            {
                result = _evaluator.EvaluatePredicate(rule.Predicate, batch);
            }
            catch (Exception ex) when (ex is not DeltaFormatException)
            {
                // An expression that parses can still fail to evaluate — an unimplemented
                // function, a column type the evaluator does not cover, an ANSI overflow inside
                // the constraint itself. All of them mean the same thing to the caller: the rule
                // could not be checked, so the write must not proceed.
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.UnevaluableTableExpression,
                    $"{rule.Description} ({rule.Sql}) could not be evaluated against the data "
                    + $"being written, so the write was refused: {ex.Message}",
                    ex);
            }

            for (var row = 0; row < batch.Length; row++)
            {
                var satisfied = result.GetValue(row);
                if (satisfied is true)
                    continue;

                throw new DeltaFormatException(
                    DeltaTableErrorCodes.ConstraintViolated,
                    $"{rule.Description} ({rule.Sql}) is violated by a row being written"
                    + (satisfied is null
                        ? " — the expression evaluated to null, which the protocol requires be "
                          + "treated as a violation rather than a pass."
                        : ".")
                    + " No data was committed.");
            }
        }
    }

    private static Rule Parse(string description, string sql)
    {
        try
        {
            return new Rule(description, sql, SparkSqlParser.ParsePredicate(sql));
        }
        catch (SparkSqlParseException ex)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.UnevaluableTableExpression,
                $"Table declares {description}, which this writer cannot parse, so the write was "
                + $"refused rather than committed unvalidated: {ex.Reason} at position {ex.Position} "
                + $"in: {sql}",
                ex);
        }
    }

    /// <summary>
    /// Reads the SQL out of a <c>delta.invariants</c> value.
    /// </summary>
    /// <remarks>
    /// Invariants are not stored the way CHECK constraints are. A constraint's value is the SQL
    /// itself, while an invariant wraps it in JSON — <c>{"expression":{"expression":"id &gt; 0"}}</c>
    /// — a shape inherited from the legacy writer-version-2 feature that predates
    /// <c>checkConstraints</c>.
    /// </remarks>
    private static string UnwrapInvariant(string column, string json)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            if (document.RootElement.TryGetProperty("expression", out var outer)
                && outer.TryGetProperty("expression", out var inner)
                && inner.GetString() is { } sql)
            {
                return sql;
            }
        }
        catch (JsonException)
        {
            // Fall through to the same refusal as a missing expression.
        }

        throw new DeltaFormatException(
            DeltaTableErrorCodes.UnevaluableTableExpression,
            $"Column '{column}' declares delta.invariants that is not of the form "
            + $"{{\"expression\":{{\"expression\":\"…\"}}}}, so the write was refused rather than "
            + $"committed unvalidated. Found: {json}");
    }
}
