// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The <c>commitInfo.operationParameters</c> a DELETE, UPDATE, MERGE or replaceWhere records (#591): what the
/// operation was asked to do, under Spark's keys and in Spark's shapes.
/// </summary>
/// <remarks>
/// <para>Measured on Spark 4.0.3 / Delta 4.0.0. A DELETE or UPDATE records <c>predicate</c>, a JSON array of
/// condition strings (<c>["(id = 2)"]</c>). A MERGE records its ON condition the same way, and each clause list
/// as an array of <c>{"predicate": …, "actionType": …}</c> with <c>predicate</c> left out of an unconditional
/// clause. A replaceWhere records <c>mode</c> <c>Overwrite</c>, <c>partitionBy</c> <c>[]</c> (the writer's own
/// <c>partitionBy</c> option, which this library has none of), and the condition as a plain string.</para>
///
/// <para>The condition text is Spark SQL from <see cref="SparkSqlRenderer"/>, which reads back as the same
/// condition. That is not Spark's text for a DML condition. Spark records the optimized Catalyst tree's
/// <c>toString</c>, with expression ids and unquoted strings (<c>(id#494L = 2)</c>, <c>(value#5725 = bq)</c>,
/// <c>StartsWith(value#52516, b)</c>), which no other engine could reproduce and no parser reads. Spark records
/// its own SQL rendering only for <c>INSERT … REPLACE WHERE</c> (<c>((id &gt;= 1L) AND (value = 'a'))</c>), and
/// that is the form used here for all of them.</para>
///
/// <para>A condition that cannot be rendered records no parameters at all. That covers a delegate predicate
/// (a <c>Func</c> over batches), a host's row selection, and a tree <see cref="SparkSqlRenderer"/> refuses. An
/// empty array or a placeholder would read as a condition: for an UPDATE, <c>[]</c> is what Spark records when
/// there is no WHERE clause.</para>
/// </remarks>
internal static class DmlOperationParameters
{
    // Spark writes these nested JSON values with Jackson, which escapes only what JSON requires. The default
    // encoder would also escape '<', '>', '\'' and every non-ASCII character, turning (id <= 1) into
    // (id <= 1).
    private static readonly JsonWriterOptions Relaxed = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    /// <summary>A DELETE's or UPDATE's condition as Spark SQL, or null for none (a delegate, a row selection) or one
    /// with no Spark SQL.</summary>
    /// <remarks>Called when the operation is staged, not when it commits: what is recorded is then the condition the
    /// operation ran with, whatever its caller does with the objects afterwards.</remarks>
    internal static string? Render(Predicate? condition) =>
        condition is not null && SparkSqlRenderer.TryRender(condition, out string? sql) ? sql : null;

    /// <summary>A DELETE's or UPDATE's parameters: each staged condition (from <see cref="Render"/>), in order. Null
    /// when any is null.</summary>
    internal static IReadOnlyDictionary<string, string>? ForConditions(IReadOnlyList<string?> conditions)
    {
        if (conditions.Count == 0)
            return null;

        var rendered = new string[conditions.Count];
        for (int i = 0; i < conditions.Count; i++)
        {
            if (conditions[i] is not { } sql)
                return null;
            rendered[i] = sql;
        }

        return new Dictionary<string, string>(StringComparer.Ordinal) { ["predicate"] = StringArray(rendered) };
    }

    /// <summary>A MERGE's parameters. Null when a clause condition has no Spark SQL. Called when the MERGE is staged:
    /// <see cref="MergeOptions"/> holds lists its caller may change afterwards.</summary>
    internal static IReadOnlyDictionary<string, string>? ForMerge(MergeOptions options)
    {
        var on = new List<Predicate>(options.Keys.Count);
        foreach (string key in options.Keys)
        {
            on.Add(new ComparisonPredicate(
                UnboundReference.FromParts(["target", key]), ComparisonOperator.Equal,
                UnboundReference.FromParts(["source", key])));
        }
        if (!SparkSqlRenderer.TryRender(Expressions.Expressions.And([.. on]), out string? onSql))
            return null;

        if (Clauses(options.WhenMatched) is not { } matched
            || Clauses(options.WhenNotMatched) is not { } notMatched
            || Clauses(options.WhenNotMatchedBySource) is not { } notMatchedBySource)
        {
            return null;
        }

        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["predicate"] = StringArray([onSql]),
            ["matchedPredicates"] = matched,
            ["notMatchedPredicates"] = notMatched,
            ["notMatchedBySourcePredicates"] = notMatchedBySource,
        };
    }

    /// <summary>A replaceWhere's parameters. Null when the condition has no Spark SQL.</summary>
    internal static IReadOnlyDictionary<string, string>? ForReplaceWhere(Predicate condition) =>
        SparkSqlRenderer.TryRender(condition, out string? sql)
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["mode"] = "Overwrite",
                ["partitionBy"] = "[]",
                ["predicate"] = sql,
            }
            : null;

    private static string? Clauses(IReadOnlyList<MergeAction> actions)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, Relaxed))
        {
            writer.WriteStartArray();
            foreach (var action in actions)
            {
                writer.WriteStartObject();
                if (action.Condition is { } condition)
                {
                    if (!SparkSqlRenderer.TryRender(condition, out string? sql))
                        return null;
                    writer.WriteString("predicate", sql);
                }
                writer.WriteString("actionType", action.Kind switch
                {
                    MergeActionKind.Update => "update",
                    MergeActionKind.Delete => "delete",
                    _ => "insert",
                });
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static string StringArray(IReadOnlyList<string> values)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, Relaxed))
        {
            writer.WriteStartArray();
            foreach (string value in values)
                writer.WriteStringValue(value);
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
