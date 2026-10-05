// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Refuses schema changes that would break something else declared against a column: a CHECK
/// constraint, a generation expression, or the clustering spec.
/// </summary>
/// <remarks>
/// <para>
/// Constraints and generation expressions bind by NAME, and both are evaluated on every write. A
/// DROP of a column one reads leaves the table unwritable; a RENAME leaves it reading a name that
/// is gone, and once a new column takes that name, it silently reads the wrong column. Spark
/// refuses both rather than rewriting the SQL, and so does this.
/// </para>
/// <para>
/// The match is Spark's (<c>SchemaUtils.containsDependentExpression</c>): an expression depends
/// on a change when the changed column IS one of its references or a prefix of one, compared
/// case-insensitively. An expression referring to <c>s</c> as a whole does not depend on a DROP
/// of <c>s.b</c>. The parser joins a nested access into one dotted name, which a quoted name
/// containing a dot also produces, so the comparison is on the dotted text — over-refusing only
/// when a quoted name happens to spell another column's path.
/// </para>
/// </remarks>
internal static class SchemaChangeDependents
{
    private const string ConstraintPrefix = "delta.constraints.";
    private const string GenerationExpressionKey = "delta.generationExpression";
    internal const string ClusteringDomain = "delta.clustering";

    private static readonly IReadOnlyDictionary<string, string> EmptyConfiguration =
        new Dictionary<string, string>();

    /// <summary>
    /// Throws if anything depends on the field at <paramref name="path"/> (LOGICAL names, as they
    /// stand in <paramref name="schema"/>), which a RENAME or DROP is about to change.
    /// </summary>
    public static void EnsureChangeable(
        StructType schema, MetadataAction metadata, IReadOnlyDictionary<string, DomainMetadata> domains,
        IReadOnlyList<string> path, bool isDrop)
    {
        string verb = isDrop ? "drop" : "rename";
        string target = string.Join(".", path);

        foreach (var (key, sql) in Constraints(metadata))
        {
            if (References($"CHECK constraint '{key}'", sql).Any(r => DependsOn(r, target)))
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.ConstraintDependentColumnChange,
                    $"Cannot {verb} column '{target}': CHECK constraint '{key}' ({sql}) reads it. "
                    + "Drop the constraint first.");
            }
        }

        foreach (var field in schema.Fields)
        {
            if (field.Metadata is null
                || !field.Metadata.TryGetValue(GenerationExpressionKey, out var sql))
            {
                continue;
            }

            if (References($"generated column '{field.Name}'", sql).Any(r => DependsOn(r, target)))
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.GeneratedColumnsDependentColumnChange,
                    $"Cannot {verb} column '{target}': generated column '{field.Name}' is computed "
                    + $"from it ({sql}).");
            }
        }

        // A RENAME keeps the physical name, which is what the clustering spec stores.
        if (isDrop && ClusteringPaths(domains) is { } clustering)
        {
            var mode = ColumnMapping.GetMode(metadata.Configuration);
            var physical = PhysicalPath(schema, path, mode);
            foreach (var column in clustering)
            {
                // A spec path the schema cannot resolve might be anything — a stale name, or another
                // writer's logical one — including the column being dropped, so it fails closed.
                if (TranslatePath(schema, column, f => ColumnMapping.GetPhysicalName(f, mode), f => f.Name) is null)
                {
                    throw new DeltaFormatException(
                        DeltaTableErrorCodes.UnsupportedDropClusteringColumn,
                        $"Cannot drop column '{target}': the {ClusteringDomain} domain names "
                        + $"'{string.Join(".", column)}', which is not a column of the table, so whether "
                        + "the drop affects clustering cannot be told. Re-declare the clustering columns first.");
                }

                if (IsPrefix(physical, column))
                {
                    throw new DeltaFormatException(
                        DeltaTableErrorCodes.UnsupportedDropClusteringColumn,
                        $"Cannot drop column '{target}': the table is clustered by it. Change the "
                        + "clustering columns first.");
                }
            }
        }
    }

    /// <summary>
    /// Throws if a schema replacement leaves an expression reading a column that
    /// <paramref name="newSchema"/> does not have: a CHECK constraint in <paramref name="metadata"/>
    /// (which the replacement keeps), or a generation expression declared in
    /// <paramref name="newSchema"/> itself. The schema-replacement form of
    /// <see cref="EnsureChangeable"/>.
    /// </summary>
    /// <remarks>
    /// A reference must keep the binding it had in <paramref name="oldSchema"/>. The parser's dotted
    /// text cannot say whether <c>`a.b`</c> was one quoted column or <c>a</c>'s field <c>b</c>, so
    /// accepting any split that resolves would let a replacement swap the one for the other, and the
    /// write path, which binds the old way, would then fail. A reference the old schema cannot bind
    /// (a new generated column's, say) only has to resolve somehow.
    /// </remarks>
    public static void EnsureReplacementResolves(
        MetadataAction metadata, StructType oldSchema, StructType newSchema)
    {
        foreach (var (key, sql) in Constraints(metadata))
        {
            foreach (var reference in References($"CHECK constraint '{key}'", sql))
            {
                if (!StillResolves(oldSchema, newSchema, reference))
                {
                    throw new DeltaFormatException(
                        DeltaTableErrorCodes.ConstraintDependentColumnChange,
                        $"The new schema has no column '{reference}', which CHECK constraint '{key}' "
                        + $"({sql}) reads. Drop the constraint first.");
                }
            }
        }

        foreach (var field in newSchema.Fields)
        {
            if (field.Metadata is null
                || !field.Metadata.TryGetValue(GenerationExpressionKey, out var sql))
            {
                continue;
            }

            foreach (var reference in References($"generated column '{field.Name}'", sql))
            {
                if (!StillResolves(oldSchema, newSchema, reference))
                {
                    throw new DeltaFormatException(
                        DeltaTableErrorCodes.GeneratedColumnsDependentColumnChange,
                        $"The new schema has no column '{reference}', which generated column "
                        + $"'{field.Name}' is computed from ({sql}).");
                }
            }
        }
    }

    /// <summary>
    /// The clustering spec's column paths (PHYSICAL names), or null when the table is not
    /// clustered.
    /// </summary>
    /// <exception cref="DeltaFormatException">
    /// An active domain is not of the expected shape — including one with no
    /// <c>clusteringColumns</c> at all. Null would read as "not clustered" and let a DROP or a
    /// replacement skip the check, so a domain that cannot be read fails closed.
    /// </exception>
    public static IReadOnlyList<IReadOnlyList<string>>? ClusteringPaths(
        IReadOnlyDictionary<string, DomainMetadata> domains)
    {
        if (!domains.TryGetValue(ClusteringDomain, out var domain) || domain.Removed)
            return null;

        try
        {
            using var document = JsonDocument.Parse(domain.Configuration);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty("clusteringColumns", out var columns))
            {
                throw new FormatException("no clusteringColumns property");
            }

            var paths = new List<IReadOnlyList<string>>();
            foreach (var column in columns.EnumerateArray())
            {
                var path = new List<string>();
                foreach (var segment in column.EnumerateArray())
                    path.Add(segment.GetString() ?? throw new FormatException("null path segment"));
                paths.Add(path);
            }
            return paths;
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
        {
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidLogJson,
                $"The {ClusteringDomain} domain is not of the form "
                + $"{{\"clusteringColumns\":[[\"…\"],…]}}, so its columns cannot be checked: "
                + domain.Configuration,
                ex);
        }
    }

    /// <summary>The clustering domain naming <paramref name="physicalPaths"/>, shaped like Spark's
    /// own.</summary>
    public static DomainMetadata ClusteringDomainFor(IReadOnlyList<IReadOnlyList<string>> physicalPaths)
    {
        var sb = new StringBuilder("{\"clusteringColumns\":[");
        for (int i = 0; i < physicalPaths.Count; i++)
        {
            if (i > 0)
                sb.Append(',');
            sb.Append('[');
            for (int j = 0; j < physicalPaths[i].Count; j++)
            {
                if (j > 0)
                    sb.Append(',');
                sb.Append('"').Append(physicalPaths[i][j].Replace("\\", "\\\\").Replace("\"", "\\\"")).Append('"');
            }
            sb.Append(']');
        }
        sb.Append("],\"domainName\":\"").Append(ClusteringDomain).Append("\"}");

        return new DomainMetadata
        {
            Domain = ClusteringDomain,
            Configuration = sb.ToString(),
            Removed = false,
        };
    }

    /// <summary>
    /// Translates a path between name spaces: each segment of <paramref name="path"/> is matched
    /// against <paramref name="from"/> of a field, and the same field's <paramref name="to"/> name
    /// is emitted. Null when a segment does not resolve (or crosses a non-struct).
    /// </summary>
    public static IReadOnlyList<string>? TranslatePath(
        StructType schema, IReadOnlyList<string> path,
        Func<StructField, string> from, Func<StructField, string> to,
        StringComparison comparison = StringComparison.OrdinalIgnoreCase)
    {
        var result = new List<string>(path.Count);
        IReadOnlyList<StructField>? fields = schema.Fields;
        foreach (var segment in path)
        {
            var field = fields?.FirstOrDefault(f => string.Equals(from(f), segment, comparison));
            if (field is null)
                return null;
            result.Add(to(field));
            fields = (field.Type as StructType)?.Fields;
        }
        return result;
    }

    // The callers have already found the field by its exact name, so this matches exactly too: a
    // case-insensitive match could pick a sibling differing only in case, and its physical name.
    private static IReadOnlyList<string> PhysicalPath(
        StructType schema, IReadOnlyList<string> path, ColumnMappingMode mode) =>
        TranslatePath(schema, path, f => f.Name, f => ColumnMapping.GetPhysicalName(f, mode), StringComparison.Ordinal)
        ?? throw new InvalidOperationException($"Column '{string.Join(".", path)}' does not exist.");

    private static bool IsPrefix(IReadOnlyList<string> prefix, IReadOnlyList<string> path)
    {
        if (prefix.Count > path.Count)
            return false;
        for (int i = 0; i < prefix.Count; i++)
        {
            if (!string.Equals(prefix[i], path[i], StringComparison.OrdinalIgnoreCase))
                return false;
        }
        return true;
    }

    private static IEnumerable<(string Key, string Sql)> Constraints(MetadataAction metadata)
    {
        foreach (var pair in metadata.Configuration ?? EmptyConfiguration)
        {
            if (pair.Key.StartsWith(ConstraintPrefix, StringComparison.OrdinalIgnoreCase))
                yield return (pair.Key, pair.Value);
        }
    }

    /// <summary>Whether a reference names <paramref name="target"/> or something inside it.</summary>
    private static bool DependsOn(string reference, string target) =>
        reference.Equals(target, StringComparison.OrdinalIgnoreCase)
        || (reference.Length > target.Length
            && reference[target.Length] == '.'
            && reference.StartsWith(target, StringComparison.OrdinalIgnoreCase));

    private static bool StillResolves(StructType oldSchema, StructType newSchema, string reference)
    {
        var bound = Bindings(oldSchema.Fields, reference).ToList();
        return bound.Count == 0
            ? Bindings(newSchema.Fields, reference).Any()
            : bound.All(path => TranslatePath(newSchema, path, f => f.Name, f => f.Name) is not null);
    }

    /// <summary>Every field path a dotted reference can name in <paramref name="fields"/>, trying every
    /// split, since a quoted name may itself contain a dot.</summary>
    private static IEnumerable<IReadOnlyList<string>> Bindings(IReadOnlyList<StructField> fields, string reference)
    {
        foreach (var field in fields)
        {
            if (reference.Equals(field.Name, StringComparison.OrdinalIgnoreCase))
                yield return [field.Name];
            if (field.Type is StructType st
                && reference.Length > field.Name.Length
                && reference[field.Name.Length] == '.'
                && reference.StartsWith(field.Name, StringComparison.OrdinalIgnoreCase))
            {
                foreach (var rest in Bindings(st.Fields, reference.Substring(field.Name.Length + 1)))
                    yield return [field.Name, .. rest];
            }
        }
    }

    /// <summary>The column names an expression reads.</summary>
    /// <exception cref="DeltaFormatException">
    /// The expression does not parse. Not knowing what it reads, the change is refused: the
    /// alternative is guessing, and a wrong guess is the unwritable table this class exists to
    /// prevent.
    /// </exception>
    private static List<string> References(string description, string sql)
    {
        Expression expression;
        try
        {
            expression = SparkSqlParser.ParseExpression(sql);
        }
        catch (SparkSqlParseException ex)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.UnevaluableTableExpression,
                $"Table declares {description}, which this writer cannot parse, so it cannot tell "
                + $"whether the schema change breaks it: {ex.Reason} at position {ex.Position} in: {sql}",
                ex);
        }

        var references = new List<string>();
        Collect(expression, references);
        return references;
    }

    private static void Collect(Expression expression, List<string> references)
    {
        switch (expression)
        {
            case UnboundReference reference:
                references.Add(reference.Name);
                break;
            case BoundReference reference:
                references.Add(reference.Name);
                break;
            case FunctionCall call:
                foreach (var argument in call.Arguments)
                    Collect(argument, references);
                break;
            case AndPredicate and:
                foreach (var child in and.Children)
                    Collect(child, references);
                break;
            case OrPredicate or:
                foreach (var child in or.Children)
                    Collect(child, references);
                break;
            case NotPredicate not:
                Collect(not.Child, references);
                break;
            case ComparisonPredicate comparison:
                Collect(comparison.Left, references);
                Collect(comparison.Right, references);
                break;
            case UnaryPredicate unary:
                Collect(unary.Operand, references);
                break;
            case SetPredicate set:
                Collect(set.Operand, references);
                foreach (var value in set.Values)
                    Collect(value, references);
                break;
        }
    }
}
