// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Translates a scan predicate, written against the table's LOGICAL column names, into one over a single
/// data file's own leaf columns, for row-group and page pruning by
/// <see cref="ParquetFileReader.ReadWithPositionsAsync(EngineeredWood.Expressions.Predicate?, IReadOnlyList{string}?, CancellationToken)"/>.
/// <para>
/// The Parquet statistics accessor resolves a reference by the file's leaf path, part by part. Under column mapping
/// that is <c>col-&lt;uuid&gt;</c>, not the logical name, so an untranslated predicate would miss every
/// column and prune nothing at all: safe, but silent. The translation is per FILE rather than per table,
/// because the file is the authority on what it holds: id mode resolves by field id (as the read path
/// does), a file written before an ADD COLUMN lacks the column, and one written before a type change holds
/// the narrower type.
/// </para>
/// <para>
/// Every reference that does not resolve to exactly one leaf is pointed at a name no Parquet file can
/// contain, so the accessor answers null and the evaluator Unknown: the row group is KEPT. That covers
/// partition columns (whose values come from the log, even where IcebergCompat also stores them in the file),
/// list and map elements, columns absent from an older file,
/// and any column whose type has changed (the file's statistics are of the old
/// type, and a Bloom filter hashes the old physical encoding). Pruning never guesses.
/// </para>
/// </summary>
internal static class RowGroupPushdown
{
    /// <summary>A column name no Parquet schema holds, for a reference that must evaluate Unknown.</summary>
    internal const string Unresolved = "\0unresolved";

    /// <summary>
    /// Rewrites <paramref name="predicate"/>'s references from the table's logical paths to
    /// <paramref name="fileSchema"/>'s leaf paths. A reference is matched by its
    /// <see cref="UnboundReference.NameParts"/>, as the row evaluator resolves it, and never by its dotted text,
    /// which cannot tell field <c>b</c> of struct <c>a</c> from a top-level column named <c>a.b</c> (#771). A
    /// <paramref name="partitionColumns"/> reference never
    /// resolves, even to a leaf the file holds: an IcebergCompat file materializes its partition values, but the
    /// log's are the ones a read returns, and the two can disagree (a stored empty string reads as NULL).
    /// </summary>
    public static Predicate ToFileColumns(
        Predicate predicate, StructType tableSchema, ColumnMappingMode mode, SchemaDescriptor fileSchema,
        IReadOnlyCollection<string> partitionColumns)
    {
        var map = LogicalToFileLeaf(tableSchema, mode, fileSchema);
        foreach (var column in partitionColumns)
            map.Remove([column]);
        return Rewrite(predicate, path => map.TryGetValue(path, out var leaf)
            ? UnboundReference.FromParts(leaf)
            : new UnboundReference(Unresolved));
    }

    /// <summary>
    /// Maps each logical leaf path of <paramref name="tableSchema"/> that is a primitive column, reached only
    /// through structs, and unchanged in type, to the path of the file leaf holding it. Both are paths of parts,
    /// so a literal dotted name beside a struct leaf of the same spelling is two entries, not a collision.
    /// </summary>
    internal static Dictionary<IReadOnlyList<string>, string[]> LogicalToFileLeaf(
        StructType tableSchema, ColumnMappingMode mode, SchemaDescriptor fileSchema)
    {
        var map = new Dictionary<IReadOnlyList<string>, string[]>(ColumnPathComparer.Ordinal);
        AddFields(tableSchema, fileSchema.Root.Children, [], [], mode, map);
        return map;
    }

    private static void AddFields(
        StructType schema, IReadOnlyList<SchemaNode> fileChildren, string[] logicalPrefix, string[] filePrefix,
        ColumnMappingMode mode, Dictionary<IReadOnlyList<string>, string[]> map)
    {
        foreach (var field in schema.Fields)
        {
            string[] logical = [.. logicalPrefix, field.Name];

            // A widened column's older files hold the narrower type; leave the whole subtree unresolved.
            if (field.Metadata is not null
                && field.Metadata.ContainsKey(Schema.TypeWidening.TypeChangesKey))
            {
                continue;
            }

            var node = FindChild(field, fileChildren, mode);
            if (node is null || node.Element.RepetitionType == FieldRepetitionType.Repeated)
                continue;

            string[] path = [.. filePrefix, node.Name];
            if (field.Type is PrimitiveType)
            {
                if (!node.IsLeaf)
                    continue; // e.g. a variant: a primitive to Delta, a group of binaries in the file

                map[logical] = path;
            }
            else if (field.Type is StructType st && !node.IsLeaf)
            {
                AddFields(st, node.Children, logical, path, mode, map);
            }
            // list/map: statistics of a repeated leaf do not bound the row, so nothing to register.
        }
    }

    private static SchemaNode? FindChild(StructField field, IReadOnlyList<SchemaNode> fileChildren, ColumnMappingMode mode)
    {
        if (mode == ColumnMappingMode.Id)
        {
            // As the read path's projection: id mode resolves by field id and nothing else.
            if (ColumnMapping.GetFieldId(field) is not { } id)
                return null;
            SchemaNode? found = null;
            foreach (var child in fileChildren)
            {
                if (child.Element.FieldId == id)
                {
                    if (found is not null)
                        return null; // two children claim the id: ambiguous
                    found = child;
                }
            }
            return found;
        }

        string name = ColumnMapping.GetPhysicalName(field, mode);
        SchemaNode? match = null;
        foreach (var child in fileChildren)
        {
            if (string.Equals(child.Name, name, StringComparison.Ordinal))
            {
                if (match is not null)
                    return null;
                match = child;
            }
        }
        return match;
    }

    /// <summary>Rewrites every column reference in <paramref name="predicate"/> through
    /// <paramref name="rename"/>, which is given the reference's path.</summary>
    internal static Predicate Rewrite(Predicate predicate, Func<IReadOnlyList<string>, UnboundReference> rename) =>
        predicate switch
    {
        TruePredicate or FalsePredicate => predicate,
        AndPredicate and => new AndPredicate(and.Children.Select(c => Rewrite(c, rename)).ToList()),
        OrPredicate or => new OrPredicate(or.Children.Select(c => Rewrite(c, rename)).ToList()),
        NotPredicate not => new NotPredicate(Rewrite(not.Child, rename)),
        ComparisonPredicate cmp => new ComparisonPredicate(
            Rewrite(cmp.Left, rename), cmp.Op, Rewrite(cmp.Right, rename)),
        UnaryPredicate unary => new UnaryPredicate(Rewrite(unary.Operand, rename), unary.Op),
        SetPredicate set => new SetPredicate(
            Rewrite(set.Operand, rename), set.Values.Select(v => Rewrite(v, rename)).ToList(), set.Op),
        // A predicate kind this rewrite does not know may hold references it cannot see. Replace it with
        // one the evaluator can only answer Unknown (a comparison of two columns), which is Unknown under
        // NOT as well — a constant would be inverted by one.
        _ => new ComparisonPredicate(
            new UnboundReference(Unresolved), ComparisonOperator.Equal, new UnboundReference(Unresolved)),
    };

    private static Expression Rewrite(Expression expression, Func<IReadOnlyList<string>, UnboundReference> rename) =>
        expression switch
    {
        Predicate p => Rewrite(p, rename),
        UnboundReference u => rename(u.NameParts),
        BoundReference b => Rebind(b, rename([b.Name])),
        FunctionCall f => new FunctionCall(f.Name, f.Arguments.Select(a => Rewrite(a, rename)).ToList()),
        LiteralExpression => expression,
        _ => new UnboundReference(Unresolved),
    };

    /// <summary>
    /// A bound reference after <paramref name="renamed"/>: still bound when it maps to one top-level file column.
    /// Otherwise it is the renamed reference itself: a struct field's path, which a bound reference has no parts to
    /// hold, or <see cref="Unresolved"/>, which must not carry the column's field id to a name it does not have.
    /// </summary>
    private static Expression Rebind(BoundReference b, UnboundReference renamed) =>
        renamed.NameParts.Count == 1 && !string.Equals(renamed.Name, Unresolved, StringComparison.Ordinal)
            ? new BoundReference(b.FieldId, renamed.Name)
            : renamed;
}
