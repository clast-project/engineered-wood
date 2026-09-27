// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Translates a scan predicate, written against the table's LOGICAL column names, into one over a single
/// data file's own leaf columns, for row-group pruning by
/// <see cref="ParquetFileReader.GetCandidateRowGroupsAsync(Predicate, CancellationToken)"/>.
/// <para>
/// The Parquet statistics accessor resolves a reference by the file's dotted leaf path. Under column mapping
/// that is <c>col-&lt;uuid&gt;</c>, not the logical name, so an untranslated predicate would miss every
/// column and prune nothing at all: safe, but silent. The translation is per FILE rather than per table,
/// because the file is the authority on what it holds: id mode resolves by field id (as the read path
/// does), a file written before an ADD COLUMN lacks the column, and one written before a type change holds
/// the narrower type.
/// </para>
/// <para>
/// Every reference that does not resolve to exactly one leaf is pointed at a name no Parquet file can
/// contain, so the accessor answers null and the evaluator Unknown: the row group is KEPT. That covers
/// partition columns (not stored in the file), list and map elements, columns absent from an older file,
/// ambiguous dotted names, and any column whose type has changed (the file's statistics are of the old
/// type, and a Bloom filter hashes the old physical encoding). Pruning never guesses.
/// </para>
/// </summary>
internal static class RowGroupPushdown
{
    /// <summary>A column name no Parquet schema holds, for a reference that must evaluate Unknown.</summary>
    internal const string Unresolved = "\0unresolved";

    /// <summary>
    /// Rewrites <paramref name="predicate"/>'s references from the table's logical names to
    /// <paramref name="fileSchema"/>'s dotted leaf paths.
    /// </summary>
    public static Predicate ToFileColumns(
        Predicate predicate, StructType tableSchema, ColumnMappingMode mode, SchemaDescriptor fileSchema)
    {
        var map = LogicalToFileLeaf(tableSchema, mode, fileSchema);
        return Rewrite(predicate, name => map.TryGetValue(name, out var leaf) ? leaf : Unresolved);
    }

    /// <summary>
    /// Maps each logical dotted leaf path of <paramref name="tableSchema"/> that is a primitive column,
    /// reached only through structs, and unchanged in type, to the file leaf holding it. A logical path
    /// that two table columns share (a literal dotted name beside a struct leaf) or a file path that two
    /// leaves share is left out, which makes it unresolvable.
    /// </summary>
    internal static Dictionary<string, string> LogicalToFileLeaf(
        StructType tableSchema, ColumnMappingMode mode, SchemaDescriptor fileSchema)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        AddFields(tableSchema, fileSchema.Root.Children, "", "", mode, map);

        // Ambiguity is a property of the TABLE, not of this file: a file written before the literal "a.b"
        // column was added holds only struct a's leaf, yet the predicate's "a.b" still names both.
        var logicalCount = new Dictionary<string, int>(StringComparer.Ordinal);
        CountLogicalPaths(tableSchema, "", logicalCount);
        foreach (var entry in logicalCount.Where(e => e.Value > 1))
            map.Remove(entry.Key);

        // The accessor indexes leaves by dotted path, last one winning, so a path two file leaves share
        // (a literal "a.b" leaf beside struct a's leaf b) may be answered with the OTHER column's bounds.
        var fileLeafCount = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var column in fileSchema.Columns)
        {
            fileLeafCount.TryGetValue(column.DottedPath, out int n);
            fileLeafCount[column.DottedPath] = n + 1;
        }
        foreach (var entry in map.Where(e => fileLeafCount.TryGetValue(e.Value, out int n) && n > 1).ToList())
            map.Remove(entry.Key);

        return map;
    }

    private static void AddFields(
        StructType schema, IReadOnlyList<SchemaNode> fileChildren, string logicalPrefix, string filePrefix,
        ColumnMappingMode mode, Dictionary<string, string> map)
    {
        foreach (var field in schema.Fields)
        {
            string logical = logicalPrefix.Length == 0 ? field.Name : logicalPrefix + "." + field.Name;

            // A widened column's older files hold the narrower type; leave the whole subtree unresolved.
            if (field.Metadata is not null
                && field.Metadata.ContainsKey(Schema.TypeWidening.TypeChangesKey))
            {
                continue;
            }

            var node = FindChild(field, fileChildren, mode);
            if (node is null || node.Element.RepetitionType == FieldRepetitionType.Repeated)
                continue;

            string path = filePrefix.Length == 0 ? node.Name : filePrefix + "." + node.Name;
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

    /// <summary>Counts every dotted path the table's columns and struct fields spell, file or no file.</summary>
    private static void CountLogicalPaths(StructType schema, string prefix, Dictionary<string, int> counts)
    {
        foreach (var field in schema.Fields)
        {
            string logical = prefix.Length == 0 ? field.Name : prefix + "." + field.Name;
            counts.TryGetValue(logical, out int n);
            counts[logical] = n + 1;
            if (field.Type is StructType st)
                CountLogicalPaths(st, logical, counts);
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
    /// <paramref name="rename"/>.</summary>
    internal static Predicate Rewrite(Predicate predicate, Func<string, string> rename) => predicate switch
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

    private static Expression Rewrite(Expression expression, Func<string, string> rename) => expression switch
    {
        Predicate p => Rewrite(p, rename),
        UnboundReference u => new UnboundReference(rename(u.Name)),
        BoundReference b => new BoundReference(b.FieldId, rename(b.Name)),
        FunctionCall f => new FunctionCall(f.Name, f.Arguments.Select(a => Rewrite(a, rename)).ToList()),
        LiteralExpression => expression,
        _ => new UnboundReference(Unresolved),
    };
}
