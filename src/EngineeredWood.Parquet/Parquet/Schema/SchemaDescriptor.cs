// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Schema;

/// <summary>
/// Reconstructs the schema tree from the flat list of <see cref="SchemaElement"/>s
/// and computes definition/repetition levels for each leaf column.
/// </summary>
public sealed class SchemaDescriptor
{
    /// <summary>The root node of the schema tree.</summary>
    public SchemaNode Root { get; }

    /// <summary>All leaf (primitive) columns in pre-order traversal order.</summary>
    public IReadOnlyList<ColumnDescriptor> Columns { get; }

    /// <param name="schemaElements">The file's flattened schema.</param>
    /// <param name="columnOrders">
    /// The file's <c>column_orders</c>, one per leaf column, or null when it has none. Sets each
    /// column's <see cref="ColumnDescriptor.ColumnOrder"/>.
    /// </param>
    public SchemaDescriptor(IReadOnlyList<SchemaElement> schemaElements, IReadOnlyList<ColumnOrder>? columnOrders = null)
    {
        if (schemaElements.Count == 0)
            throw new ParquetFormatException("Schema must contain at least a root element.");

        int index = 0;
        Root = BuildTree(schemaElements, ref index, parent: null);

        if (index != schemaElements.Count)
            throw new ParquetFormatException(
                $"Schema element count mismatch: consumed {index} of {schemaElements.Count} elements.");

        var columns = new List<ColumnDescriptor>();
        // Pre-allocate a path buffer sized to the maximum possible tree depth.
        var pathBuffer = new string[schemaElements.Count];
        CollectLeaves(Root, pathBuffer, depth: 0, defLevel: 0, repLevel: 0, columns);

        // A list that does not match the leaves cannot be attributed to them, so no column's order is
        // known. That costs pruning, never a wrong answer.
        bool ordersUsable = columnOrders is null || columnOrders.Count == columns.Count;
        for (int i = 0; i < columns.Count; i++)
        {
            var declared = columnOrders is null ? ColumnOrder.TypeDefined
                : ordersUsable ? columnOrders[i]
                : ColumnOrder.Undefined;
            var order = EffectiveOrder(columns[i], declared);
            if (order != ColumnOrder.TypeDefined)
                columns[i] = columns[i].WithColumnOrder(order);
        }
        Columns = columns;
    }

    /// <summary>
    /// <paramref name="declared"/>, unless the reader cannot use it: a column whose type annotation was
    /// ignored has no known order (parquet-format 2.14, GH-606), IEEE 754 total order applies only
    /// to FLOAT, DOUBLE and FLOAT16, and INT96 timestamp order only to INT96.
    /// </summary>
    private static ColumnOrder EffectiveOrder(ColumnDescriptor column, ColumnOrder declared)
    {
        if (column.SchemaNode.AnnotationIgnored)
            return ColumnOrder.Undefined;

        if (declared == ColumnOrder.Ieee754TotalOrder
            && column.PhysicalType is not (PhysicalType.Float or PhysicalType.Double)
            && column.SchemaElement.LogicalType is not LogicalType.Float16Type)
        {
            return ColumnOrder.Undefined;
        }

        if (declared == ColumnOrder.Int96TimestampOrder && column.PhysicalType != PhysicalType.Int96)
            return ColumnOrder.Undefined;

        return declared;
    }

    private static SchemaNode BuildTree(
        IReadOnlyList<SchemaElement> elements,
        ref int index,
        SchemaNode? parent)
    {
        if (index >= elements.Count)
            throw new ParquetFormatException("Unexpected end of schema elements.");

        var element = SchemaAnnotations.Honoured(elements[index++], out bool annotationIgnored);
        int numChildren = element.NumChildren ?? 0;

        if (numChildren == 0)
        {
            return new SchemaNode
            {
                Element = element,
                Parent = parent,
                Children = Array.Empty<SchemaNode>(),
                AnnotationIgnored = annotationIgnored,
            };
        }

        var children = new SchemaNode[numChildren];
        var node = new SchemaNode
        {
            Element = element,
            Parent = parent,
            Children = children,
            AnnotationIgnored = annotationIgnored,
        };

        for (int i = 0; i < numChildren; i++)
            children[i] = BuildTree(elements, ref index, node);

        return node;
    }

    /// <summary>
    /// The index in <see cref="Columns"/> of the leaf <paramref name="name"/> names, or -1: a top-level leaf of
    /// that exact name first, then the first nested leaf whose dotted path it is. A name may hold a dot, so a
    /// top-level column literally named <c>s.a</c> beside a struct <c>s</c> with a field <c>a</c> is that column,
    /// not the struct's leaf, whose dotted path is the same text. A projected read resolves names by the same
    /// rule, so a filter prunes by the column the read returns.
    /// </summary>
    internal int FindLeaf(string name)
    {
        for (int i = 0; i < Columns.Count; i++)
        {
            if (Columns[i].Path.Count == 1 && Columns[i].Path[0] == name)
                return i;
        }
        for (int i = 0; i < Columns.Count; i++)
        {
            if (Columns[i].Path.Count > 1 && Columns[i].DottedPath == name)
                return i;
        }
        return -1;
    }

    /// <summary>
    /// The index in <see cref="Columns"/> of the leaf a predicate's column path names, or -1, resolved as the row
    /// evaluator resolves a reference (#771): the top-level field <paramref name="path"/>'s first part names, then a
    /// field of each struct below it, every part matched exactly. A one-part path is never a nested leaf whose
    /// dotted path spells the same text, and a longer one never a top-level leaf named with dots. The path must
    /// end at a leaf and pass only through groups that are not repeated (a list or map's elements are not a
    /// struct's fields), and a part two siblings share resolves to nothing.
    /// </summary>
    internal int FindLeaf(IReadOnlyList<string> path)
    {
        if (path.Count == 0)
            return -1;
        SchemaNode node = Root;
        foreach (string part in path)
        {
            if (node.IsLeaf)
                return -1;
            SchemaNode? match = null;
            foreach (var child in node.Children)
            {
                if (!string.Equals(child.Name, part, StringComparison.Ordinal))
                    continue;
                if (match is not null)
                    return -1;
                match = child;
            }
            if (match is null || match.Element.RepetitionType == FieldRepetitionType.Repeated)
                return -1;
            node = match;
        }
        if (!node.IsLeaf)
            return -1;
        for (int i = 0; i < Columns.Count; i++)
        {
            if (ReferenceEquals(Columns[i].SchemaNode, node))
                return i;
        }
        return -1;
    }

    /// <summary>
    /// Resolves Parquet field_ids to top-level column names.
    /// For leaf columns, returns the dotted path. For group columns
    /// (struct, list, map), returns the top-level group name.
    /// Returns null for any field_id that is not found.
    /// </summary>
    public IReadOnlyList<string?> ResolveFieldIds(IReadOnlyList<int> fieldIds)
    {
        // Build a map from field_id to top-level node name
        var fieldIdToName = new Dictionary<int, string>();
        foreach (var child in Root.Children)
            CollectFieldIds(child, child.Name, fieldIdToName);

        var result = new string?[fieldIds.Count];
        for (int i = 0; i < fieldIds.Count; i++)
        {
            fieldIdToName.TryGetValue(fieldIds[i], out result[i]);
        }
        return result;
    }

    private static void CollectFieldIds(
        SchemaNode node, string topLevelName, Dictionary<int, string> map)
    {
        if (node.Element.FieldId.HasValue)
            map[node.Element.FieldId.Value] = topLevelName;

        foreach (var child in node.Children)
            CollectFieldIds(child, topLevelName, map);
    }

    private static void CollectLeaves(
        SchemaNode node,
        string[] pathBuffer,
        int depth,
        int defLevel,
        int repLevel,
        List<ColumnDescriptor> columns)
    {
        // The root has no repetition type and doesn't contribute to levels.
        if (node.Parent != null)
        {
            pathBuffer[depth++] = node.Name;

            var rep = node.Element.RepetitionType;
            if (rep == FieldRepetitionType.Optional)
                defLevel++;
            else if (rep == FieldRepetitionType.Repeated)
            {
                defLevel++;
                repLevel++;
            }
        }

        if (node.IsLeaf)
        {
            columns.Add(new ColumnDescriptor
            {
#if NET8_0_OR_GREATER
                Path = pathBuffer[..depth].ToArray(),
#else
                Path = pathBuffer.AsSpan(0, depth).ToArray(),
#endif
                PhysicalType = node.Element.Type!.Value,
                TypeLength = node.Element.TypeLength,
                MaxDefinitionLevel = defLevel,
                MaxRepetitionLevel = repLevel,
                SchemaElement = node.Element,
                SchemaNode = node,
            });
        }
        else
        {
            foreach (var child in node.Children)
                CollectLeaves(child, pathBuffer, depth, defLevel, repLevel, columns);
        }
    }
}
