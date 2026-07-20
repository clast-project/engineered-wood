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

    public SchemaDescriptor(IReadOnlyList<SchemaElement> schemaElements)
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
        Columns = columns;
    }

    private static SchemaNode BuildTree(
        IReadOnlyList<SchemaElement> elements,
        ref int index,
        SchemaNode? parent)
    {
        if (index >= elements.Count)
            throw new ParquetFormatException("Unexpected end of schema elements.");

        var element = elements[index++];
        int numChildren = element.NumChildren ?? 0;

        if (numChildren == 0)
        {
            return new SchemaNode
            {
                Element = element,
                Parent = parent,
                Children = Array.Empty<SchemaNode>(),
            };
        }

        var children = new SchemaNode[numChildren];
        var node = new SchemaNode
        {
            Element = element,
            Parent = parent,
            Children = children,
        };

        for (int i = 0; i < numChildren; i++)
            children[i] = BuildTree(elements, ref index, node);

        return node;
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
