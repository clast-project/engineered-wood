// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.DeltaLake.Table.TypeWidening;

/// <summary>
/// Picks the leaves of a data file that were written as <c>byte</c>/<c>short</c> and that the table has since
/// widened to <c>int</c>, <c>long</c> or <c>double</c>, so the read can return them as the INT32 values they
/// store (#839). Read with their INT(8)/INT(16) annotation they would be narrowed with a checked cast, and
/// <see cref="ValueWidener"/> would widen them straight back; read as stored, an <c>int</c> column needs
/// nothing more, and a <c>long</c> or <c>double</c> one a single widening.
/// </summary>
/// <remarks>
/// <para>The file's nodes are paired with the table's fields the way the column-mapping rename pairs a batch's
/// fields with them (<see cref="ColumnMappingRecursive"/>): by field id in id mode when the node carries one,
/// and otherwise by physical name, then logical name. A list's element and a map's key and value pair by
/// position. A node with no counterpart is left as it is; so is anything this does not recognise, which still
/// reads correctly, by the narrow-then-widen route.</para>
/// <para>Decimal targets are left out on purpose: a <c>byte</c> may widen to a decimal too narrow for an
/// <c>int</c> (<c>decimal(4,1)</c>), and the relabel gives up the reader's check that the values fit the
/// annotation, so a corrupt value could then exceed the decimal's precision. <c>short</c> targets gain nothing
/// (they would need narrowing from INT32 again).</para>
/// </remarks>
internal static class StoredInt32Columns
{
    /// <summary>
    /// Asks <paramref name="reader"/> to read the qualifying leaves of its file as stored INT32. Does nothing
    /// unless the table has type changes to read through (the condition under which the read widens at all).
    /// </summary>
    public static async ValueTask ApplyAsync(
        ParquetFileReader reader, Snapshot.Snapshot snapshot, ColumnMappingMode mappingMode,
        CancellationToken cancellationToken)
    {
        if (!DeltaTable.ReadsWiden(snapshot))
            return;

        var schema = await reader.GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var leaves = Resolve(schema, snapshot.Schema, mappingMode);
        if (leaves is not null)
            reader.ReadStoredInt32(leaves);
    }

    /// <summary>
    /// The leaves of <paramref name="file"/> to read as stored INT32 under <paramref name="table"/>, or null when
    /// there are none.
    /// </summary>
    internal static HashSet<SchemaElement>? Resolve(SchemaDescriptor file, StructType table, ColumnMappingMode mode)
    {
        HashSet<SchemaElement>? leaves = null;
        MatchFields(file.Root.Children, table, mode, ref leaves);
        return leaves;
    }

    private static void MatchFields(
        IReadOnlyList<SchemaNode> nodes, StructType table, ColumnMappingMode mode, ref HashSet<SchemaElement>? leaves)
    {
        foreach (var node in nodes)
        {
            if (FindField(table, node.Element, mode) is { } field)
                MatchNode(node, field.Type, mode, ref leaves);
        }
    }

    private static void MatchNode(
        SchemaNode node, DeltaDataType type, ColumnMappingMode mode, ref HashSet<SchemaElement>? leaves)
    {
        if (node.IsLeaf)
        {
            // A bare repeated primitive reads as a list of itself.
            if (node.Element.RepetitionType == FieldRepetitionType.Repeated)
            {
                if (type is not ArrayType bare)
                    return;
                type = bare.ElementType;
            }

            if (IsWidenedNarrowInt(node.Element, type))
                (leaves ??= new HashSet<SchemaElement>()).Add(node.Element);
            return;
        }

        // The same classification, in the same order, as the reader's own (ArrowSchemaConverter.NodeToArrowField).
        if (ArrowSchemaConverter.IsListNode(node))
        {
            if (type is not ArrayType array)
                return;
            var repeated = node.Children[0];
            if (repeated.IsLeaf)
                MatchLeafElement(repeated, array.ElementType, ref leaves); // 2-level: the repeated leaf is the element
            else if (repeated.Children.Count == 1)
                MatchNode(repeated.Children[0], array.ElementType, mode, ref leaves);
            else if (array.ElementType is StructType element)
                MatchFields(repeated.Children, element, mode, ref leaves); // the repeated group is the element
            return;
        }

        if (ArrowSchemaConverter.IsMapNode(node))
        {
            if (type is not MapType map)
                return;
            var keyValue = node.Children[0];
            MatchNode(keyValue.Children[0], map.KeyType, mode, ref leaves);
            MatchNode(keyValue.Children[1], map.ValueType, mode, ref leaves);
            return;
        }

        if (type is StructType st)
            MatchFields(node.Children, st, mode, ref leaves);
    }

    private static void MatchLeafElement(SchemaNode leaf, DeltaDataType type, ref HashSet<SchemaElement>? leaves)
    {
        if (IsWidenedNarrowInt(leaf.Element, type))
            (leaves ??= new HashSet<SchemaElement>()).Add(leaf.Element);
    }

    // A signed INT(8)/INT(16) stored as INT32, whose table type is int, long or double. The reader re-checks the
    // physical half; checking it here too keeps the set to leaves it will honour.
    private static bool IsWidenedNarrowInt(SchemaElement element, DeltaDataType type)
    {
        if (element.Type != PhysicalType.Int32)
            return false;

        bool narrow = element.LogicalType is LogicalType.IntType { BitWidth: 8 or 16, IsSigned: true }
            || (element.LogicalType is null
                && element.ConvertedType is ConvertedType.Int8 or ConvertedType.Int16);
        return narrow && type is PrimitiveType { TypeName: "integer" or "long" or "double" };
    }

    // Mirrors ColumnMappingRecursive.FindField in the read direction: by id when in id mode and the node has one
    // (and then ONLY by id), otherwise by physical name across every field, then by logical name.
    private static StructField? FindField(StructType table, SchemaElement element, ColumnMappingMode mode)
    {
        var index = Indexes.GetValue(table, static s => new FieldIndex(s));
        if (mode == ColumnMappingMode.Id && element.FieldId is { } id)
            return index.ById.TryGetValue(id, out var byId) ? byId : null;

        return index.ByPhysical.TryGetValue(element.Name, out var field)
            || index.ByLogical.TryGetValue(element.Name, out field)
            ? field
            : null;
    }

    // Every file of a scan is matched against the same schema levels, so each level is indexed once (as
    // ColumnMappingRecursive does): scanning the level per file node made matching a wide table quadratic in its
    // column count, per file. Keyed weakly on the StructType instance, which lives as long as its snapshot. A key's
    // FIRST field wins, as a linear scan's would.
    private static readonly ConditionalWeakTable<StructType, FieldIndex> Indexes = new();

    private sealed class FieldIndex
    {
        public readonly Dictionary<int, StructField> ById = new();
        public readonly Dictionary<string, StructField> ByPhysical = new(StringComparer.Ordinal);
        public readonly Dictionary<string, StructField> ByLogical = new(StringComparer.Ordinal);

        public FieldIndex(StructType schema)
        {
            foreach (var f in schema.Fields)
            {
                if (ColumnMapping.GetFieldId(f) is { } id && !ById.ContainsKey(id))
                    ById[id] = f;
                if (f.Metadata is { } md
                    && md.TryGetValue(ColumnMapping.PhysicalNameKey, out var physical)
                    && !ByPhysical.ContainsKey(physical))
                {
                    ByPhysical[physical] = f;
                }
                if (!ByLogical.ContainsKey(f.Name))
                    ByLogical[f.Name] = f;
            }
        }
    }
}
