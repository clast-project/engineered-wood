// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Groups flat leaf arrays into nested Arrow arrays (struct, list, map)
/// based on the schema tree, deriving validity bitmaps and offsets from raw definition
/// and repetition levels.
/// </summary>
internal static class NestedAssembler
{
    /// <summary>
    /// Assembles top-level arrays from flat leaf arrays, grouping nested columns.
    /// </summary>
    /// <param name="root">Schema tree root.</param>
    /// <param name="leafArrays">Flat leaf arrays in pre-order traversal order.</param>
    /// <param name="leafDefLevels">Raw definition levels per leaf (null for required leaves).</param>
    /// <param name="leafRepLevels">Raw repetition levels per leaf (null for non-repeated leaves).</param>
    /// <param name="rowCount">Number of rows in the row group.</param>
    /// <param name="leafFixedListLengths">
    /// Optional per-leaf fixed list lengths from <see cref="FixedListDetector"/>. A non-zero entry
    /// means that leaf's levels were proven to describe fully-defined lists of that exact length
    /// and were therefore never materialised; the list offsets are derived arithmetically.
    /// </param>
    /// <param name="extensionRegistry">
    /// Optional Arrow extension registry. When supplied and the registry knows
    /// <c>arrow.parquet.variant</c>, top-level groups annotated with the
    /// Parquet <c>VARIANT</c> logical type are returned as
    /// <see cref="VariantArray"/> rather than the bare storage
    /// <see cref="StructArray"/>. Variants nested inside a struct/list/map are
    /// wrapped separately, after assembly, by <see cref="VariantNestedWrapper"/>.
    /// </param>
    /// <returns>Top-level arrays matching the root's children.</returns>
    public static IArrowArray[] Assemble(
        SchemaNode root,
        IArrowArray[] leafArrays,
        int[]?[] leafDefLevels,
        int[]?[] leafRepLevels,
        int[]? leafFixedLengths,
        int rowCount,
        ExtensionTypeRegistry? extensionRegistry = null)
    {
        var result = new IArrowArray[root.Children.Count];
        int leafIndex = 0;

        for (int i = 0; i < root.Children.Count; i++)
        {
            var child = root.Children[i];
            var array = AssembleNode(child, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,rowCount, ref leafIndex);
            result[i] = WrapTopLevelExtension(array, child, extensionRegistry);
        }

        return result;
    }

    private static IArrowArray WrapTopLevelExtension(
        IArrowArray array, SchemaNode node, ExtensionTypeRegistry? registry)
    {
        if (registry is null) return array;
        if (node.Element.LogicalType is LogicalType.VariantType
            && array is StructArray sa
            && registry.TryGetDefinition("arrow.parquet.variant", out var def)
            && def.TryCreateType(sa.Data.DataType, metadata: string.Empty, out var ext))
        {
            // A shredded column carries its data in `typed_value`, leaving `value` empty; hand back
            // the reassembled canonical form so GetValueBytes returns the real variant rather than
            // silently returning nothing. See VariantShredding for the trade-offs.
            var wrapped = ext.CreateArray(sa);
            return wrapped is VariantArray va ? VariantShredding.Reassemble(va) : wrapped;
        }
        return array;
    }

    private static IArrowArray AssembleNode(
        SchemaNode node,
        IArrowArray[] leafArrays,
        int[]?[] leafDefLevels,
        int[]?[] leafRepLevels,
        int[]? leafFixedLengths,
        int parentCount,
        ref int leafIndex)
    {
        if (node.IsLeaf)
        {
            // Bare repeated primitive: leaf with Repeated → wrap in list
            if (node.Element.RepetitionType == FieldRepetitionType.Repeated)
                return AssembleBareRepeatedLeaf(node, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,parentCount, ref leafIndex);

            return leafArrays[leafIndex++];
        }

        if (ArrowSchemaConverter.IsListNode(node))
            return AssembleList(node, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,parentCount, ref leafIndex);
        if (ArrowSchemaConverter.IsMapNode(node))
            return AssembleMap(node, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,parentCount, ref leafIndex);

        return AssembleStruct(node, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,parentCount, ref leafIndex);
    }

    private static IArrowArray AssembleStruct(
        SchemaNode node,
        IArrowArray[] leafArrays,
        int[]?[] leafDefLevels,
        int[]?[] leafRepLevels,
        int[]? leafFixedLengths,
        int parentCount,
        ref int leafIndex)
    {
        int firstLeafIndex = leafIndex;

        var childArrays = new IArrowArray[node.Children.Count];
        for (int i = 0; i < node.Children.Count; i++)
            childArrays[i] = AssembleNode(node.Children[i], leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,parentCount, ref leafIndex);

        int lastLeafIndex = leafIndex;

        var childFields = BuildChildFields(node);
        var structType = new StructType(childFields);

        if (node.Element.RepetitionType != FieldRepetitionType.Optional)
            return new StructArray(structType, parentCount, childArrays, ArrowBuffer.Empty, nullCount: 0);

        int structDefLevel = ComputeAccumulatedDefLevel(node);

        int[]? defLevels = null;
        for (int i = firstLeafIndex; i < lastLeafIndex; i++)
        {
            if (leafDefLevels[i] != null)
            {
                defLevels = leafDefLevels[i];
                break;
            }
        }

        if (defLevels == null)
            return new StructArray(structType, parentCount, childArrays, ArrowBuffer.Empty, nullCount: 0);

        int nullCount = 0;
        var bitmapBytes = new byte[(parentCount + 7) / 8];

        for (int i = 0; i < parentCount; i++)
        {
            if (defLevels[i] >= structDefLevel)
                bitmapBytes[i >> 3] |= (byte)(1 << (i & 7));
            else
                nullCount++;
        }

        var bitmapBuffer = new ArrowBuffer(bitmapBytes);
        return new StructArray(structType, parentCount, childArrays, bitmapBuffer, nullCount);
    }

    private static IArrowArray AssembleBareRepeatedLeaf(
        SchemaNode node,
        IArrowArray[] leafArrays,
        int[]?[] leafDefLevels,
        int[]?[] leafRepLevels,
        int[]? leafFixedLengths,
        int parentCount,
        ref int leafIndex)
    {
        int li = leafIndex++;
        var elementArray = leafArrays[li];
        var repLevels = leafRepLevels[li];
        var defLevels = leafDefLevels[li];
        int numValues = repLevels?.Length ?? elementArray.Length;

        // Bare repeated leaf: no parent LIST group, the node itself is repeated.
        // nullDefThreshold = parent's accumulated def (can't be null unless parent is optional)
        // emptyDefThreshold = node's accumulated def (def < this = empty list)
        int nodeDefLevel = ComputeAccumulatedDefLevel(node);
        int parentDefLevel = node.Parent != null ? ComputeAccumulatedDefLevel(node.Parent) : 0;
        int repThreshold = ComputeAccumulatedRepLevel(node);

        int fixedLength = FixedLengthFor(leafFixedLengths, li, repLevels, repThreshold);

        var (offsets, bitmap, nullCount, _) = fixedLength > 0
            ? (BuildFixedOffsets(parentCount, fixedLength), (byte[]?)null, 0, parentCount * fixedLength)
            : BuildOffsetsAndBitmap(
                repLevels, defLevels, parentDefLevel, nodeDefLevel, parentCount, numValues, repThreshold);

        // Filter out phantom entries from the element array
        elementArray = FilterElementArray(elementArray, defLevels, nodeDefLevel);

        var elementType = ArrowSchemaConverter.ToArrowField(BuildTempDescriptor(node)).DataType;
        var elementField = new Apache.Arrow.Field("element", elementType, nullable: false);
        var listType = new ListType(elementField);

        var offsetsBuffer = ToArrowBuffer(offsets);
        var bitmapBuffer = nullCount > 0 ? new ArrowBuffer(bitmap!) : ArrowBuffer.Empty;

        return new ListArray(listType, parentCount, offsetsBuffer, elementArray, bitmapBuffer, nullCount);
    }

    private static IArrowArray AssembleList(
        SchemaNode node,
        IArrowArray[] leafArrays,
        int[]?[] leafDefLevels,
        int[]?[] leafRepLevels,
        int[]? leafFixedLengths,
        int parentCount,
        ref int leafIndex)
    {
        var repeatedChild = node.Children[0];

        // Get the first descendant leaf index to retrieve rep/def levels
        int firstLeafIndex = leafIndex;

        // Get rep/def levels from the first descendant leaf
        var repLevels = leafRepLevels[firstLeafIndex];
        var defLevels = leafDefLevels[firstLeafIndex];

        // Compute thresholds
        int nullDefThreshold = ComputeAccumulatedDefLevel(node);
        int emptyDefThreshold = ComputeAccumulatedDefLevel(repeatedChild);
        int repThreshold = ComputeAccumulatedRepLevel(repeatedChild);

        int numValues = repLevels?.Length ?? 0;

        int fixedLength = FixedLengthFor(leafFixedLengths, firstLeafIndex, repLevels, repThreshold);

        // Build offsets FIRST to determine elementCount for inner assembly.
        // A detected fixed length makes that arithmetic: offsets[i] = i * n, no nulls, no scan.
        var (offsets, bitmap, nullCount, elementCount) = fixedLength > 0
            ? (BuildFixedOffsets(parentCount, fixedLength), (byte[]?)null, 0, parentCount * fixedLength)
            : BuildOffsetsAndBitmap(
                repLevels, defLevels, nullDefThreshold, emptyDefThreshold, parentCount, numValues, repThreshold);

        // Filter phantom entries (outer null/empty list markers) before inner recursive assembly
        var keepIndices = ComputeKeepIndices(defLevels, emptyDefThreshold, numValues);
        if (keepIndices != null)
        {
            int subtreeLeafEnd = firstLeafIndex + CountLeaves(repeatedChild);
            FilterSubtree(ref leafArrays, ref leafDefLevels, ref leafRepLevels,
                keepIndices, firstLeafIndex, subtreeLeafEnd);
        }

        // Determine element node and assemble element array
        IArrowArray elementArray;
        Apache.Arrow.Field elementField;

        if (repeatedChild.IsLeaf)
        {
            // 2-level: repeated leaf is the element
            elementArray = leafArrays[leafIndex++];
            var elementType = ArrowSchemaConverter.ToArrowField(BuildTempDescriptor(repeatedChild)).DataType;
            elementField = new Apache.Arrow.Field(repeatedChild.Name, elementType, nullable: false);
        }
        else if (ArrowSchemaConverter.IsListNode(repeatedChild))
        {
            // Nested list: repeated child is itself a LIST → recurse with elementCount
            elementArray = AssembleList(repeatedChild, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,elementCount, ref leafIndex);
            elementField = NodeToField(repeatedChild);
        }
        else if (ArrowSchemaConverter.IsMapNode(repeatedChild))
        {
            // Nested map: repeated child is itself a MAP → recurse with elementCount
            elementArray = AssembleMap(repeatedChild, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,elementCount, ref leafIndex);
            elementField = NodeToField(repeatedChild);
        }
        else if (repeatedChild.Children.Count == 1)
        {
            // 3-level standard: recurse into the single element child
            var elementNode = repeatedChild.Children[0];

            if (ArrowSchemaConverter.IsListNode(elementNode))
            {
                // Element is itself a LIST → recurse with elementCount
                elementArray = AssembleList(elementNode, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,elementCount, ref leafIndex);
            }
            else if (ArrowSchemaConverter.IsMapNode(elementNode))
            {
                // Element is itself a MAP → recurse with elementCount
                elementArray = AssembleMap(elementNode, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,elementCount, ref leafIndex);
            }
            else
            {
                elementArray = AssembleNode(elementNode, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,elementCount, ref leafIndex);
            }
            elementField = NodeToField(elementNode);
        }
        else
        {
            // 3-level with multiple children → struct element
            var structChildArrays = new IArrowArray[repeatedChild.Children.Count];
            for (int i = 0; i < repeatedChild.Children.Count; i++)
                structChildArrays[i] = AssembleNode(repeatedChild.Children[i], leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,elementCount, ref leafIndex);

            var structChildFields = BuildChildFields(repeatedChild);
            var structType = new StructType(structChildFields);
            elementArray = new StructArray(structType, elementCount, structChildArrays, ArrowBuffer.Empty, nullCount: 0);
            elementField = new Apache.Arrow.Field(repeatedChild.Name, structType, nullable: false);
        }

        // Filter out phantom entries (null/empty list markers) from the element array
        elementArray = FilterElementArray(elementArray, defLevels, emptyDefThreshold);

        var listType = new ListType(elementField);
        var offsetsBuffer = ToArrowBuffer(offsets);
        var bitmapBuffer = nullCount > 0 ? new ArrowBuffer(bitmap!) : ArrowBuffer.Empty;

        return new ListArray(listType, parentCount, offsetsBuffer, elementArray, bitmapBuffer, nullCount);
    }

    private static IArrowArray AssembleMap(
        SchemaNode node,
        IArrowArray[] leafArrays,
        int[]?[] leafDefLevels,
        int[]?[] leafRepLevels,
        int[]? leafFixedLengths,
        int parentCount,
        ref int leafIndex)
    {
        var keyValueGroup = node.Children[0]; // repeated key_value group
        int firstLeafIndex = leafIndex;

        var repLevels = leafRepLevels[firstLeafIndex];
        var defLevels = leafDefLevels[firstLeafIndex];

        int nullDefThreshold = ComputeAccumulatedDefLevel(node);
        int emptyDefThreshold = ComputeAccumulatedDefLevel(keyValueGroup);
        int repThreshold = ComputeAccumulatedRepLevel(keyValueGroup);
        int numValues = repLevels?.Length ?? 0;

        // Build offsets FIRST to determine elementCount for inner assembly
        var (offsets, bitmap, nullCount, elementCount) = BuildOffsetsAndBitmap(
            repLevels, defLevels, nullDefThreshold, emptyDefThreshold, parentCount, numValues, repThreshold);

        // Filter phantom entries (outer null/empty map markers) before inner recursive assembly
        var keepIndices = ComputeKeepIndices(defLevels, emptyDefThreshold, numValues);
        if (keepIndices != null)
        {
            int subtreeLeafEnd = firstLeafIndex + CountLeaves(keyValueGroup);
            FilterSubtree(ref leafArrays, ref leafDefLevels, ref leafRepLevels,
                keepIndices, firstLeafIndex, subtreeLeafEnd);
        }

        // Assemble key and value arrays with elementCount as parentCount
        var keyNode = keyValueGroup.Children[0];
        var keyArray = AssembleNode(keyNode, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,elementCount, ref leafIndex);

        IArrowArray? valueArray = null;
        if (keyValueGroup.Children.Count > 1)
        {
            var valueNode = keyValueGroup.Children[1];
            valueArray = AssembleNode(valueNode, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths,elementCount, ref leafIndex);
        }

        // Filter out phantom entries from key and value arrays
        keyArray = FilterElementArray(keyArray, defLevels, emptyDefThreshold);
        if (valueArray != null)
            valueArray = FilterElementArray(valueArray, leafDefLevels[firstLeafIndex + 1] ?? defLevels, emptyDefThreshold);

        // Build the key_value struct array
        var keyField = NodeToField(keyNode);
        keyField = new Apache.Arrow.Field(keyField.Name, keyField.DataType, nullable: false); // keys are non-nullable

        Apache.Arrow.Field valueField;
        IArrowArray[] structChildren;
        if (valueArray != null)
        {
            var valueNode = keyValueGroup.Children[1];
            valueField = NodeToField(valueNode);
            structChildren = [keyArray, valueArray];
        }
        else
        {
            valueField = new Apache.Arrow.Field("value", Apache.Arrow.Types.StringType.Default, nullable: true);
            structChildren = [keyArray];
        }

        var mapType = new MapType(keyField, valueField);

        // StructArray length = total number of key-value entries
        int entryCount = keyArray.Length;
        var kvStructType = new StructType(new[] { keyField, valueField });
        var kvStruct = new StructArray(kvStructType, entryCount, structChildren, ArrowBuffer.Empty, nullCount: 0);

        var offsetsBuffer = ToArrowBuffer(offsets);
        var bitmapBuffer = nullCount > 0 ? new ArrowBuffer(bitmap!) : ArrowBuffer.Empty;

        return new MapArray(mapType, parentCount, offsetsBuffer, kvStruct, bitmapBuffer, nullCount);
    }

    /// <summary>
    /// Builds offsets and validity bitmap from rep/def levels.
    /// </summary>
    /// <param name="repLevels">Repetition levels for each encoded value.</param>
    /// <param name="defLevels">Definition levels for each encoded value.</param>
    /// <param name="nullDefThreshold">Accumulated def level of the LIST/MAP group node.
    /// defLevel &lt; this → the list/map itself is null.</param>
    /// <param name="emptyDefThreshold">Accumulated def level of the repeated child node.
    /// defLevel &lt; this but &gt;= nullDefThreshold → the list/map is present but empty.</param>
    /// <param name="parentCount">Number of parent slots (rows for top-level, element count for nested).</param>
    /// <param name="numValues">Number of encoded rep/def values.</param>
    /// <param name="repThreshold">Repetition level threshold for this list level.
    /// rep &lt; this → new parent slot; rep == this → new element; rep &gt; this → deeper nesting (skip).</param>
    private static (int[] offsets, byte[]? bitmap, int nullCount, int elementCount) BuildOffsetsAndBitmap(
        int[]? repLevels, int[]? defLevels,
        int nullDefThreshold, int emptyDefThreshold,
        int parentCount, int numValues, int repThreshold)
    {
        var offsets = new int[parentCount + 1];
        byte[]? bitmap = null;
        int nullCount = 0;

        if (repLevels == null || numValues == 0)
        {
            for (int i = 0; i <= parentCount; i++)
                offsets[i] = 0;
            return (offsets, bitmap, nullCount, 0);
        }

        int slot = 0;
        int elementOffset = 0;

        for (int i = 0; i < numValues; i++)
        {
            if (repLevels[i] < repThreshold)
            {
                // Start of a new parent slot
                if (i > 0)
                    slot++;

                offsets[slot] = elementOffset;

                if (defLevels != null && defLevels[i] < nullDefThreshold)
                {
                    // List/map is null at this slot
                    if (bitmap == null)
                        bitmap = CreateFullBitmap(parentCount);
                    bitmap[slot >> 3] &= (byte)~(1 << (slot & 7));
                    nullCount++;
                }
                else if (defLevels != null && defLevels[i] < emptyDefThreshold)
                {
                    // List/map is present but empty — no elements appended
                }
                else
                {
                    // Element present (possibly null element if def < maxDef)
                    elementOffset++;
                }
            }
            else if (repLevels[i] == repThreshold)
            {
                // New element at this list level
                elementOffset++;
            }
            // else: repLevels[i] > repThreshold → deeper nesting, skip
        }

        // Fill remaining slot starts (last slot + terminal offset)
        slot++;
        for (int i = slot; i <= parentCount; i++)
            offsets[i] = elementOffset;

        return (offsets, bitmap, nullCount, elementOffset);
    }

    /// <summary>
    /// Returns the detected fixed list length for a leaf, or 0 if the fast path does not apply here.
    /// </summary>
    /// <remarks>
    /// The detector only fires for <c>maxRepetitionLevel == 1</c>, so it can only describe the list
    /// level whose accumulated repetition level is 1; and it only fires when the reader skipped
    /// materialising levels, so a leaf that still carries repetition levels is not on the fast path.
    /// </remarks>
    private static int FixedLengthFor(int[]? leafFixedLengths, int leafIndex, int[]? repLevels, int repThreshold)
    {
        if (leafFixedLengths is null || repLevels is not null || repThreshold != 1)
            return 0;
        if ((uint)leafIndex >= (uint)leafFixedLengths.Length)
            return 0;
        return leafFixedLengths[leafIndex];
    }

    /// <summary>
    /// Builds list offsets for <paramref name="parentCount"/> lists of exactly
    /// <paramref name="fixedLength"/> elements each.
    /// </summary>
    private static int[] BuildFixedOffsets(int parentCount, int fixedLength)
    {
        var offsets = new int[parentCount + 1];
        int offset = 0;
        for (int i = 0; i <= parentCount; i++, offset += fixedLength)
            offsets[i] = offset;
        return offsets;
    }

    /// <summary>
    /// Creates a bitmap with all bits set to 1 (all valid).
    /// </summary>
    private static byte[] CreateFullBitmap(int count)
    {
        var bitmap = new byte[(count + 7) / 8];
        bitmap.AsSpan().Fill(0xFF);
        // Clear extra bits in the last byte
        int extra = count & 7;
        if (extra > 0)
            bitmap[^1] = (byte)((1 << extra) - 1);
        return bitmap;
    }

    private static Apache.Arrow.Field NodeToField(SchemaNode node)
    {
        bool nullable = node.Element.RepetitionType == FieldRepetitionType.Optional;

        if (node.IsLeaf)
        {
            var arrowType = ArrowSchemaConverter.ToArrowType(BuildTempDescriptor(node));
            return new Apache.Arrow.Field(node.Name, arrowType, nullable);
        }

        if (ArrowSchemaConverter.IsListNode(node))
        {
            var fields = ArrowSchemaConverter.ToArrowFields(
                new SchemaNode { Element = node.Element, Children = node.Children, Parent = node.Parent });
            // ToArrowFields returns fields for children; we need the list field for this node itself
            // Use the converter's field-building instead
        }

        // Delegate to the ArrowSchemaConverter for full recursive handling
        var dummyRoot = new SchemaNode
        {
            Element = new SchemaElement { Name = "__root__", NumChildren = 1 },
            Children = [node],
            Parent = null,
        };
        return ArrowSchemaConverter.ToArrowFields(dummyRoot)[0];
    }

    private static Field[] BuildChildFields(SchemaNode groupNode)
    {
        // Delegate to ArrowSchemaConverter for correct list/map/struct field building
        var fields = new Field[groupNode.Children.Count];
        for (int i = 0; i < groupNode.Children.Count; i++)
            fields[i] = NodeToField(groupNode.Children[i]);
        return fields;
    }

    /// <summary>
    /// Computes the accumulated repetition level for a node by counting repeated ancestors
    /// (including itself) up to but not including the root.
    /// </summary>
    private static int ComputeAccumulatedRepLevel(SchemaNode node)
    {
        int level = 0;
        var current = node;
        while (current.Parent != null)
        {
            if (current.Element.RepetitionType == FieldRepetitionType.Repeated)
                level++;
            current = current.Parent;
        }
        return level;
    }

    /// <summary>
    /// Computes the accumulated definition level for a node by counting optional/repeated ancestors
    /// (including itself) up to but not including the root.
    /// </summary>
    private static int ComputeAccumulatedDefLevel(SchemaNode node)
    {
        int level = 0;
        var current = node;
        while (current.Parent != null)
        {
            if (current.Element.RepetitionType == FieldRepetitionType.Optional ||
                current.Element.RepetitionType == FieldRepetitionType.Repeated)
                level++;
            current = current.Parent;
        }
        return level;
    }

    private static ColumnDescriptor BuildTempDescriptor(SchemaNode node) => new()
    {
        Path = [node.Name],
        PhysicalType = node.Element.Type!.Value,
        TypeLength = node.Element.TypeLength,
        MaxDefinitionLevel = 0,
        MaxRepetitionLevel = 0,
        SchemaElement = node.Element,
        SchemaNode = node,
    };

    /// <summary>
    /// Filters a dense leaf array by removing phantom entries (null/empty list markers)
    /// where defLevel &lt; emptyDefThreshold. These entries exist in the level data but
    /// don't correspond to actual list/map elements.
    /// </summary>
    private static IArrowArray FilterElementArray(
        IArrowArray denseArray, int[]? defLevels, int emptyDefThreshold)
    {
        if (defLevels == null)
            return denseArray;

        // Composite types assembled recursively already have the correct size
        if (denseArray is ListArray or MapArray or StructArray)
            return denseArray;

        // Count actual elements (entries where def >= emptyDefThreshold)
        int elementCount = 0;
        for (int i = 0; i < defLevels.Length; i++)
        {
            if (defLevels[i] >= emptyDefThreshold)
                elementCount++;
        }

        if (elementCount == denseArray.Length)
            return denseArray; // No phantom entries — array is already correct

        // Build index mapping: which positions in the dense array are actual elements
        var indices = new int[elementCount];
        int idx = 0;
        for (int i = 0; i < defLevels.Length; i++)
        {
            if (defLevels[i] >= emptyDefThreshold)
                indices[idx++] = i;
        }

        // Use Arrow's Take operation equivalent — build a new array from selected indices
        return ArrowCompute.Take(denseArray, indices.AsSpan(0, elementCount));
    }

    /// <summary>
    /// Recursively counts the number of leaf nodes in a schema subtree.
    /// </summary>
    private static int CountLeaves(SchemaNode node)
    {
        if (node.IsLeaf) return 1;
        int count = 0;
        for (int i = 0; i < node.Children.Count; i++)
            count += CountLeaves(node.Children[i]);
        return count;
    }

    /// <summary>
    /// Returns indices where defLevels[i] >= threshold (entries that belong to actual elements,
    /// not phantom null/empty markers from an outer list). Returns null if all entries qualify.
    /// </summary>
    private static int[]? ComputeKeepIndices(int[]? defLevels, int threshold, int numValues)
    {
        if (defLevels == null) return null;

        int keepCount = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[i] >= threshold)
                keepCount++;
        }

        if (keepCount == numValues) return null; // No phantoms

        var indices = new int[keepCount];
        int idx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[i] >= threshold)
                indices[idx++] = i;
        }
        return indices;
    }

    /// <summary>
    /// Extracts entries at the given keep positions from a level array.
    /// </summary>
    private static int[]? FilterLevelArray(int[]? levels, int[] keepIndices)
    {
        if (levels == null) return null;

        var filtered = new int[keepIndices.Length];
        for (int i = 0; i < keepIndices.Length; i++)
            filtered[i] = levels[keepIndices[i]];
        return filtered;
    }

    /// <summary>
    /// Filters leaf arrays, def levels, and rep levels for leaves in [startLeaf, endLeaf)
    /// by removing phantom entries (positions not in keepIndices). Creates shallow clones
    /// of the arrays so that the caller's originals are not modified.
    /// </summary>
    private static void FilterSubtree(
        ref IArrowArray[] leafArrays,
        ref int[]?[] leafDefLevels,
        ref int[]?[] leafRepLevels,
        int[] keepIndices,
        int startLeaf, int endLeaf)
    {
        leafArrays = (IArrowArray[])leafArrays.Clone();
        leafDefLevels = (int[]?[])leafDefLevels.Clone();
        leafRepLevels = (int[]?[])leafRepLevels.Clone();

        for (int i = startLeaf; i < endLeaf; i++)
        {
            leafDefLevels[i] = FilterLevelArray(leafDefLevels[i], keepIndices);
            leafRepLevels[i] = FilterLevelArray(leafRepLevels[i], keepIndices);
            leafArrays[i] = ArrowCompute.Take(leafArrays[i], keepIndices);
        }
    }

    /// <summary>
    /// Converts an int[] of offsets to an ArrowBuffer (reinterprets as bytes).
    /// </summary>
    private static ArrowBuffer ToArrowBuffer(int[] offsets)
    {
        var bytes = new byte[offsets.Length * sizeof(int)];
        MemoryMarshal.AsBytes(offsets.AsSpan()).CopyTo(bytes);
        return new ArrowBuffer(bytes);
    }
}
