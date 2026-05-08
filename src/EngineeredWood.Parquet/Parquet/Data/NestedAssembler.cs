// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
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
    /// <param name="extensionRegistry">
    /// Optional Arrow extension registry. When supplied and the registry knows
    /// <c>arrow.parquet.variant</c>, top-level groups annotated with the
    /// Parquet <c>VARIANT</c> logical type are returned as
    /// <see cref="VariantArray"/> rather than the bare storage
    /// <see cref="StructArray"/>. Deeply nested VARIANT groups (inside
    /// list/map) are not yet wrapped.
    /// </param>
    /// <returns>Top-level arrays matching the root's children.</returns>
    public static IArrowArray[] Assemble(
        SchemaNode root,
        IArrowArray[] leafArrays,
        int[]?[] leafDefLevels,
        int[]?[] leafRepLevels,
        int rowCount,
        ExtensionTypeRegistry? extensionRegistry = null)
    {
        var result = new IArrowArray[root.Children.Count];
        int leafIndex = 0;

        for (int i = 0; i < root.Children.Count; i++)
        {
            var child = root.Children[i];
            var array = AssembleNode(child, leafArrays, leafDefLevels, leafRepLevels, rowCount, ref leafIndex);
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
            return ext.CreateArray(sa);
        }
        return array;
    }

    private static IArrowArray AssembleNode(
        SchemaNode node,
        IArrowArray[] leafArrays,
        int[]?[] leafDefLevels,
        int[]?[] leafRepLevels,
        int parentCount,
        ref int leafIndex)
    {
        if (node.IsLeaf)
        {
            // Bare repeated primitive: leaf with Repeated → wrap in list
            if (node.Element.RepetitionType == FieldRepetitionType.Repeated)
                return AssembleBareRepeatedLeaf(node, leafArrays, leafDefLevels, leafRepLevels, parentCount, ref leafIndex);

            return leafArrays[leafIndex++];
        }

        if (ArrowSchemaConverter.IsListNode(node))
            return AssembleList(node, leafArrays, leafDefLevels, leafRepLevels, parentCount, ref leafIndex);
        if (ArrowSchemaConverter.IsMapNode(node))
            return AssembleMap(node, leafArrays, leafDefLevels, leafRepLevels, parentCount, ref leafIndex);

        return AssembleStruct(node, leafArrays, leafDefLevels, leafRepLevels, parentCount, ref leafIndex);
    }

    private static IArrowArray AssembleStruct(
        SchemaNode node,
        IArrowArray[] leafArrays,
        int[]?[] leafDefLevels,
        int[]?[] leafRepLevels,
        int parentCount,
        ref int leafIndex)
    {
        int firstLeafIndex = leafIndex;

        var childArrays = new IArrowArray[node.Children.Count];
        for (int i = 0; i < node.Children.Count; i++)
            childArrays[i] = AssembleNode(node.Children[i], leafArrays, leafDefLevels, leafRepLevels, parentCount, ref leafIndex);

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

        var (offsets, bitmap, nullCount, _) = BuildOffsetsAndBitmap(
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

        // Build offsets FIRST to determine elementCount for inner assembly
        var (offsets, bitmap, nullCount, elementCount) = BuildOffsetsAndBitmap(
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
            elementArray = AssembleList(repeatedChild, leafArrays, leafDefLevels, leafRepLevels, elementCount, ref leafIndex);
            elementField = NodeToField(repeatedChild);
        }
        else if (ArrowSchemaConverter.IsMapNode(repeatedChild))
        {
            // Nested map: repeated child is itself a MAP → recurse with elementCount
            elementArray = AssembleMap(repeatedChild, leafArrays, leafDefLevels, leafRepLevels, elementCount, ref leafIndex);
            elementField = NodeToField(repeatedChild);
        }
        else if (repeatedChild.Children.Count == 1)
        {
            // 3-level standard: recurse into the single element child
            var elementNode = repeatedChild.Children[0];

            if (ArrowSchemaConverter.IsListNode(elementNode))
            {
                // Element is itself a LIST → recurse with elementCount
                elementArray = AssembleList(elementNode, leafArrays, leafDefLevels, leafRepLevels, elementCount, ref leafIndex);
            }
            else if (ArrowSchemaConverter.IsMapNode(elementNode))
            {
                // Element is itself a MAP → recurse with elementCount
                elementArray = AssembleMap(elementNode, leafArrays, leafDefLevels, leafRepLevels, elementCount, ref leafIndex);
            }
            else
            {
                elementArray = AssembleNode(elementNode, leafArrays, leafDefLevels, leafRepLevels, elementCount, ref leafIndex);
            }
            elementField = NodeToField(elementNode);
        }
        else
        {
            // 3-level with multiple children → struct element
            var structChildArrays = new IArrowArray[repeatedChild.Children.Count];
            for (int i = 0; i < repeatedChild.Children.Count; i++)
                structChildArrays[i] = AssembleNode(repeatedChild.Children[i], leafArrays, leafDefLevels, leafRepLevels, elementCount, ref leafIndex);

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
        var keyArray = AssembleNode(keyNode, leafArrays, leafDefLevels, leafRepLevels, elementCount, ref leafIndex);

        IArrowArray? valueArray = null;
        if (keyValueGroup.Children.Count > 1)
        {
            var valueNode = keyValueGroup.Children[1];
            valueArray = AssembleNode(valueNode, leafArrays, leafDefLevels, leafRepLevels, elementCount, ref leafIndex);
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
        return TakeArray(denseArray, indices, elementCount);
    }

    /// <summary>
    /// Creates a new array by selecting elements at the given indices from the source array.
    /// </summary>
    private static IArrowArray TakeArray(IArrowArray source, int[] indices, int count)
    {
        // For each array type, extract the selected elements
        switch (source)
        {
            case Int8Array a:
                return TakeFixedWidth<sbyte>(a, indices, count, a.Data.DataType);
            case UInt8Array a:
                return TakeFixedWidth<byte>(a, indices, count, a.Data.DataType);
            case Int16Array a:
                return TakeFixedWidth<short>(a, indices, count, a.Data.DataType);
            case UInt16Array a:
                return TakeFixedWidth<ushort>(a, indices, count, a.Data.DataType);
            case Int32Array a:
                return TakeFixedWidth<int>(a, indices, count, a.Data.DataType);
            case UInt32Array a:
                return TakeFixedWidth<uint>(a, indices, count, a.Data.DataType);
            case Int64Array a:
                return TakeFixedWidth<long>(a, indices, count, a.Data.DataType);
            case UInt64Array a:
                return TakeFixedWidth<ulong>(a, indices, count, a.Data.DataType);
            case FloatArray a:
                return TakeFixedWidth<float>(a, indices, count, a.Data.DataType);
            case DoubleArray a:
                return TakeFixedWidth<double>(a, indices, count, a.Data.DataType);
#if NET6_0_OR_GREATER
            case HalfFloatArray a:
                return TakeFixedWidth<Half>(a, indices, count, a.Data.DataType);
#endif
            case Date32Array a:
                return TakeFixedWidth<int>(a, indices, count, a.Data.DataType);
            case TimestampArray a:
                return TakeFixedWidth<long>(a, indices, count, a.Data.DataType);
            case Time32Array a:
                return TakeFixedWidth<int>(a, indices, count, a.Data.DataType);
            case Time64Array a:
                return TakeFixedWidth<long>(a, indices, count, a.Data.DataType);
            case BooleanArray a:
                return TakeBoolean(a, indices, count);
            case StringArray a:
                return TakeVarBinary(a, indices, count, Apache.Arrow.Types.StringType.Default);
            case BinaryArray a:
                return TakeVarBinary(a, indices, count, BinaryType.Default);
            case Decimal32Array:
                return TakeFixedBytes(source, indices, count, 4);
            case Decimal64Array:
                return TakeFixedBytes(source, indices, count, 8);
            case Decimal128Array:
                return TakeFixedBytes(source, indices, count, 16);
            case Decimal256Array:
                return TakeFixedBytes(source, indices, count, 32);
            case FixedSizeBinaryArray fsb:
                return TakeFixedBytes(source, indices, count, ((FixedSizeBinaryType)fsb.Data.DataType).ByteWidth);
            case NullArray:
                return new NullArray(count);
            default:
                throw new NotSupportedException(
                    $"TakeArray not supported for {source.GetType().Name}");
        }
    }

    private static IArrowArray TakeFixedWidth<T>(IArrowArray source, int[] indices, int count, IArrowType arrowType)
        where T : struct
    {
        var values = new T[count];
        var bitmapBytes = new byte[(count + 7) / 8];
        int nullCount = 0;

        for (int i = 0; i < count; i++)
        {
            int srcIdx = indices[i];
            if (!source.IsNull(srcIdx))
            {
                // Read value from source array data
                var srcSpan = source.Data.Buffers[1].Span;
                int size = System.Runtime.CompilerServices.Unsafe.SizeOf<T>();
                values[i] = MemoryMarshal.Read<T>(srcSpan.Slice(srcIdx * size, size));
                bitmapBytes[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                nullCount++;
            }
        }

        var valueBytes = new byte[count * System.Runtime.CompilerServices.Unsafe.SizeOf<T>()];
        MemoryMarshal.AsBytes(values.AsSpan()).CopyTo(valueBytes);

        var buffers = nullCount > 0
            ? new[] { new ArrowBuffer(bitmapBytes), new ArrowBuffer(valueBytes) }
            : new[] { ArrowBuffer.Empty, new ArrowBuffer(valueBytes) };

        var data = new ArrayData(arrowType, count, nullCount, offset: 0, buffers);
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray TakeFixedBytes(IArrowArray source, int[] indices, int count, int byteWidth)
    {
        var valueBytes = new byte[count * byteWidth];
        var bitmapBytes = new byte[(count + 7) / 8];
        int nullCount = 0;
        var srcSpan = source.Data.Buffers[1].Span;

        for (int i = 0; i < count; i++)
        {
            int srcIdx = indices[i];
            if (!source.IsNull(srcIdx))
            {
                srcSpan.Slice(srcIdx * byteWidth, byteWidth)
                    .CopyTo(valueBytes.AsSpan(i * byteWidth, byteWidth));
                bitmapBytes[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                nullCount++;
            }
        }

        var buffers = nullCount > 0
            ? new[] { new ArrowBuffer(bitmapBytes), new ArrowBuffer(valueBytes) }
            : new[] { ArrowBuffer.Empty, new ArrowBuffer(valueBytes) };

        var data = new ArrayData(source.Data.DataType, count, nullCount, offset: 0, buffers);
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray TakeBoolean(BooleanArray source, int[] indices, int count)
    {
        var valueBits = new byte[(count + 7) / 8];
        var bitmapBytes = new byte[(count + 7) / 8];
        int nullCount = 0;

        for (int i = 0; i < count; i++)
        {
            int srcIdx = indices[i];
            if (!source.IsNull(srcIdx))
            {
                if (source.GetValue(srcIdx) == true)
                    valueBits[i >> 3] |= (byte)(1 << (i & 7));
                bitmapBytes[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                nullCount++;
            }
        }

        var buffers = nullCount > 0
            ? new[] { new ArrowBuffer(bitmapBytes), new ArrowBuffer(valueBits) }
            : new[] { ArrowBuffer.Empty, new ArrowBuffer(valueBits) };

        return new BooleanArray(new ArrayData(BooleanType.Default, count, nullCount, 0, buffers));
    }

    private static IArrowArray TakeVarBinary(IArrowArray source, int[] indices, int count, IArrowType arrowType)
    {
        // Read source offsets and data
        var srcOffsets = MemoryMarshal.Cast<byte, int>(source.Data.Buffers[1].Span);
        var srcData = source.Data.Buffers[2].Span;

        var bitmapBytes = new byte[(count + 7) / 8];
        int nullCount = 0;

        // First pass: compute total data size and new offsets
        var newOffsets = new int[count + 1];
        int totalDataLen = 0;
        for (int i = 0; i < count; i++)
        {
            int srcIdx = indices[i] + source.Data.Offset;
            newOffsets[i] = totalDataLen;
            if (!source.IsNull(indices[i]))
            {
                int len = srcOffsets[srcIdx + 1] - srcOffsets[srcIdx];
                totalDataLen += len;
                bitmapBytes[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                nullCount++;
            }
        }
        newOffsets[count] = totalDataLen;

        // Second pass: copy data
        var newData = new byte[totalDataLen];
        int pos = 0;
        for (int i = 0; i < count; i++)
        {
            int srcIdx = indices[i] + source.Data.Offset;
            if (!source.IsNull(indices[i]))
            {
                int start = srcOffsets[srcIdx];
                int len = srcOffsets[srcIdx + 1] - start;
                srcData.Slice(start, len).CopyTo(newData.AsSpan(pos, len));
                pos += len;
            }
        }

        var offsetBytes = new byte[newOffsets.Length * sizeof(int)];
        MemoryMarshal.AsBytes(newOffsets.AsSpan()).CopyTo(offsetBytes);

        var buffers = nullCount > 0
            ? new[] { new ArrowBuffer(bitmapBytes), new ArrowBuffer(offsetBytes), new ArrowBuffer(newData) }
            : new[] { ArrowBuffer.Empty, new ArrowBuffer(offsetBytes), new ArrowBuffer(newData) };

        var data = new ArrayData(arrowType, count, nullCount, 0, buffers);
        return ArrowArrayFactory.BuildArray(data);
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
            leafArrays[i] = TakeArray(leafArrays[i], keepIndices, keepIndices.Length);
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
