// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decomposes Arrow nested arrays (struct, list, map) into flat leaf columns
/// with definition and repetition levels for Parquet writing.
/// Reverse of <see cref="NestedAssembler"/>.
/// </summary>
internal static class NestedLevelWriter
{
    /// <summary>
    /// Result of decomposing a nested Arrow column.
    /// </summary>
    internal readonly struct LeafColumn
    {
        /// <summary>Flat leaf array containing only non-null values (dense).</summary>
        public required IArrowArray Array { get; init; }

        /// <summary>Definition levels for every row/entry.</summary>
        public required int[] DefLevels { get; init; }

        /// <summary>Repetition levels for every row/entry (null if maxRepLevel == 0).</summary>
        public required int[]? RepLevels { get; init; }

        /// <summary>Path from root to this leaf (e.g., ["struct_col", "child_field"]).</summary>
        public required string[] PathInSchema { get; init; }

        /// <summary>Physical type of this leaf.</summary>
        public required PhysicalType PhysicalType { get; init; }

        /// <summary>Type length for FIXED_LEN_BYTE_ARRAY, else 0.</summary>
        public required int TypeLength { get; init; }

        /// <summary>Max definition level for this leaf.</summary>
        public required int MaxDefLevel { get; init; }

        /// <summary>Max repetition level for this leaf.</summary>
        public required int MaxRepLevel { get; init; }

        /// <summary>Number of non-null leaf values.</summary>
        public required int NonNullCount { get; init; }

        /// <summary>Total number of level entries (rows for top-level).</summary>
        public required int LevelCount { get; init; }
    }

    /// <summary>
    /// Decomposes a top-level Arrow column into one or more leaf columns with levels.
    /// </summary>
    public static List<LeafColumn> Decompose(
        IArrowArray array, Field field, int rowCount)
    {
        var leaves = new List<LeafColumn>();
        var path = new List<string> { field.Name };

        DecomposeRecursive(array, field, path, leaves,
            parentDefLevel: 0, parentRepLevel: 0,
            parentDefLevels: null, parentRepLevels: null,
            parentCount: rowCount);

        return leaves;
    }

    private static void DecomposeRecursive(
        IArrowArray array, Field field, List<string> path,
        List<LeafColumn> leaves,
        int parentDefLevel, int parentRepLevel,
        int[]? parentDefLevels, int[]? parentRepLevels,
        int parentCount)
    {
        // ExtensionType (e.g. VariantType wrapping StructType): unwrap to the
        // storage array and continue with the storage type. The schema-side
        // mapping in ArrowToSchemaConverter has already emitted the matching
        // logical-type annotation; here we only care about the physical
        // layout, which is the storage type's.
        if (field.DataType is ExtensionType ext)
        {
            var storageField = new Field(field.Name, ext.StorageType, field.IsNullable);
            var storageArray = array is ExtensionArray ea ? ea.Storage : array;
            DecomposeRecursive(storageArray, storageField, path, leaves,
                parentDefLevel, parentRepLevel,
                parentDefLevels, parentRepLevels, parentCount);
            return;
        }

        switch (field.DataType)
        {
            case StructType:
                DecomposeStruct(array, field, path, leaves,
                    parentDefLevel, parentRepLevel,
                    parentDefLevels, parentRepLevels, parentCount);
                break;

            case ListType:
                DecomposeList(array, field, path, leaves,
                    parentDefLevel, parentRepLevel,
                    parentDefLevels, parentRepLevels, parentCount);
                break;

            case MapType:
                DecomposeMap(array, field, path, leaves,
                    parentDefLevel, parentRepLevel,
                    parentDefLevels, parentRepLevels, parentCount);
                break;

            default:
                DecomposeLeaf(array, field, path, leaves,
                    parentDefLevel, parentRepLevel,
                    parentDefLevels, parentRepLevels, parentCount);
                break;
        }
    }

    private static void DecomposeLeaf(
        IArrowArray array, Field field, List<string> path,
        List<LeafColumn> leaves,
        int parentDefLevel, int parentRepLevel,
        int[]? parentDefLevels, int[]? parentRepLevels,
        int parentCount)
    {
        int maxDefLevel = parentDefLevel + (field.IsNullable ? 1 : 0);
        int maxRepLevel = parentRepLevel;

        var (physicalType, typeLength, _, _, _, _) = ArrowToSchemaConverter.MapArrowType(field.DataType);

        // Build def/rep levels
        int levelCount;
        int[] defLevels;
        int[]? repLevels;
        int nonNullCount;
        IArrowArray leafArray = array;

        if (parentDefLevels == null && parentRepLevels == null)
        {
            // Top-level flat column — 1:1 level-to-value mapping
            levelCount = parentCount;
            defLevels = new int[levelCount];
            repLevels = maxRepLevel > 0 ? new int[levelCount] : null;
            nonNullCount = 0;

            for (int i = 0; i < levelCount; i++)
            {
                if (!field.IsNullable || !array.IsNull(i))
                {
                    defLevels[i] = maxDefLevel;
                    nonNullCount++;
                }
                else
                {
                    defLevels[i] = maxDefLevel - 1;
                }
            }
        }
        else
        {
            // Nested leaf — level entries may exceed array values (phantom entries
            // for null/empty lists). Build a mapping from level index → array value index.
            levelCount = parentDefLevels?.Length ?? parentCount;
            defLevels = new int[levelCount];
            repLevels = parentRepLevels != null ? new int[levelCount] : (maxRepLevel > 0 ? new int[levelCount] : null);
            nonNullCount = 0;

            // valueMap[i] = array index for level entry i, or -1 if phantom
            var valueMap = new int[levelCount];
            int valueIdx = 0;

            for (int i = 0; i < levelCount; i++)
            {
                int pDef = parentDefLevels?[i] ?? parentDefLevel;
                if (repLevels != null && parentRepLevels != null)
                    repLevels[i] = parentRepLevels[i];

                if (pDef < parentDefLevel)
                {
                    // Parent is null/absent — phantom entry
                    defLevels[i] = pDef;
                    valueMap[i] = -1;
                }
                else
                {
                    // Parent is present — this maps to an actual array value
                    valueMap[i] = valueIdx;
                    if (!field.IsNullable || !array.IsNull(valueIdx))
                    {
                        defLevels[i] = maxDefLevel;
                        nonNullCount++;
                    }
                    else
                    {
                        defLevels[i] = maxDefLevel - 1;
                    }
                    valueIdx++;
                }
            }

            // If level count exceeds array length (repeated columns with phantoms),
            // expand the array to match level count so value encoding can index by level position
            if (levelCount > array.Length)
                leafArray = ExpandArray(array, valueMap, levelCount);
        }

        leaves.Add(new LeafColumn
        {
            Array = leafArray,
            DefLevels = defLevels,
            RepLevels = repLevels,
            PathInSchema = path.ToArray(),
            PhysicalType = physicalType,
            TypeLength = typeLength ?? 0,
            MaxDefLevel = maxDefLevel,
            MaxRepLevel = maxRepLevel,
            NonNullCount = nonNullCount,
            LevelCount = levelCount,
        });
    }

    /// <summary>
    /// Expands a dense array to match level count by inserting placeholder values
    /// at phantom positions (where valueMap[i] == -1).
    /// </summary>
    private static IArrowArray ExpandArray(IArrowArray denseArray, int[] valueMap, int expandedLength)
    {
        switch (denseArray)
        {
            case Int32Array:
                return ExpandFixedWidth<int>(denseArray, valueMap, expandedLength, denseArray.Data.DataType);
            case Int64Array:
                return ExpandFixedWidth<long>(denseArray, valueMap, expandedLength, denseArray.Data.DataType);
            case FloatArray:
                return ExpandFixedWidth<float>(denseArray, valueMap, expandedLength, denseArray.Data.DataType);
            case DoubleArray:
                return ExpandFixedWidth<double>(denseArray, valueMap, expandedLength, denseArray.Data.DataType);
            case StringArray or BinaryArray:
                return ExpandVarBinary(denseArray, valueMap, expandedLength, denseArray.Data.DataType);
            case BooleanArray:
                return ExpandBoolean((BooleanArray)denseArray, valueMap, expandedLength);
            case FixedSizeBinaryArray fsb:
                return ExpandFixedBytes(denseArray, valueMap, expandedLength,
                    ((FixedSizeBinaryType)fsb.Data.DataType).ByteWidth);
            default:
                // For other types, try generic fixed-width
                return ExpandFixedWidth<long>(denseArray, valueMap, expandedLength, denseArray.Data.DataType);
        }
    }

    private static IArrowArray ExpandFixedWidth<T>(
        IArrowArray source, int[] valueMap, int expandedLength, IArrowType arrowType)
        where T : struct
    {
        int elementSize = System.Runtime.CompilerServices.Unsafe.SizeOf<T>();
        var srcSpan = source.Data.Buffers[1].Span;
        var values = new byte[expandedLength * elementSize];
        var bitmap = new byte[(expandedLength + 7) / 8];
        int nullCount = 0;

        for (int i = 0; i < expandedLength; i++)
        {
            int srcIdx = valueMap[i];
            if (srcIdx >= 0 && !source.IsNull(srcIdx))
            {
                srcSpan.Slice(srcIdx * elementSize, elementSize).CopyTo(values.AsSpan(i * elementSize));
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                nullCount++;
            }
        }

        var buffers = new[] { new ArrowBuffer(bitmap), new ArrowBuffer(values) };
        var data = new ArrayData(arrowType, expandedLength, nullCount, 0, buffers);
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray ExpandVarBinary(
        IArrowArray source, int[] valueMap, int expandedLength, IArrowType arrowType)
    {
        var srcOffsets = MemoryMarshal.Cast<byte, int>(source.Data.Buffers[1].Span);
        var srcData = source.Data.Buffers[2].Span;
        var bitmap = new byte[(expandedLength + 7) / 8];
        int nullCount = 0;

        // First pass: compute new offsets and total data size
        var newOffsets = new int[expandedLength + 1];
        int totalLen = 0;
        for (int i = 0; i < expandedLength; i++)
        {
            newOffsets[i] = totalLen;
            int srcIdx = valueMap[i];
            if (srcIdx >= 0 && !source.IsNull(srcIdx))
            {
                int len = srcOffsets[srcIdx + 1] - srcOffsets[srcIdx];
                totalLen += len;
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                nullCount++;
            }
        }
        newOffsets[expandedLength] = totalLen;

        // Second pass: copy data
        var newData = new byte[totalLen];
        int pos = 0;
        for (int i = 0; i < expandedLength; i++)
        {
            int srcIdx = valueMap[i];
            if (srcIdx >= 0 && !source.IsNull(srcIdx))
            {
                int start = srcOffsets[srcIdx];
                int len = srcOffsets[srcIdx + 1] - start;
                srcData.Slice(start, len).CopyTo(newData.AsSpan(pos));
                pos += len;
            }
        }

        var offsetBytes = new byte[newOffsets.Length * sizeof(int)];
        MemoryMarshal.AsBytes(newOffsets.AsSpan()).CopyTo(offsetBytes);

        var buffers = new[] { new ArrowBuffer(bitmap), new ArrowBuffer(offsetBytes), new ArrowBuffer(newData) };
        var data = new ArrayData(arrowType, expandedLength, nullCount, 0, buffers);
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray ExpandBoolean(BooleanArray source, int[] valueMap, int expandedLength)
    {
        var valueBits = new byte[(expandedLength + 7) / 8];
        var bitmap = new byte[(expandedLength + 7) / 8];
        int nullCount = 0;

        for (int i = 0; i < expandedLength; i++)
        {
            int srcIdx = valueMap[i];
            if (srcIdx >= 0 && !source.IsNull(srcIdx))
            {
                if (source.GetValue(srcIdx) == true)
                    valueBits[i >> 3] |= (byte)(1 << (i & 7));
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                nullCount++;
            }
        }

        var buffers = new[] { new ArrowBuffer(bitmap), new ArrowBuffer(valueBits) };
        return new BooleanArray(new ArrayData(BooleanType.Default, expandedLength, nullCount, 0, buffers));
    }

    private static IArrowArray ExpandFixedBytes(
        IArrowArray source, int[] valueMap, int expandedLength, int byteWidth)
    {
        var srcSpan = source.Data.Buffers[1].Span;
        var values = new byte[expandedLength * byteWidth];
        var bitmap = new byte[(expandedLength + 7) / 8];
        int nullCount = 0;

        for (int i = 0; i < expandedLength; i++)
        {
            int srcIdx = valueMap[i];
            if (srcIdx >= 0 && !source.IsNull(srcIdx))
            {
                srcSpan.Slice(srcIdx * byteWidth, byteWidth).CopyTo(values.AsSpan(i * byteWidth));
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                nullCount++;
            }
        }

        var buffers = new[] { new ArrowBuffer(bitmap), new ArrowBuffer(values) };
        var data = new ArrayData(source.Data.DataType, expandedLength, nullCount, 0, buffers);
        return ArrowArrayFactory.BuildArray(data);
    }

    private static void DecomposeStruct(
        IArrowArray array, Field field, List<string> path,
        List<LeafColumn> leaves,
        int parentDefLevel, int parentRepLevel,
        int[]? parentDefLevels, int[]? parentRepLevels,
        int parentCount)
    {
        var structArray = (StructArray)array;
        var structType = (StructType)field.DataType;
        int myDefLevel = parentDefLevel + (field.IsNullable ? 1 : 0);
        int myRepLevel = parentRepLevel;

        // Compute def/rep levels for children
        int levelCount = parentDefLevels?.Length ?? parentCount;
        int[]? myDefLevels = null;

        if (field.IsNullable || parentDefLevels != null)
        {
            myDefLevels = new int[levelCount];
            int valueIdx = 0;

            for (int i = 0; i < levelCount; i++)
            {
                int pDef = parentDefLevels?[i] ?? parentDefLevel;

                if (pDef < parentDefLevel)
                {
                    // Ancestor is null
                    myDefLevels[i] = pDef;
                }
                else if (field.IsNullable && structArray.IsNull(valueIdx))
                {
                    myDefLevels[i] = myDefLevel - 1;
                    valueIdx++;
                }
                else
                {
                    myDefLevels[i] = myDefLevel;
                    valueIdx++;
                }
            }
        }

        // Recurse into children
        for (int c = 0; c < structType.Fields.Count; c++)
        {
            var childField = structType.Fields[c];
            var childArray = structArray.Fields[c];

            path.Add(childField.Name);
            DecomposeRecursive(childArray, childField, path, leaves,
                myDefLevel, myRepLevel, myDefLevels, parentRepLevels, parentCount);
            path.RemoveAt(path.Count - 1);
        }
    }

    private static void DecomposeList(
        IArrowArray array, Field field, List<string> path,
        List<LeafColumn> leaves,
        int parentDefLevel, int parentRepLevel,
        int[]? parentDefLevels, int[]? parentRepLevels,
        int parentCount)
    {
        var listArray = (ListArray)array;
        var listType = (ListType)field.DataType;

        // 3-level: optional group (LIST) → repeated group "list" → element
        // LIST group adds 1 def level if nullable
        int listDefLevel = parentDefLevel + (field.IsNullable ? 1 : 0);
        // Repeated "list" group adds 1 def level and 1 rep level
        int repeatedDefLevel = listDefLevel + 1;
        int repeatedRepLevel = parentRepLevel + 1;

        var elementField = listType.ValueField;
        var elementArray = listArray.Values;
        var offsets = listArray.ValueOffsets;

        // Build def/rep levels
        var defList = new List<int>();
        var repList = new List<int>();
        int inputCount = parentDefLevels?.Length ?? parentCount;

        int slotIdx = 0; // index into listArray slots
        for (int i = 0; i < inputCount; i++)
        {
            int pDef = parentDefLevels?[i] ?? parentDefLevel;
            int pRep = parentRepLevels?[i] ?? 0;

            if (pDef < parentDefLevel)
            {
                // Ancestor is null — emit phantom entry
                defList.Add(pDef);
                repList.Add(pRep);
            }
            else if (field.IsNullable && listArray.IsNull(slotIdx))
            {
                // List itself is null
                defList.Add(listDefLevel - 1);
                repList.Add(pRep);
                slotIdx++;
            }
            else
            {
                // List is present
                int start = offsets[slotIdx];
                int end = offsets[slotIdx + 1];
                int length = end - start;

                if (length == 0)
                {
                    // Empty list
                    defList.Add(listDefLevel);
                    repList.Add(pRep);
                }
                else
                {
                    for (int j = 0; j < length; j++)
                    {
                        defList.Add(repeatedDefLevel); // placeholder — child will add more
                        repList.Add(j == 0 ? pRep : repeatedRepLevel);
                    }
                }

                slotIdx++;
            }
        }

        var myDefLevels = defList.ToArray();
        var myRepLevels = repList.ToArray();

        // Recurse into element
        path.Add("list");
        path.Add(elementField.Name);
        DecomposeRecursive(elementArray, elementField, path, leaves,
            repeatedDefLevel, repeatedRepLevel, myDefLevels, myRepLevels, parentCount);
        path.RemoveAt(path.Count - 1);
        path.RemoveAt(path.Count - 1);
    }

    private static void DecomposeMap(
        IArrowArray array, Field field, List<string> path,
        List<LeafColumn> leaves,
        int parentDefLevel, int parentRepLevel,
        int[]? parentDefLevels, int[]? parentRepLevels,
        int parentCount)
    {
        var mapArray = (MapArray)array;
        var mapType = (MapType)field.DataType;

        // MAP group → repeated group "key_value" → key + value
        int mapDefLevel = parentDefLevel + (field.IsNullable ? 1 : 0);
        int repeatedDefLevel = mapDefLevel + 1;
        int repeatedRepLevel = parentRepLevel + 1;

        var keyField = new Field(mapType.KeyField.Name, mapType.KeyField.DataType, nullable: false);
        var valueField = mapType.ValueField;
        var keyArray = mapArray.Keys;
        var valueArray = mapArray.Values;
        var offsets = mapArray.ValueOffsets;

        // Build def/rep levels (same structure as list)
        var defList = new List<int>();
        var repList = new List<int>();
        int inputCount = parentDefLevels?.Length ?? parentCount;

        int slotIdx = 0;
        for (int i = 0; i < inputCount; i++)
        {
            int pDef = parentDefLevels?[i] ?? parentDefLevel;
            int pRep = parentRepLevels?[i] ?? 0;

            if (pDef < parentDefLevel)
            {
                defList.Add(pDef);
                repList.Add(pRep);
            }
            else if (field.IsNullable && mapArray.IsNull(slotIdx))
            {
                defList.Add(mapDefLevel - 1);
                repList.Add(pRep);
                slotIdx++;
            }
            else
            {
                int start = offsets[slotIdx];
                int end = offsets[slotIdx + 1];
                int length = end - start;

                if (length == 0)
                {
                    defList.Add(mapDefLevel);
                    repList.Add(pRep);
                }
                else
                {
                    for (int j = 0; j < length; j++)
                    {
                        defList.Add(repeatedDefLevel);
                        repList.Add(j == 0 ? pRep : repeatedRepLevel);
                    }
                }

                slotIdx++;
            }
        }

        var myDefLevels = defList.ToArray();
        var myRepLevels = repList.ToArray();

        // Recurse into key and value
        path.Add("key_value");

        path.Add(keyField.Name);
        DecomposeRecursive(keyArray, keyField, path, leaves,
            repeatedDefLevel, repeatedRepLevel, myDefLevels, myRepLevels, parentCount);
        path.RemoveAt(path.Count - 1);

        path.Add(valueField.Name);
        DecomposeRecursive(valueArray, valueField, path, leaves,
            repeatedDefLevel, repeatedRepLevel, myDefLevels, myRepLevels, parentCount);
        path.RemoveAt(path.Count - 1);

        path.RemoveAt(path.Count - 1);
    }
}
