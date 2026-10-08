// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
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

        // The recursive walk reads raw spans off the caller's array and its children while building the
        // leaf columns. Roots it for the duration; see doc/arrow-span-lifetime.md.
        GC.KeepAlive(array);
        return leaves;
    }

    // parentValueMap (optional): level index -> this array's value index, or -1 where an ANCESTOR is absent.
    // Produced by STRUCT parents: a null struct row still OCCUPIES a child slot (Arrow struct children are 1:1
    // with parent rows), unlike a null/empty list — sequential "consume only when present" indexing therefore
    // misaligns every child value after a null struct row (and corrupted the written file). Null = derive
    // sequentially (the historical list semantics, still correct for children of lists/maps).
    private static void DecomposeRecursive(
        IArrowArray array, Field field, List<string> path,
        List<LeafColumn> leaves,
        int parentDefLevel, int parentRepLevel,
        int[]? parentDefLevels, int[]? parentRepLevels,
        int parentCount, int[]? parentValueMap = null)
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
                parentDefLevels, parentRepLevels, parentCount, parentValueMap);
            return;
        }

        switch (field.DataType)
        {
            case StructType:
                DecomposeStruct(array, field, path, leaves,
                    parentDefLevel, parentRepLevel,
                    parentDefLevels, parentRepLevels, parentCount, parentValueMap);
                break;

            case ListType:
                DecomposeList(array, field, path, leaves,
                    parentDefLevel, parentRepLevel,
                    parentDefLevels, parentRepLevels, parentCount, parentValueMap);
                break;

            case FixedSizeListType:
                DecomposeFixedList(array, field, path, leaves,
                    parentDefLevel, parentRepLevel,
                    parentDefLevels, parentRepLevels, parentCount, parentValueMap);
                break;

            case MapType:
                DecomposeMap(array, field, path, leaves,
                    parentDefLevel, parentRepLevel,
                    parentDefLevels, parentRepLevels, parentCount, parentValueMap);
                break;

            default:
                DecomposeLeaf(array, field, path, leaves,
                    parentDefLevel, parentRepLevel,
                    parentDefLevels, parentRepLevels, parentCount, parentValueMap);
                break;
        }
    }

    private static void DecomposeLeaf(
        IArrowArray array, Field field, List<string> path,
        List<LeafColumn> leaves,
        int parentDefLevel, int parentRepLevel,
        int[]? parentDefLevels, int[]? parentRepLevels,
        int parentCount, int[]? parentValueMap = null)
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
        // A run-end encoded array keeps its nulls in its values, so its own IsNull is false everywhere (#480
        // review); read the runs instead.
        bool[]? runNulls = array is RunEndEncodedArray runs ? RunNulls(runs) : null;
        bool IsNullAt(int index) => runNulls?[index] ?? array.IsNull(index);

        if (parentDefLevels == null && parentRepLevels == null)
        {
            // Top-level flat column — 1:1 level-to-value mapping
            levelCount = parentCount;
            defLevels = new int[levelCount];
            repLevels = maxRepLevel > 0 ? new int[levelCount] : null;
            nonNullCount = 0;

            for (int i = 0; i < levelCount; i++)
            {
                if (!field.IsNullable || !IsNullAt(i))
                {
                    defLevels[i] = maxDefLevel;
                    nonNullCount++;
                }
                else
                {
                    defLevels[i] = maxDefLevel - 1;
                }
            }

            // A required struct's fields come here too (it gives them no levels), and a field may be a slice of
            // its own or hold more values than the struct has rows; the encoders read raw buffers from slot 0,
            // for the array's whole length.
            if (levelCount != array.Length || array.Data.Offset != 0)
            {
                var identity = new int[levelCount];
                for (int i = 0; i < levelCount; i++)
                    identity[i] = i;
                leafArray = ExpandArray(array, identity, levelCount);
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

            // valueMap[i] = array index for level entry i, or -1 if phantom. A STRUCT parent supplies the
            // mapping explicitly (a null struct row still occupies a child slot); without one, values are
            // consumed sequentially per present level (list semantics).
            var valueMap = new int[levelCount];
            int valueIdx = 0;
            bool identityMap = true;

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
                    identityMap = false;
                }
                else
                {
                    // Parent is present — this maps to an actual array value
                    int idx = parentValueMap?[i] ?? valueIdx++;
                    valueMap[i] = idx;
                    if (idx != i)
                        identityMap = false;
                    if (!field.IsNullable || !IsNullAt(idx))
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

            // Value encoding indexes the array's raw buffers BY LEVEL POSITION from slot 0. So rebuild it in level
            // order whenever the level->value mapping is not the identity (phantoms, or a non-trivial struct/list
            // mapping), the array holds values past the last level (a list's unreferenced trailing elements, #470),
            // or the array is a slice, as a struct's fields are when the struct is.
            if (!identityMap || levelCount != array.Length || array.Data.Offset != 0)
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
    /// The leaf array in level order: level <c>i</c> holds <paramref name="source"/>'s element
    /// <c>valueMap[i]</c>, or null where that is -1 (a phantom level, under an absent ancestor).
    /// </summary>
    /// <remarks>
    /// Built from <see cref="ArrowCompute.Take(IArrowArray, List{int})"/>, which gathers every layout the writer
    /// accepts and reads logical positions (so a sliced child is read at its own offset), and
    /// <see cref="ArrowCompute.Scatter(IArrowArray, List{int}, int)"/> for the phantoms. Encoders and statistics
    /// skip a phantom by its definition level; the null keeps <c>IsNull</c> truthful for anything that asks.
    /// Without phantoms the gather alone is the answer, and a run-end encoded leaf keeps its runs. A run-end
    /// encoded array has no validity bitmap to scatter nulls into, so with phantoms it is expanded to its plain
    /// values first; the column writer takes either form (#480 review).
    /// </remarks>
    private static IArrowArray ExpandArray(IArrowArray source, int[] valueMap, int expandedLength)
    {
        var sourceRows = new List<int>(expandedLength);
        var levels = new List<int>(expandedLength);
        for (int i = 0; i < expandedLength; i++)
        {
            if (valueMap[i] >= 0)
            {
                sourceRows.Add(valueMap[i]);
                levels.Add(i);
            }
        }

        var gathered = ArrowCompute.Take(source, sourceRows);
        if (levels.Count == expandedLength)
            return gathered;
        if (gathered is RunEndEncodedArray runs)
            gathered = RunEndEncoding.Expand(runs);
        return ArrowCompute.Scatter(gathered, levels, expandedLength);
    }

    /// <summary>
    /// Which logical rows of a run-end encoded array are null, or null when none is (the common case, which then
    /// allocates nothing). One flag per row: the levels built beside it already hold an int per row.
    /// </summary>
    internal static bool[]? RunNulls(RunEndEncodedArray array)
    {
        if (array.Values.NullCount == 0)
            return null;
        var nulls = new bool[array.Length];
        int row = 0;
        foreach (var run in RunEndEncoding.EnumerateRuns(array))
        {
            if (array.Values.IsNull(run.PhysicalIndex))
                nulls.AsSpan(row, run.Length).Fill(true);
            row += run.Length;
        }

        return nulls;
    }

    private static void DecomposeStruct(
        IArrowArray array, Field field, List<string> path,
        List<LeafColumn> leaves,
        int parentDefLevel, int parentRepLevel,
        int[]? parentDefLevels, int[]? parentRepLevels,
        int parentCount, int[]? parentValueMap = null)
    {
        var structArray = (StructArray)array;
        var structType = (StructType)field.DataType;
        int myDefLevel = parentDefLevel + (field.IsNullable ? 1 : 0);
        int myRepLevel = parentRepLevel;

        // Compute def/rep levels for children — and the CHILD VALUE MAP: an Arrow struct's children are 1:1
        // with the struct's rows, so a NULL struct row still occupies a child slot. childValueMap[i] therefore
        // maps every level with a struct row (present OR null) to that row's index; -1 only where an ancestor
        // is absent. Children index their arrays through it (sequential consume-when-present misaligns after a
        // null struct row).
        int levelCount = parentDefLevels?.Length ?? parentCount;
        int[]? myDefLevels = null;
        int[]? childValueMap = null;

        if (field.IsNullable || parentDefLevels != null)
        {
            myDefLevels = new int[levelCount];
            childValueMap = new int[levelCount];
            int valueIdx = 0;
            // StructArray.Fields slices each child with the struct (Arrow 23, probed: a struct sliced to start at
            // row 1 hands out children with offset 1), so a child's logical index is the struct's own, and the
            // leaf reads it through ArrowCompute.Take, which takes logical positions. (Data.Children is NOT sliced;
            // only Fields is, which is what this reads.)

            for (int i = 0; i < levelCount; i++)
            {
                int pDef = parentDefLevels?[i] ?? parentDefLevel;

                if (pDef < parentDefLevel)
                {
                    // Ancestor is null
                    myDefLevels[i] = pDef;
                    childValueMap[i] = -1;
                    continue;
                }

                int idx = parentValueMap?[i] ?? valueIdx++;
                childValueMap[i] = idx;
                myDefLevels[i] = field.IsNullable && structArray.IsNull(idx) ? myDefLevel - 1 : myDefLevel;
            }
        }

        // Recurse into children
        for (int c = 0; c < structType.Fields.Count; c++)
        {
            var childField = structType.Fields[c];
            var childArray = structArray.Fields[c];

            path.Add(childField.Name);
            DecomposeRecursive(childArray, childField, path, leaves,
                myDefLevel, myRepLevel, myDefLevels, parentRepLevels, parentCount, childValueMap);
            path.RemoveAt(path.Count - 1);
        }
    }

    private static void DecomposeList(
        IArrowArray array, Field field, List<string> path,
        List<LeafColumn> leaves,
        int parentDefLevel, int parentRepLevel,
        int[]? parentDefLevels, int[]? parentRepLevels,
        int parentCount, int[]? parentValueMap = null)
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

        // Build def/rep levels, and the CHILD VALUE MAP: each element level's index into the values array. The
        // values need not be consumed in order from 0 — offsets may start past 0, stop short of the end, leave gaps
        // between rows, or span values under a null row (all valid Arrow, and what another library's filter/take
        // leaves behind), and none of those values is written (#470). So the element index is mapped explicitly,
        // as DecomposeFixedList does.
        var defList = new List<int>();
        var repList = new List<int>();
        var childMap = new List<int>();
        int inputCount = parentDefLevels?.Length ?? parentCount;

        int slotIdx = 0; // index into listArray slots (a STRUCT parent supplies the mapping explicitly —
                         // a null struct row still occupies a list slot)
        for (int i = 0; i < inputCount; i++)
        {
            int pDef = parentDefLevels?[i] ?? parentDefLevel;
            int pRep = parentRepLevels?[i] ?? 0;

            if (pDef < parentDefLevel)
            {
                // Ancestor is null — emit phantom entry
                defList.Add(pDef);
                repList.Add(pRep);
                childMap.Add(-1);
                continue;
            }

            int slot = parentValueMap?[i] ?? slotIdx++;
            if (field.IsNullable && listArray.IsNull(slot))
            {
                // List itself is null
                defList.Add(listDefLevel - 1);
                repList.Add(pRep);
                childMap.Add(-1);
            }
            else
            {
                // List is present. ValueOffsets already applies the list's own slice offset.
                int start = offsets[slot];
                int end = offsets[slot + 1];
                int length = end - start;

                if (length == 0)
                {
                    // Empty list
                    defList.Add(listDefLevel);
                    repList.Add(pRep);
                    childMap.Add(-1);
                }
                else
                {
                    for (int j = 0; j < length; j++)
                    {
                        defList.Add(repeatedDefLevel); // placeholder — child will add more
                        repList.Add(j == 0 ? pRep : repeatedRepLevel);
                        childMap.Add(start + j);
                    }
                }
            }
        }

        var myDefLevels = defList.ToArray();
        var myRepLevels = repList.ToArray();

        // Recurse into element
        path.Add("list");
        path.Add(elementField.Name);
        DecomposeRecursive(elementArray, elementField, path, leaves,
            repeatedDefLevel, repeatedRepLevel, myDefLevels, myRepLevels, parentCount, childMap.ToArray());
        path.RemoveAt(path.Count - 1);
        path.RemoveAt(path.Count - 1);
    }

    /// <summary>
    /// Decomposes a fixed-size list, which Parquet has no type for: it writes as an ordinary
    /// 3-level LIST, exactly as PyArrow, Polars, and DuckDB write theirs. The declared width is not
    /// representable on disk and does not survive the round trip.
    /// </summary>
    /// <remarks>
    /// This cannot reuse <see cref="DecomposeList"/>, and not only because there is no offsets
    /// buffer to read. A null slot of a fixed-size list still occupies its full width of child
    /// positions, so the child cannot be consumed sequentially the way a variable list's is — the
    /// first null would shift every value after it. The child index is therefore mapped explicitly,
    /// the same way <see cref="DecomposeStruct"/> maps around a null struct row.
    /// </remarks>
    private static void DecomposeFixedList(
        IArrowArray array, Field field, List<string> path,
        List<LeafColumn> leaves,
        int parentDefLevel, int parentRepLevel,
        int[]? parentDefLevels, int[]? parentRepLevels,
        int parentCount, int[]? parentValueMap = null)
    {
        var fixedArray = (FixedSizeListArray)array;
        var fixedType = (FixedSizeListType)field.DataType;
        int width = fixedType.ListSize;

        // 3-level: optional group (LIST) → repeated group "list" → element
        int listDefLevel = parentDefLevel + (field.IsNullable ? 1 : 0);
        int repeatedDefLevel = listDefLevel + 1;
        int repeatedRepLevel = parentRepLevel + 1;

        var elementField = fixedType.ValueField;
        var elementArray = fixedArray.Values;

        var defList = new List<int>();
        var repList = new List<int>();
        var childMap = new List<int>();
        int inputCount = parentDefLevels?.Length ?? parentCount;

        // A sliced fixed-size list's child is NOT sliced with it, so the child span for a logical
        // slot starts at (offset + slot) * width — the same arithmetic ArrowCompute uses to gather
        // one. The slot's own IsNull applies the offset internally, so it takes the logical index.
        int arrayOffset = fixedArray.Data.Offset;

        int slotIdx = 0;
        for (int i = 0; i < inputCount; i++)
        {
            int pDef = parentDefLevels?[i] ?? parentDefLevel;
            int pRep = parentRepLevels?[i] ?? 0;

            if (pDef < parentDefLevel)
            {
                // Ancestor is null — emit phantom entry
                defList.Add(pDef);
                repList.Add(pRep);
                childMap.Add(-1);
                continue;
            }

            int slot = parentValueMap?[i] ?? slotIdx++;
            if (field.IsNullable && fixedArray.IsNull(slot))
            {
                defList.Add(listDefLevel - 1);
                repList.Add(pRep);
                childMap.Add(-1);
                continue;
            }

            // Arrow requires a positive width, so unlike a variable list there is no empty case.
            int start = checked((arrayOffset + slot) * width);
            for (int j = 0; j < width; j++)
            {
                defList.Add(repeatedDefLevel); // placeholder — child will add more
                repList.Add(j == 0 ? pRep : repeatedRepLevel);
                childMap.Add(start + j);
            }
        }

        path.Add("list");
        path.Add(elementField.Name);
        DecomposeRecursive(elementArray, elementField, path, leaves,
            repeatedDefLevel, repeatedRepLevel, defList.ToArray(), repList.ToArray(),
            parentCount, childMap.ToArray());
        path.RemoveAt(path.Count - 1);
        path.RemoveAt(path.Count - 1);
    }

    private static void DecomposeMap(
        IArrowArray array, Field field, List<string> path,
        List<LeafColumn> leaves,
        int parentDefLevel, int parentRepLevel,
        int[]? parentDefLevels, int[]? parentRepLevels,
        int parentCount, int[]? parentValueMap = null)
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

        // Build def/rep levels and the child value map (same structure as list, and for the same reason: the
        // entries need not be consumed in order from 0, #470). Keys and Values are sliced with the entries struct,
        // as every StructArray field is, so an entry index needs no shift.
        var defList = new List<int>();
        var repList = new List<int>();
        var childMap = new List<int>();
        int inputCount = parentDefLevels?.Length ?? parentCount;

        int slotIdx = 0; // a STRUCT parent supplies the mapping (a null struct row still occupies a map slot)
        for (int i = 0; i < inputCount; i++)
        {
            int pDef = parentDefLevels?[i] ?? parentDefLevel;
            int pRep = parentRepLevels?[i] ?? 0;

            if (pDef < parentDefLevel)
            {
                defList.Add(pDef);
                repList.Add(pRep);
                childMap.Add(-1);
                continue;
            }

            int slot = parentValueMap?[i] ?? slotIdx++;
            if (field.IsNullable && mapArray.IsNull(slot))
            {
                defList.Add(mapDefLevel - 1);
                repList.Add(pRep);
                childMap.Add(-1);
            }
            else
            {
                int start = offsets[slot];
                int end = offsets[slot + 1];
                int length = end - start;

                if (length == 0)
                {
                    defList.Add(mapDefLevel);
                    repList.Add(pRep);
                    childMap.Add(-1);
                }
                else
                {
                    for (int j = 0; j < length; j++)
                    {
                        defList.Add(repeatedDefLevel);
                        repList.Add(j == 0 ? pRep : repeatedRepLevel);
                        childMap.Add(start + j);
                    }
                }
            }
        }

        var myDefLevels = defList.ToArray();
        var myRepLevels = repList.ToArray();

        // Recurse into key and value
        path.Add("key_value");

        var entryMap = childMap.ToArray();
        path.Add(keyField.Name);
        DecomposeRecursive(keyArray, keyField, path, leaves,
            repeatedDefLevel, repeatedRepLevel, myDefLevels, myRepLevels, parentCount, entryMap);
        path.RemoveAt(path.Count - 1);

        path.Add(valueField.Name);
        DecomposeRecursive(valueArray, valueField, path, leaves,
            repeatedDefLevel, repeatedRepLevel, myDefLevels, myRepLevels, parentCount, entryMap);
        path.RemoveAt(path.Count - 1);

        path.RemoveAt(path.Count - 1);
    }
}
