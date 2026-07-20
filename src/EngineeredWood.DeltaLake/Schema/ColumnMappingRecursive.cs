// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Recursive column-mapping transform for WRITE batches: renames every level of a RecordBatch to the
/// PHYSICAL column names and stamps each mapped field's <c>PARQUET:field_id</c>, per the Delta protocol
/// (data files store physical names + parquet field ids at ALL nesting depths in both mapping modes).
/// The top-level-only <see cref="ColumnMapping.RenameToPhysical"/> + <see cref="ColumnMapping.SetParquetFieldIds"/>
/// pair breaks on nested structs — a substituted struct column would be written with logical child names and
/// no ids, silently unreadable for spec readers (Spark, delta-kernel).
/// Matching tolerates EITHER name at every level (an already-physical child read from a data file passes
/// through with just the field id stamped). Arrays are rebuilt by re-wrapping <see cref="ArrayData"/> with
/// the renamed type tree — buffers are shared, no data is copied. Structs recurse to any depth; lists
/// recurse into a struct element; maps recurse into key/value. (List/map INNER elements have structural
/// parquet names — only struct fields carry a physicalName/id to map.)
/// </summary>
public static class ColumnMappingRecursive
{
    /// <summary>
    /// Returns <paramref name="batch"/> with physical names + parquet field ids applied at every level.
    /// No-op when <paramref name="mode"/> is <see cref="ColumnMappingMode.None"/>.
    /// </summary>
    public static RecordBatch ToPhysical(RecordBatch batch, StructType deltaSchema, ColumnMappingMode mode)
        => Transform(batch, deltaSchema, mode, toPhysical: true);

    /// <summary>
    /// The READ direction: renames a physical-named batch (as stored in data files) back to the LOGICAL
    /// schema at every level. The flat <see cref="ColumnMapping.RenameColumns"/>/<c>RenameByFieldId</c>
    /// handle the top level only — nested struct children stay under their physical <c>col-&lt;guid&gt;</c>
    /// names without this. Tolerant matching: an already-logical level passes through unchanged.
    /// </summary>
    public static RecordBatch ToLogical(RecordBatch batch, StructType deltaSchema, ColumnMappingMode mode)
        => Transform(batch, deltaSchema, mode, toPhysical: false);

    /// <summary>True when the schema has any nested (struct-carrying) mapped field — the cheap gate for the
    /// recursive transform (top-level-only tables are fully handled by the flat renames).</summary>
    public static bool HasNestedFields(StructType schema)
    {
        foreach (var f in schema.Fields)
        {
            if (ContainsStruct(f.Type))
                return true;
        }
        return false;
    }

    private static bool ContainsStruct(DeltaDataType type) => type switch
    {
        StructType => true,
        ArrayType at => ContainsStruct(at.ElementType),
        MapType mt => ContainsStruct(mt.KeyType) || ContainsStruct(mt.ValueType),
        _ => false,
    };

    private static RecordBatch Transform(
        RecordBatch batch, StructType deltaSchema, ColumnMappingMode mode, bool toPhysical)
    {
        if (mode == ColumnMappingMode.None)
            return batch;

        var fields = new List<Field>(batch.Schema.FieldsList.Count);
        var arrays = new List<IArrowArray>(batch.ColumnCount);
        bool changed = false;
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var f = batch.Schema.FieldsList[i];
            var renamed = RenameField(f, FindField(deltaSchema, f.Name), toPhysical);
            if (ReferenceEquals(renamed, f))
            {
                fields.Add(f);
                arrays.Add(batch.Column(i));
            }
            else
            {
                changed = true;
                fields.Add(renamed);
                arrays.Add(Rebuild(batch.Column(i).Data, renamed.DataType));
            }
        }

        if (!changed)
            return batch;

        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            builder.Field(f);
        return new RecordBatch(builder.Build(), arrays, batch.Length);
    }

    // Finds the Delta field an Arrow name refers to: the logical name OR the physicalName (tolerant —
    // the source may be a logical-named substitution or a physical-named column read back from a data file).
    private static StructField? FindField(StructType schema, string arrowName)
    {
        foreach (var f in schema.Fields)
        {
            if (string.Equals(f.Name, arrowName, StringComparison.Ordinal))
                return f;
            if (f.Metadata is { } md
                && md.TryGetValue(ColumnMapping.PhysicalNameKey, out var phys)
                && string.Equals(phys, arrowName, StringComparison.Ordinal))
            {
                return f;
            }
        }
        return null;
    }

    // Returns the SAME instance when nothing changes (the no-op signal used to avoid rebuilding arrays).
    private static Field RenameField(Field arrow, StructField? delta, bool toPhysical)
    {
        if (delta is null)
            return arrow; // not a table column (e.g. a transient metadata column) — pass through

        string name = toPhysical
            && delta.Metadata is { } md
            && md.TryGetValue(ColumnMapping.PhysicalNameKey, out var phys)
            && !string.IsNullOrEmpty(phys)
                ? phys
                : delta.Name;
        var type = RenameType(arrow.DataType, delta.Type, toPhysical);
        // field ids are stamped on the WRITE direction only (the read direction leaves metadata untouched).
        int? fieldId = toPhysical ? ColumnMapping.GetFieldId(delta) : null;

        bool sameName = string.Equals(name, arrow.Name, StringComparison.Ordinal);
        bool sameId = fieldId is null
            || (arrow.Metadata is { } am
                && am.TryGetValue("PARQUET:field_id", out var existing)
                && string.Equals(existing, fieldId.Value.ToString(), StringComparison.Ordinal));
        if (sameName && sameId && ReferenceEquals(type, arrow.DataType))
            return arrow;

        Dictionary<string, string>? meta = null;
        if (arrow.Metadata is not null || fieldId is not null)
        {
            meta = new Dictionary<string, string>();
            if (arrow.Metadata is { } src)
            {
                foreach (var kvp in src)
                    meta[kvp.Key] = kvp.Value;
            }
            if (fieldId is { } id)
                meta["PARQUET:field_id"] = id.ToString();
        }
        return new Field(name, type, arrow.IsNullable, meta);
    }

    private static Apache.Arrow.Types.IArrowType RenameType(
        Apache.Arrow.Types.IArrowType arrow, DeltaDataType delta, bool toPhysical)
    {
        switch (arrow)
        {
            case Apache.Arrow.Types.StructType st when delta is StructType ds:
            {
                var children = new List<Field>(st.Fields.Count);
                bool changed = false;
                foreach (var child in st.Fields)
                {
                    var renamed = RenameField(child, FindField(ds, child.Name), toPhysical);
                    changed |= !ReferenceEquals(renamed, child);
                    children.Add(renamed);
                }
                return changed ? new Apache.Arrow.Types.StructType(children) : arrow;
            }
            case Apache.Arrow.Types.ListType lt when delta is ArrayType da:
            {
                var elemType = RenameType(lt.ValueField.DataType, da.ElementType, toPhysical);
                return ReferenceEquals(elemType, lt.ValueField.DataType)
                    ? arrow
                    : new Apache.Arrow.Types.ListType(
                        new Field(lt.ValueField.Name, elemType, lt.ValueField.IsNullable, lt.ValueField.Metadata));
            }
            case Apache.Arrow.Types.LargeListType llt when delta is ArrayType da:
            {
                var elemType = RenameType(llt.ValueField.DataType, da.ElementType, toPhysical);
                return ReferenceEquals(elemType, llt.ValueField.DataType)
                    ? arrow
                    : new Apache.Arrow.Types.LargeListType(
                        new Field(llt.ValueField.Name, elemType, llt.ValueField.IsNullable, llt.ValueField.Metadata));
            }
            case Apache.Arrow.Types.MapType mt when delta is MapType dm:
            {
                var keyType = RenameType(mt.KeyField.DataType, dm.KeyType, toPhysical);
                var valType = RenameType(mt.ValueField.DataType, dm.ValueType, toPhysical);
                if (ReferenceEquals(keyType, mt.KeyField.DataType)
                    && ReferenceEquals(valType, mt.ValueField.DataType))
                {
                    return arrow;
                }
                return new Apache.Arrow.Types.MapType(
                    new Field(mt.KeyField.Name, keyType, mt.KeyField.IsNullable, mt.KeyField.Metadata),
                    new Field(mt.ValueField.Name, valType, mt.ValueField.IsNullable, mt.ValueField.Metadata),
                    mt.KeySorted);
            }
            default:
                return arrow; // primitive (or a shape the Delta type doesn't mirror) — unchanged
        }
    }

    // Re-wraps ArrayData with the renamed type, recursing into children. Buffers are shared (no copy);
    // only the type tree (which carries the field names) is rebuilt.
    private static IArrowArray Rebuild(ArrayData data, Apache.Arrow.Types.IArrowType newType)
    {
        return ArrowArrayFactory.BuildArray(RebuildData(data, newType));
    }

    private static ArrayData RebuildData(ArrayData data, Apache.Arrow.Types.IArrowType newType)
    {
        if (ReferenceEquals(data.DataType, newType))
            return data;
        ArrayData[]? children = data.Children;
        if (children is { Length: > 0 })
        {
            var newChildren = new ArrayData[children.Length];
            for (int i = 0; i < children.Length; i++)
                newChildren[i] = RebuildData(children[i], ChildType(newType, children[i].DataType, i));
            children = newChildren;
        }
        return new ArrayData(newType, data.Length, data.NullCount, data.Offset, data.Buffers, children,
                             data.Dictionary);
    }

    // The renamed type of child i of a container type (falls back to the child's own type when the container
    // shape is unexpected — a defensive no-op, never a crash).
    private static Apache.Arrow.Types.IArrowType ChildType(
        Apache.Arrow.Types.IArrowType container, Apache.Arrow.Types.IArrowType fallback, int index)
        => container switch
        {
            Apache.Arrow.Types.StructType st when index < st.Fields.Count => st.Fields[index].DataType,
            Apache.Arrow.Types.ListType lt when index == 0 => lt.ValueField.DataType,
            Apache.Arrow.Types.LargeListType llt when index == 0 => llt.ValueField.DataType,
            // Arrow MapType's single child is the entries struct<key, value>.
            Apache.Arrow.Types.MapType mt when index == 0 =>
                new Apache.Arrow.Types.StructType(new[] { mt.KeyField, mt.ValueField }),
            _ => fallback,
        };
}
