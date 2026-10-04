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
/// through with just the field id stamped), but tries the kind of name the input is expected to carry FIRST,
/// across every field, before the other: a table upgraded to name mode keeps each column's original name as
/// its physical name, so after a rename one column's physical name can be another column's logical name, and
/// a single pass that accepts either name binds whichever of the two fields comes first. Arrays are rebuilt by re-wrapping <see cref="ArrayData"/> with
/// the renamed type tree — buffers are shared, no data is copied. Structs recurse to any depth; lists
/// recurse into a struct element; maps recurse into key/value. (List/map INNER elements have structural
/// parquet names — only struct fields carry a physicalName/id to map.)
/// </summary>
public static class ColumnMappingRecursive
{
    private const string ParquetFieldIdKey = ColumnMapping.ParquetFieldIdKey;

    /// <summary>
    /// Returns <paramref name="batch"/> with physical names + parquet field ids applied at every level.
    /// No-op when <paramref name="mode"/> is <see cref="ColumnMappingMode.None"/>.
    /// </summary>
    public static RecordBatch ToPhysical(RecordBatch batch, StructType deltaSchema, ColumnMappingMode mode)
        => Transform(batch, deltaSchema, mode, toPhysical: true, preferPhysical: false);

    /// <summary>
    /// <see cref="ToPhysical(RecordBatch, StructType, ColumnMappingMode)"/> for a batch whose names are
    /// <paramref name="inputIsPhysical"/> already — one read back from a data file, as compaction rewrites — so
    /// each field is matched by physical name before logical name. A logical-named batch (anything a caller
    /// wrote) passes false.
    /// </summary>
    public static RecordBatch ToPhysical(
        RecordBatch batch, StructType deltaSchema, ColumnMappingMode mode, bool inputIsPhysical)
        => Transform(batch, deltaSchema, mode, toPhysical: true, preferPhysical: inputIsPhysical);

    /// <summary>
    /// The READ direction: renames a physical-named batch (as stored in data files) back to the LOGICAL
    /// schema at every level, the top level included — pass the batch as read, NOT after a flat
    /// <see cref="ColumnMapping.RenameColumns"/>/<c>RenameByFieldId</c>, since names are matched physical-first
    /// and a top level that is already logical could match another column's physical name. A field that
    /// matches no physical name falls back to its logical name, so an already-logical level still passes.
    /// </summary>
    /// <remarks>
    /// In <see cref="ColumnMappingMode.Id"/> mode a field that carries a <c>PARQUET:field_id</c> (the Parquet
    /// reader puts one on every field whose schema node has an id) is matched to the Delta field with that id,
    /// at every level, and only by it: id mode resolves a data file's columns by field id, and a file's physical
    /// names need not be the ones the table schema records. A field without an id falls back to name matching.
    /// </remarks>
    public static RecordBatch ToLogical(RecordBatch batch, StructType deltaSchema, ColumnMappingMode mode)
        => Transform(batch, deltaSchema, mode, toPhysical: false, preferPhysical: true);

    /// <summary>
    /// Returns <paramref name="batch"/> with every <c>PARQUET:field_id</c> removed from its field metadata, at
    /// every level. The Parquet reader carries a file's ids into Arrow; they identify PHYSICAL columns of that
    /// one file, so once a read has used them to resolve columns they must not reach a caller (where files
    /// with and without ids would hand back batches with different schemas) or a writer (where they would
    /// leak into new files). Returns the same instance when there is nothing to remove. Buffers are shared;
    /// only a type tree that carried an id is rebuilt.
    /// </summary>
    public static RecordBatch StripParquetFieldIds(RecordBatch batch)
    {
        Field[]? fields = null;
        IArrowArray[]? arrays = null;
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var field = batch.Schema.FieldsList[i];
            var stripped = StripField(field);
            if (ReferenceEquals(stripped, field))
                continue;

            fields ??= [.. batch.Schema.FieldsList];
            arrays ??= [.. batch.Arrays];
            fields[i] = stripped;
            // As in Transform: a field whose type tree is untouched keeps the reader's array verbatim.
            if (!ReferenceEquals(stripped.DataType, field.DataType))
                arrays[i] = Rebuild(batch.Column(i).Data, stripped.DataType);
        }

        return fields is null
            ? batch
            : new RecordBatch(new Apache.Arrow.Schema(fields, batch.Schema.Metadata), arrays!, batch.Length);
    }

    // Returns the SAME instance when neither the field nor its type tree carries an id.
    private static Field StripField(Field field)
    {
        var type = StripType(field.DataType);
        bool hasId = field.Metadata is { } md && md.ContainsKey(ParquetFieldIdKey);
        if (!hasId && ReferenceEquals(type, field.DataType))
            return field;

        Dictionary<string, string>? meta = null;
        if (field.Metadata is { } src)
        {
            foreach (var kvp in src)
            {
                if (kvp.Key != ParquetFieldIdKey)
                    (meta ??= new Dictionary<string, string>())[kvp.Key] = kvp.Value;
            }
        }
        return new Field(field.Name, type, field.IsNullable, meta);
    }

    private static Apache.Arrow.Types.IArrowType StripType(Apache.Arrow.Types.IArrowType type)
    {
        switch (type)
        {
            case Apache.Arrow.Types.StructType st:
            {
                Field[]? children = null;
                for (int i = 0; i < st.Fields.Count; i++)
                {
                    var stripped = StripField(st.Fields[i]);
                    if (!ReferenceEquals(stripped, st.Fields[i]))
                    {
                        children ??= [.. st.Fields];
                        children[i] = stripped;
                    }
                }
                return children is null ? type : new Apache.Arrow.Types.StructType(children);
            }
            // MapType derives from ListType, so it has to be matched first.
            case Apache.Arrow.Types.MapType mt:
            {
                var key = StripField(mt.KeyField);
                var value = StripField(mt.ValueField);
                return ReferenceEquals(key, mt.KeyField) && ReferenceEquals(value, mt.ValueField)
                    ? type
                    : new Apache.Arrow.Types.MapType(key, value, mt.KeySorted);
            }
            case Apache.Arrow.Types.ListType lt:
            {
                var value = StripField(lt.ValueField);
                return ReferenceEquals(value, lt.ValueField) ? type : new Apache.Arrow.Types.ListType(value);
            }
            case Apache.Arrow.Types.LargeListType llt:
            {
                var value = StripField(llt.ValueField);
                return ReferenceEquals(value, llt.ValueField) ? type : new Apache.Arrow.Types.LargeListType(value);
            }
            default:
                // A leaf, or a shape RebuildData does not walk (an extension type's storage is left alone).
                return type;
        }
    }

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
        RecordBatch batch, StructType deltaSchema, ColumnMappingMode mode, bool toPhysical, bool preferPhysical)
    {
        if (mode == ColumnMappingMode.None)
            return batch;

        // Only the read direction resolves by id: a write batch's ids, if any, are the ones being replaced.
        bool byId = !toPhysical && mode == ColumnMappingMode.Id;
        var fields = new List<Field>(batch.Schema.FieldsList.Count);
        var arrays = new List<IArrowArray>(batch.ColumnCount);
        bool changed = false;
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var f = batch.Schema.FieldsList[i];
            var renamed = RenameField(
                f, FindField(deltaSchema, f, byId, preferPhysical), toPhysical, byId, preferPhysical);
            if (ReferenceEquals(renamed, f))
            {
                fields.Add(f);
                arrays.Add(batch.Column(i));
            }
            else
            {
                changed = true;
                fields.Add(renamed);
                // A rename that leaves the TYPE TREE untouched (RenameType returns the same instance when it
                // changes nothing) needs no new array — only the field's name/id moved. Re-materializing it
                // through ArrowArrayFactory would revalidate a layout the reader already produced and reject
                // the legal-but-non-canonical ones ("Buffer count <2> must be at exactly <3>").
                arrays.Add(ReferenceEquals(renamed.DataType, f.DataType)
                    ? batch.Column(i)
                    : Rebuild(batch.Column(i).Data, renamed.DataType));
            }
        }

        if (!changed)
            return batch;

        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            builder.Field(f);
        return new RecordBatch(builder.Build(), arrays, batch.Length);
    }

    // Finds the Delta field an Arrow field refers to. By id (read direction, id mode) when the Arrow field carries a
    // PARQUET:field_id: the field with that id, or none — a name must not rebind a column whose id the table no
    // longer has. Otherwise by name: the preferred kind (physical or logical) across EVERY field first, then the
    // other kind (tolerant — the source may be a logical-named substitution or a physical-named column read back
    // from a data file). Never both kinds in one pass: with a reused name, that binds whichever field is first.
    private static StructField? FindField(StructType schema, Field arrow, bool byId, bool preferPhysical)
    {
        if (byId && ColumnMapping.GetParquetFieldId(arrow) is { } id)
        {
            foreach (var f in schema.Fields)
            {
                if (ColumnMapping.GetFieldId(f) == id)
                    return f;
            }
            return null;
        }

        return FindByName(schema, arrow.Name, physical: preferPhysical)
            ?? FindByName(schema, arrow.Name, physical: !preferPhysical);
    }

    private static StructField? FindByName(StructType schema, string arrowName, bool physical)
    {
        foreach (var f in schema.Fields)
        {
            string? candidate = physical
                ? f.Metadata is { } md && md.TryGetValue(ColumnMapping.PhysicalNameKey, out var phys) ? phys : null
                : f.Name;
            if (string.Equals(candidate, arrowName, StringComparison.Ordinal))
                return f;
        }
        return null;
    }

    // Returns the SAME instance when nothing changes (the no-op signal used to avoid rebuilding arrays).
    private static Field RenameField(
        Field arrow, StructField? delta, bool toPhysical, bool byId, bool preferPhysical)
    {
        if (delta is null)
            return arrow; // not a table column (e.g. a transient metadata column) — pass through

        string name = toPhysical
            && delta.Metadata is { } md
            && md.TryGetValue(ColumnMapping.PhysicalNameKey, out var phys)
            && !string.IsNullOrEmpty(phys)
                ? phys
                : delta.Name;
        var type = RenameType(arrow.DataType, delta.Type, toPhysical, byId, preferPhysical);
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
        Apache.Arrow.Types.IArrowType arrow, DeltaDataType delta, bool toPhysical, bool byId, bool preferPhysical)
    {
        switch (arrow)
        {
            case Apache.Arrow.Types.StructType st when delta is StructType ds:
            {
                var children = new List<Field>(st.Fields.Count);
                bool changed = false;
                foreach (var child in st.Fields)
                {
                    var renamed = RenameField(
                        child, FindField(ds, child, byId, preferPhysical), toPhysical, byId, preferPhysical);
                    changed |= !ReferenceEquals(renamed, child);
                    children.Add(renamed);
                }
                return changed ? new Apache.Arrow.Types.StructType(children) : arrow;
            }
            case Apache.Arrow.Types.ListType lt when delta is ArrayType da:
            {
                var elemType = RenameType(lt.ValueField.DataType, da.ElementType, toPhysical, byId, preferPhysical);
                return ReferenceEquals(elemType, lt.ValueField.DataType)
                    ? arrow
                    : new Apache.Arrow.Types.ListType(
                        new Field(lt.ValueField.Name, elemType, lt.ValueField.IsNullable, lt.ValueField.Metadata));
            }
            case Apache.Arrow.Types.LargeListType llt when delta is ArrayType da:
            {
                var elemType = RenameType(llt.ValueField.DataType, da.ElementType, toPhysical, byId, preferPhysical);
                return ReferenceEquals(elemType, llt.ValueField.DataType)
                    ? arrow
                    : new Apache.Arrow.Types.LargeListType(
                        new Field(llt.ValueField.Name, elemType, llt.ValueField.IsNullable, llt.ValueField.Metadata));
            }
            case Apache.Arrow.Types.MapType mt when delta is MapType dm:
            {
                var keyType = RenameType(mt.KeyField.DataType, dm.KeyType, toPhysical, byId, preferPhysical);
                var valType = RenameType(mt.ValueField.DataType, dm.ValueType, toPhysical, byId, preferPhysical);
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
