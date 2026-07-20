// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Wraps VARIANT groups that appear NESTED inside a struct, list, or map so the materialised array
/// matches the (already variant-aware) Arrow schema at every depth.
/// </summary>
/// <remarks>
/// <para><see cref="NestedAssembler"/> wraps only the TOP-LEVEL variant columns; a variant nested
/// inside a container is assembled as its bare storage <c>struct&lt;metadata, value&gt;</c>. Meanwhile
/// <see cref="ArrowSchemaConverter"/> already produces a <see cref="VariantType"/> FIELD at every
/// depth (it recurses and applies the extension registry to nested groups). Left unreconciled, a file
/// with e.g. <c>struct&lt;v: variant&gt;</c> yields a batch whose schema says the child is variant
/// while the array is a plain struct — a silent type mismatch.</para>
///
/// <para>This pass walks each top-level (field, array) pair against the Arrow schema and rebuilds any
/// container that holds a nested variant, re-presenting that leaf as a <see cref="VariantArray"/>
/// (reassembling shredding, and reordering the storage children by name — see
/// <see cref="VariantShredding"/> and the positional-factory note below). A top-level variant that
/// <see cref="NestedAssembler"/> already wrapped passes through untouched, so this composes with the
/// existing top-level path rather than duplicating it.</para>
/// </remarks>
internal static class VariantNestedWrapper
{
    /// <summary>Rewrites <paramref name="array"/> so every variant the <paramref name="fieldType"/>
    /// declares — at this level or nested inside a struct/list/map — is a <see cref="VariantArray"/>.
    /// Returns the same instance when nothing needed wrapping.</summary>
    internal static IArrowArray Wrap(IArrowArray array, IArrowType fieldType)
    {
        switch (fieldType)
        {
            // A variant leaf the assembler left as a bare struct. Mirror the top-level path
            // (NestedAssembler.WrapTopLevelExtension) exactly: hand the full storage struct — including
            // a shredded file's typed_value child — to the factory, then reassemble. The assembler has
            // already normalised the child order for an annotated group (this is why a Spark value-first
            // file reads correctly at top level), so no by-name reorder is needed or wanted here;
            // reordering would drop typed_value and break shredded reassembly. (An already-wrapped
            // VariantArray is not a StructArray, so it falls through unchanged — idempotent.)
            case VariantType variant when array is StructArray storage:
            {
                var wrapped = variant.CreateArray(storage);
                return wrapped is VariantArray va ? VariantShredding.Reassemble(va) : wrapped;
            }

            case StructType structType when array is StructArray sa:
                return WrapStruct(structType, sa);

            case ListType listType when array is ListArray la:
                return WrapList(listType, la);

            case MapType mapType when array is MapArray ma:
                return WrapMap(mapType, ma);

            default:
                return array;
        }
    }

    private static IArrowArray WrapStruct(StructType structType, StructArray array)
    {
        IArrowArray[]? children = null;
        for (int i = 0; i < structType.Fields.Count; i++)
        {
            var original = array.Fields[i];
            var wrapped = Wrap(original, structType.Fields[i].DataType);
            if (!ReferenceEquals(wrapped, original))
                (children ??= CopyFields(array))[i] = wrapped;
        }

        if (children is null)
            return array;

        // Rebuild the struct type from the wrapped children's ACTUAL types (a wrapped child is now
        // VariantType, not struct), keeping each field's name and nullability.
        var fields = new Field[structType.Fields.Count];
        for (int i = 0; i < fields.Length; i++)
            fields[i] = new Field(structType.Fields[i].Name, children[i].Data.DataType,
                                  structType.Fields[i].IsNullable);

        var data = new ArrayData(new StructType(fields), array.Length, array.NullCount, array.Offset,
                                 array.Data.Buffers, children.Select(c => c.Data).ToArray());
        return new StructArray(data);
    }

    private static IArrowArray WrapList(ListType listType, ListArray array)
    {
        var wrappedValues = Wrap(array.Values, listType.ValueDataType);
        if (ReferenceEquals(wrappedValues, array.Values))
            return array;

        var valueField = new Field(listType.ValueField.Name, wrappedValues.Data.DataType,
                                   listType.ValueField.IsNullable);
        var data = new ArrayData(new ListType(valueField), array.Length, array.NullCount, array.Offset,
                                 array.Data.Buffers, new[] { wrappedValues.Data });
        return new ListArray(data);
    }

    private static IArrowArray WrapMap(MapType mapType, MapArray array)
    {
        // A map is a list of struct<key, value>; only the value realistically carries a variant, but
        // both are handled uniformly. KeyValues is that entries struct.
        var entries = array.KeyValues;
        var wrappedKey = Wrap(entries.Fields[0], mapType.KeyField.DataType);
        var wrappedValue = Wrap(entries.Fields[1], mapType.ValueField.DataType);
        if (ReferenceEquals(wrappedKey, entries.Fields[0])
            && ReferenceEquals(wrappedValue, entries.Fields[1]))
        {
            return array;
        }

        var keyField = new Field(mapType.KeyField.Name, wrappedKey.Data.DataType, mapType.KeyField.IsNullable);
        var valueField = new Field(mapType.ValueField.Name, wrappedValue.Data.DataType, mapType.ValueField.IsNullable);
        var entriesData = new ArrayData(
            new StructType(new[] { keyField, valueField }), entries.Length, entries.NullCount,
            entries.Offset, entries.Data.Buffers, new[] { wrappedKey.Data, wrappedValue.Data });
        var mapData = new ArrayData(
            new MapType(keyField, valueField, mapType.KeySorted), array.Length, array.NullCount,
            array.Offset, array.Data.Buffers, new[] { entriesData });
        return new MapArray(mapData);
    }

    private static IArrowArray[] CopyFields(StructArray array)
    {
        var copy = new IArrowArray[array.Fields.Count];
        for (int i = 0; i < copy.Length; i++)
            copy[i] = array.Fields[i];
        return copy;
    }
}
