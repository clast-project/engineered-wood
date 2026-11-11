// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Puts a scan's string and binary columns in the one representation <see cref="ParquetReadOptions.ByteArrayOutput"/>
/// asks for (#793). The Parquet reader decodes a data column in it, but a partition column, a column backfilled for a
/// file written before it was added, and a codec-seam host's column are built elsewhere, so without this one batch
/// mixed <c>utf8</c> with <c>utf8view</c> or <c>large_utf8</c>, and none of it matched what
/// <see cref="DeltaTable.GetReadSchema"/> reports.
/// </summary>
internal static class ByteArrayRepresentation
{
    /// <summary><paramref name="type"/> with every string and binary type, at any depth, in <paramref name="kind"/>'s
    /// representation. The same instance when nothing changes.</summary>
    public static IArrowType Of(IArrowType type, ByteArrayOutputKind kind)
    {
        switch (type)
        {
            case StringType or StringViewType or LargeStringType:
                return Target(isString: true, kind) is var s && s.TypeId == type.TypeId ? type : s;
            case BinaryType or BinaryViewType or LargeBinaryType:
                return Target(isString: false, kind) is var b && b.TypeId == type.TypeId ? type : b;
            case StructType structType:
            {
                var fields = Fields(structType.Fields, kind);
                return fields is null ? type : new StructType(fields);
            }

            case MapType map:
            {
                var key = Field(map.KeyField, kind);
                var value = Field(map.ValueField, kind);
                return ReferenceEquals(key, map.KeyField) && ReferenceEquals(value, map.ValueField)
                    ? type
                    : new MapType(key, value, map.KeySorted);
            }

            case ListType list:
            {
                var value = Field(list.ValueField, kind);
                return ReferenceEquals(value, list.ValueField) ? type : new ListType(value);
            }

            case LargeListType list:
            {
                var value = Field(list.ValueField, kind);
                return ReferenceEquals(value, list.ValueField) ? type : new LargeListType(value);
            }

            case FixedSizeListType list:
            {
                var value = Field(list.ValueField, kind);
                return ReferenceEquals(value, list.ValueField) ? type : new FixedSizeListType(value, list.ListSize);
            }

            default:
                return type;
        }
    }

    /// <summary><paramref name="fields"/> in <paramref name="kind"/>'s representation; null when none changes.</summary>
    public static Field[]? Fields(IReadOnlyList<Field> fields, ByteArrayOutputKind kind)
    {
        Field[]? result = null;
        for (int i = 0; i < fields.Count; i++)
        {
            var field = Field(fields[i], kind);
            if (!ReferenceEquals(field, fields[i]))
            {
                result ??= [.. fields];
                result[i] = field;
            }
        }

        return result;
    }

    private static Field Field(Field field, ByteArrayOutputKind kind)
    {
        var type = Of(field.DataType, kind);
        return ReferenceEquals(type, field.DataType) ? field : new Field(field.Name, type, field.IsNullable, field.Metadata);
    }

    private static IArrowType Target(bool isString, ByteArrayOutputKind kind) => kind switch
    {
        ByteArrayOutputKind.ViewType => isString ? StringViewType.Default : BinaryViewType.Default,
        ByteArrayOutputKind.LargeOffsets => isString ? LargeStringType.Default : LargeBinaryType.Default,
        _ => isString ? StringType.Default : BinaryType.Default,
    };

    /// <summary>
    /// <paramref name="batch"/> with every column in <paramref name="kind"/>'s representation: the same batch when
    /// all already are. A string or binary column that is converted whole is copied, and its original disposed (the
    /// scan owns it); a nested column is rebuilt around its converted children, sharing the rest of its buffers.
    /// </summary>
    public static RecordBatch Apply(RecordBatch batch, ByteArrayOutputKind kind)
    {
        IArrowArray[]? columns = null;
        Field[]? fields = null;
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var column = batch.Column(i);
            var target = Of(column.Data.DataType, kind);
            if (ReferenceEquals(target, column.Data.DataType))
                continue;

            columns ??= [.. Enumerable.Range(0, batch.ColumnCount).Select(batch.Column)];
            fields ??= [.. batch.Schema.FieldsList];
            columns[i] = ArrowArrayFactory.BuildArray(Convert(column.Data, kind));
            var field = batch.Schema.FieldsList[i];
            fields[i] = new Field(field.Name, columns[i].Data.DataType, field.IsNullable, field.Metadata);
            if (IsLeaf(column.Data.DataType))
                column.Dispose();
        }

        if (columns is null)
            return batch;

        return new RecordBatch(new Apache.Arrow.Schema(fields!, batch.Schema.Metadata), columns, batch.Length);
    }

    private static bool IsLeaf(IArrowType type) =>
        type is StringType or StringViewType or LargeStringType or BinaryType or BinaryViewType or LargeBinaryType;

    // `data` in `kind`'s representation: the same instance when nothing in it changes.
    private static ArrayData Convert(ArrayData data, ByteArrayOutputKind kind)
    {
        var target = Of(data.DataType, kind);
        if (ReferenceEquals(target, data.DataType))
            return data;
        if (IsLeaf(data.DataType))
            return ConvertLeaf(ArrowArrayFactory.BuildArray(data), target).Data;

        // A container keeps its own buffers and offset; only its children change, and Of built the target from the
        // same per-child decisions. ArrayData.Children are unsliced (the parent's offset applies to the parent), so
        // each is converted whole, at its own length.
        var children = new ArrayData[data.Children.Length];
        for (int i = 0; i < children.Length; i++)
            children[i] = Convert(data.Children[i], kind);

        return new ArrayData(target, data.Length, data.NullCount, data.Offset, data.Buffers, children, data.Dictionary);
    }

    private static IArrowArray ConvertLeaf(IArrowArray source, IArrowType target)
    {
        int length = source.Length;
        switch (target)
        {
            case StringType:
            {
                var builder = new StringArray.Builder().Reserve(length);
                for (int i = 0; i < length; i++)
                {
                    if (source.IsNull(i)) builder.AppendNull();
                    else builder.Append(Bytes(source, i));
                }

                return builder.Build();
            }

            case StringViewType:
            {
                var builder = new StringViewArray.Builder().Reserve(length);
                for (int i = 0; i < length; i++)
                {
                    if (source.IsNull(i)) builder.AppendNull();
                    else builder.Append(Bytes(source, i));
                }

                return builder.Build();
            }

            case LargeStringType:
            {
                var builder = new LargeStringArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (source.IsNull(i)) builder.AppendNull();
                    else builder.Append(System.Text.Encoding.UTF8.GetString(Bytes(source, i).ToArray()));
                }

                return builder.Build();
            }

            case BinaryType:
            {
                var builder = new BinaryArray.Builder().Reserve(length);
                for (int i = 0; i < length; i++)
                {
                    if (source.IsNull(i)) builder.AppendNull();
                    else builder.Append(Bytes(source, i));
                }

                return builder.Build();
            }

            case BinaryViewType:
            {
                var builder = new BinaryViewArray.Builder().Reserve(length);
                for (int i = 0; i < length; i++)
                {
                    if (source.IsNull(i)) builder.AppendNull();
                    else builder.Append(Bytes(source, i));
                }

                return builder.Build();
            }

            case LargeBinaryType:
            {
                var builder = new LargeBinaryArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (source.IsNull(i)) builder.AppendNull();
                    else builder.Append(Bytes(source, i));
                }

                return builder.Build();
            }

            default:
                throw new NotSupportedException(target.Name);
        }
    }

    private static ReadOnlySpan<byte> Bytes(IArrowArray array, int index) => array switch
    {
        BinaryArray a => a.GetBytes(index),          // StringArray derives from it
        BinaryViewArray a => a.GetBytes(index),      // StringViewArray too
        LargeBinaryArray a => a.GetBytes(index),     // LargeStringArray too
        _ => throw new NotSupportedException(array.GetType().Name),
    };
}
