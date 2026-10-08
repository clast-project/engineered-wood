// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Rewrites large and view byte arrays as plain string and binary arrays before they are encoded.
/// </summary>
/// <remarks>
/// <para>Every BYTE_ARRAY encoder reads a column's values as 32-bit offsets into one data buffer, which is the layout
/// of string and binary only. Large string and binary carry 64-bit offsets, and the view types carry no offsets at
/// all, yet <see cref="ArrowToSchemaConverter"/> maps all of them to BYTE_ARRAY. A list of large strings was
/// written with every value empty, and a view column could not be written (#481). Converting at the writers' entry
/// fixes every encoder at once, and the conversion is lossless: each value's bytes are kept, and the caller's
/// declared types still reach the footer through <c>ARROW:schema</c>.</para>
///
/// <para>Containers are rebuilt over their children's <see cref="ArrayData"/>, not the sliced accessors, so a
/// container keeps its own offset and buffers and each child is converted whole. A converted child keeps its
/// logical positions (its own window becomes offset 0), so the parent's indexing into it is unchanged.</para>
/// </remarks>
internal static class ByteArrayLayoutNormalizer
{
    /// <summary>
    /// Rewrites every large or view byte-array column of <paramref name="batch"/>, at any depth, and rebuilds the
    /// schema to match. Returns the same batch when there is none, which is the usual case.
    /// </summary>
    internal static RecordBatch ToOffsetLayout(RecordBatch batch)
    {
        IArrowArray[]? columns = null;
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var original = batch.Column(i);
            var data = Rewrite(original.Data);
            if (!ReferenceEquals(data, original.Data))
            {
                if (columns is null)
                {
                    columns = new IArrowArray[batch.ColumnCount];
                    for (int c = 0; c < columns.Length; c++)
                        columns[c] = batch.Column(c);
                }

                columns[i] = Apache.Arrow.ArrowArrayFactory.BuildArray(data);
            }
        }

        if (columns is null)
            return batch;

        var fields = new Field[columns.Length];
        for (int i = 0; i < fields.Length; i++)
        {
            var field = batch.Schema.FieldsList[i];
            fields[i] = new Field(field.Name, columns[i].Data.DataType, field.IsNullable, field.Metadata);
        }

        return new RecordBatch(new Apache.Arrow.Schema(fields, batch.Schema.Metadata), columns, batch.Length);
    }

    private static ArrayData Rewrite(ArrayData data)
    {
        switch (data.DataType)
        {
            case LargeStringType:
                return Copy(new LargeStringArray(data), StringType.Default);
            case LargeBinaryType:
                return Copy(new LargeBinaryArray(data), BinaryType.Default);
            case StringViewType:
                return Copy(new StringViewArray(data), StringType.Default);
            case BinaryViewType:
                return Copy(new BinaryViewArray(data), BinaryType.Default);

            case StructType structType:
            {
                var children = RewriteChildren(data.Children);
                if (children is null)
                    return data;
                var fields = new Field[children.Length];
                for (int i = 0; i < fields.Length; i++)
                    fields[i] = WithType(structType.Fields[i], children[i].DataType);
                return With(data, new StructType(fields), children);
            }

            // MapType derives from ListType, so it has to be matched first.
            case MapType mapType:
            {
                var children = RewriteChildren(data.Children);
                if (children is null)
                    return data;
                var entries = (StructType)children[0].DataType;
                return With(data, new MapType(entries.Fields[0], entries.Fields[1], mapType.KeySorted), children);
            }

            case ListType listType:
            {
                var children = RewriteChildren(data.Children);
                return children is null
                    ? data
                    : With(data, new ListType(WithType(listType.ValueField, children[0].DataType)), children);
            }

            // Not writable yet (ArrowToSchemaConverter refuses it), but converted here so that its elements are
            // already right when it is.
            case LargeListType largeListType:
            {
                var children = RewriteChildren(data.Children);
                return children is null
                    ? data
                    : With(data, new LargeListType(WithType(largeListType.ValueField, children[0].DataType)), children);
            }

            case FixedSizeListType fixedType:
            {
                var children = RewriteChildren(data.Children);
                return children is null
                    ? data
                    : With(data, new FixedSizeListType(WithType(fixedType.ValueField, children[0].DataType),
                        fixedType.ListSize), children);
            }

            // Children are [run ends, values].
            case RunEndEncodedType reeType:
            {
                var children = RewriteChildren(data.Children);
                return children is null
                    ? data
                    : With(data, new RunEndEncodedType(reeType.RunEndsDataType, children[1].DataType), children);
            }

            // An extension is written as its storage, so its storage needs the same conversion. Apache.Arrow has no
            // general way to re-create an extension type over a different storage type, so a converted one is
            // unwrapped to that storage. That loses nothing for an extension the writer writes as bare storage:
            // the Parquet schema is the same, and the caller's declared type still reaches ARROW:schema. One the
            // writer annotates (UUID, VARIANT) would lose its annotation, so it is refused instead.
            case ExtensionType extensionType:
            {
                var storage = new ArrayData(extensionType.StorageType, data.Length, data.NullCount, data.Offset,
                    data.Buffers, data.Children, data.Dictionary);
                var rewritten = Rewrite(storage);
                if (ReferenceEquals(rewritten, storage))
                    return data;
                if (ArrowToSchemaConverter.AnnotatesExtension(extensionType))
                {
                    throw new NotSupportedException(
                        $"Extension type '{extensionType.Name}' over storage '{extensionType.StorageType.Name}' cannot " +
                        "be written: Parquet needs its byte arrays as string or binary, and converting the storage " +
                        "would drop the extension's Parquet annotation.");
                }
                return rewritten;
            }

            default:
                return data;
        }
    }

    private static ArrayData[]? RewriteChildren(ArrayData[] children)
    {
        ArrayData[]? result = null;
        for (int i = 0; i < children.Length; i++)
        {
            var rewritten = Rewrite(children[i]);
            if (!ReferenceEquals(rewritten, children[i]))
            {
                result ??= [.. children];
                result[i] = rewritten;
            }
        }
        return result;
    }

    private static ArrayData With(ArrayData data, IArrowType type, ArrayData[] children) =>
        new(type, data.Length, data.NullCount, data.Offset, data.Buffers, children, data.Dictionary);

    private static Field WithType(Field field, IArrowType type) =>
        new(field.Name, type, field.IsNullable, field.Metadata);

    // Reads through the typed accessors, which apply the array's own offset, so the copy holds exactly the rows the
    // array exposes, from offset 0.
    private static ArrayData Copy(IArrowArray array, IArrowType type)
    {
        int length = array.Length;
        long total = 0;
        for (int i = 0; i < length; i++)
        {
            if (!array.IsNull(i))
                total += Value(array, i).Length;
        }

        if (total > int.MaxValue)
        {
            throw new NotSupportedException(
                $"A {array.Data.DataType.Name} array holds {total} bytes of values, more than a {type.Name} array's " +
                "32-bit offsets can address. Write it in smaller batches.");
        }

        var offsets = new byte[(length + 1) * sizeof(int)];
        var values = new byte[total];
        byte[]? validity = array.NullCount > 0 ? new byte[(length + 7) / 8] : null;
        var offsetSpan = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(offsets.AsSpan());
        int position = 0;
        for (int i = 0; i < length; i++)
        {
            offsetSpan[i] = position;
            if (array.IsNull(i))
                continue;
            if (validity is not null)
                validity[i / 8] |= (byte)(1 << (i % 8));
            var value = Value(array, i);
            value.CopyTo(values.AsSpan(position));
            position += value.Length;
        }
        offsetSpan[length] = position;

        return new ArrayData(type, length, array.NullCount, 0,
            [validity is null ? ArrowBuffer.Empty : new ArrowBuffer(validity), new ArrowBuffer(offsets),
                new ArrowBuffer(values)]);
    }

    private static ReadOnlySpan<byte> Value(IArrowArray array, int index) => array switch
    {
        LargeBinaryArray large => large.GetBytes(index),
        BinaryViewArray view => view.GetBytes(index),
        _ => throw new InvalidOperationException(array.GetType().Name),
    };
}
