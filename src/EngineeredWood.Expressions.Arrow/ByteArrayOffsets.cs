// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow;

/// <summary>
/// Puts a column's large and view strings and binaries, at any depth, in the plain 32-bit-offset layout the evaluator
/// and the Spark functions read (#859).
/// </summary>
/// <remarks>
/// <para>A reader may hand back any of the three layouts for one logical string or binary column (Parquet's
/// <c>ByteArrayOutput</c>, #793), and Spark has one STRING and one BINARY type. Every type test, cast and kernel here
/// is written against <see cref="StringType"/>/<see cref="StringArray"/> and <see cref="BinaryType"/>/
/// <see cref="BinaryArray"/>, so a <c>utf8view</c> column used to raise "Cannot evaluate". Converting where a column
/// is read fixes every one of those paths at once, and loses nothing: each value keeps its bytes.</para>
/// <para>The conversion copies the values. The direct comparison kernels read a top-level <c>utf8view</c> or
/// <c>large_utf8</c> column in place instead, so the common filter costs no copy.</para>
/// <para>Containers are rebuilt over their children's <see cref="ArrayData"/>, so a container keeps its own offset,
/// validity and offsets, and only its byte-array descendants are copied. An extension column is left as it is: its
/// type cannot be re-created over a different storage.</para>
/// </remarks>
internal static class ByteArrayOffsets
{
    /// <summary><paramref name="array"/> in the offset layout: the same instance when it already is.</summary>
    public static IArrowArray Of(IArrowArray array)
    {
        var data = Rewrite(array.Data);
        return ReferenceEquals(data, array.Data) ? array : ArrowArrayFactory.BuildArray(data);
    }

    /// <summary>The type <see cref="Of"/> gives a top-level column of <paramref name="type"/>, when it is a large or
    /// view string or binary; otherwise <paramref name="type"/>.</summary>
    public static IArrowType TopLevel(IArrowType type) => type switch
    {
        LargeStringType or StringViewType => StringType.Default,
        LargeBinaryType or BinaryViewType => BinaryType.Default,
        _ => type,
    };

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
    // array exposes, from offset 0. A container over it indexes logical positions, which this keeps.
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
                "32-bit offsets can address, so it cannot be evaluated over. Read it in smaller batches.");
        }

        var offsets = new byte[(length + 1) * sizeof(int)];
        var values = new byte[total];
        int nulls = 0;
        byte[]? validity = array.NullCount != 0 ? new byte[(length + 7) / 8] : null;   // -1 is "not counted"
        var offsetSpan = MemoryMarshal.Cast<byte, int>(offsets.AsSpan());
        int position = 0;
        for (int i = 0; i < length; i++)
        {
            offsetSpan[i] = position;
            if (array.IsNull(i))
            {
                nulls++;
                continue;
            }
            if (validity is not null)
                validity[i >> 3] |= (byte)(1 << (i & 7));
            var value = Value(array, i);
            value.CopyTo(values.AsSpan(position));
            position += value.Length;
        }
        offsetSpan[length] = position;

        // The values were read from the array's own buffers; a native one must outlive the copy.
        GC.KeepAlive(array);

        return new ArrayData(type, length, nulls, 0,
            [nulls == 0 ? ArrowBuffer.Empty : new ArrowBuffer(validity!), new ArrowBuffer(offsets),
                new ArrowBuffer(values)]);
    }

    private static ReadOnlySpan<byte> Value(IArrowArray array, int index) => array switch
    {
        LargeBinaryArray large => large.GetBytes(index),
        BinaryViewArray view => view.GetBytes(index),
        _ => throw new InvalidOperationException(array.GetType().Name),
    };
}
