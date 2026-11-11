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
    /// all already are. A string or binary array is copied; a nested column is rebuilt around its converted children,
    /// sharing the rest of its buffers.
    /// </summary>
    /// <param name="batch">The batch.</param>
    /// <param name="kind">The representation.</param>
    /// <param name="borrowed">Arrays the caller does not own, which must survive: a row mask's batch is a view of the
    /// reader's, which gathers its rows from the same arrays afterwards (review round 3). A replaced array sharing a
    /// buffer with it is not released; one built from it (a partition value, a widened column) still is (review round
    /// 4). Null when the caller owns every array of <paramref name="batch"/>.</param>
    /// <remarks>
    /// The replaced arrays, at any depth (a nested column's string child as much as a string column: review round 3),
    /// are released once every column is converted, each array once, and only those that share no buffer with the
    /// result (review round 1): a codec-seam host may hand one array, or arrays over the same buffers, for several
    /// columns, so releasing one as soon as it was converted freed bytes a later conversion, or an unchanged column,
    /// still read.
    /// </remarks>
    public static RecordBatch Apply(RecordBatch batch, ByteArrayOutputKind kind, RecordBatch? borrowed = null)
    {
        IArrowArray[]? columns = null;
        Field[]? fields = null;
        var replaced = new List<ArrayData>();
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var column = batch.Column(i);
            var target = Of(column.Data.DataType, kind);
            if (ReferenceEquals(target, column.Data.DataType))
                continue;

            columns ??= [.. Enumerable.Range(0, batch.ColumnCount).Select(batch.Column)];
            fields ??= [.. batch.Schema.FieldsList];
            columns[i] = ArrowArrayFactory.BuildArray(Convert(column.Data, kind, replaced));
            var field = batch.Schema.FieldsList[i];
            fields[i] = new Field(field.Name, columns[i].Data.DataType, field.IsNullable, field.Metadata);
        }

        if (columns is null)
            return batch;

        var result = new RecordBatch(new Apache.Arrow.Schema(fields!, batch.Schema.Metadata), columns, batch.Length);
        Release(replaced, result, borrowed);
        return result;
    }

    // Disposes each replaced array once, unless the result still holds one of its buffers. Every decision is made
    // before anything is disposed: a disposed array's buffers can't be inspected. Distinct arrays over the same
    // buffers (slices) are each disposed: Arrow counts their references to the buffers, which are freed with the last.
    // A nested column's original is left to the collector: its own buffers and unconverted children are the result's.
    private static void Release(List<ArrayData> replaced, RecordBatch result, RecordBatch? borrowed)
    {
        var kept = new HashSet<ReadOnlyMemory<byte>>();
        for (int i = 0; i < result.ColumnCount; i++)
            Parquet.BatchSelector.CollectBuffers(result.Column(i).Data, kept);
        for (int i = 0; i < (borrowed?.ColumnCount ?? 0); i++)
            Parquet.BatchSelector.CollectBuffers(borrowed!.Column(i).Data, kept);

        var doomed = new List<ArrayData>();
        foreach (var original in replaced)
        {
            if (doomed.Exists(d => ReferenceEquals(d, original)) || Parquet.BatchSelector.SharesAny(original, kept))
                continue;
            doomed.Add(original);
        }

        foreach (var original in doomed)
            original.Dispose();
    }

    /// <summary>
    /// Releases what a row mask built to answer (review round 4): every column of <paramref name="reshaped"/>, the batch
    /// it evaluated, that shares no buffer with <paramref name="borrowed"/> (the reader's arrays, which it gathers from
    /// afterwards) or with <paramref name="answer"/> (a predicate may answer with one of its own input columns). These
    /// are the partition values, backfilled NULLs, widened columns and conversions the reshaping created, which nothing
    /// else holds once the mask has answered. Each array once; every decision before anything is disposed.
    /// </summary>
    public static void ReleaseReshaped(RecordBatch reshaped, RecordBatch borrowed, IArrowArray? answer)
    {
        if (ReferenceEquals(reshaped, borrowed))
            return;

        var kept = new HashSet<ReadOnlyMemory<byte>>();
        for (int i = 0; i < borrowed.ColumnCount; i++)
            Parquet.BatchSelector.CollectBuffers(borrowed.Column(i).Data, kept);
        if (answer is not null)
            Parquet.BatchSelector.CollectBuffers(answer.Data, kept);

        var doomed = new List<IArrowArray>();
        for (int i = 0; i < reshaped.ColumnCount; i++)
        {
            var column = reshaped.Column(i);
            if (doomed.Exists(d => ReferenceEquals(d, column)) || Parquet.BatchSelector.SharesAny(column.Data, kept))
                continue;
            doomed.Add(column);
        }

        foreach (var column in doomed)
            column.Dispose();
    }

    private static bool IsLeaf(IArrowType type) =>
        type is StringType or StringViewType or LargeStringType or BinaryType or BinaryViewType or LargeBinaryType;

    // `data` in `kind`'s representation: the same instance when nothing in it changes. Each string or binary array it
    // copies is added to `replaced`, when that is given.
    private static ArrayData Convert(ArrayData data, ByteArrayOutputKind kind, List<ArrayData>? replaced)
    {
        var target = Of(data.DataType, kind);
        if (ReferenceEquals(target, data.DataType))
            return data;
        if (IsLeaf(data.DataType))
        {
            replaced?.Add(data);
            return ConvertLeaf(ArrowArrayFactory.BuildArray(data), target).Data;
        }

        // A container keeps its own buffers and offset; only its children change, and Of built the target from the
        // same per-child decisions. ArrayData.Children are unsliced (the parent's offset applies to the parent), so
        // each is converted whole, at its own length.
        var children = new ArrayData[data.Children.Length];
        for (int i = 0; i < children.Length; i++)
            children[i] = Convert(data.Children[i], kind, replaced);

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
                // The bytes copied as they are (review round 1: not decoded to a string and encoded back per value),
                // then the large-binary layout, which a large string shares, relabelled.
                var binary = (LargeBinaryArray)ConvertLeaf(source, LargeBinaryType.Default);
                var data = binary.Data;
                return new LargeStringArray(new ArrayData(
                    LargeStringType.Default, data.Length, data.NullCount, data.Offset, data.Buffers, data.Children));
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
