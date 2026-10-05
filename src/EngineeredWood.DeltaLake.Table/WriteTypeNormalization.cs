// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowMapType = Apache.Arrow.Types.MapType;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Converts the Arrow types a Delta type accepts but does not read back as into the one it does, on
/// batches about to be written.
/// </summary>
/// <remarks>
/// <para>
/// Several Arrow types map onto one Delta type: <c>binary</c> takes FixedSizeBinary, <c>date</c>
/// takes Date64, and <c>decimal(p,s)</c> takes Decimal32 and Decimal64. Written as they arrive, each
/// would reach Parquet in its own physical form, and the files of one column would disagree with
/// each other and with the table's read schema (Binary, Date32, Decimal128). Converting at the write
/// boundary keeps every file this library writes in the canonical form.
/// </para>
/// <para>
/// Every conversion is lossless or refused. FixedSizeBinary to Binary and the decimal widenings are
/// lossless by construction. Date64 is milliseconds that the Arrow format requires to be "evenly
/// divisible by 86400000", so it converts to a day count exactly — and a value that breaks that rule
/// is refused rather than truncated, since there is no telling what the writer meant by it.
/// </para>
/// <para>
/// Works on <see cref="ArrayData"/> so a sliced array keeps its offset: each converted buffer is laid
/// out over the same slot positions as the original, and untouched buffers (validity bitmaps, list
/// offsets, FixedSizeBinary's value bytes) are shared rather than copied.
/// </para>
/// </remarks>
internal static class WriteTypeNormalization
{
    private const long MillisecondsPerDay = 86_400_000;

    /// <summary>Returns <paramref name="batch"/> with every convertible column in canonical form, or
    /// the batch itself when it has none.</summary>
    public static RecordBatch Normalize(RecordBatch batch)
    {
        bool any = false;
        foreach (var field in batch.Schema.FieldsList)
            any |= Needs(field.DataType);
        if (!any)
            return batch;

        var fields = new List<Field>(batch.ColumnCount);
        var columns = new List<IArrowArray>(batch.ColumnCount);
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var field = batch.Schema.FieldsList[i];
            var data = Normalize(batch.Column(i).Data, field.Name);
            fields.Add(WithType(field, data.DataType));
            columns.Add(ArrowArrayFactory.BuildArray(data));
        }

        return new RecordBatch(
            new Apache.Arrow.Schema(fields, batch.Schema.Metadata), columns, batch.Length);
    }

    private static bool Needs(IArrowType type) => type switch
    {
        // By TypeId: every Arrow decimal type derives from FixedSizeBinaryType.
        FixedSizeBinaryType when type.TypeId == ArrowTypeId.FixedSizedBinary => true,
        Date64Type or Decimal32Type or Decimal64Type => true,
        ArrowStructType s => s.Fields.Any(f => Needs(f.DataType)),
        ListType l => Needs(l.ValueDataType),
        ArrowMapType m => Needs(m.KeyField.DataType) || Needs(m.ValueField.DataType),
        _ => false,
    };

    private static Field WithType(Field field, IArrowType type) =>
        ReferenceEquals(field.DataType, type)
            ? field
            : new Field(field.Name, type, field.IsNullable, field.Metadata);

    private static ArrayData Normalize(ArrayData data, string path)
    {
        if (!Needs(data.DataType))
            return data;

        switch (data.DataType)
        {
            case FixedSizeBinaryType fsb when fsb.TypeId == ArrowTypeId.FixedSizedBinary:
                return FixedSizeBinaryToBinary(data, fsb.ByteWidth);
            case Date64Type:
                return Date64ToDate32(data, path);
            case Decimal32Type d32:
                return WidenDecimal(data, new Decimal128Type(d32.Precision, d32.Scale), sourceWidth: 4);
            case Decimal64Type d64:
                return WidenDecimal(data, new Decimal128Type(d64.Precision, d64.Scale), sourceWidth: 8);

            case ArrowStructType st:
            {
                var children = new ArrayData[data.Children.Length];
                var fields = new List<Field>(st.Fields.Count);
                for (int i = 0; i < children.Length; i++)
                {
                    children[i] = Normalize(data.Children[i], path + "." + st.Fields[i].Name);
                    fields.Add(WithType(st.Fields[i], children[i].DataType));
                }
                return new ArrayData(
                    new ArrowStructType(fields), data.Length, data.NullCount, data.Offset, data.Buffers, children);
            }

            case ListType lt:
            {
                var values = Normalize(data.Children[0], path + ".element");
                return new ArrayData(
                    new ListType(WithType(lt.ValueField, values.DataType)),
                    data.Length, data.NullCount, data.Offset, data.Buffers, [values]);
            }

            case ArrowMapType mt:
            {
                // The single child is the key/value entries struct; normalize it as one.
                var entries = Normalize(data.Children[0], path);
                var entryType = (ArrowStructType)entries.DataType;
                var mapType = new ArrowMapType(entryType.Fields[0], entryType.Fields[1], mt.KeySorted);
                return new ArrayData(
                    mapType, data.Length, data.NullCount, data.Offset, data.Buffers, [entries]);
            }

            default:
                return data;
        }
    }

    /// <summary>Binary over the SAME value bytes: slot <c>j</c> of a FixedSizeBinary array lives at
    /// <c>j * width</c>, so only an offsets buffer saying so is new.</summary>
    private static ArrayData FixedSizeBinaryToBinary(ArrayData data, int width)
    {
        int slots = data.Offset + data.Length;
        if ((long)slots * width > int.MaxValue)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.UnwritableValue,
                $"A FixedSizeBinary column of {slots} values of {width} bytes is too large to write as "
                + "Delta binary in one batch; split the batch.");
        }

        var offsets = new byte[(slots + 1) * sizeof(int)];
        for (int j = 0; j <= slots; j++)
            BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan(j * sizeof(int)), j * width);

        return new ArrayData(
            BinaryType.Default, data.Length, data.NullCount, data.Offset,
            [data.Buffers[0], new ArrowBuffer(offsets), data.Buffers[1]]);
    }

    private static ArrayData Date64ToDate32(ArrayData data, string path)
    {
        var source = new Date64Array(data);
        var days = new byte[(data.Offset + data.Length) * sizeof(int)];
        var millis = data.Buffers[1].Span;
        for (int i = 0; i < data.Length; i++)
        {
            if (!source.IsValid(i))
                continue;

            int slot = data.Offset + i;
            long ms = BinaryPrimitives.ReadInt64LittleEndian(millis.Slice(slot * sizeof(long)));
            long day = ms / MillisecondsPerDay;
            if (ms % MillisecondsPerDay != 0 || day is < int.MinValue or > int.MaxValue)
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.UnwritableValue,
                    $"Column '{path}': Date64 value {ms} at row {i} is not a whole number of days (the Arrow "
                    + "format requires Date64 values to be evenly divisible by 86400000), so it cannot be "
                    + "written as a Delta date without guessing which day was meant.");
            }
            BinaryPrimitives.WriteInt32LittleEndian(days.AsSpan(slot * sizeof(int)), (int)day);
        }

        return new ArrayData(
            Date32Type.Default, data.Length, data.NullCount, data.Offset,
            [data.Buffers[0], new ArrowBuffer(days)]);
    }

    /// <summary>Sign-extends each 4- or 8-byte unscaled value to Decimal128's 16 bytes; precision and
    /// scale are unchanged, so the values are too.</summary>
    private static ArrayData WidenDecimal(ArrayData data, Decimal128Type target, int sourceWidth)
    {
        int slots = data.Offset + data.Length;
        var wide = new byte[slots * 16];
        var narrow = data.Buffers[1].Span;
        for (int j = data.Offset; j < slots; j++)
        {
            long value = sourceWidth == 4
                ? BinaryPrimitives.ReadInt32LittleEndian(narrow.Slice(j * 4))
                : BinaryPrimitives.ReadInt64LittleEndian(narrow.Slice(j * 8));
            var dest = wide.AsSpan(j * 16);
            BinaryPrimitives.WriteInt64LittleEndian(dest, value);
            BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(8), value < 0 ? -1L : 0L);
        }

        return new ArrayData(
            target, data.Length, data.NullCount, data.Offset, [data.Buffers[0], new ArrowBuffer(wide)]);
    }
}
