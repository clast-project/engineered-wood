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
/// takes Date64, and <c>decimal(p,s)</c> takes Decimal32, Decimal64 and Decimal256. Written as they
/// arrive, each would reach Parquet in its own physical form, and the files of one column would
/// disagree with each other and with the table's read schema (Binary, Date32, Decimal128).
/// Converting at the write boundary keeps every file this library writes in the canonical form. That
/// includes rewrites (UPDATE, DELETE, OPTIMIZE, change files), whose rows come back from a reader in
/// whatever form the source file implied.
/// </para>
/// <para>
/// Every conversion is lossless or refused. FixedSizeBinary to Binary and the decimal widenings are
/// lossless by construction. Two can meet a value that does not fit, and refuse it rather than guess:
/// a Date64 that is not a whole number of days (the Arrow format requires values "evenly divisible by
/// 86400000"), and a Decimal256 whose value does not fit Decimal128 (its precision promises it does).
/// </para>
/// <para>
/// Only VISIBLE values are checked. A slot under a null struct, or a list element no row references,
/// may hold anything; it is never written, so it is converted without complaint (to zero when it does
/// not fit). Refusing it would refuse a batch for bytes nobody can see.
/// </para>
/// <para>
/// Works on <see cref="ArrayData"/>, and every array it converts comes out ZERO-OFFSET, sized to its
/// own length: a one-row slice of a large batch costs one row, not the prefix before it. Buffers that
/// need no conversion are shared where they can be (FixedSizeBinary's value bytes, as a slice of the
/// original; a byte-aligned validity bitmap). A sliced struct is rebased by slicing its children —
/// struct children are not sliced with their parent in Arrow C#, so parent slot <c>i</c> is child slot
/// <c>parent.Offset + i</c>. A list keeps its offsets: rebasing them would leave values no row
/// references, which the Parquet writer cannot yet write (#470), and a sliced list writes as it is.
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
            var data = Normalize(batch.Column(i).Data, field.Name, visible: null);
            fields.Add(WithType(field, data.DataType));
            columns.Add(ArrowArrayFactory.BuildArray(data));
        }

        return new RecordBatch(
            new Apache.Arrow.Schema(fields, batch.Schema.Metadata), columns, batch.Length);
    }

    /// <summary>
    /// <see cref="Normalize(RecordBatch)"/> over a list: the caller's OWN list when no batch needed
    /// converting — the common case on the write hot path — so nothing is allocated until a batch
    /// actually changes.
    /// </summary>
    public static IReadOnlyList<RecordBatch> NormalizeAll(IReadOnlyList<RecordBatch> batches)
    {
        List<RecordBatch>? converted = null;
        for (int i = 0; i < batches.Count; i++)
        {
            var batch = Normalize(batches[i]);
            if (converted is null && ReferenceEquals(batch, batches[i]))
                continue;
            if (converted is null)
            {
                converted = new List<RecordBatch>(batches.Count);
                for (int j = 0; j < i; j++)
                    converted.Add(batches[j]);
            }
            converted.Add(batch);
        }
        return converted ?? batches;
    }

    private static bool Needs(IArrowType type) => type switch
    {
        // By TypeId: every Arrow decimal type derives from FixedSizeBinaryType.
        FixedSizeBinaryType when type.TypeId == ArrowTypeId.FixedSizedBinary => true,
        Date64Type or Decimal32Type or Decimal64Type or Decimal256Type => true,
        ArrowStructType s => s.Fields.Any(f => Needs(f.DataType)),
        ListType l => Needs(l.ValueDataType),
        ArrowMapType m => Needs(m.KeyField.DataType) || Needs(m.ValueField.DataType),
        _ => false,
    };

    private static Field WithType(Field field, IArrowType type) =>
        ReferenceEquals(field.DataType, type)
            ? field
            : new Field(field.Name, type, field.IsNullable, field.Metadata);

    /// <param name="visible">Which LOGICAL slots of <paramref name="data"/> (indexes 0 to
    /// <c>Length</c>) a reader can see through every ancestor; null for a top-level column, where all
    /// of them can. A slot's own validity is checked separately.</param>
    private static ArrayData Normalize(ArrayData data, string path, bool[]? visible)
    {
        if (!Needs(data.DataType))
            return data;

        switch (data.DataType)
        {
            case FixedSizeBinaryType fsb when fsb.TypeId == ArrowTypeId.FixedSizedBinary:
                return FixedSizeBinaryToBinary(data, fsb.ByteWidth);
            case Date64Type:
                return Date64ToDate32(data, path, visible);
            case Decimal32Type d32:
                return WidenDecimal(data, new Decimal128Type(d32.Precision, d32.Scale), sourceWidth: 4);
            case Decimal64Type d64:
                return WidenDecimal(data, new Decimal128Type(d64.Precision, d64.Scale), sourceWidth: 8);
            case Decimal256Type d256:
                return NarrowDecimal256(data, new Decimal128Type(d256.Precision, d256.Scale), path, visible);

            case ArrowStructType st:
            {
                if (data.Offset != 0)
                    data = RebaseStruct(data);
                var childVisible = StructChildVisibility(data, visible);
                var children = new ArrayData[data.Children.Length];
                var fields = new List<Field>(st.Fields.Count);
                for (int i = 0; i < children.Length; i++)
                {
                    children[i] = Normalize(data.Children[i], path + "." + st.Fields[i].Name, childVisible);
                    fields.Add(WithType(st.Fields[i], children[i].DataType));
                }
                return new ArrayData(
                    new ArrowStructType(fields), data.Length, data.NullCount, data.Offset, data.Buffers, children);
            }

            case ListType lt:
            {
                var values = Normalize(
                    data.Children[0], path + ".element", ListChildVisibility(data, visible));
                return new ArrayData(
                    new ListType(WithType(lt.ValueField, values.DataType)),
                    data.Length, data.NullCount, data.Offset, data.Buffers, [values]);
            }

            case ArrowMapType mt:
            {
                // The single child is the key/value entries struct; normalize it as one.
                var entries = Normalize(data.Children[0], path, ListChildVisibility(data, visible));
                var entryType = (ArrowStructType)entries.DataType;
                var mapType = new ArrowMapType(entryType.Fields[0], entryType.Fields[1], mt.KeySorted);
                return new ArrayData(
                    mapType, data.Length, data.NullCount, data.Offset, data.Buffers, [entries]);
            }

            default:
                return data;
        }
    }

    // ── Visibility ────────────────────────────────────────────────────────────────────────────────────

    /// <summary>Whether LOGICAL slot <paramref name="i"/> of <paramref name="data"/> is non-null.</summary>
    private static bool IsValid(ArrayData data, int i)
    {
        var bitmap = data.Buffers[0];
        if (bitmap.IsEmpty)
            return true;
        int bit = data.Offset + i;
        return (bitmap.Span[bit >> 3] & (1 << (bit & 7))) != 0;
    }

    private static bool Shows(ArrayData data, bool[]? visible, int i) =>
        (visible is null || visible[i]) && IsValid(data, i);

    private static bool[] StructChildVisibility(ArrayData parent, bool[]? visible)
    {
        var child = new bool[parent.Children[0].Length];
        for (int i = 0; i < parent.Length; i++)
            child[parent.Offset + i] = Shows(parent, visible, i);
        return child;
    }

    private static bool[] ListChildVisibility(ArrayData parent, bool[]? visible)
    {
        var child = new bool[parent.Children[0].Length];
        var offsets = parent.Buffers[1].Span;
        for (int i = 0; i < parent.Length; i++)
        {
            if (!Shows(parent, visible, i))
                continue;
            int slot = parent.Offset + i;
            int start = BinaryPrimitives.ReadInt32LittleEndian(offsets.Slice(slot * sizeof(int)));
            int end = BinaryPrimitives.ReadInt32LittleEndian(offsets.Slice((slot + 1) * sizeof(int)));
            for (int e = start; e < end; e++)
                child[e] = true;
        }
        return child;
    }

    // ── Conversions ───────────────────────────────────────────────────────────────────────────────────

    /// <summary>A zero-offset struct over the same rows: each child sliced to the struct's slots.</summary>
    private static ArrayData RebaseStruct(ArrayData data)
    {
        var children = new ArrayData[data.Children.Length];
        for (int i = 0; i < children.Length; i++)
            children[i] = data.Children[i].Slice(data.Offset, data.Length);
        return new ArrayData(
            data.DataType, data.Length, data.NullCount, 0,
            [RebaseBitmap(data.Buffers[0], data.Offset, data.Length)], children);
    }

    /// <summary>The validity bits of slots <c>offset .. offset + length</c>, starting at bit 0. Shared when
    /// the slice starts on a byte boundary; copied otherwise.</summary>
    private static ArrowBuffer RebaseBitmap(ArrowBuffer bitmap, int offset, int length)
    {
        if (bitmap.IsEmpty || offset == 0)
            return bitmap;
        int bytes = (length + 7) / 8;
        if (offset % 8 == 0)
            return new ArrowBuffer(bitmap.Memory.Slice(offset / 8, bytes));

        var rebased = new byte[bytes];
        var source = bitmap.Span;
        for (int i = 0; i < length; i++)
        {
            int bit = offset + i;
            if ((source[bit >> 3] & (1 << (bit & 7))) != 0)
                rebased[i >> 3] |= (byte)(1 << (i & 7));
        }
        return new ArrowBuffer(rebased);
    }

    /// <summary>Binary over the same value bytes: the slice's bytes are shared, and only an offsets
    /// buffer saying where each value starts is new.</summary>
    private static ArrayData FixedSizeBinaryToBinary(ArrayData data, int width)
    {
        if ((long)data.Length * width > int.MaxValue)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.UnwritableValue,
                $"A FixedSizeBinary column of {data.Length} values of {width} bytes is too large to write as "
                + "Delta binary in one batch; split the batch.");
        }

        var offsets = new byte[(data.Length + 1) * sizeof(int)];
        for (int j = 0; j <= data.Length; j++)
            BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan(j * sizeof(int)), j * width);

        var values = data.Buffers[1].Memory.Slice(data.Offset * width, data.Length * width);
        return new ArrayData(
            BinaryType.Default, data.Length, data.NullCount, 0,
            [RebaseBitmap(data.Buffers[0], data.Offset, data.Length), new ArrowBuffer(offsets), new ArrowBuffer(values)]);
    }

    private static ArrayData Date64ToDate32(ArrayData data, string path, bool[]? visible)
    {
        var days = new byte[data.Length * sizeof(int)];
        var millis = data.Buffers[1].Span;
        for (int i = 0; i < data.Length; i++)
        {
            long ms = BinaryPrimitives.ReadInt64LittleEndian(millis.Slice((data.Offset + i) * sizeof(long)));
            long day = ms / MillisecondsPerDay;
            bool fits = ms % MillisecondsPerDay == 0 && day is >= int.MinValue and <= int.MaxValue;
            if (!fits)
            {
                if (!Shows(data, visible, i))
                    continue; // never written; left as zero
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.UnwritableValue,
                    $"Column '{path}': Date64 value {ms} at position {i} is not a whole number of days (the "
                    + "Arrow format requires Date64 values to be evenly divisible by 86400000), so it cannot "
                    + "be written as a Delta date without guessing which day was meant.");
            }
            BinaryPrimitives.WriteInt32LittleEndian(days.AsSpan(i * sizeof(int)), (int)day);
        }

        return new ArrayData(
            Date32Type.Default, data.Length, data.NullCount, 0,
            [RebaseBitmap(data.Buffers[0], data.Offset, data.Length), new ArrowBuffer(days)]);
    }

    /// <summary>Sign-extends each 4- or 8-byte unscaled value to Decimal128's 16 bytes; precision and
    /// scale are unchanged, so the values are too.</summary>
    private static ArrayData WidenDecimal(ArrayData data, Decimal128Type target, int sourceWidth)
    {
        var wide = new byte[data.Length * 16];
        var narrow = data.Buffers[1].Span;
        for (int i = 0; i < data.Length; i++)
        {
            int j = data.Offset + i;
            long value = sourceWidth == 4
                ? BinaryPrimitives.ReadInt32LittleEndian(narrow.Slice(j * 4))
                : BinaryPrimitives.ReadInt64LittleEndian(narrow.Slice(j * 8));
            var dest = wide.AsSpan(i * 16);
            BinaryPrimitives.WriteInt64LittleEndian(dest, value);
            BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(8), value < 0 ? -1L : 0L);
        }

        return new ArrayData(
            target, data.Length, data.NullCount, 0,
            [RebaseBitmap(data.Buffers[0], data.Offset, data.Length), new ArrowBuffer(wide)]);
    }

    /// <summary>Keeps the low 16 bytes of each 32-byte value. Schema conversion admits Decimal256 only
    /// up to precision 38, which Decimal128 holds; a value whose high half is not the sign extension of
    /// its low half breaks that promise and is refused.</summary>
    private static ArrayData NarrowDecimal256(ArrayData data, Decimal128Type target, string path, bool[]? visible)
    {
        if (target.Precision > 38)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.UnwritableValue,
                $"Column '{path}': decimal({target.Precision},{target.Scale}) exceeds Delta's maximum decimal "
                + "precision of 38.");
        }

        var narrow = new byte[data.Length * 16];
        var wide = data.Buffers[1].Span;
        for (int i = 0; i < data.Length; i++)
        {
            var value = wide.Slice((data.Offset + i) * 32, 32);
            long sign = (sbyte)value[15] < 0 ? -1L : 0L;
            bool fits = BinaryPrimitives.ReadInt64LittleEndian(value.Slice(16)) == sign
                && BinaryPrimitives.ReadInt64LittleEndian(value.Slice(24)) == sign;
            if (!fits)
            {
                if (!Shows(data, visible, i))
                    continue; // never written; left as zero
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.UnwritableValue,
                    $"Column '{path}': the Decimal256 value at position {i} does not fit the column's "
                    + $"decimal({target.Precision},{target.Scale}).");
            }
            value.Slice(0, 16).CopyTo(narrow.AsSpan(i * 16));
        }

        return new ArrayData(
            target, data.Length, data.NullCount, 0,
            [RebaseBitmap(data.Buffers[0], data.Offset, data.Length), new ArrowBuffer(narrow)]);
    }
}
