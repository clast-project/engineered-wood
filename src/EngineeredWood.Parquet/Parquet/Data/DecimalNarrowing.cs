// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Chooses a <c>Decimal128</c>/<c>Decimal256</c> column's physical type from its PRECISION, as Spark does (#504):
/// INT32 up to precision 9, INT64 up to 18, and otherwise the narrowest FIXED_LEN_BYTE_ARRAY that holds the
/// precision — Spark's and pyarrow's <c>minBytesForPrecision</c>. Choosing from the Arrow type instead stored
/// every Delta decimal as FLBA(16), four times the bytes a <c>decimal(9,2)</c> needs and out of reach of the
/// integer encodings.
///
/// <para>The values are narrowed here from Arrow's 16- or 32-byte little-endian slots. The precision bounds
/// the magnitude, so a well-formed value always fits. One that does not is refused rather than truncated, since
/// truncating would write a different number. This guards the layout, not the precision: a value past its
/// precision that the layout still holds (10^9 in an INT32 <c>decimal(9,2)</c>, anything in a full-width slot)
/// is written as given, as it was before narrowing, and reads back unchanged.</para>
/// </summary>
internal static class DecimalNarrowing
{
    /// <summary>Largest precision INT32 holds: 10^9 - 1 &lt; 2^31.</summary>
    public const int MaxInt32Precision = 9;

    /// <summary>Largest precision INT64 holds: 10^18 - 1 &lt; 2^63.</summary>
    public const int MaxInt64Precision = 18;

    // Decimal256's largest precision is 76; index 0 is unused.
    private static readonly int[] MinBytes = BuildMinBytes(76);

    /// <summary>
    /// The fewest bytes whose signed two's-complement range holds every value of
    /// <paramref name="precision"/> digits: the smallest n with 2^(8n-1) &gt; 10^p - 1.
    /// </summary>
    public static int MinBytesForPrecision(int precision) => MinBytes[precision];

    /// <summary>The physical type and FLBA width for a decimal of <paramref name="precision"/> digits.</summary>
    public static (PhysicalType PhysicalType, int? TypeLength) PhysicalTypeFor(int precision) =>
        precision <= MaxInt32Precision ? (PhysicalType.Int32, null)
        : precision <= MaxInt64Precision ? (PhysicalType.Int64, null)
        : (PhysicalType.FixedLenByteArray, MinBytesForPrecision(precision));

    /// <summary>
    /// The Arrow type the writers carry a decimal column as on its way to <paramref name="physicalType"/>:
    /// <c>Decimal32</c>/<c>Decimal64</c> for the integer layouts, whose encoders, dictionary and statistics
    /// already handle those, and the type itself otherwise.
    /// </summary>
    public static IArrowType EncodingType(IArrowType type, PhysicalType physicalType) => (type, physicalType) switch
    {
        (Decimal128Type d, PhysicalType.Int32) => new Decimal32Type(d.Precision, d.Scale),
        (Decimal256Type d, PhysicalType.Int32) => new Decimal32Type(d.Precision, d.Scale),
        (Decimal128Type d, PhysicalType.Int64) => new Decimal64Type(d.Precision, d.Scale),
        (Decimal256Type d, PhysicalType.Int64) => new Decimal64Type(d.Precision, d.Scale),
        _ => type,
    };

    /// <summary>Whether <paramref name="type"/> is a decimal held in a 16- or 32-byte slot.</summary>
    public static bool IsWideDecimal(IArrowType type) => type is Decimal128Type or Decimal256Type;

    /// <summary>
    /// Narrows a <c>Decimal128</c>/<c>Decimal256</c> array to the <c>Decimal32</c> or <c>Decimal64</c>
    /// <paramref name="target"/> (from <see cref="EncodingType"/>). The result starts at offset 0.
    /// </summary>
    public static IArrowArray ToInteger(IArrowArray array, IArrowType target)
    {
        int width = target is Decimal32Type ? 4 : 8;
        int length = array.Length;
        var values = new byte[length * width];
        CopyNarrowed(array, values, width, bigEndian: false);
        return Build(array, target, values);
    }

    /// <summary>
    /// Narrows a <c>Decimal128</c>/<c>Decimal256</c> array to <paramref name="typeLength"/> BIG-endian bytes per
    /// value, as Parquet stores a decimal FIXED_LEN_BYTE_ARRAY. The result is a
    /// <see cref="FixedSizeBinaryType"/> array at offset 0: what reaches the file, for the dictionary, page
    /// encoders, statistics and Bloom filter to see.
    /// </summary>
    public static IArrowArray ToBigEndian(IArrowArray array, int typeLength)
    {
        var values = new byte[array.Length * typeLength];
        CopyNarrowed(array, values, typeLength, bigEndian: true);
        return Build(array, new FixedSizeBinaryType(typeLength), values);
    }

    /// <summary>
    /// Refuses, as <see cref="ToInteger"/> and <see cref="ToBigEndian"/> would, an array holding a non-null
    /// value that <paramref name="width"/> bytes cannot hold, without narrowing anything: for a caller that has
    /// to know a batch is writable before it changes any state.
    /// </summary>
    public static void EnsureFits(IArrowArray array, int width)
    {
        int slot = ((FixedSizeBinaryType)array.Data.DataType).ByteWidth;
        if (width >= slot)
            return;

        int length = array.Length;
        ReadOnlySpan<byte> src = array.Data.Buffers[1].Span.Slice(array.Data.Offset * slot, length * slot);
        bool checkNulls = array.NullCount != 0;
        for (int i = 0; i < length; i++)
        {
            if (!Fits(src.Slice(i * slot, slot), width) && (!checkNulls || !array.IsNull(i)))
                throw OutOfRange(slot, width);
        }

        GC.KeepAlive(array);
    }

    /// <summary>
    /// Writes the low <paramref name="width"/> bytes of the little-endian <paramref name="value"/> to
    /// <paramref name="dest"/>, refusing a value they cannot hold.
    /// </summary>
    public static void Narrow(ReadOnlySpan<byte> value, Span<byte> dest, int width, bool bigEndian)
    {
        if (!Fits(value, width))
            throw OutOfRange(value.Length, width);
        value.Slice(0, width).CopyTo(dest);
        if (bigEndian)
            dest.Slice(0, width).Reverse();
    }

    /// <summary>
    /// The inverse of <see cref="Narrow"/>: sign-extends a <paramref name="source"/> of
    /// <c>source.Length</c> bytes to the whole of <paramref name="dest"/>, little-endian.
    /// </summary>
    public static void Widen(ReadOnlySpan<byte> source, Span<byte> dest, bool sourceBigEndian)
    {
        int width = source.Length;
        source.CopyTo(dest);
        if (sourceBigEndian)
            dest.Slice(0, width).Reverse();
        byte fill = (dest[width - 1] & 0x80) != 0 ? (byte)0xFF : (byte)0x00;
        dest.Slice(width).Fill(fill);
    }

    private static void CopyNarrowed(IArrowArray array, byte[] values, int width, bool bigEndian)
    {
        int slot = ((FixedSizeBinaryType)array.Data.DataType).ByteWidth;
        int length = array.Length;
        ReadOnlySpan<byte> src = array.Data.Buffers[1].Span.Slice(array.Data.Offset * slot, length * slot);
        bool checkNulls = array.NullCount != 0;

        for (int i = 0; i < length; i++)
        {
            var value = src.Slice(i * slot, slot);
            var dest = values.AsSpan(i * width, width);
            if (width == slot || Fits(value, width))
            {
                value.Slice(0, width).CopyTo(dest);
                if (bigEndian)
                    dest.Reverse();
            }
            // The bytes under a null are arbitrary and may not fit; they are left zero. Asking only here keeps
            // the validity lookup off the common path.
            else if (!checkNulls || !array.IsNull(i))
            {
                throw OutOfRange(slot, width);
            }
        }

        GC.KeepAlive(array);
    }

    private static IArrowArray Build(IArrowArray source, IArrowType type, byte[] values)
    {
        var (validity, nullCount) = ArrowCompute.AlignedValidity(source);
        return ArrowArrayFactory.BuildArray(new ArrayData(
            type, source.Length, nullCount, offset: 0, [validity, new ArrowBuffer(values)]));
    }

    // A value fits in its low `width` bytes when every byte above them repeats the sign of the top kept byte.
    private static bool Fits(ReadOnlySpan<byte> value, int width)
    {
        byte fill = (value[width - 1] & 0x80) != 0 ? (byte)0xFF : (byte)0x00;
        for (int i = width; i < value.Length; i++)
        {
            if (value[i] != fill)
                return false;
        }
        return true;
    }

    private static ArgumentException OutOfRange(int slot, int width) => new(
        $"A {slot * 8}-bit decimal value does not fit the {width}-byte Parquet layout its precision calls for, "
        + "so it lies well outside the column's declared precision. Writing it narrowed would store a different "
        + "number; correct the value or declare a larger precision.");

    private static int[] BuildMinBytes(int maxPrecision)
    {
        var table = new int[maxPrecision + 1];
        var limit = System.Numerics.BigInteger.One; // 10^p
        int bytes = 1;
        for (int p = 1; p <= maxPrecision; p++)
        {
            limit *= 10;
            // 2^(8n-1) must exceed 10^p - 1, i.e. be at least 10^p.
            while (System.Numerics.BigInteger.Pow(2, 8 * bytes - 1) < limit)
                bytes++;
            table[p] = bytes;
        }
        return table;
    }
}
