// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// The 96-bit carrier proposed by apache/parquet-format#601: a TIMESTAMP-annotated
/// <c>FIXED_LEN_BYTE_ARRAY(12)</c> holding a signed two's-complement LITTLE-ENDIAN count of the
/// declared <c>TimeUnit</c> since the Unix epoch. Ninety-six bits covers the whole ANSI SQL
/// TIMESTAMP(9) range (years 0001-9999), which INT64 nanoseconds does not — it stops at
/// 1677-09-21 and 2262-04-11.
/// </summary>
/// <remarks>
/// <para><b>The byte order is not settled upstream.</b> The proposal text, the parquet-java
/// reference implementation (apache/parquet-java#3680) and the proposed conformance fixture
/// (apache/parquet-testing#123) are all little-endian, and the proposal defends it with a
/// benchmark. But a co-author argued for big-endian on the spec PR so readers could reuse the
/// DECIMAL comparator, and the approving reviewer said in as many words that the choice was still
/// open. Little-endian is what this implements, and it is why every entry point here goes through
/// this one file: if the spec flips, this is the file that changes.</para>
///
/// <para>Nothing on the wire distinguishes the two orders, so a flip would make already-written
/// files silently wrong-valued rather than unreadable. That is the risk the
/// <c>EWPARQUET0004</c> experimental gate exists to carry.</para>
///
/// <para>Values are held as <see cref="Int128"/> rather than a hand-rolled 96-bit struct.
/// netstandard2.0 has no 128-bit integer, so the type comes from Clast.DatabaseDecimal's public
/// polyfill (see the package reference for why). The comparator below deliberately does NOT go
/// through <see cref="Int128"/>: it runs once per value while collecting statistics, and reading
/// two words beats materialising two 16-byte values.</para>
/// </remarks>
internal static class ExtendedTimestamp
{
    /// <summary>Width of the carrier, in bytes. The annotation is only legal at this width.</summary>
    public const int ByteWidth = 12;

    /// <summary>
    /// Decodes the 12-byte little-endian two's-complement value at the start of <paramref name="source"/>.
    /// </summary>
    public static Int128 Read(ReadOnlySpan<byte> source)
    {
        // Sign lives in the top bit of the LAST byte, so the high word is read signed and the low word
        // unsigned. Shifting the sign-extended high word into place and OR-ing the zero-extended low word
        // reproduces the full two's-complement value without a separate sign fixup.
        ulong low = BinaryPrimitives.ReadUInt64LittleEndian(source);
        int high = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(8));
        return ((Int128)high << 64) | (Int128)low;
    }

    /// <summary>
    /// Encodes <paramref name="value"/> as 12 little-endian two's-complement bytes. Bits above the
    /// 96th are discarded; callers that cannot tolerate that must check <see cref="IsRepresentable"/>.
    /// </summary>
    public static void Write(Int128 value, Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(destination, (ulong)(value & LowWordMask));
        BinaryPrimitives.WriteUInt32LittleEndian(
            destination.Slice(8), (uint)(ulong)((value >> 64) & HighWordMask));
    }

    /// <summary>
    /// Orders two encoded values as signed 96-bit integers, which is what TYPE_DEFINED_ORDER means for
    /// this column. Note that this is NOT the lexicographic byte order every other
    /// FIXED_LEN_BYTE_ARRAY column is compared with.
    /// </summary>
    public static int Compare(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        // Signed on the high word, unsigned on the low. Comparing the low word signed would rank any
        // value with bit 63 set below one without it — 2^63 would sort under 1.
        int leftHigh = BinaryPrimitives.ReadInt32LittleEndian(left.Slice(8));
        int rightHigh = BinaryPrimitives.ReadInt32LittleEndian(right.Slice(8));
        if (leftHigh != rightHigh)
            return leftHigh < rightHigh ? -1 : 1;

        ulong leftLow = BinaryPrimitives.ReadUInt64LittleEndian(left);
        ulong rightLow = BinaryPrimitives.ReadUInt64LittleEndian(right);
        return leftLow == rightLow ? 0 : (leftLow < rightLow ? -1 : 1);
    }

    /// <summary>True when <paramref name="value"/> fits the 96-bit carrier.</summary>
    public static bool IsRepresentable(Int128 value) => value >= MinValue && value <= MaxValue;

    /// <summary>
    /// Narrows to <see cref="long"/> when the value fits. Out-of-range is the ordinary case for this
    /// type rather than a corrupt file — year 9999 in nanoseconds is exactly what the wider carrier
    /// exists for — so it is reported rather than wrapped.
    /// </summary>
    public static bool TryToInt64(Int128 value, out long result)
    {
        if (value < (Int128)long.MinValue || value > (Int128)long.MaxValue)
        {
            result = 0;
            return false;
        }

        result = (long)value;
        return true;
    }

    /// <summary>
    /// Divides toward negative infinity. Rescaling a unit is a floor, not a truncation: truncating
    /// toward zero would make a pre-epoch value round the opposite way from a post-epoch one, so the
    /// result would stop being monotonic in the value it came from. The INT96 path floors for the same
    /// reason.
    /// </summary>
    public static Int128 DivideFloor(Int128 value, long divisor)
    {
        Int128 quotient = value / (Int128)divisor;
        if (value % (Int128)divisor != Int128.Zero && (value < Int128.Zero) != (divisor < 0))
            quotient--;
        return quotient;
    }


    /// <summary>
    /// The <c>TimeUnit</c> this column declares if it is an extended-precision timestamp carrier, or
    /// <see langword="null"/> if it is anything else. TIMESTAMP is only a carrier at exactly
    /// <see cref="ByteWidth"/> bytes; the annotation on any other width is malformed and falls through
    /// to the physical type rather than being decoded.
    /// </summary>
    public static Metadata.TimeUnit? DeclaredUnit(ColumnDescriptor column) =>
        column.PhysicalType == PhysicalType.FixedLenByteArray
            && column.TypeLength == ByteWidth
            && column.SchemaElement.LogicalType is LogicalType.TimestampType timestamp
            ? timestamp.Unit
            : null;

    /// <summary>Units per second for a Parquet <c>TimeUnit</c>.</summary>
    public static long ScaleOf(Metadata.TimeUnit unit) => unit switch
    {
        Metadata.TimeUnit.Millis => 1_000L,
        Metadata.TimeUnit.Micros => 1_000_000L,
        _ => 1_000_000_000L,
    };

    /// <summary>Units per second for an Arrow <c>TimeUnit</c>.</summary>
    public static long ScaleOf(Apache.Arrow.Types.TimeUnit unit) => unit switch
    {
        Apache.Arrow.Types.TimeUnit.Second => 1L,
        Apache.Arrow.Types.TimeUnit.Millisecond => 1_000L,
        Apache.Arrow.Types.TimeUnit.Microsecond => 1_000_000L,
        _ => 1_000_000_000L,
    };

    /// <summary>
    /// Converts a count in <paramref name="fromScale"/> units per second into one in
    /// <paramref name="toScale"/>. Widening multiplies exactly; narrowing floors, for the reason
    /// <see cref="DivideFloor"/> describes.
    /// </summary>
    public static Int128 Rescale(Int128 value, long fromScale, long toScale)
    {
        if (fromScale == toScale)
            return value;

        return toScale > fromScale
            ? value * (Int128)(toScale / fromScale)
            : DivideFloor(value, fromScale / toScale);
    }


    /// <summary>
    /// Rewrites an Arrow timestamp column as this carrier: twelve little-endian two's-complement bytes
    /// per value, in the unit the column already carries. Nulls and row positions are preserved.
    /// </summary>
    /// <remarks>
    /// <para>No range check is needed in this direction. An Arrow timestamp is int64 and the carrier
    /// holds +/-2^95, so every value that exists in Arrow fits with room to spare -- which is also why
    /// the promotion can never be automatic: nothing Arrow can hold NEEDS it.</para>
    ///
    /// <para>Shared by both writers on purpose. The buffered writer is an independent implementation and
    /// has historically drifted from the streaming one; a carrier encoded two ways would drift silently,
    /// because both files would be well-formed.</para>
    /// </remarks>
    public static IArrowArray EncodeColumn(IArrowArray array)
    {
        var data = array.Data;
        var source = MemoryMarshal.Cast<byte, long>(
            data.Buffers[1].Span.Slice(data.Offset * sizeof(long), array.Length * sizeof(long)));

        var encoded = new byte[array.Length * ByteWidth];
        for (int i = 0; i < array.Length; i++)
        {
            Write((Int128)source[i], encoded.AsSpan(i * ByteWidth, ByteWidth));
        }

        // Offset 0: the slice above has already applied it, and carrying it forward would double-count.
        var newData = new ArrayData(
            new FixedSizeBinaryType(ByteWidth), array.Length, data.NullCount, offset: 0,
            [data.Buffers[0], new ArrowBuffer(encoded)]);
        return ArrowArrayFactory.BuildArray(newData);
    }

    /// <summary>Largest value the 96-bit carrier can hold (2^95 - 1).</summary>
    public static Int128 MaxValue => (Int128.One << 95) - Int128.One;

    /// <summary>Smallest value the 96-bit carrier can hold (-2^95).</summary>
    public static Int128 MinValue => -(Int128.One << 95);

    private static Int128 LowWordMask => (Int128)ulong.MaxValue;

    private static Int128 HighWordMask => (Int128)uint.MaxValue;
}
