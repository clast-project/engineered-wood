// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes PFOR (Patched Frame of Reference) pages for INT32 and INT64 columns, in both the
/// plain and the delta mode.
/// </summary>
/// <remarks>
/// <para>The page layout is deliberately close to ALP's — a 7-byte header, an offset array whose
/// offsets are measured from its own start, then self-describing vectors — so this decoder is
/// shaped like <see cref="AlpDecoder"/> and the two should be read together. What PFOR adds is
/// the delta mode: a per-vector flag in the top bit of the width byte, a start value, and a
/// prefix sum at the end.</para>
/// <para>Specified in apache/parquet-format#617 (<c>PforEncoding.md</c>), which is a work in
/// progress. Implemented against it and cross-checked against apache/parquet-java#3775 and
/// apache/arrow-rs#10977.</para>
/// </remarks>
internal static class PforDecoder
{
    private const int PageHeaderSize = 7;

    /// <summary>frame_of_reference(4) + bit_width(1) + num_exceptions(2).</summary>
    private const int Int32VectorInfoSize = 7;

    /// <summary>frame_of_reference(8) + bit_width(1) + num_exceptions(2).</summary>
    private const int Int64VectorInfoSize = 11;

    private const int ExceptionPositionSize = 2;

    /// <summary>Bit 7 of the width byte: the vector packs differences, not values.</summary>
    private const int DeltaFlag = 0x80;

    /// <summary>
    /// Bits 0..6 of the width byte. Seven bits rather than six because the width's range is
    /// 0..64 inclusive and 64 does not fit in six — masking with six would read a full-width
    /// INT64 vector as width 0, which decodes without error as a constant vector.
    /// </summary>
    private const int BitWidthMask = 0x7F;

    /// <summary>
    /// Values unpacked per call into the scratch buffer. Matches <see cref="AlpDecoder"/>: the
    /// residuals are unpacked in bulk and then reduced, rather than extracted one at a time.
    /// </summary>
    private const int UnpackTile = 1024;

    /// <summary>
    /// Above this width a value can straddle two 64-bit words, which the group-of-eight kernel
    /// does not handle. Wider vectors fall back to the general per-value path, which is the
    /// right trade: a PFOR vector needing 58+ bits per residual is one where the encoding is
    /// not paying, and our own writer emits PLAIN for those pages instead.
    /// </summary>
    private const int MaxKernelBitWidth = 57;

    [ThreadStatic]
    private static ulong[]? t_unpackScratch;

    /// <summary>Decodes a PFOR page of INT32 values.</summary>
    public static void DecodeInt32s(ReadOnlySpan<byte> data, Span<int> destination, int count)
    {
        var (vectorSize, numVectors) = ReadPageHeader(data, sizeof(int), count);
        ulong[] scratch = t_unpackScratch ??= new ulong[UnpackTile];

        int produced = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int n = Math.Min(vectorSize, count - produced);
            DecodeInt32Vector(SliceVector(data, numVectors, v), n,
                destination.Slice(produced, n), scratch);
            produced += n;
        }
    }

    /// <summary>Decodes a PFOR page of INT64 values.</summary>
    public static void DecodeInt64s(ReadOnlySpan<byte> data, Span<long> destination, int count)
    {
        var (vectorSize, numVectors) = ReadPageHeader(data, sizeof(long), count);
        ulong[] scratch = t_unpackScratch ??= new ulong[UnpackTile];

        int produced = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int n = Math.Min(vectorSize, count - produced);
            DecodeInt64Vector(SliceVector(data, numVectors, v), n,
                destination.Slice(produced, n), scratch);
            produced += n;
        }
    }

    // ───── Page framing ─────

    private static (int VectorSize, int NumVectors) ReadPageHeader(
        ReadOnlySpan<byte> data, int expectedByteWidth, int count)
    {
        if (data.Length < PageHeaderSize)
            throw new ParquetFormatException(
                $"PFOR page is too small to contain a header ({data.Length} bytes).");

        byte packingMode = data[0];
        if (packingMode != 0)
            throw new ParquetFormatException(
                $"Unsupported PFOR packing_mode {packingMode} (only 0 = FOR + bit-packing is supported).");

        byte logVectorSize = data[1];
        if (logVectorSize < 3 || logVectorSize > 15)
            throw new ParquetFormatException(
                $"PFOR log_vector_size {logVectorSize} is out of the allowed range [3, 15].");

        // The column type already says how wide the values are. Checking the header against it
        // anyway is what the field is for: it makes the page self-describing, so a page written
        // for the other width is rejected here rather than misread as a truncated one.
        byte valueByteWidth = data[2];
        if (valueByteWidth != expectedByteWidth)
            throw new ParquetFormatException(
                $"PFOR value_byte_width {valueByteWidth} does not match the column's {expectedByteWidth}-byte type.");

        int numElements = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(3, 4));
        if (numElements < 0)
            throw new ParquetFormatException($"PFOR num_elements {numElements} is negative.");
        if (numElements != count)
            throw new ParquetFormatException(
                $"PFOR page header num_elements ({numElements}) does not match expected count ({count}).");

        int vectorSize = 1 << logVectorSize;
        int numVectors = (numElements + vectorSize - 1) / vectorSize;

        if (data.Length < PageHeaderSize + (long)numVectors * 4)
            throw new ParquetFormatException(
                "PFOR page is truncated before the end of its offset array.");

        return (vectorSize, numVectors);
    }

    /// <summary>
    /// Slices vector <paramref name="index"/> out of the page. Offsets are measured from the
    /// start of the offset array, not from the start of the page.
    /// </summary>
    private static ReadOnlySpan<byte> SliceVector(ReadOnlySpan<byte> data, int numVectors, int index)
    {
        var body = data.Slice(PageHeaderSize);
        uint offset = BinaryPrimitives.ReadUInt32LittleEndian(body.Slice(index * 4, 4));

        // The first vector begins just past the offset array, and every vector holds at least a
        // header, so an offset inside the array or at the very end of the page is malformed.
        uint offsetArrayBytes = (uint)numVectors * 4;
        if (offset < offsetArrayBytes || offset >= (uint)body.Length)
            throw new ParquetFormatException(
                $"PFOR vector {index} has offset {offset}, which is outside the page body " +
                $"({offsetArrayBytes}..{body.Length}).");

        return body.Slice((int)offset);
    }

    // ───── Vector decoding ─────

    private static void DecodeInt32Vector(
        ReadOnlySpan<byte> vector, int n, Span<int> destination, Span<ulong> scratch)
    {
        if (vector.Length < Int32VectorInfoSize)
            throw new ParquetFormatException("PFOR INT32 vector is too small to contain its header.");

        int frameOfReference = BinaryPrimitives.ReadInt32LittleEndian(vector);
        byte widthByte = vector[4];
        bool delta = (widthByte & DeltaFlag) != 0;
        int bitWidth = widthByte & BitWidthMask;
        if (bitWidth > 32)
            throw new ParquetFormatException($"PFOR INT32 bit_width {bitWidth} exceeds 32.");
        int numExceptions = BinaryPrimitives.ReadUInt16LittleEndian(vector.Slice(5, 2));

        int headerSize = Int32VectorInfoSize;
        int startValue = 0;
        if (delta)
        {
            // Its own bound check, before the one below. The header check above was satisfied
            // while the delta flag was still unknown, so a reader that validates only the header
            // and then the residual section reads the start value from past the end of the page
            // and then checks the residual bound from an offset that has already moved past it.
            if (vector.Length < headerSize + sizeof(int))
                throw new ParquetFormatException(
                    "PFOR INT32 delta vector is truncated before its start value.");
            startValue = BinaryPrimitives.ReadInt32LittleEndian(vector.Slice(headerSize, sizeof(int)));
            headerSize += sizeof(int);
        }

        int packedSize = PackedByteLength(n, bitWidth);
        if (vector.Length < (long)headerSize + packedSize +
            (long)numExceptions * (ExceptionPositionSize + sizeof(int)))
            throw new ParquetFormatException("PFOR INT32 vector is truncated.");

        uint frame = unchecked((uint)frameOfReference);

        if (bitWidth == 0)
        {
            // No bytes are stored: every residual is zero, so every value is the frame.
            destination.Fill(frameOfReference);
        }
        else
        {
            var packed = vector.Slice(headerSize, packedSize);
            int produced = 0;
            while (produced < n)
            {
                int tile = Math.Min(UnpackTile, n - produced);
                int unpacked = Unpack(packed.Slice((produced * bitWidth) >> 3), bitWidth, tile, scratch);
                if (unpacked == 0)
                {
                    // Widths the kernel declines, and the tail of any vector whose length is not
                    // a multiple of eight. Indexes are absolute, so this reads from `packed`.
                    for (int i = produced; i < n; i++)
                        destination[i] = unchecked((int)((uint)ExtractBits(packed, i, bitWidth) + frame));
                    break;
                }

                for (int i = 0; i < unpacked; i++)
                    destination[produced + i] = unchecked((int)((uint)scratch[i] + frame));
                produced += unpacked;
            }
        }

        PatchInt32Exceptions(vector, n, destination, headerSize + packedSize, numExceptions);

        if (delta)
            PrefixSum(destination, startValue);
    }

    private static void DecodeInt64Vector(
        ReadOnlySpan<byte> vector, int n, Span<long> destination, Span<ulong> scratch)
    {
        if (vector.Length < Int64VectorInfoSize)
            throw new ParquetFormatException("PFOR INT64 vector is too small to contain its header.");

        long frameOfReference = BinaryPrimitives.ReadInt64LittleEndian(vector);
        byte widthByte = vector[8];
        bool delta = (widthByte & DeltaFlag) != 0;
        int bitWidth = widthByte & BitWidthMask;
        if (bitWidth > 64)
            throw new ParquetFormatException($"PFOR INT64 bit_width {bitWidth} exceeds 64.");
        int numExceptions = BinaryPrimitives.ReadUInt16LittleEndian(vector.Slice(9, 2));

        int headerSize = Int64VectorInfoSize;
        long startValue = 0;
        if (delta)
        {
            // See DecodeInt32Vector: the start value needs its own bound check.
            if (vector.Length < headerSize + sizeof(long))
                throw new ParquetFormatException(
                    "PFOR INT64 delta vector is truncated before its start value.");
            startValue = BinaryPrimitives.ReadInt64LittleEndian(vector.Slice(headerSize, sizeof(long)));
            headerSize += sizeof(long);
        }

        int packedSize = PackedByteLength(n, bitWidth);
        if (vector.Length < (long)headerSize + packedSize +
            (long)numExceptions * (ExceptionPositionSize + sizeof(long)))
            throw new ParquetFormatException("PFOR INT64 vector is truncated.");

        ulong frame = unchecked((ulong)frameOfReference);

        if (bitWidth == 0)
        {
            destination.Fill(frameOfReference);
        }
        else
        {
            var packed = vector.Slice(headerSize, packedSize);
            int produced = 0;
            while (produced < n)
            {
                int tile = Math.Min(UnpackTile, n - produced);
                int unpacked = Unpack(packed.Slice((produced * bitWidth) >> 3), bitWidth, tile, scratch);
                if (unpacked == 0)
                {
                    for (int i = produced; i < n; i++)
                        destination[i] = unchecked((long)(ExtractBits(packed, i, bitWidth) + frame));
                    break;
                }

                for (int i = 0; i < unpacked; i++)
                    destination[produced + i] = unchecked((long)(scratch[i] + frame));
                produced += unpacked;
            }
        }

        PatchInt64Exceptions(vector, n, destination, headerSize + packedSize, numExceptions);

        if (delta)
            PrefixSum(destination, startValue);
    }

    // ───── Exceptions ─────

    /// <summary>
    /// Overwrites the exception slots with their stored values. These are never residuals: each
    /// is the value the packed stream would have carried had it fitted — the original value in a
    /// plain vector, the difference in a delta one — so the frame is not added back to them.
    /// </summary>
    private static void PatchInt32Exceptions(
        ReadOnlySpan<byte> vector, int n, Span<int> destination, int positionsAt, int numExceptions)
    {
        int valuesAt = positionsAt + numExceptions * ExceptionPositionSize;
        for (int j = 0; j < numExceptions; j++)
        {
            int position = BinaryPrimitives.ReadUInt16LittleEndian(
                vector.Slice(positionsAt + j * ExceptionPositionSize, ExceptionPositionSize));
            if (position >= n)
                throw new ParquetFormatException(
                    $"PFOR exception position {position} is outside its vector of {n} elements.");

            destination[position] = BinaryPrimitives.ReadInt32LittleEndian(
                vector.Slice(valuesAt + j * sizeof(int), sizeof(int)));
        }
    }

    /// <inheritdoc cref="PatchInt32Exceptions"/>
    private static void PatchInt64Exceptions(
        ReadOnlySpan<byte> vector, int n, Span<long> destination, int positionsAt, int numExceptions)
    {
        int valuesAt = positionsAt + numExceptions * ExceptionPositionSize;
        for (int j = 0; j < numExceptions; j++)
        {
            int position = BinaryPrimitives.ReadUInt16LittleEndian(
                vector.Slice(positionsAt + j * ExceptionPositionSize, ExceptionPositionSize));
            if (position >= n)
                throw new ParquetFormatException(
                    $"PFOR exception position {position} is outside its vector of {n} elements.");

            destination[position] = BinaryPrimitives.ReadInt64LittleEndian(
                vector.Slice(valuesAt + j * sizeof(long), sizeof(long)));
        }
    }

    // ───── Prefix sum ─────

    /// <summary>
    /// Reverses the differencing, in place, starting from the vector's start value.
    /// </summary>
    /// <remarks>
    /// <para>This runs after the exceptions are patched, not before. An exception in a delta
    /// vector is a difference like any other, and summing first would carry its zero placeholder
    /// into every value that follows it.</para>
    /// <para>The sum runs on the unsigned bit patterns, matching how the writer took the
    /// differences. A column that spans the type's range overflows, and computing both
    /// directions modularly is what makes the bits round-trip exactly.</para>
    /// </remarks>
    private static void PrefixSum(Span<int> values, int startValue)
    {
        uint accumulator = unchecked((uint)startValue);
        for (int i = 0; i < values.Length; i++)
        {
            accumulator = unchecked(accumulator + (uint)values[i]);
            values[i] = unchecked((int)accumulator);
        }
    }

    /// <inheritdoc cref="PrefixSum(Span{int}, int)"/>
    private static void PrefixSum(Span<long> values, long startValue)
    {
        ulong accumulator = unchecked((ulong)startValue);
        for (int i = 0; i < values.Length; i++)
        {
            accumulator = unchecked(accumulator + (ulong)values[i]);
            values[i] = unchecked((long)accumulator);
        }
    }

    // ───── Bit unpacking ─────

    private static int PackedByteLength(int count, int bitWidth) =>
        (int)(((long)count * bitWidth + 7) / 8);

    /// <summary>
    /// Unpacks whole groups of eight residuals, returning how many it produced. Returns 0 when
    /// it cannot help — a width past <see cref="MaxKernelBitWidth"/>, or too few bytes left —
    /// and the caller finishes with <see cref="ExtractBits"/>.
    /// </summary>
    /// <remarks>
    /// The per-value offsets and shifts are hoisted out of the loop, which is what makes this
    /// worth having over a bit cursor; see the same kernel in <see cref="AlpDecoder"/>, where it
    /// was measured. PFOR keeps its own copy rather than sharing ALP's because ALP's is tuned
    /// against ALP's benchmarks and its width ceiling is a property of ALP's data, not of a
    /// bit-packed stream.
    /// </remarks>
#if NET8_0_OR_GREATER
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
#endif
    private static int Unpack(
        ReadOnlySpan<byte> packed, int bitWidth, int count, Span<ulong> destination)
    {
        if (bitWidth > MaxKernelBitWidth)
            return 0;

        int o1 = bitWidth >> 3, o2 = (2 * bitWidth) >> 3, o3 = (3 * bitWidth) >> 3;
        int o4 = (4 * bitWidth) >> 3, o5 = (5 * bitWidth) >> 3;
        int o6 = (6 * bitWidth) >> 3, o7 = (7 * bitWidth) >> 3;

        // Every group reads a whole word at o7, which for narrow widths reaches past the group's
        // own bytes. Groups whose read would leave the buffer are left to the caller.
        if (packed.Length < o7 + 8)
            return 0;

        int groups = Math.Min(count / 8, (packed.Length - o7 - 8) / bitWidth + 1);
        if (groups <= 0)
            return 0;

        int s1 = bitWidth & 7, s2 = (2 * bitWidth) & 7, s3 = (3 * bitWidth) & 7;
        int s4 = (4 * bitWidth) & 7, s5 = (5 * bitWidth) & 7;
        int s6 = (6 * bitWidth) & 7, s7 = (7 * bitWidth) & 7;
        ulong mask = Mask(bitWidth);

        ref byte source = ref MemoryMarshal.GetReference(packed);

        for (int g = 0; g < groups; g++)
        {
            // A group of eight values at `bitWidth` bits each occupies exactly `bitWidth` bytes,
            // so the group's byte offset is g * bitWidth and o1..o7 are offsets within it.
            int b = g * bitWidth;
            int o = g * 8;

            destination[o] = ReadWord(ref source, b) & mask;
            destination[o + 1] = (ReadWord(ref source, b + o1) >> s1) & mask;
            destination[o + 2] = (ReadWord(ref source, b + o2) >> s2) & mask;
            destination[o + 3] = (ReadWord(ref source, b + o3) >> s3) & mask;
            destination[o + 4] = (ReadWord(ref source, b + o4) >> s4) & mask;
            destination[o + 5] = (ReadWord(ref source, b + o5) >> s5) & mask;
            destination[o + 6] = (ReadWord(ref source, b + o6) >> s6) & mask;
            destination[o + 7] = (ReadWord(ref source, b + o7) >> s7) & mask;
        }

        return groups * 8;
    }

    /// <summary>
    /// Extracts the residual at value index <paramref name="index"/> from an LSB-first
    /// bit-packed stream, for any width in [1, 64].
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ExtractBits(ReadOnlySpan<byte> packed, int index, int bitWidth)
    {
        long bitPosition = (long)index * bitWidth;
        int byteIndex = (int)(bitPosition >> 3);
        int shift = (int)(bitPosition & 7);

        ulong value = ReadUInt64Padded(packed, byteIndex) >> shift;
        if (shift + bitWidth > 64)
        {
            // Only reachable for widths of 58 and up, and never when `shift` is 0 — which is
            // what makes the shift below safe. C# masks a 64-bit shift count to 6 bits, so
            // `<< (64 - 0)` would be a shift by 0 and would OR the whole high word in unshifted.
            value |= ReadUInt64Padded(packed, byteIndex + 8) << (64 - shift);
        }

        return value & Mask(bitWidth);
    }

    /// <summary>
    /// The low <paramref name="bitWidth"/> bits set.
    /// </summary>
    /// <remarks>
    /// Width 64 is special-cased rather than computed. C# masks a 64-bit shift count to 6 bits,
    /// so <c>1UL &lt;&lt; 64</c> is 1, not 0, and the usual <c>(1UL &lt;&lt; w) - 1</c> would
    /// produce a mask of 0 at exactly the width that needs every bit — reading a full-width
    /// vector back as all zeroes, with no error to say so.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Mask(int bitWidth) =>
        bitWidth == 64 ? ulong.MaxValue : (1UL << bitWidth) - 1UL;

    /// <summary>
    /// Reads a little-endian 64-bit word at a byte offset, aligned or not. Unchecked: the
    /// <c>groups</c> bound in <see cref="Unpack"/> is what keeps every read inside the span.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ReadWord(ref byte source, int byteOffset)
    {
        ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref source, byteOffset));
        return BitConverter.IsLittleEndian ? value : BinaryPrimitives.ReverseEndianness(value);
    }

    /// <summary>
    /// Reads a little-endian 64-bit word, zero-filling any bytes past the end of the span. The
    /// last values of a vector live in fewer than eight bytes, and the packed section is sized
    /// to the exact packed length.
    /// </summary>
    private static ulong ReadUInt64Padded(ReadOnlySpan<byte> packed, int byteIndex)
    {
        if (byteIndex >= packed.Length)
            return 0;

        int available = packed.Length - byteIndex;
        if (available >= 8)
            return BinaryPrimitives.ReadUInt64LittleEndian(packed.Slice(byteIndex, 8));

        ulong value = 0;
        for (int i = 0; i < available; i++)
            value |= (ulong)packed[byteIndex + i] << (i * 8);
        return value;
    }
}
