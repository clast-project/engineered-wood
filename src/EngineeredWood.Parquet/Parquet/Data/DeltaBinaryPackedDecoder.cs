// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using EngineeredWood.Encodings;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes DELTA_BINARY_PACKED encoded values for INT32 and INT64 columns.
/// </summary>
/// <remarks>
/// Format (all varints are unsigned unless noted):
/// <list type="number">
/// <item>Block size in values (varint)</item>
/// <item>Number of miniblocks per block (varint)</item>
/// <item>Total value count (varint)</item>
/// <item>First value (zigzag varint)</item>
/// </list>
/// Then for each block:
/// <list type="number">
/// <item>Min delta (zigzag varint)</item>
/// <item>Bit widths: one byte per miniblock</item>
/// <item>Bit-packed deltas for each miniblock (values per miniblock = blockSize / miniblockCount)</item>
/// </list>
/// Each miniblock is unpacked straight into the destination, the running sum in the same loop. A value
/// is one unaligned <c>ulong</c> read + shift + mask; for bit widths &gt; 56 (rare), a value may span
/// two reads.
/// </remarks>
internal ref struct DeltaBinaryPackedDecoder
{
    private readonly ReadOnlySpan<byte> _data;
    private int _pos;

    private readonly int _blockSize;
    private readonly int _miniblockCount;
    private readonly int _valuesPerMiniblock;
    private readonly int _totalValueCount;
    private readonly long _firstValue;

    public DeltaBinaryPackedDecoder(ReadOnlySpan<byte> data)
    {
        _data = data;
        _pos = 0;

        _blockSize = checked((int)ReadUnsignedVarInt());
        _miniblockCount = checked((int)ReadUnsignedVarInt());
        _totalValueCount = checked((int)ReadUnsignedVarInt());
        _firstValue = ReadZigZagVarInt();
        if (_miniblockCount <= 0)
            throw new InvalidDataException("DELTA_BINARY_PACKED miniblock count must be positive.");
        _valuesPerMiniblock = _blockSize / _miniblockCount;
    }

    /// <summary>
    /// Number of bytes consumed from the input so far (valid after decoding).
    /// </summary>
    public readonly int BytesConsumed => _pos;

    /// <summary>
    /// Decodes all values as INT32 into <paramref name="destination"/>.
    /// Uses unchecked 32-bit arithmetic (deltas may wrap around).
    /// </summary>
    public void DecodeInt32s(Span<int> destination)
    {
        if (_totalValueCount == 0)
            return;

        int lastInt = (int)_firstValue;
        destination[0] = lastInt;
        int valuesDecoded = 1;

        Span<byte> bitWidths = _miniblockCount <= 64 ? stackalloc byte[_miniblockCount] : new byte[_miniblockCount];

        while (valuesDecoded < _totalValueCount)
        {
            int minDelta = (int)ReadZigZagVarInt();

            for (int i = 0; i < _miniblockCount; i++)
                bitWidths[i] = _data[_pos++];

            for (int mb = 0; mb < _miniblockCount && valuesDecoded < _totalValueCount; mb++)
            {
                int bitWidth = bitWidths[mb];
                int valuesToDecode = Math.Min(_valuesPerMiniblock, _totalValueCount - valuesDecoded);

                if (bitWidth == 0)
                {
                    for (int i = 0; i < valuesToDecode; i++)
                    {
                        lastInt += minDelta;
                        destination[valuesDecoded++] = lastInt;
                    }
                }
                else
                {
                    lastInt = UnpackSum(destination.Slice(valuesDecoded, valuesToDecode), bitWidth, minDelta, lastInt);
                    valuesDecoded += valuesToDecode;
                }
            }
        }
    }

    /// <summary>
    /// Decodes all values as INT64 into <paramref name="destination"/>.
    /// </summary>
    public void DecodeInt64s(Span<long> destination)
    {
        if (_totalValueCount == 0)
            return;

        long last = _firstValue;
        destination[0] = last;
        int valuesDecoded = 1;

        Span<byte> bitWidths = _miniblockCount <= 64 ? stackalloc byte[_miniblockCount] : new byte[_miniblockCount];

        while (valuesDecoded < _totalValueCount)
        {
            long minDelta = ReadZigZagVarInt();

            for (int i = 0; i < _miniblockCount; i++)
                bitWidths[i] = _data[_pos++];

            for (int mb = 0; mb < _miniblockCount && valuesDecoded < _totalValueCount; mb++)
            {
                int bitWidth = bitWidths[mb];
                int valuesToDecode = Math.Min(_valuesPerMiniblock, _totalValueCount - valuesDecoded);

                if (bitWidth == 0)
                {
                    for (int i = 0; i < valuesToDecode; i++)
                    {
                        last += minDelta;
                        destination[valuesDecoded++] = last;
                    }
                }
                else
                {
                    last = UnpackSum(destination.Slice(valuesDecoded, valuesToDecode), bitWidth, minDelta, last);
                    valuesDecoded += valuesToDecode;
                }
            }
        }
    }

    /// <summary>
    /// Unpacks one miniblock's deltas, adding each with <paramref name="minDelta"/> to the running value
    /// <paramref name="last"/> and storing the result: one value of <paramref name="destination"/> per
    /// delta. Returns the last value and advances <see cref="_pos"/> past the whole miniblock.
    /// </summary>
    private int UnpackSum(Span<int> destination, int bitWidth, int minDelta, int last)
    {
        ulong mask = Mask(bitWidth);
        long bytes = MiniblockBytes(bitWidth);
        if (FastPath(bitWidth, bytes, 8))
        {
            ref byte source = ref Unsafe.Add(ref MemoryMarshal.GetReference(_data), _pos);
            long bit = 0;
            for (int i = 0; i < destination.Length; i++, bit += bitWidth)
            {
                last += minDelta + (int)((ReadWord(ref source, (int)(bit >> 3)) >> (int)(bit & 7)) & mask);
                destination[i] = last;
            }
        }
        else
        {
            long bitOffset = (long)_pos * 8;
            for (int i = 0; i < destination.Length; i++, bitOffset += bitWidth)
            {
                last += minDelta + (int)ReadValue(bitOffset, bitWidth, mask);
                destination[i] = last;
            }
        }

        Advance(bytes);
        return last;
    }

    /// <inheritdoc cref="UnpackSum(Span{int}, int, int, int)"/>
    private long UnpackSum(Span<long> destination, int bitWidth, long minDelta, long last)
    {
        ulong mask = Mask(bitWidth);
        long bytes = MiniblockBytes(bitWidth);
        if (FastPath(bitWidth, bytes, 8))
        {
            ref byte source = ref Unsafe.Add(ref MemoryMarshal.GetReference(_data), _pos);
            long bit = 0;
            for (int i = 0; i < destination.Length; i++, bit += bitWidth)
            {
                last += minDelta + (long)((ReadWord(ref source, (int)(bit >> 3)) >> (int)(bit & 7)) & mask);
                destination[i] = last;
            }
        }
        else if (FastPath(bitWidth, bytes, 16))
        {
            // Over 56 bits a value may run into a second word. Shifting the second word left by
            // 64 - b in two steps makes it contribute nothing when b = 0, where one shift by 64 would
            // be a shift by 0.
            ref byte source = ref Unsafe.Add(ref MemoryMarshal.GetReference(_data), _pos);
            long bit = 0;
            for (int i = 0; i < destination.Length; i++, bit += bitWidth)
            {
                int at = (int)(bit >> 3), b = (int)(bit & 7);
                ulong raw = (ReadWord(ref source, at) >> b) | ((ReadWord(ref source, at + 8) << 1) << (63 - b));
                last += minDelta + (long)(raw & mask);
                destination[i] = last;
            }
        }
        else
        {
            long bitOffset = (long)_pos * 8;
            for (int i = 0; i < destination.Length; i++, bitOffset += bitWidth)
            {
                last += minDelta + (long)ReadValue(bitOffset, bitWidth, mask);
                destination[i] = last;
            }
        }

        Advance(bytes);
        return last;
    }

    private static ulong Mask(int bitWidth) => bitWidth == 64 ? ulong.MaxValue : (1UL << bitWidth) - 1;

    /// <summary>
    /// The bytes of a miniblock at <paramref name="bitWidth"/>. The values per miniblock come from the
    /// file, so the product is taken in 64 bits: in 32 it can wrap to a small number.
    /// </summary>
    private readonly long MiniblockBytes(int bitWidth) => ((long)_valuesPerMiniblock * bitWidth + 7) / 8;

    /// <summary>
    /// Whether every value of the miniblock at <see cref="_pos"/> can be read unchecked: its
    /// <paramref name="bytes"/> plus <paramref name="slack"/> lie inside the data. A value starts within
    /// the miniblock's bytes, and its read is one word (8 bytes) up to 56 bits, or two words (16) up to
    /// 64. Usually only the last miniblock of a page falls short of that.
    /// </summary>
    /// <remarks>
    /// The fast paths count bits from the start of the miniblock, in 64 bits: a bit offset in the
    /// miniblock is under 8 × <paramref name="bytes"/>, so its byte index fits an <see cref="int"/>,
    /// where a bit offset from the start of the data would wrap past 256 MiB.
    /// </remarks>
    private readonly bool FastPath(int bitWidth, long bytes, int slack) =>
        bitWidth <= (slack == 8 ? 56 : 64) && (long)_pos + bytes + slack <= _data.Length;

    /// <summary>
    /// Moves past a miniblock. A truncated last miniblock can leave <see cref="_pos"/> past the end of
    /// the data, as before; one that would put it past <see cref="int.MaxValue"/> is refused.
    /// </summary>
    private void Advance(long bytes)
    {
        long next = _pos + bytes;
        if (next > int.MaxValue)
            throw new InvalidDataException(
                $"DELTA_BINARY_PACKED miniblock of {_valuesPerMiniblock} values runs past the end of the data.");
        _pos = (int)next;
    }

    /// <summary>An unaligned little-endian read; the caller has checked it is in range.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ReadWord(ref byte source, int byteOffset)
    {
        ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref source, byteOffset));
        return BitConverter.IsLittleEndian ? value : BinaryPrimitives.ReverseEndianness(value);
    }

    /// <summary>
    /// One value at <paramref name="bitOffset"/> from the start of the data, read with bounds checks:
    /// bytes past the end of the data read as zeros, and a value over 56 bits may take two reads.
    /// </summary>
    private readonly ulong ReadValue(long bitOffset, int bitWidth, ulong mask)
    {
        long byteIdx = bitOffset >> 3;
        int bitIdx = (int)(bitOffset & 7);
        if (bitWidth <= 56 || bitIdx == 0)
            return (ReadUInt64LE(_data, byteIdx) >> bitIdx) & mask;

        ulong lo = ReadUInt64LE(_data, byteIdx);
        ulong hi = ReadUInt64LE(_data, byteIdx + 8);
        return ((lo >> bitIdx) | (hi << (64 - bitIdx))) & mask;
    }

    /// <summary>
    /// Reads 8 bytes as a little-endian <c>ulong</c>, padding with zeros past the end of the data.
    /// </summary>
    private static ulong ReadUInt64LE(ReadOnlySpan<byte> data, long offset)
    {
        if (offset + 8 <= data.Length)
            return BinaryPrimitives.ReadUInt64LittleEndian(data.Slice((int)offset));

        // Near or past the end of the data: read what there is (only the tail of a last miniblock).
        ulong value = 0;
        for (long i = offset; i < data.Length; i++)
            value |= (ulong)data[(int)i] << (int)((i - offset) * 8);
        return value;
    }

    private long ReadUnsignedVarInt() => Varint.ReadUnsigned(_data, ref _pos);

    private long ReadZigZagVarInt() => Varint.ReadSigned(_data, ref _pos);
}
