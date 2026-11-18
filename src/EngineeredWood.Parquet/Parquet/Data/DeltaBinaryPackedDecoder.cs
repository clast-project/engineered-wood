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
        int bitOffset = _pos * 8;
        int bytes = MiniblockBytes(bitWidth);
        if (FastPath(bitWidth, bytes))
        {
            ref byte source = ref MemoryMarshal.GetReference(_data);
            for (int i = 0; i < destination.Length; i++, bitOffset += bitWidth)
            {
                last += minDelta + (int)((ReadWord(ref source, bitOffset >> 3) >> (bitOffset & 7)) & mask);
                destination[i] = last;
            }
        }
        else
        {
            for (int i = 0; i < destination.Length; i++, bitOffset += bitWidth)
            {
                last += minDelta + (int)ReadValue(bitOffset, bitWidth, mask);
                destination[i] = last;
            }
        }

        _pos += bytes;
        return last;
    }

    /// <inheritdoc cref="UnpackSum(Span{int}, int, int, int)"/>
    private long UnpackSum(Span<long> destination, int bitWidth, long minDelta, long last)
    {
        ulong mask = Mask(bitWidth);
        int bitOffset = _pos * 8;
        int bytes = MiniblockBytes(bitWidth);
        if (FastPath(bitWidth, bytes))
        {
            ref byte source = ref MemoryMarshal.GetReference(_data);
            for (int i = 0; i < destination.Length; i++, bitOffset += bitWidth)
            {
                last += minDelta + (long)((ReadWord(ref source, bitOffset >> 3) >> (bitOffset & 7)) & mask);
                destination[i] = last;
            }
        }
        else if (bitWidth <= 64 && (long)_pos + bytes + 16 <= _data.Length)
        {
            // Over 56 bits a value may run into a second word. Shifting the second word left by
            // 64 - b in two steps makes it contribute nothing when b = 0, where one shift by 64 would
            // be a shift by 0.
            ref byte source = ref MemoryMarshal.GetReference(_data);
            for (int i = 0; i < destination.Length; i++, bitOffset += bitWidth)
            {
                int at = bitOffset >> 3, b = bitOffset & 7;
                ulong raw = (ReadWord(ref source, at) >> b) | ((ReadWord(ref source, at + 8) << 1) << (63 - b));
                last += minDelta + (long)(raw & mask);
                destination[i] = last;
            }
        }
        else
        {
            for (int i = 0; i < destination.Length; i++, bitOffset += bitWidth)
            {
                last += minDelta + (long)ReadValue(bitOffset, bitWidth, mask);
                destination[i] = last;
            }
        }

        _pos += bytes;
        return last;
    }

    private static ulong Mask(int bitWidth) => bitWidth == 64 ? ulong.MaxValue : (1UL << bitWidth) - 1;

    private readonly int MiniblockBytes(int bitWidth) => (_valuesPerMiniblock * bitWidth + 7) / 8;

    /// <summary>
    /// Whether every value of the miniblock at <see cref="_pos"/> is one 8-byte read that stays inside
    /// the data. A value of at most 56 bits fits one read from its first byte (7 + 56 = 63 bits). The
    /// last value of the miniblock starts within its <paramref name="bytes"/>, so its read ends before
    /// <c>_pos + bytes + 8</c>. Usually only the last miniblock of a page falls short of that.
    /// </summary>
    private readonly bool FastPath(int bitWidth, int bytes) =>
        bitWidth <= 56 && (long)_pos + bytes + 8 <= _data.Length;

    /// <summary>An unaligned little-endian read; the caller has checked it is in range.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ReadWord(ref byte source, int byteOffset)
    {
        ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref source, byteOffset));
        return BitConverter.IsLittleEndian ? value : BinaryPrimitives.ReverseEndianness(value);
    }

    /// <summary>
    /// One value at <paramref name="bitOffset"/>, read with bounds checks: bytes past the end of the
    /// data read as zeros, and a value over 56 bits may take two reads.
    /// </summary>
    private readonly ulong ReadValue(int bitOffset, int bitWidth, ulong mask)
    {
        int byteIdx = bitOffset >> 3;
        int bitIdx = bitOffset & 7;
        if (bitWidth <= 56 || bitIdx == 0)
            return (ReadUInt64LE(_data, byteIdx) >> bitIdx) & mask;

        ulong lo = ReadUInt64LE(_data, byteIdx);
        ulong hi = ReadUInt64LE(_data, byteIdx + 8);
        return ((lo >> bitIdx) | (hi << (64 - bitIdx))) & mask;
    }

    /// <summary>
    /// Reads 8 bytes as a little-endian <c>ulong</c>, padding with zeros if near end of buffer.
    /// </summary>
    private static ulong ReadUInt64LE(ReadOnlySpan<byte> data, int offset)
    {
        if (offset + 8 <= data.Length)
            return BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(offset));

        // Near end of buffer — read available bytes (only hit at tail of last miniblock)
        ulong value = 0;
        for (int i = offset; i < data.Length; i++)
            value |= (ulong)data[i] << ((i - offset) * 8);
        return value;
    }

    private long ReadUnsignedVarInt() => Varint.ReadUnsigned(_data, ref _pos);

    private long ReadZigZagVarInt() => Varint.ReadSigned(_data, ref _pos);
}
