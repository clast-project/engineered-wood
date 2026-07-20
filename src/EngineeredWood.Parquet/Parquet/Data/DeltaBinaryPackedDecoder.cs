// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
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
/// Bit-unpacking uses 8-byte aligned reads: each value is extracted with a single
/// <c>ulong</c> read + shift + mask, eliminating the per-byte loop of the naive decoder.
/// For bit widths &gt; 56 (rare), a two-read path handles values that span a 64-bit boundary.
/// </remarks>
internal ref struct DeltaBinaryPackedDecoder
{
    private readonly ReadOnlySpan<byte> _data;
    private int _pos;

    private readonly int _blockSize;
    private readonly int _miniblockCount;
    private readonly int _valuesPerMiniblock;
    private readonly int _totalValueCount;
    private long _lastValue;

    public DeltaBinaryPackedDecoder(ReadOnlySpan<byte> data)
    {
        _data = data;
        _pos = 0;

        _blockSize = checked((int)ReadUnsignedVarInt());
        _miniblockCount = checked((int)ReadUnsignedVarInt());
        _totalValueCount = checked((int)ReadUnsignedVarInt());
        _lastValue = ReadZigZagVarInt();
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

        int lastInt = (int)_lastValue;
        destination[0] = lastInt;
        int valuesDecoded = 1;

        Span<byte> bitWidths = _miniblockCount <= 64 ? stackalloc byte[_miniblockCount] : new byte[_miniblockCount];
        // Cap stack buffer at 128 longs (1 KB); fall back to a heap allocation otherwise.
        Span<long> unpacked = _valuesPerMiniblock <= 128 ? stackalloc long[_valuesPerMiniblock] : new long[_valuesPerMiniblock];

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
                    UnpackValues(unpacked, valuesToDecode, bitWidth);

                    for (int i = 0; i < valuesToDecode; i++)
                    {
                        lastInt += minDelta + (int)unpacked[i];
                        destination[valuesDecoded++] = lastInt;
                    }
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

        destination[0] = _lastValue;
        int valuesDecoded = 1;

        Span<byte> bitWidths = _miniblockCount <= 64 ? stackalloc byte[_miniblockCount] : new byte[_miniblockCount];
        // Cap stack buffer at 128 longs (1 KB); fall back to a heap allocation otherwise.
        Span<long> unpacked = _valuesPerMiniblock <= 128 ? stackalloc long[_valuesPerMiniblock] : new long[_valuesPerMiniblock];

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
                        _lastValue += minDelta;
                        destination[valuesDecoded++] = _lastValue;
                    }
                }
                else
                {
                    UnpackValues(unpacked, valuesToDecode, bitWidth);

                    for (int i = 0; i < valuesToDecode; i++)
                    {
                        _lastValue += minDelta + unpacked[i];
                        destination[valuesDecoded++] = _lastValue;
                    }
                }
            }
        }
    }

    /// <summary>
    /// Unpacks bit-packed unsigned values using fast 8-byte aligned reads.
    /// Each value is extracted with a single <c>ulong</c> read, right-shift, and mask —
    /// no per-byte loop. Advances <see cref="_pos"/> by the full miniblock byte size.
    /// </summary>
    private void UnpackValues(scoped Span<long> output, int count, int bitWidth)
    {
        ulong mask = bitWidth == 64 ? ulong.MaxValue : (1UL << bitWidth) - 1;
        int bitOffset = _pos * 8;
        ReadOnlySpan<byte> data = _data;

        if (bitWidth <= 56)
        {
            // Single ulong read always sufficient: worst case bitIndex=7 + 56 = 63 bits
            for (int i = 0; i < count; i++)
            {
                int byteIdx = bitOffset >> 3;
                int bitIdx = bitOffset & 7;
                ulong raw = ReadUInt64LE(data, byteIdx);
                output[i] = (long)((raw >> bitIdx) & mask);
                bitOffset += bitWidth;
            }
        }
        else
        {
            // Bit widths 57-64: value may span across two 8-byte reads
            for (int i = 0; i < count; i++)
            {
                int byteIdx = bitOffset >> 3;
                int bitIdx = bitOffset & 7;

                if (bitIdx == 0)
                {
                    output[i] = (long)(ReadUInt64LE(data, byteIdx) & mask);
                }
                else
                {
                    ulong lo = ReadUInt64LE(data, byteIdx);
                    ulong hi = ReadUInt64LE(data, byteIdx + 8);
                    output[i] = (long)(((lo >> bitIdx) | (hi << (64 - bitIdx))) & mask);
                }

                bitOffset += bitWidth;
            }
        }

        _pos += (_valuesPerMiniblock * bitWidth + 7) / 8;
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
