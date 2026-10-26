// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Encodings;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes INT32 or INT64 values using the DELTA_BINARY_PACKED encoding.
/// Mirror of <see cref="DeltaBinaryPackedDecoder"/>.
/// </summary>
/// <remarks>
/// Format:
/// <list type="number">
/// <item>Block size in values (unsigned varint)</item>
/// <item>Number of miniblocks per block (unsigned varint)</item>
/// <item>Total value count (unsigned varint)</item>
/// <item>First value (zigzag varint)</item>
/// </list>
/// Then for each block:
/// <list type="number">
/// <item>Min delta (zigzag varint)</item>
/// <item>Bit widths: one byte per miniblock</item>
/// <item>Bit-packed (delta - minDelta) values for each miniblock</item>
/// </list>
/// </remarks>
internal sealed class DeltaBinaryPackedEncoder
{
    private const int BlockSize = 128;
    private const int MiniblockCount = 4;
    private const int ValuesPerMiniblock = BlockSize / MiniblockCount; // 32

    private byte[] _buffer;
    private int _position;

    public DeltaBinaryPackedEncoder(int initialCapacity = 1024)
    {
        _buffer = new byte[initialCapacity];
    }

    /// <summary>
    /// Creates an encoder that writes into <paramref name="buffer"/> from its start. The buffer is
    /// replaced only if it fills; size it with <see cref="MaxEncodedLength"/> and it never is.
    /// </summary>
    public DeltaBinaryPackedEncoder(byte[] buffer)
    {
        _buffer = buffer;
    }

    /// <summary>
    /// The most bytes <paramref name="count"/> values can encode to, when every delta needs
    /// <paramref name="bitWidth"/> bits (32 for INT32, whose deltas wrap; 64 for INT64).
    /// </summary>
    public static int MaxEncodedLength(int count, int bitWidth)
    {
        // Header: four varints, at most 5 + 5 + 5 + 10 bytes. Each block: a min-delta varint of at
        // most 10 bytes, a bit width per miniblock, and its 128 values at the full width.
        const int headerBytes = 25;
        long blocks = count <= 1 ? 0 : (count - 1 + BlockSize - 1) / BlockSize;
        long blockBytes = 10 + MiniblockCount + BlockSize * bitWidth / 8;
        return checked((int)(headerBytes + blocks * blockBytes));
    }

    /// <summary>
    /// The buffer written into: the one passed to the constructor unless it had to grow.
    /// </summary>
    public byte[] Buffer => _buffer;

    /// <summary>Number of bytes written.</summary>
    public int Length => _position;

    /// <summary>Returns the written bytes as a span (no copy).</summary>
    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _position);

    /// <summary>Returns the written bytes as a new array.</summary>
    public byte[] ToArray() => _buffer.AsSpan(0, _position).ToArray();

    /// <summary>Resets the encoder for reuse.</summary>
    public void Reset() => _position = 0;

    /// <summary>
    /// Encodes INT32 values. Deltas are computed with wrapping arithmetic
    /// to match the decoder's unchecked 32-bit accumulation.
    /// </summary>
    public void EncodeInt32s(ReadOnlySpan<int> values) => EncodeInt32s(values, default, values.Length);

    /// <summary>
    /// Encodes the INT32 values among <paramref name="slots"/> whose definition level is not 0, of
    /// which there are <paramref name="count"/>. Empty <paramref name="definitionLevels"/> means every
    /// slot holds a value. Reading past the nulls here spares a page the copy that compacting it
    /// first would take.
    /// </summary>
    public void EncodeInt32s(ReadOnlySpan<int> slots, ReadOnlySpan<int> definitionLevels, int count)
    {
        if (count == 0)
        {
            WriteHeader(0, 0);
            return;
        }

        bool dense = definitionLevels.IsEmpty;
        int slot = dense ? 0 : NextDefined(definitionLevels, 0);
        int previous = slots[slot];
        WriteHeader(count, previous);

        // Compute deltas with wrapping 32-bit subtraction (matches decoder's unchecked accumulation).
        // Using 64-bit subtraction would produce deltas > 32 bits for values spanning the full
        // INT32 range, which violates the spec constraint that bit widths ≤ physical type width.
        // They are computed a block at a time, so a page allocates no array of them.
        Span<long> deltas = stackalloc long[BlockSize];
        for (int remaining = count - 1; remaining > 0;)
        {
            int blockValues = Math.Min(BlockSize, remaining);
            for (int i = 0; i < blockValues; i++)
            {
                slot = dense ? slot + 1 : NextDefined(definitionLevels, slot + 1);
                int value = slots[slot];
                deltas[i] = unchecked(value - previous);
                previous = value;
            }

            EncodeBlock(deltas.Slice(0, blockValues));
            remaining -= blockValues;
        }
    }

    /// <summary>
    /// Encodes INT64 values. Deltas are computed with wrapping arithmetic.
    /// </summary>
    public void EncodeInt64s(ReadOnlySpan<long> values) => EncodeInt64s(values, default, values.Length);

    /// <inheritdoc cref="EncodeInt32s(ReadOnlySpan{int}, ReadOnlySpan{int}, int)"/>
    public void EncodeInt64s(ReadOnlySpan<long> slots, ReadOnlySpan<int> definitionLevels, int count)
    {
        if (count == 0)
        {
            WriteHeader(0, 0);
            return;
        }

        bool dense = definitionLevels.IsEmpty;
        int slot = dense ? 0 : NextDefined(definitionLevels, 0);
        long previous = slots[slot];
        WriteHeader(count, previous);

        Span<long> deltas = stackalloc long[BlockSize];
        for (int remaining = count - 1; remaining > 0;)
        {
            int blockValues = Math.Min(BlockSize, remaining);
            for (int i = 0; i < blockValues; i++)
            {
                slot = dense ? slot + 1 : NextDefined(definitionLevels, slot + 1);
                long value = slots[slot];
                deltas[i] = unchecked(value - previous); // wrapping
                previous = value;
            }

            EncodeBlock(deltas.Slice(0, blockValues));
            remaining -= blockValues;
        }
    }

    private static int NextDefined(ReadOnlySpan<int> definitionLevels, int slot)
    {
        while (definitionLevels[slot] == 0)
            slot++;
        return slot;
    }

    private void WriteHeader(int totalCount, long firstValue)
    {
        WriteUnsignedVarInt(BlockSize);
        WriteUnsignedVarInt(MiniblockCount);
        WriteUnsignedVarInt(totalCount);
        WriteZigZagVarInt(firstValue);
    }

    /// <summary>Writes one block: up to <see cref="BlockSize"/> deltas.</summary>
    private void EncodeBlock(ReadOnlySpan<long> deltas)
    {
        int blockValues = deltas.Length;
        Span<byte> bitWidths = stackalloc byte[MiniblockCount];

        // Find min delta for this block
        long minDelta = long.MaxValue;
        for (int i = 0; i < blockValues; i++)
            minDelta = Math.Min(minDelta, deltas[i]);

        WriteZigZagVarInt(minDelta);

        // Compute bit widths for each miniblock
        for (int mb = 0; mb < MiniblockCount; mb++)
        {
            int mbStart = mb * ValuesPerMiniblock;
            int mbEnd = Math.Min(mbStart + ValuesPerMiniblock, blockValues);

            if (mbStart >= blockValues)
            {
                bitWidths[mb] = 0;
                continue;
            }

            ulong maxRemainder = 0;
            for (int i = mbStart; i < mbEnd; i++)
            {
                ulong remainder = unchecked((ulong)(deltas[i] - minDelta));
                if (remainder > maxRemainder) maxRemainder = remainder;
            }

#if NET8_0_OR_GREATER
            bitWidths[mb] = maxRemainder == 0 ? (byte)0 : (byte)(64 - ulong.LeadingZeroCount(maxRemainder));
#else
            bitWidths[mb] = maxRemainder == 0 ? (byte)0 : (byte)(64 - BitPolyfills.LeadingZeroCount(maxRemainder));
#endif
        }

        // Write bit widths
        EnsureCapacity(MiniblockCount);
        for (int mb = 0; mb < MiniblockCount; mb++)
            _buffer[_position++] = bitWidths[mb];

        // Bit-pack values for each miniblock
        for (int mb = 0; mb < MiniblockCount; mb++)
        {
            int bw = bitWidths[mb];
            if (bw == 0)
                continue;

            int mbStart = mb * ValuesPerMiniblock;
            int bytesNeeded = (ValuesPerMiniblock * bw + 7) / 8;
            EnsureCapacity(bytesNeeded);
            _buffer.AsSpan(_position, bytesNeeded).Clear();

            int bitPos = 0;
            for (int i = 0; i < ValuesPerMiniblock; i++)
            {
                ulong val = (mbStart + i < blockValues)
                    ? unchecked((ulong)(deltas[mbStart + i] - minDelta))
                    : 0UL; // padding for partial miniblock

                if (val != 0)
                {
                    int byteOffset = bitPos / 8;
                    int bitOffset = bitPos % 8;

                    int bytesToWrite = (bitOffset + bw + 7) / 8;
                    // First byte: shift left (only low byte matters)
                    _buffer[_position + byteOffset] |= (byte)(val << bitOffset);
                    // Remaining bytes: shift right
                    for (int b = 1; b < bytesToWrite; b++)
                        _buffer[_position + byteOffset + b] |= (byte)(val >> (b * 8 - bitOffset));
                }
                bitPos += bw;
            }

            _position += bytesNeeded;
        }
    }

    private void WriteUnsignedVarInt(long value)
    {
        EnsureCapacity(10);
        _position += Varint.WriteUnsigned(_buffer.AsSpan(_position), unchecked((ulong)value));
    }

    private void WriteZigZagVarInt(long value)
    {
        EnsureCapacity(10);
        _position += Varint.WriteSigned(_buffer.AsSpan(_position), value);
    }

    private void EnsureCapacity(int additionalBytes)
    {
        int required = _position + additionalBytes;
        if (required <= _buffer.Length)
            return;

        int newSize = Math.Max(_buffer.Length * 2, required);
        var newBuffer = new byte[newSize];
        _buffer.AsSpan(0, _position).CopyTo(newBuffer);
        _buffer = newBuffer;
    }
}
