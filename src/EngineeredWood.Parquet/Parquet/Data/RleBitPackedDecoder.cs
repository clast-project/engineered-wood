// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using EngineeredWood.Encodings;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes RLE/Bit-Packing Hybrid encoded data as defined in the Parquet specification.
/// </summary>
/// <remarks>
/// The encoding uses a header byte (varint) whose LSB determines the mode:
/// - LSB = 0: RLE run. Header >> 1 = repeat count. Followed by a value of <c>bitWidth</c> bits (ceil-byte-aligned).
/// - LSB = 1: Bit-packed group. Header >> 1 = group count (each group is 8 values). Followed by bitWidth * 8 bits per group.
/// </remarks>
internal ref struct RleBitPackedDecoder
{
    private readonly ReadOnlySpan<byte> _data;
    private int _position;
    private readonly int _bitWidth;

    // Current run state
    private bool _isRle;
    private int _remaining; // values remaining in current run/group
    private int _rleValue;

    // Bit-packed state
    private int _bitPackedPos;  // byte position in data where current bit-packed group started
    private int _bitOffset;     // bit offset within bit-packed bytes

    public RleBitPackedDecoder(ReadOnlySpan<byte> data, int bitWidth)
    {
        _data = data;
        _position = 0;
        _bitWidth = bitWidth;
        _isRle = false;
        _remaining = 0;
        _rleValue = 0;
        _bitPackedPos = 0;
        _bitOffset = 0;
    }

    /// <summary>Current byte position in the data.</summary>
    public int Position => _position;

    /// <summary>
    /// Reads the next value from the RLE/Bit-Packing Hybrid stream.
    /// </summary>
    public int ReadNext()
    {
        if (_remaining == 0)
            ReadNextGroup();

        _remaining--;

        if (_isRle)
            return _rleValue;

        return ReadBitPackedValue();
    }

    /// <summary>
    /// Reads <paramref name="count"/> values into the destination span.
    /// </summary>
    public void ReadBatch(Span<int> destination)
    {
        int offset = 0;
        while (offset < destination.Length)
        {
            if (_remaining == 0)
                ReadNextGroup();

            int toCopy = Math.Min(_remaining, destination.Length - offset);

            if (_isRle)
            {
                destination.Slice(offset, toCopy).Fill(_rleValue);
                _remaining -= toCopy;
                offset += toCopy;
            }
            else
            {
                // Specialise for bitWidth == 1 (def levels with maxDefLevel == 1):
                // unpack 8 values per byte instead of 1 value per 4-byte read.
                if (_bitWidth == 1 && (_bitOffset & 7) == 0)
                {
                    int byteStart = _bitPackedPos + (_bitOffset >> 3);
                    while (toCopy >= 8)
                    {
                        byte packed = _data[byteStart++];
                        destination[offset]     =  packed        & 1;
                        destination[offset + 1] = (packed >> 1) & 1;
                        destination[offset + 2] = (packed >> 2) & 1;
                        destination[offset + 3] = (packed >> 3) & 1;
                        destination[offset + 4] = (packed >> 4) & 1;
                        destination[offset + 5] = (packed >> 5) & 1;
                        destination[offset + 6] = (packed >> 6) & 1;
                        destination[offset + 7] = (packed >> 7) & 1;
                        offset += 8;
                        _bitOffset += 8;
                        _remaining -= 8;
                        toCopy -= 8;
                    }
                }

                // General path for remaining values or other bit widths
                int mask = (1 << _bitWidth) - 1;
                for (int i = 0; i < toCopy; i++)
                {
                    int byteIdx = _bitPackedPos + (_bitOffset >> 3);
                    int bitIdx = _bitOffset & 7;
                    _bitOffset += _bitWidth;

                    int rem = _data.Length - byteIdx;
                    uint raw = rem >= 4
                        ? BinaryPrimitives.ReadUInt32LittleEndian(_data.Slice(byteIdx))
                        : AssemblePartial(byteIdx, rem);
                    destination[offset++] = (int)((raw >> bitIdx) & (uint)mask);
                    _remaining--;
                }
            }
        }
    }

    /// <summary>
    /// Reads values into a byte destination span.
    /// </summary>
    public void ReadBatch(Span<byte> destination)
    {
        int offset = 0;
        while (offset < destination.Length)
        {
            if (_remaining == 0)
                ReadNextGroup();

            int toCopy = Math.Min(_remaining, destination.Length - offset);

            if (_isRle)
            {
                destination.Slice(offset, toCopy).Fill((byte)_rleValue);
                _remaining -= toCopy;
                offset += toCopy;
            }
            else
            {
                if (_bitWidth == 1 && (_bitOffset & 7) == 0)
                {
                    int byteStart = _bitPackedPos + (_bitOffset >> 3);
                    while (toCopy >= 8)
                    {
                        byte packed = _data[byteStart++];
                        destination[offset]     = (byte)( packed        & 1);
                        destination[offset + 1] = (byte)((packed >> 1) & 1);
                        destination[offset + 2] = (byte)((packed >> 2) & 1);
                        destination[offset + 3] = (byte)((packed >> 3) & 1);
                        destination[offset + 4] = (byte)((packed >> 4) & 1);
                        destination[offset + 5] = (byte)((packed >> 5) & 1);
                        destination[offset + 6] = (byte)((packed >> 6) & 1);
                        destination[offset + 7] = (byte)((packed >> 7) & 1);
                        offset += 8;
                        _bitOffset += 8;
                        _remaining -= 8;
                        toCopy -= 8;
                    }
                }

                int mask = (1 << _bitWidth) - 1;
                for (int i = 0; i < toCopy; i++)
                {
                    int byteIdx = _bitPackedPos + (_bitOffset >> 3);
                    int bitIdx = _bitOffset & 7;
                    _bitOffset += _bitWidth;

                    int rem = _data.Length - byteIdx;
                    uint raw = rem >= 4
                        ? BinaryPrimitives.ReadUInt32LittleEndian(_data.Slice(byteIdx))
                        : AssemblePartial(byteIdx, rem);
                    destination[offset++] = (byte)((raw >> bitIdx) & (uint)mask);
                    _remaining--;
                }
            }
        }
    }

    /// <summary>
    /// Reads values into a byte destination span and simultaneously
    /// counts how many decoded values equal <paramref name="matchValue"/>.
    /// </summary>
    public void ReadBatch(Span<byte> destination, int matchValue, out int matchCount)
    {
        matchCount = 0;
        int offset = 0;
        while (offset < destination.Length)
        {
            if (_remaining == 0)
                ReadNextGroup();

            int toCopy = Math.Min(_remaining, destination.Length - offset);

            if (_isRle)
            {
                destination.Slice(offset, toCopy).Fill((byte)_rleValue);
                if (_rleValue == matchValue) matchCount += toCopy;
                _remaining -= toCopy;
                offset += toCopy;
            }
            else
            {
                if (_bitWidth == 1 && (_bitOffset & 7) == 0)
                {
                    int byteStart = _bitPackedPos + (_bitOffset >> 3);
                    while (toCopy >= 8)
                    {
                        byte packed = _data[byteStart++];
                        destination[offset]     = (byte)( packed        & 1);
                        destination[offset + 1] = (byte)((packed >> 1) & 1);
                        destination[offset + 2] = (byte)((packed >> 2) & 1);
                        destination[offset + 3] = (byte)((packed >> 3) & 1);
                        destination[offset + 4] = (byte)((packed >> 4) & 1);
                        destination[offset + 5] = (byte)((packed >> 5) & 1);
                        destination[offset + 6] = (byte)((packed >> 6) & 1);
                        destination[offset + 7] = (byte)((packed >> 7) & 1);
#if NET8_0_OR_GREATER
                        if (matchValue == 1)      matchCount += BitOperations.PopCount(packed);
                        else if (matchValue == 0) matchCount += 8 - BitOperations.PopCount(packed);
#else
                        if (matchValue == 1)      matchCount += BitPolyfills.PopCount(packed);
                        else if (matchValue == 0) matchCount += 8 - BitPolyfills.PopCount(packed);
#endif
                        offset += 8;
                        _bitOffset += 8;
                        _remaining -= 8;
                        toCopy -= 8;
                    }
                }

                int mask = (1 << _bitWidth) - 1;
                for (int i = 0; i < toCopy; i++)
                {
                    int byteIdx = _bitPackedPos + (_bitOffset >> 3);
                    int bitIdx = _bitOffset & 7;
                    _bitOffset += _bitWidth;

                    int rem = _data.Length - byteIdx;
                    uint raw = rem >= 4
                        ? BinaryPrimitives.ReadUInt32LittleEndian(_data.Slice(byteIdx))
                        : AssemblePartial(byteIdx, rem);
                    byte val = (byte)((raw >> bitIdx) & (uint)mask);
                    destination[offset++] = val;
                    if (val == matchValue) matchCount++;
                    _remaining--;
                }
            }
        }
    }

    /// <summary>
    /// Reads <paramref name="count"/> values into the destination span and simultaneously
    /// counts how many decoded values equal <paramref name="matchValue"/>.
    /// Avoids a separate pass to count non-null values after level decoding.
    /// </summary>
    public void ReadBatch(Span<int> destination, int matchValue, out int matchCount)
    {
        matchCount = 0;
        int offset = 0;
        while (offset < destination.Length)
        {
            if (_remaining == 0)
                ReadNextGroup();

            int toCopy = Math.Min(_remaining, destination.Length - offset);

            if (_isRle)
            {
                destination.Slice(offset, toCopy).Fill(_rleValue);
                if (_rleValue == matchValue) matchCount += toCopy;
                _remaining -= toCopy;
                offset += toCopy;
            }
            else
            {
                if (_bitWidth == 1 && (_bitOffset & 7) == 0)
                {
                    int byteStart = _bitPackedPos + (_bitOffset >> 3);
                    while (toCopy >= 8)
                    {
                        byte packed = _data[byteStart++];
                        destination[offset]     =  packed        & 1;
                        destination[offset + 1] = (packed >> 1) & 1;
                        destination[offset + 2] = (packed >> 2) & 1;
                        destination[offset + 3] = (packed >> 3) & 1;
                        destination[offset + 4] = (packed >> 4) & 1;
                        destination[offset + 5] = (packed >> 5) & 1;
                        destination[offset + 6] = (packed >> 6) & 1;
                        destination[offset + 7] = (packed >> 7) & 1;
                        // For maxDefLevel == 1 (most common case), PopCount counts set bits.
                        // For maxDefLevel == 0 the whole column is non-nullable and this path isn't taken.
#if NET8_0_OR_GREATER
                        if (matchValue == 1)      matchCount += BitOperations.PopCount(packed);
                        else if (matchValue == 0) matchCount += 8 - BitOperations.PopCount(packed);
#else
                        if (matchValue == 1)      matchCount += BitPolyfills.PopCount(packed);
                        else if (matchValue == 0) matchCount += 8 - BitPolyfills.PopCount(packed);
#endif
                        offset += 8;
                        _bitOffset += 8;
                        _remaining -= 8;
                        toCopy -= 8;
                    }
                }

                int mask = (1 << _bitWidth) - 1;
                for (int i = 0; i < toCopy; i++)
                {
                    int byteIdx = _bitPackedPos + (_bitOffset >> 3);
                    int bitIdx = _bitOffset & 7;
                    _bitOffset += _bitWidth;

                    int rem = _data.Length - byteIdx;
                    uint raw = rem >= 4
                        ? BinaryPrimitives.ReadUInt32LittleEndian(_data.Slice(byteIdx))
                        : AssemblePartial(byteIdx, rem);
                    int val = (int)((raw >> bitIdx) & (uint)mask);
                    destination[offset++] = val;
                    if (val == matchValue) matchCount++;
                    _remaining--;
                }
            }
        }
    }

    private void ReadNextGroup()
    {
        int header = ReadVarInt();
        if ((header & 1) == 0)
        {
            // RLE run
            _isRle = true;
            _remaining = header >> 1;
            _rleValue = ReadRleValue();
        }
        else
        {
            // Bit-packed run
            _isRle = false;
            int groupCount = header >> 1;
            _remaining = groupCount * 8;
            _bitPackedPos = _position;
            _bitOffset = 0;
            // Advance _position past the bit-packed bytes
            int totalBits = groupCount * 8 * _bitWidth;
            _position += (totalBits + 7) / 8;
        }
    }

    private int ReadVarInt()
    {
        if (_position >= _data.Length)
            throw new ParquetFormatException("Unexpected end of RLE data reading varint.");
        return checked((int)Varint.ReadUnsigned(_data, ref _position));
    }

    private int ReadRleValue()
    {
        int byteWidth = (_bitWidth + 7) / 8;
        if (_position + byteWidth > _data.Length)
            throw new ParquetFormatException("Unexpected end of RLE data reading value.");

        int value = 0;
        for (int i = 0; i < byteWidth; i++)
            value |= _data[_position++] << (i * 8);

        return value;
    }

    private int ReadBitPackedValue()
    {
        if (_bitWidth == 0)
            return 0;

        int byteIndex = _bitPackedPos + (_bitOffset >> 3);
        int bitIndex = _bitOffset & 7;
        _bitOffset += _bitWidth;

        // Fast path: read up to 4 bytes and extract value with shift+mask
        // Safe when bitWidth <= 24 (value spans at most 4 bytes)
        int remaining = _data.Length - byteIndex;
        uint raw;
        if (remaining >= 4)
        {
            raw = BinaryPrimitives.ReadUInt32LittleEndian(_data.Slice(byteIndex));
        }
        else
        {
            // Near end of buffer — assemble from available bytes
            raw = 0;
            for (int i = 0; i < remaining; i++)
                raw |= (uint)_data[byteIndex + i] << (i * 8);
        }

        int mask = (1 << _bitWidth) - 1;
        return (int)((raw >> bitIndex) & (uint)mask);
    }

    private uint AssemblePartial(int byteIndex, int remaining)
    {
        uint raw = 0;
        for (int i = 0; i < remaining; i++)
            raw |= (uint)_data[byteIndex + i] << (i * 8);
        return raw;
    }
}
