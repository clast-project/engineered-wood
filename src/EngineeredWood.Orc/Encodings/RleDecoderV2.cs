// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.Encodings;

namespace EngineeredWood.Orc.Encodings;

/// <summary>
/// ORC Run Length Encoding version 2 for integers.
/// Supports 4 sub-encodings: Short Repeat, Direct, Patched Base, Delta.
/// Maintains internal buffer for values that span across ReadValues calls.
/// </summary>
internal sealed class RleDecoderV2
{
    private readonly OrcByteStream _input;
    private readonly bool _signed;
    private readonly BitReader _bitReader;

    // Buffer for leftover values from a run that didn't fit in the output
    private long[]? _buffer;
    private int _bufferOffset;
    private int _bufferCount;
    private bool _bufferFromPool;

    public RleDecoderV2(OrcByteStream input, bool signed)
    {
        _input = input;
        _signed = signed;
        _bitReader = new BitReader(input);
    }

    public void ReadValues(Span<long> values)
    {
        int offset = 0;

        // First, drain any buffered values from a previous run
        if (_bufferCount > 0)
        {
            int toCopy = Math.Min(_bufferCount, values.Length);
            _buffer.AsSpan(_bufferOffset, toCopy).CopyTo(values);
            _bufferOffset += toCopy;
            _bufferCount -= toCopy;
            offset += toCopy;
            if (_bufferCount == 0)
                ReturnBuffer();
        }

        while (offset < values.Length)
        {
            int firstByte = _input.ReadByte();
            if (firstByte < 0)
                throw new InvalidDataException("Unexpected end of RLE v2 stream.");

            int encodingType = (firstByte >> 6) & 0x03;
            var remaining = values.Slice(offset);

            int consumed = encodingType switch
            {
                0 => DecodeShortRepeat(firstByte, remaining),
                1 => DecodeDirect(firstByte, remaining),
                2 => DecodePatchedBase(firstByte, remaining),
                3 => DecodeDelta(firstByte, remaining),
                _ => throw new InvalidDataException($"Unknown RLE v2 encoding type: {encodingType}")
            };

            offset += consumed;
        }
    }

    private void ReturnBuffer()
    {
        if (_bufferFromPool && _buffer != null)
            ArrayPool<long>.Shared.Return(_buffer);
        _buffer = null;
        _bufferOffset = 0;
        _bufferFromPool = false;
    }

    private void BufferExcess(long[] values, int start, int count, bool fromPool)
    {
        _buffer = values;
        _bufferOffset = start;
        _bufferCount = count;
        _bufferFromPool = fromPool;
    }

    /// <summary>
    /// Returns the number of values written to output.
    /// </summary>
    private int DecodeShortRepeat(int firstByte, Span<long> output)
    {
        int width = ((firstByte >> 3) & 0x07) + 1;
        int count = (firstByte & 0x07) + 3;

        long value = 0;
        for (int i = 0; i < width; i++)
        {
            int b = _input.ReadByte();
            if (b < 0) throw new InvalidDataException("Unexpected end of stream in ShortRepeat.");
            value = (value << 8) | (byte)b;
        }

        if (_signed)
            value = Varint.ZigzagDecode(value);

        int toWrite = Math.Min(count, output.Length);
        output.Slice(0, toWrite).Fill(value);

        if (toWrite < count)
        {
            // Buffer excess - ShortRepeat max is 10, so no pool needed
            var excess = new long[count - toWrite];
            excess.AsSpan().Fill(value);
            BufferExcess(excess, 0, count - toWrite, false);
        }

        return toWrite;
    }

    private int DecodeDirect(int firstByte, Span<long> output)
    {
        int encodedWidth = (firstByte >> 1) & 0x1F;
        int bitWidth = WidthEncoding.Decode(encodedWidth);

        int secondByte = ReadByte();
        int length = ((firstByte & 0x01) << 8) | secondByte;
        length += 1;

        if (length <= output.Length)
        {
            // Fast path: decode directly into output
            _bitReader.Reset();
            for (int i = 0; i < length; i++)
            {
                long val = _bitReader.ReadBits(bitWidth);
                if (_signed) val = Varint.ZigzagDecode(val);
                output[i] = val;
            }
            return length;
        }

        // Slow path: need to buffer overflow
        var pool = ArrayPool<long>.Shared.Rent(length);
        _bitReader.Reset();
        for (int i = 0; i < length; i++)
        {
            long val = _bitReader.ReadBits(bitWidth);
            if (_signed) val = Varint.ZigzagDecode(val);
            pool[i] = val;
        }

        pool.AsSpan(0, output.Length).CopyTo(output);
        BufferExcess(pool, output.Length, length - output.Length, true);
        return output.Length;
    }

    private int DecodePatchedBase(int firstByte, Span<long> output)
    {
        int encodedWidth = (firstByte >> 1) & 0x1F;
        int bitWidth = WidthEncoding.Decode(encodedWidth);

        int secondByte = ReadByte();
        int length = ((firstByte & 0x01) << 8) | secondByte;
        length += 1;

        int thirdByte = ReadByte();
        int baseWidth = ((thirdByte >> 5) & 0x07) + 1;
        int encodedPatchWidth = thirdByte & 0x1F;
        int patchWidth = WidthEncoding.Decode(encodedPatchWidth);

        int fourthByte = ReadByte();
        int patchGapWidth = ((fourthByte >> 5) & 0x07) + 1;
        int patchListLength = fourthByte & 0x1F;

        // Read base value
        long baseValue = 0;
        for (int i = 0; i < baseWidth; i++)
            baseValue = (baseValue << 8) | ReadByte();

        long signMask = 1L << (baseWidth * 8 - 1);
        if ((baseValue & signMask) != 0)
        {
            baseValue &= ~signMask;
            baseValue = -baseValue;
        }

        // Read data values into pooled array
        var dataValues = ArrayPool<long>.Shared.Rent(length);
        _bitReader.Reset();
        _bitReader.ReadPackedInts(dataValues.AsSpan(0, length), bitWidth);

        // Read and apply patches
        if (patchListLength > 0)
        {
            int patchBitSize = patchGapWidth + patchWidth;
            long patchMask = (1L << patchWidth) - 1;

            // Patch list is small (max 31), use stackalloc
            Span<long> patches = patchListLength <= 32 ? stackalloc long[patchListLength] : new long[patchListLength];
            for (int i = 0; i < patchListLength; i++)
                patches[i] = _bitReader.ReadBits(patchBitSize);

            int patchIdx = 0;
            int currentPos = 0;

            while (patchIdx < patchListLength)
            {
                int gap = (int)(patches[patchIdx] >> patchWidth);
                long patchBits = patches[patchIdx] & patchMask;
                patchIdx++;

                int actualGap = 0;
                while (gap == 255 && patchBits == 0 && patchIdx < patchListLength)
                {
                    actualGap += 255;
                    gap = (int)(patches[patchIdx] >> patchWidth);
                    patchBits = patches[patchIdx] & patchMask;
                    patchIdx++;
                }
                actualGap += gap;
                currentPos += actualGap;

                if (currentPos < length)
                    dataValues[currentPos] |= (patchBits << bitWidth);
            }
        }

        // Apply base + zigzag, write to output or buffer
        int toWrite = Math.Min(length, output.Length);
        for (int i = 0; i < toWrite; i++)
        {
            long val = dataValues[i] + baseValue;
            if (_signed) val = Varint.ZigzagDecode(val);
            output[i] = val;
        }

        if (toWrite < length)
        {
            // Need to buffer excess - convert remaining in-place
            for (int i = toWrite; i < length; i++)
            {
                long val = dataValues[i] + baseValue;
                if (_signed) val = Varint.ZigzagDecode(val);
                dataValues[i] = val;
            }
            BufferExcess(dataValues, toWrite, length - toWrite, true);
        }
        else
        {
            ArrayPool<long>.Shared.Return(dataValues);
        }

        return toWrite;
    }

    private int DecodeDelta(int firstByte, Span<long> output)
    {
        int encodedWidth = (firstByte >> 1) & 0x1F;
        int deltaWidth = WidthEncoding.DecodeDelta(encodedWidth);

        int secondByte = ReadByte();
        int length = ((firstByte & 0x01) << 8) | secondByte;
        length += 1;

        long baseValue = _signed ? ReadSignedVarInt() : ReadUnsignedVarInt();
        long deltaBase = ReadSignedVarInt();

        // Fast path: fits in output
        if (length <= output.Length)
        {
            output[0] = baseValue;
            if (length == 1) return 1;
            output[1] = baseValue + deltaBase;
            if (length == 2) return 2;

            if (deltaWidth == 0)
            {
                for (int i = 2; i < length; i++)
                    output[i] = baseValue + (long)i * deltaBase;
            }
            else
            {
                var deltas = ArrayPool<long>.Shared.Rent(length - 2);
                _bitReader.Reset();
                _bitReader.ReadPackedInts(deltas.AsSpan(0, length - 2), deltaWidth);

                long current = baseValue + deltaBase;
                for (int i = 0; i < length - 2; i++)
                {
                    current = deltaBase < 0 ? current - deltas[i] : current + deltas[i];
                    output[i + 2] = current;
                }
                ArrayPool<long>.Shared.Return(deltas);
            }
            return length;
        }

        // Slow path: buffer overflow
        var pool = ArrayPool<long>.Shared.Rent(length);
        pool[0] = baseValue;
        if (length > 1) pool[1] = baseValue + deltaBase;

        if (length > 2)
        {
            if (deltaWidth == 0)
            {
                for (int i = 2; i < length; i++)
                    pool[i] = baseValue + (long)i * deltaBase;
            }
            else
            {
                var deltas = ArrayPool<long>.Shared.Rent(length - 2);
                _bitReader.Reset();
                _bitReader.ReadPackedInts(deltas.AsSpan(0, length - 2), deltaWidth);

                long current = baseValue + deltaBase;
                for (int i = 0; i < length - 2; i++)
                {
                    current = deltaBase < 0 ? current - deltas[i] : current + deltas[i];
                    pool[i + 2] = current;
                }
                ArrayPool<long>.Shared.Return(deltas);
            }
        }

        pool.AsSpan(0, output.Length).CopyTo(output);
        BufferExcess(pool, output.Length, length - output.Length, true);
        return output.Length;
    }

    private byte ReadByte()
    {
        int b = _input.ReadByte();
        if (b < 0) throw new InvalidDataException("Unexpected end of stream.");
        return (byte)b;
    }

    private long ReadUnsignedVarInt()
    {
        long result = 0;
        int shift = 0;
        while (true)
        {
            int b = _input.ReadByte();
            if (b < 0) throw new InvalidDataException("Unexpected end of varint.");
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
    }

    private long ReadSignedVarInt()
    {
        return Varint.ZigzagDecode(ReadUnsignedVarInt());
    }
}
