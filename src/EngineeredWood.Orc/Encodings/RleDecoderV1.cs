// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.Encodings;

namespace EngineeredWood.Orc.Encodings;

/// <summary>
/// ORC Run Length Encoding version 1 for integers.
/// Maintains internal buffer for values that span across ReadValues calls.
/// </summary>
internal sealed class RleDecoderV1
{
    private readonly OrcByteStream _input;
    private readonly bool _signed;

    private long[]? _buffer;
    private int _bufferOffset;
    private int _bufferCount;
    private bool _bufferFromPool;

    public RleDecoderV1(OrcByteStream input, bool signed)
    {
        _input = input;
        _signed = signed;
    }

    public void ReadValues(Span<long> values)
    {
        int offset = 0;

        // Drain buffered values first
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
            int control = _input.ReadByte();
            if (control < 0)
                throw new InvalidDataException("Unexpected end of RLE v1 stream.");

            var remaining = values.Slice(offset);

            if (control <= 127)
            {
                // Run: (control + 3) values
                int runLength = control + 3;
                sbyte delta = (sbyte)ReadByte();
                long baseValue = _signed ? ReadSignedVarInt() : ReadUnsignedVarInt();

                int toWrite = Math.Min(runLength, remaining.Length);
                for (int i = 0; i < toWrite; i++)
                    remaining[i] = baseValue + (long)i * delta;

                if (toWrite < runLength)
                {
                    var excess = ArrayPool<long>.Shared.Rent(runLength - toWrite);
                    for (int i = 0; i < runLength - toWrite; i++)
                        excess[i] = baseValue + (long)(i + toWrite) * delta;
                    BufferExcess(excess, 0, runLength - toWrite, true);
                }

                offset += toWrite;
            }
            else
            {
                // Literals: (-control) values
                int literalCount = -(sbyte)control;
                int toWrite = Math.Min(literalCount, remaining.Length);

                for (int i = 0; i < toWrite; i++)
                    remaining[i] = _signed ? ReadSignedVarInt() : ReadUnsignedVarInt();

                if (toWrite < literalCount)
                {
                    int excessCount = literalCount - toWrite;
                    var excess = ArrayPool<long>.Shared.Rent(excessCount);
                    for (int i = 0; i < excessCount; i++)
                        excess[i] = _signed ? ReadSignedVarInt() : ReadUnsignedVarInt();
                    BufferExcess(excess, 0, excessCount, true);
                }

                offset += toWrite;
            }
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
