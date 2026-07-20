// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;

namespace EngineeredWood.Orc.Encodings;

/// <summary>
/// ORC Byte Run Length Encoding decoder.
/// Maintains internal buffer for values that span across ReadValues calls.
/// </summary>
internal sealed class ByteRleDecoder
{
    private readonly OrcByteStream _input;
    private byte[]? _buffer;
    private int _bufferOffset;
    private int _bufferCount;
    private bool _bufferFromPool;

    public ByteRleDecoder(OrcByteStream input)
    {
        _input = input;
    }

    public void ReadValues(Span<byte> values)
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
                throw new InvalidDataException("Unexpected end of byte RLE stream.");

            var remaining = values.Slice(offset);

            if ((sbyte)control >= 0)
            {
                // Run: (control + 3) identical values
                int runLength = control + 3;
                int b = _input.ReadByte();
                if (b < 0) throw new InvalidDataException("Unexpected end of stream in byte RLE run.");

                int toWrite = Math.Min(runLength, remaining.Length);
                remaining.Slice(0, toWrite).Fill((byte)b);

                if (toWrite < runLength)
                {
                    int excess = runLength - toWrite;
                    var buf = ArrayPool<byte>.Shared.Rent(excess);
                    buf.AsSpan(0, excess).Fill((byte)b);
                    BufferExcess(buf, 0, excess, true);
                }

                offset += toWrite;
            }
            else
            {
                // Literals: (-control) values
                int literalCount = -(sbyte)control;
                int toWrite = Math.Min(literalCount, remaining.Length);

                _input.ReadExactly(remaining.Slice(0, toWrite));

                if (toWrite < literalCount)
                {
                    int excess = literalCount - toWrite;
                    var buf = ArrayPool<byte>.Shared.Rent(excess);
                    _input.ReadExactly(buf.AsSpan(0, excess));
                    BufferExcess(buf, 0, excess, true);
                }

                offset += toWrite;
            }
        }
    }

    private void ReturnBuffer()
    {
        if (_bufferFromPool && _buffer != null)
            ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = null;
        _bufferOffset = 0;
        _bufferFromPool = false;
    }

    private void BufferExcess(byte[] values, int start, int count, bool fromPool)
    {
        _buffer = values;
        _bufferOffset = start;
        _bufferCount = count;
        _bufferFromPool = fromPool;
    }
}

/// <summary>
/// Decodes a boolean stream encoded as byte RLE of bit-packed bytes.
/// Booleans are packed MSB first: bit 7 of each byte is the first value.
/// </summary>
internal sealed class BooleanDecoder
{
    private readonly ByteRleDecoder _byteDecoder;
    private byte _currentByte;
    private int _bitsLeft;

    public BooleanDecoder(OrcByteStream input)
    {
        _byteDecoder = new ByteRleDecoder(input);
    }

    public void ReadValues(Span<bool> values)
    {
        int offset = 0;

        // Use remaining bits from current byte
        while (offset < values.Length && _bitsLeft > 0)
        {
            _bitsLeft--;
            values[offset++] = ((_currentByte >> _bitsLeft) & 1) == 1;
        }

        if (offset >= values.Length)
            return;

        // Calculate how many full bytes we need, plus one extra if there are leftover bits
        int bitsNeeded = values.Length - offset;
        int fullBytes = bitsNeeded >> 3; // bitsNeeded / 8
        int trailingBits = bitsNeeded & 7; // bitsNeeded % 8
        int bytesToRead = fullBytes + (trailingBits > 0 ? 1 : 0);

        // Read all needed bytes in one batch (use stackalloc for small, rent for large)
        const int StackLimit = 256;
        byte[]? rented = null;
        Span<byte> byteBuffer = bytesToRead <= StackLimit
            ? stackalloc byte[StackLimit]
            : (rented = ArrayPool<byte>.Shared.Rent(bytesToRead));
        byteBuffer = byteBuffer.Slice(0, bytesToRead);

        _byteDecoder.ReadValues(byteBuffer);

        // Extract bits from full bytes (MSB first)
        int byteIndex = 0;
        for (; byteIndex < fullBytes; byteIndex++)
        {
            byte b = byteBuffer[byteIndex];
            values[offset]     = (b & 0x80) != 0;
            values[offset + 1] = (b & 0x40) != 0;
            values[offset + 2] = (b & 0x20) != 0;
            values[offset + 3] = (b & 0x10) != 0;
            values[offset + 4] = (b & 0x08) != 0;
            values[offset + 5] = (b & 0x04) != 0;
            values[offset + 6] = (b & 0x02) != 0;
            values[offset + 7] = (b & 0x01) != 0;
            offset += 8;
        }

        // Handle trailing bits from the last byte (if any)
        if (trailingBits > 0)
        {
            _currentByte = byteBuffer[byteIndex];
            _bitsLeft = 8;
            while (offset < values.Length)
            {
                _bitsLeft--;
                values[offset++] = ((_currentByte >> _bitsLeft) & 1) == 1;
            }
        }

        if (rented != null)
            ArrayPool<byte>.Shared.Return(rented);
    }
}
