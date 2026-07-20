// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;

namespace EngineeredWood.Orc.Encodings;

/// <summary>
/// Reads bits and packed integers from a byte stream. Big-endian bit order (MSB first).
/// </summary>
internal sealed class BitReader
{
    private readonly OrcByteStream _stream;
    private int _currentByte;
    private int _bitsLeft;

    public BitReader(OrcByteStream stream)
    {
        _stream = stream;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadBits(int numBits)
    {
        if (numBits == 0) return 0;

        long result = 0;
        int remaining = numBits;

        while (remaining > 0)
        {
            if (_bitsLeft == 0)
            {
                int b = _stream.ReadByte();
                if (b < 0) throw new InvalidDataException("Unexpected end of bit stream.");
                _currentByte = b;
                _bitsLeft = 8;
            }

            int bitsToRead = Math.Min(remaining, _bitsLeft);
            int shift = _bitsLeft - bitsToRead;
            long bits = (_currentByte >> shift) & ((1 << bitsToRead) - 1);
            result = (result << bitsToRead) | bits;
            _bitsLeft -= bitsToRead;
            remaining -= bitsToRead;
        }

        return result;
    }

    /// <summary>
    /// Reads multiple packed integers, each of the given bit width.
    /// </summary>
    public void ReadPackedInts(Span<long> values, int bitWidth)
    {
        if (bitWidth == 0)
        {
            values.Clear();
            return;
        }

        // Fast paths for byte-aligned bit widths avoid the per-bit loop entirely.
        // All fast paths require that we are on a byte boundary (_bitsLeft == 0),
        // which is the common case after reading RLE headers.
        switch (bitWidth)
        {
            case 1 when _bitsLeft == 0:
                ReadPacked1(values);
                return;
            case 2 when _bitsLeft == 0:
                ReadPacked2(values);
                return;
            case 4 when _bitsLeft == 0:
                ReadPacked4(values);
                return;
            case 8 when _bitsLeft == 0:
                ReadPacked8(values);
                return;
            case 16 when _bitsLeft == 0:
                ReadPacked16(values);
                return;
            case 24 when _bitsLeft == 0:
                ReadPacked24(values);
                return;
            case 32 when _bitsLeft == 0:
                ReadPacked32(values);
                return;
        }

        // Generic fallback for arbitrary bit widths or mid-byte starts.
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = ReadBits(bitWidth);
        }
    }

    private void ReadPacked1(Span<long> values)
    {
        int i = 0;
        int count = values.Length;

        // Process 8 values per byte
        while (i + 8 <= count)
        {
            int b = _stream.ReadByte();
            if (b < 0) throw new InvalidDataException("Unexpected end of bit stream.");
            values[i]     = (b >> 7) & 1;
            values[i + 1] = (b >> 6) & 1;
            values[i + 2] = (b >> 5) & 1;
            values[i + 3] = (b >> 4) & 1;
            values[i + 4] = (b >> 3) & 1;
            values[i + 5] = (b >> 2) & 1;
            values[i + 6] = (b >> 1) & 1;
            values[i + 7] = b & 1;
            i += 8;
        }

        // Remaining values: read one byte, extract MSB-first
        if (i < count)
        {
            int b = _stream.ReadByte();
            if (b < 0) throw new InvalidDataException("Unexpected end of bit stream.");
            _currentByte = b;
            _bitsLeft = 8;
            while (i < count)
            {
                _bitsLeft--;
                values[i] = (_currentByte >> _bitsLeft) & 1;
                i++;
            }
        }
    }

    private void ReadPacked2(Span<long> values)
    {
        int i = 0;
        int count = values.Length;

        // Process 4 values per byte
        while (i + 4 <= count)
        {
            int b = _stream.ReadByte();
            if (b < 0) throw new InvalidDataException("Unexpected end of bit stream.");
            values[i]     = (b >> 6) & 3;
            values[i + 1] = (b >> 4) & 3;
            values[i + 2] = (b >> 2) & 3;
            values[i + 3] = b & 3;
            i += 4;
        }

        if (i < count)
        {
            int b = _stream.ReadByte();
            if (b < 0) throw new InvalidDataException("Unexpected end of bit stream.");
            _currentByte = b;
            _bitsLeft = 8;
            while (i < count)
            {
                _bitsLeft -= 2;
                values[i] = (_currentByte >> _bitsLeft) & 3;
                i++;
            }
        }
    }

    private void ReadPacked4(Span<long> values)
    {
        int i = 0;
        int count = values.Length;

        // Process 2 values per byte
        while (i + 2 <= count)
        {
            int b = _stream.ReadByte();
            if (b < 0) throw new InvalidDataException("Unexpected end of bit stream.");
            values[i]     = (b >> 4) & 0xF;
            values[i + 1] = b & 0xF;
            i += 2;
        }

        if (i < count)
        {
            int b = _stream.ReadByte();
            if (b < 0) throw new InvalidDataException("Unexpected end of bit stream.");
            _currentByte = b;
            _bitsLeft = 4;
            values[i] = (b >> 4) & 0xF;
        }
    }

    private void ReadPacked8(Span<long> values)
    {
        Span<byte> buf = stackalloc byte[Math.Min(values.Length, 256)];
        int i = 0;
        int count = values.Length;

        while (i < count)
        {
            int toRead = Math.Min(count - i, buf.Length);
            var slice = buf[..toRead];
            _stream.ReadExactly(slice);
            for (int j = 0; j < toRead; j++)
            {
                values[i + j] = slice[j];
            }
            i += toRead;
        }
    }

    private void ReadPacked16(Span<long> values)
    {
        Span<byte> buf = stackalloc byte[2];
        for (int i = 0; i < values.Length; i++)
        {
            _stream.ReadExactly(buf);
            values[i] = (long)(buf[0] << 8 | buf[1]);
        }
    }

    private void ReadPacked24(Span<long> values)
    {
        Span<byte> buf = stackalloc byte[3];
        for (int i = 0; i < values.Length; i++)
        {
            _stream.ReadExactly(buf);
            values[i] = (long)(buf[0] << 16 | buf[1] << 8 | buf[2]);
        }
    }

    private void ReadPacked32(Span<long> values)
    {
        Span<byte> buf = stackalloc byte[4];
        for (int i = 0; i < values.Length; i++)
        {
            _stream.ReadExactly(buf);
            values[i] = (long)((uint)buf[0] << 24 | (uint)buf[1] << 16 | (uint)buf[2] << 8 | buf[3]);
        }
    }

    /// <summary>
    /// Resets bit state to start reading from the next byte boundary.
    /// </summary>
    public void Reset()
    {
        _bitsLeft = 0;
    }
}
