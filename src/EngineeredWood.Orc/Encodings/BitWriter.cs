// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Orc.Encodings;

/// <summary>
/// Writes bits and packed integers to a growable byte buffer. Big-endian bit order (MSB first).
/// </summary>
internal sealed class BitWriter
{
    private readonly GrowableBuffer _buffer;
    private int _currentByte;
    private int _bitsUsed;

    public BitWriter(GrowableBuffer buffer)
    {
        _buffer = buffer;
    }

    public void WriteBits(long value, int numBits)
    {
        if (numBits == 0) return;

        int remaining = numBits;
        while (remaining > 0)
        {
            int bitsToWrite = Math.Min(remaining, 8 - _bitsUsed);
            int shift = remaining - bitsToWrite;
            int bits = (int)((value >> shift) & ((1 << bitsToWrite) - 1));
            _currentByte |= bits << (8 - _bitsUsed - bitsToWrite);
            _bitsUsed += bitsToWrite;
            remaining -= bitsToWrite;

            if (_bitsUsed == 8)
                FlushByte();
        }
    }

    public void WritePackedInts(ReadOnlySpan<long> values, int bitWidth)
    {
        for (int i = 0; i < values.Length; i++)
            WriteBits(values[i], bitWidth);
    }

    /// <summary>
    /// Flushes any partial byte, padding with zeros on the right.
    /// </summary>
    public void Flush()
    {
        if (_bitsUsed > 0)
            FlushByte();
    }

    private void FlushByte()
    {
        _buffer.WriteByte((byte)_currentByte);
        _currentByte = 0;
        _bitsUsed = 0;
    }
}
