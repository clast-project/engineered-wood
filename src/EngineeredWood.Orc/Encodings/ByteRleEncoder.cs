// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Orc.Encodings;

/// <summary>
/// ORC Byte Run Length Encoder.
/// Runs of 3+ identical bytes -> run header + value.
/// Non-repeating sequences -> literal header + bytes.
/// </summary>
internal sealed class ByteRleEncoder
{
    private const int MaxRunLength = 130;
    private const int MaxLiteralLength = 128;
    private readonly GrowableBuffer _output;
    private readonly byte[] _literals = new byte[MaxLiteralLength];
    private int _literalCount;

    public int BufferedCount => _literalCount;

    public ByteRleEncoder(GrowableBuffer output)
    {
        _output = output;
    }

    public void WriteValues(ReadOnlySpan<byte> values)
    {
        int i = 0;
        while (i < values.Length)
        {
            // Check for a run of identical bytes (need at least 3)
            int runLength = 1;
            while (i + runLength < values.Length && runLength < MaxRunLength && values[i + runLength] == values[i])
                runLength++;

            if (runLength >= 3)
            {
                // Flush any pending literals first
                FlushLiterals();

                // Write run: control byte = runLength - 3 (0..127)
                _output.WriteByte((byte)(runLength - 3));
                _output.WriteByte(values[i]);
                i += runLength;
            }
            else
            {
                // Add to literal buffer
                _literals[_literalCount++] = values[i];
                i++;

                if (_literalCount == MaxLiteralLength)
                    FlushLiterals();
            }
        }
    }

    public void Flush()
    {
        FlushLiterals();
    }

    private void FlushLiterals()
    {
        if (_literalCount == 0) return;

        // Control byte = -(literalCount) as signed byte, i.e. (256 - literalCount)
        _output.WriteByte((byte)(256 - _literalCount));
        _output.Write(_literals, 0, _literalCount);
        _literalCount = 0;
    }
}

/// <summary>
/// ORC Boolean Encoder. Packs booleans into bytes (MSB first) and writes via ByteRleEncoder.
/// </summary>
internal sealed class BooleanEncoder
{
    private readonly ByteRleEncoder _byteEncoder;
    private int _currentByte;
    private int _bitsUsed;

    public BooleanEncoder(GrowableBuffer output)
    {
        _byteEncoder = new ByteRleEncoder(output);
    }

    public void WriteValues(ReadOnlySpan<bool> values)
    {
        Span<byte> b = stackalloc byte[1];
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i])
                _currentByte |= 1 << (7 - _bitsUsed);
            _bitsUsed++;

            if (_bitsUsed == 8)
            {
                b[0] = (byte)_currentByte;
                _byteEncoder.WriteValues(b);
                _currentByte = 0;
                _bitsUsed = 0;
            }
        }
    }

    public void Flush()
    {
        if (_bitsUsed > 0)
        {
            Span<byte> b = stackalloc byte[1];
            b[0] = (byte)_currentByte;
            _byteEncoder.WriteValues(b);
            _currentByte = 0;
            _bitsUsed = 0;
        }
        _byteEncoder.Flush();
    }
}
