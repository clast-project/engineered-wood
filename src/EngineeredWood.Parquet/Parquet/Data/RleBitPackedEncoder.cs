// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes values using the RLE/Bit-Packing Hybrid encoding as defined in the Parquet specification.
/// Used for definition/repetition levels and dictionary indices.
/// Mirror of <see cref="RleBitPackedDecoder"/>.
/// </summary>
internal sealed class RleBitPackedEncoder
{
    /// <summary>
    /// Largest number of bit-packed groups a single literal run may carry. 63 keeps the run header a
    /// one-byte varint (<c>(63 &lt;&lt; 1) | 1 == 127</c>), which is also parquet-mr's limit.
    /// </summary>
    public const int MaxLiteralGroups = 63;

    private readonly int _bitWidth;
    private readonly int _maxLiteralValues;
    private byte[] _buffer;
    private int _position;

    // Literal values awaiting a flush. Only whole groups of 8 may be emitted mid-stream, so a
    // partial group stays here until either the stream ends (where zero padding is safe) or
    // enough values arrive to complete it.
    private int[]? _pending;
    private int _pendingCount;

    /// <param name="maxLiteralGroups">
    /// How many bit-packed groups may be batched into one literal run. The default of 1 emits a run
    /// header for every 8 values; raising it amortises that header across more values, shrinking
    /// bit-packed-heavy streams and giving decoders a contiguous block to scan.
    /// </param>
    public RleBitPackedEncoder(int bitWidth, int initialCapacity = 256, int maxLiteralGroups = 1)
    {
        if (maxLiteralGroups is < 1 or > MaxLiteralGroups)
            throw new ArgumentOutOfRangeException(nameof(maxLiteralGroups), maxLiteralGroups,
                $"Must be between 1 and {MaxLiteralGroups}.");

        _bitWidth = bitWidth;
        _maxLiteralValues = maxLiteralGroups * 8;
        _buffer = new byte[initialCapacity];
    }

    /// <summary>Number of bytes written.</summary>
    public int Length => _position;

    /// <summary>Returns the written bytes as a span.</summary>
    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _position);

    /// <summary>Returns the written bytes as a new array.</summary>
    public byte[] ToArray() => _buffer.AsSpan(0, _position).ToArray();

    /// <summary>Resets the encoder for reuse.</summary>
    public void Reset() => _position = 0;

    /// <summary>
    /// Encodes all values using the RLE/Bit-Packing Hybrid scheme.
    /// Runs of 8+ identical values are RLE-encoded; mixed values are collected
    /// into complete bit-packed groups of 8 (partial groups only at stream end).
    /// </summary>
    public void Encode(ReadOnlySpan<int> values)
    {
        if (_bitWidth == 0)
            return; // all values are 0, nothing to encode

        _pending ??= new int[_maxLiteralValues];
        _pendingCount = 0;

        int i = 0;
        while (i < values.Length)
        {
            // Detect run of identical values
            int runStart = i;
            int value = values[i];
            while (i < values.Length && values[i] == value)
                i++;

            int runLength = i - runStart;

            if (runLength >= 8 && _pendingCount == 0)
            {
                // Pure RLE run, no pending values to flush first
                WriteRleRun(value, runLength);
                continue;
            }

            // Add run values to pending, emitting whole groups of 8 as they become available
            int runPos = 0;
            while (runPos < runLength)
            {
                _pending[_pendingCount++] = value;
                runPos++;

                if ((_pendingCount & 7) != 0)
                    continue; // mid-group: nothing can be emitted yet

                int remaining = runLength - runPos;
                if (remaining >= 8)
                {
                    // The rest of this run is cheaper as RLE; flush the literals accumulated so far
                    // (a whole number of groups) as a single run, then switch.
                    FlushLiterals();
                    WriteRleRun(value, remaining);
                    runPos = runLength;
                }
                else if (_pendingCount == _maxLiteralValues)
                {
                    FlushLiterals();
                }
            }
        }

        // Emit remaining values as a final partial group (padding is safe at end of stream)
        FlushLiterals();
    }

    /// <summary>
    /// Emits all buffered literals as one bit-packed run. Callers must only invoke this at a group
    /// boundary, except for the final flush at end of stream where zero padding is harmless.
    /// </summary>
    private void FlushLiterals()
    {
        if (_pendingCount == 0)
            return;

        WriteBitPackedGroup(_pending.AsSpan(0, _pendingCount));
        _pendingCount = 0;
    }

    private void WriteRleRun(int value, int count)
    {
        int valueByteWidth = (_bitWidth + 7) / 8;
        EnsureCapacity(5 + valueByteWidth); // max varint + value

        // Header: count << 1 (LSB = 0 indicates RLE)
        WriteVarint((uint)(count << 1));

        // Value in ceil(bitWidth/8) bytes, little-endian
        for (int b = 0; b < valueByteWidth; b++)
        {
            _buffer[_position++] = (byte)(value & 0xFF);
            value >>= 8;
        }
    }

    private void WriteBitPackedGroup(ReadOnlySpan<int> values)
    {
        if (values.Length == 0)
            return;

        // Pad to multiple of 8 (Parquet bit-packed groups are always 8 values)
        int numGroups = (values.Length + 7) / 8;
        int totalValues = numGroups * 8;
        int bytesPerGroup = _bitWidth; // bitWidth * 8 bits / 8 = bitWidth bytes per group
        int totalBytes = numGroups * bytesPerGroup;

        EnsureCapacity(5 + totalBytes); // max varint + packed data

        // Header: numGroups << 1 | 1 (LSB = 1 indicates bit-packed)
        WriteVarint((uint)((numGroups << 1) | 1));

        // Bit-pack values, LSB first
        int bitPos = 0;
        int byteStart = _position;
        // Pre-zero the output bytes
        _buffer.AsSpan(_position, totalBytes).Clear();

        for (int i = 0; i < totalValues; i++)
        {
            int val = i < values.Length ? values[i] : 0; // pad with 0
            if (val != 0 && _bitWidth > 0)
            {
                int byteOffset = bitPos / 8;
                int bitOffset = bitPos % 8;

                // Write value across potentially multiple bytes
                long shifted = (long)val << bitOffset;
                int bytesNeeded = (bitOffset + _bitWidth + 7) / 8;
                for (int b = 0; b < bytesNeeded && byteStart + byteOffset + b < _buffer.Length; b++)
                {
                    _buffer[byteStart + byteOffset + b] |= (byte)(shifted >> (b * 8));
                }
            }
            bitPos += _bitWidth;
        }

        _position += totalBytes;
    }

    private void WriteVarint(uint value)
    {
        while (value > 0x7F)
        {
            _buffer[_position++] = (byte)(value | 0x80);
            value >>= 7;
        }
        _buffer[_position++] = (byte)value;
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
