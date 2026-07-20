// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Numerics;
using EngineeredWood.Encodings;

namespace EngineeredWood.Orc.Encodings;

/// <summary>
/// ORC Run Length Encoding version 2 for integers.
/// Supports Short Repeat, Direct, Patched Base, and Delta sub-encodings.
/// Values are buffered and flushed when the buffer is full or Flush() is called.
/// </summary>
internal sealed class RleEncoderV2
{
    private const int MaxRunLength = 512;
    private readonly GrowableBuffer _output;
    private readonly bool _signed;
    private readonly long[] _buffer = new long[MaxRunLength];
    private int _count;

    /// <summary>Number of values currently buffered but not yet flushed to the output stream.</summary>
    public int BufferedCount => _count;

    public RleEncoderV2(GrowableBuffer output, bool signed)
    {
        _output = output;
        _signed = signed;
    }

    public void WriteValues(ReadOnlySpan<long> values)
    {
        int offset = 0;
        while (offset < values.Length)
        {
            int space = MaxRunLength - _count;
            int toCopy = Math.Min(space, values.Length - offset);
            values.Slice(offset, toCopy).CopyTo(_buffer.AsSpan(_count));
            _count += toCopy;
            offset += toCopy;
            if (_count == MaxRunLength)
                FlushBuffer();
        }
    }

    public void Flush()
    {
        if (_count > 0)
            FlushBuffer();
    }

    private void FlushBuffer()
    {
        if (_count == 0) return;

        var values = _buffer.AsSpan(0, _count);

        // Try Short Repeat: 3-10 identical values (entire buffer)
        if (_count >= 3 && _count <= 10 && TryEncodeShortRepeat(values))
        {
            _count = 0;
            return;
        }

        // Try delta first — delta works on original (non-zigzag'd) values
        if (_count >= 2 && TryEncodeDelta(values))
        {
            _count = 0;
            return;
        }

        // Fall back to Patched Base or Direct — both use zigzag-encoded values
        var work = _signed ? ZigZagEncode(values) : values.ToArray();
        var workSpan = work.AsSpan(0, _count);

        if (_count >= 2 && TryEncodePatchedBase(workSpan))
        {
            _count = 0;
            return;
        }

        EncodeDirect(workSpan);
        _count = 0;
    }

    private static long[] ZigZagEncode(ReadOnlySpan<long> values)
    {
        var result = new long[values.Length];
        for (int i = 0; i < values.Length; i++)
            result[i] = (long)Varint.ZigzagEncode(values[i]);
        return result;
    }

    private bool TryEncodeShortRepeat(ReadOnlySpan<long> values)
    {
        long first = values[0];
        for (int i = 1; i < values.Length; i++)
            if (values[i] != first) return false;

        // All identical — encode as Short Repeat
        long value = _signed ? (long)Varint.ZigzagEncode(first) : first;

        int byteWidth;
        if (value == 0)
            byteWidth = 1;
        else
            byteWidth = (64 - BitOperations.LeadingZeroCount((ulong)value) + 7) / 8;

        // Header: [00][width-1:3bits][count-3:3bits]
        byte header = (byte)(((byteWidth - 1) << 3) | (values.Length - 3));
        _output.WriteByte(header);

        // Write value bytes big-endian
        for (int i = byteWidth - 1; i >= 0; i--)
            _output.WriteByte((byte)((ulong)value >> (i * 8)));

        return true;
    }

    private void EncodeDirect(ReadOnlySpan<long> values)
    {
        // Determine the maximum bit width needed (values are unsigned/zigzag'd)
        long maxValue = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if ((ulong)values[i] > (ulong)maxValue) maxValue = values[i];
        }

        int bitWidth = maxValue == 0 ? 1 : 64 - BitOperations.LeadingZeroCount((ulong)maxValue);
        int closestWidth = WidthEncoding.GetClosestWidth(bitWidth);
        int encodedWidth = WidthEncoding.Encode(closestWidth);

        int length = values.Length;

        // Header: 2 bytes
        // byte 0: [encoding=01][encodedWidth:5bits][length high bit]
        // byte 1: [length low 8 bits]
        int adjustedLength = length - 1; // 0-based
        byte b0 = (byte)(0x40 | (encodedWidth << 1) | ((adjustedLength >> 8) & 0x01));
        byte b1 = (byte)(adjustedLength & 0xFF);
        _output.WriteByte(b0);
        _output.WriteByte(b1);

        // Write packed values
        var bw = new BitWriter(_output);
        bw.WritePackedInts(values, closestWidth);
        bw.Flush();
    }

    private bool TryEncodePatchedBase(ReadOnlySpan<long> values)
    {
        // Values are zigzag'd (if signed) or raw unsigned — compare as unsigned
        // Find min (base) and max
        long minValue = values[0], maxValue = values[0];
        for (int i = 1; i < values.Length; i++)
        {
            if ((ulong)values[i] < (ulong)minValue) minValue = values[i];
            if ((ulong)values[i] > (ulong)maxValue) maxValue = values[i];
        }

        // Adjusted = value - base (all >= 0 as unsigned). values.Length <= MaxRunLength (512),
        // so up to 4KB for the long buffer; rent from the pool to avoid blowing the stack.
        long[] adjustedBuf = ArrayPool<long>.Shared.Rent(values.Length);
        int[] bitWidthsBuf = ArrayPool<int>.Shared.Rent(values.Length);
        try
        {
            Span<long> adjusted = adjustedBuf.AsSpan(0, values.Length);
            long maxAdj = 0;
            for (int i = 0; i < values.Length; i++)
            {
                adjusted[i] = (long)((ulong)values[i] - (ulong)minValue);
                if ((ulong)adjusted[i] > (ulong)maxAdj) maxAdj = adjusted[i];
            }

            // Full bit width (what Direct would use)
            int fullBitWidth = maxAdj == 0 ? 1 : 64 - BitOperations.LeadingZeroCount((ulong)maxAdj);
            int fullClosest = WidthEncoding.GetClosestWidth(fullBitWidth);

            // Find the 90th percentile bit width
            Span<int> bitWidths = bitWidthsBuf.AsSpan(0, values.Length);
            for (int i = 0; i < values.Length; i++)
                bitWidths[i] = adjusted[i] == 0 ? 0 : 64 - BitOperations.LeadingZeroCount((ulong)adjusted[i]);
#if NETSTANDARD2_0
            Array.Sort(bitWidthsBuf, 0, values.Length);
#else
            bitWidths.Sort();
#endif

            int p90Index = Math.Max(0, (int)Math.Ceiling(values.Length * 0.9) - 1);
            int p90Width = bitWidths[p90Index];
            if (p90Width < 1) p90Width = 1;
            int dataWidth = WidthEncoding.GetClosestWidth(p90Width);

            // No benefit if p90 width matches full width
            if (dataWidth >= fullClosest) return false;

            // Identify outliers
            long dataMask = dataWidth >= 64 ? long.MaxValue : (1L << dataWidth) - 1;
            long maxPatchBits = 0;
            int outlierCount = 0;
            for (int i = 0; i < values.Length; i++)
            {
                if ((ulong)adjusted[i] > (ulong)dataMask)
                {
                    outlierCount++;
                    long highBits = (long)((ulong)adjusted[i] >> dataWidth);
                    if ((ulong)highBits > (ulong)maxPatchBits) maxPatchBits = highBits;
                }
            }

            if (outlierCount == 0) return false;

            int patchWidth = maxPatchBits == 0 ? 1 : 64 - BitOperations.LeadingZeroCount((ulong)maxPatchBits);
            patchWidth = WidthEncoding.GetClosestWidth(patchWidth);

            // Build patch list with relative gaps
            const int patchGapWidth = 8; // always use 8 bits for gap
            var patchList = new List<(int gap, long patchBits)>();
            int lastPatchPos = 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (adjusted[i] > dataMask)
                {
                    long highBits = adjusted[i] >> dataWidth;
                    adjusted[i] &= dataMask; // keep only low bits in data

                    int relGap = i - lastPatchPos;
                    while (relGap > 255)
                    {
                        patchList.Add((255, 0)); // continuation
                        relGap -= 255;
                    }
                    patchList.Add((relGap, highBits));
                    lastPatchPos = i;
                }
            }

            if (patchList.Count > 31) return false; // patchListLength is 5 bits

            // Base value byte width (non-negative, but format has sign bit in MSB)
            int baseBitsNeeded = minValue == 0 ? 1 : 64 - BitOperations.LeadingZeroCount((ulong)minValue) + 1; // +1 for sign bit
            int baseByteWidth = (baseBitsNeeded + 7) / 8;
            if (baseByteWidth > 8) baseByteWidth = 8;

            // Cost comparison
            int patchedBits = 32 + baseByteWidth * 8 + values.Length * dataWidth + patchList.Count * (patchGapWidth + patchWidth);
            int directBits = 16 + values.Length * fullClosest;
            if (patchedBits >= directBits) return false;

            // Encode header
            int encodedDataWidth = WidthEncoding.Encode(dataWidth);
            int encodedPatchWidth = WidthEncoding.Encode(patchWidth);
            int adjustedLength = values.Length - 1;

            byte b0 = (byte)(0x80 | (encodedDataWidth << 1) | ((adjustedLength >> 8) & 0x01));
            byte b1 = (byte)(adjustedLength & 0xFF);
            byte b2 = (byte)(((baseByteWidth - 1) << 5) | encodedPatchWidth);
            byte b3 = (byte)(((patchGapWidth - 1) << 5) | patchList.Count);

            _output.WriteByte(b0);
            _output.WriteByte(b1);
            _output.WriteByte(b2);
            _output.WriteByte(b3);

            // Write base value big-endian (sign bit in MSB; base is always >= 0 here)
            for (int i = baseByteWidth - 1; i >= 0; i--)
                _output.WriteByte((byte)((ulong)minValue >> (i * 8)));

            // Write packed data values (adjusted, with outlier low bits only)
            var bw = new BitWriter(_output);
            bw.WritePackedInts(adjusted, dataWidth);

            // Write patch list entries
            for (int i = 0; i < patchList.Count; i++)
            {
                var (gap, patchBits) = patchList[i];
                long entry = ((long)gap << patchWidth) | patchBits;
                bw.WriteBits(entry, patchGapWidth + patchWidth);
            }
            bw.Flush();

            return true;
        }
        finally
        {
            ArrayPool<long>.Shared.Return(adjustedBuf);
            ArrayPool<int>.Shared.Return(bitWidthsBuf);
        }
    }

    private bool TryEncodeDelta(ReadOnlySpan<long> values)
    {
        if (values.Length < 2) return false;

        long initialDelta = values[1] - values[0];

        // Compute absolute deltas for values[2] onwards.
        // ORC Delta stores unsigned packed values that the decoder adds (deltaBase >= 0)
        // or subtracts (deltaBase < 0). All deltas must have the same sign as initialDelta.
        var deltas = new long[values.Length - 2];
        bool constant = true;
        long maxDelta = 0;

        for (int i = 2; i < values.Length; i++)
        {
            long delta = values[i] - values[i - 1];

            // Reject if any delta has a different sign than initialDelta
            if (initialDelta >= 0 && delta < 0) return false;
            if (initialDelta < 0 && delta > 0) return false;

            long absDelta = initialDelta >= 0 ? delta : -delta;
            deltas[i - 2] = absDelta;
            if (absDelta > maxDelta) maxDelta = absDelta;
            if (delta != initialDelta) constant = false;
        }

        int deltaBitWidth = 0;
        if (!constant)
        {
            deltaBitWidth = maxDelta == 0 ? 1 : 64 - BitOperations.LeadingZeroCount((ulong)maxDelta);
            deltaBitWidth = Math.Max(2, WidthEncoding.GetClosestWidth(deltaBitWidth)); // min 2: encoded 0 means "constant" in delta

            // If delta encoding uses more space than direct, skip it
            int directBits = EstimateDirectBits(values);
            int deltaBits = EstimateDeltaBits(values[0], initialDelta, deltas.Length, deltaBitWidth);
            if (deltaBits >= directBits) return false;
        }

        int encodedDeltaWidth = constant ? 0 : WidthEncoding.Encode(deltaBitWidth);
        int length = values.Length;
        int adjustedLength = length - 1;

        // Header: 2 bytes
        // byte 0: [encoding=11][encodedDeltaWidth:5bits][length high bit]
        // byte 1: [length low 8 bits]
        byte b0 = (byte)(0xC0 | (encodedDeltaWidth << 1) | ((adjustedLength >> 8) & 0x01));
        byte b1 = (byte)(adjustedLength & 0xFF);
        _output.WriteByte(b0);
        _output.WriteByte(b1);

        // Write base value as varint
        if (_signed)
            WriteSignedVarInt(_output, values[0]);
        else
            WriteUnsignedVarInt(_output, values[0]);

        // Write initial delta as signed varint
        WriteSignedVarInt(_output, initialDelta);

        // Write absolute deltas packed
        if (!constant && deltas.Length > 0)
        {
            var bw = new BitWriter(_output);
            bw.WritePackedInts(deltas, deltaBitWidth);
            bw.Flush();
        }

        return true;
    }

    private int EstimateDirectBits(ReadOnlySpan<long> values)
    {
        long max = 0;
        for (int i = 0; i < values.Length; i++)
        {
            long v = _signed ? (long)Varint.ZigzagEncode(values[i]) : values[i];
            if (v > max) max = v;
        }
        int bw = max == 0 ? 1 : 64 - BitOperations.LeadingZeroCount((ulong)max);
        bw = WidthEncoding.GetClosestWidth(bw);
        return 16 + values.Length * bw; // 2 byte header + packed values
    }

    private static int EstimateDeltaBits(long baseValue, long initialDelta, int numDeltas, int deltaBitWidth)
    {
        int baseBits = 16; // header
        baseBits += VarIntSize(baseValue) * 8;
        baseBits += VarIntSize(initialDelta) * 8;
        baseBits += numDeltas * deltaBitWidth;
        return baseBits;
    }

    private static int VarIntSize(long value) => Varint.SignedSize(value);

    internal static void WriteUnsignedVarInt(GrowableBuffer output, long value)
    {
        var span = output.GetSpan(10);
        output.Advance(Varint.WriteUnsigned(span, (ulong)value));
    }

    internal static void WriteSignedVarInt(GrowableBuffer output, long value)
    {
        var span = output.GetSpan(10);
        output.Advance(Varint.WriteSigned(span, value));
    }
}
