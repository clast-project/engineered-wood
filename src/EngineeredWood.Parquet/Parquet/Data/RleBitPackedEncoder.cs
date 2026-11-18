// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#if NET8_0_OR_GREATER
using System.Runtime.Intrinsics;
#endif

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

    // Width-1 values packed to bits, for EncodeBits; kept across pages like _buffer.
    private byte[]? _bits;

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

    /// <summary>
    /// Whether <see cref="Encode"/> takes the packed path at width 1 when batching. Off, it encodes as at any
    /// other width; tests and the kernel harness compare the two.
    /// </summary>
    internal bool PackWidthOne { get; init; } = true;

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
    /// At width 1 with batching, it takes the packed path in <see cref="EncodeBits"/> instead, which keeps a short run
    /// in the literals where RLE would cost more.
    /// </summary>
    public void Encode(ReadOnlySpan<int> values)
    {
        if (_bitWidth == 0)
            return; // all values are 0, nothing to encode

        if (_bitWidth == 1 && _maxLiteralValues > 8 && PackWidthOne)
        {
            EncodeBits(values);
            return;
        }

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

            AppendRun(value, i - runStart);
        }

        // Emit remaining values as a final partial group (padding is safe at end of stream)
        FlushLiterals();
    }

    /// <summary>
    /// <see cref="Encode"/> at width 1, batched: definition levels of a flat column, RLE booleans. The values
    /// are packed to bits first, and at width 1 a bit-packed group is one byte of that packing, so a literal
    /// run is a byte copy. A run of identical values becomes RLE only where that is smaller than leaving it
    /// in the literals; see <see cref="RlePays"/>.
    /// </summary>
    /// <remarks>
    /// Like <see cref="Encode"/>, an RLE run starts only at a group boundary (literals go out in whole
    /// groups) but may end anywhere, so the group grid moves with each RLE run: groups are read at any bit
    /// offset, not only on byte boundaries.
    /// </remarks>
    private void EncodeBits(ReadOnlySpan<int> values)
    {
        int n = values.Length;
        int byteCount = (n + 7) >> 3;

        // 8 zero bytes past the packing, for ReadGroup and ReadWord to read into near the end.
        if (_bits == null || _bits.Length < byteCount + 8)
            _bits = new byte[byteCount + 8];
        PackBits(values, _bits);
        _bits.AsSpan(byteCount, 8).Clear();

        int maxGroups = _maxLiteralValues >> 3;
        int header = -1; // where the open literal run's header goes, or -1 when none is open
        int groups = 0;
        int p = 0;
        while (p < n)
        {
            int group = ReadGroup(_bits, p);
            if (group is 0 or 0xFF && n - p >= 8)
            {
                int run = RunLength(_bits, p, n, group & 1);
                bool before = groups > 0;
                // Literals after the run only ever count against RLE, so the look ahead is needed only when
                // the run pays without them and not with them.
                if (RlePays(run, before, literalsAfter: false, maxGroups)
                    && (RlePays(run, before, literalsAfter: true, maxGroups)
                        || !LiteralsFollow(_bits, p + run, n, group & 1, maxGroups)))
                {
                    CloseLiterals(header, groups);
                    header = -1;
                    groups = 0;
                    WriteRleRun(group & 1, run);
                    p += run;
                    continue;
                }
            }

            if (groups == 0)
            {
                EnsureCapacity(1 + maxGroups);
                header = _position++;
            }

            // Bits past n were packed as 0: the final group's padding.
            _buffer[_position++] = (byte)group;
            p += 8;
            if (++groups == maxGroups)
            {
                CloseLiterals(header, groups);
                groups = 0;
            }
        }

        CloseLiterals(header, groups);
    }

    /// <summary>
    /// Whether <paramref name="run"/> identical bits at a group boundary cost less as an RLE run than left in
    /// the literals. RLE costs its header and a value byte; left in, the run costs a byte per 8 bits and a
    /// share of a literal header per <paramref name="maxGroups"/> bytes. Either choice may also cost a whole
    /// literal header: RLE when it splits literals on both sides into two runs, leaving it in when it opens a
    /// literal run that nothing else shares. Ties stay literal.
    /// </summary>
    private static bool RlePays(int run, bool literalsBefore, bool literalsAfter, int maxGroups)
    {
        int rle = VarintLength((uint)run << 1) + 1 + (literalsBefore && literalsAfter ? 1 : 0);
        int literal = !literalsBefore && !literalsAfter ? 1 : 0;
        // rle < literal + (run / 8) * (maxGroups + 1) / maxGroups, in integers.
        return (long)(rle - literal) * 8 * maxGroups < (long)run * (maxGroups + 1);
    }

    /// <summary>
    /// Whether literals resume at <paramref name="q"/>, where a run of <paramref name="bit"/> just ended: not
    /// at the end of the stream, and not when the run starting there pays as RLE on its own. RLE runs that
    /// follow one another start each on a group boundary and need no literal header between them.
    /// </summary>
    private static bool LiteralsFollow(byte[] bits, int q, int n, int bit, int maxGroups) =>
        q < n && !RlePays(RunLength(bits, q, n, bit ^ 1), literalsBefore: false, literalsAfter: true, maxGroups);

    private void CloseLiterals(int header, int groups)
    {
        if (groups > 0)
            _buffer[header] = (byte)((groups << 1) | 1); // groups <= 63, so a one-byte varint
    }

    /// <summary>Packs width-1 values LSB first, eight to a byte; bits past the end are 0.</summary>
    private static void PackBits(ReadOnlySpan<int> values, byte[] bits)
    {
        int n = values.Length;
        int i = 0;
        ref int v = ref MemoryMarshal.GetReference(values);
#if NET8_0_OR_GREATER
        if (Vector256.IsHardwareAccelerated)
        {
            for (; i + 8 <= n; i += 8)
            {
                // Bit 0 of each value is what bit-packing at width 1 keeps; move it to the sign bit.
                var lanes = Vector256.ShiftLeft(Vector256.LoadUnsafe(ref v, (nuint)i), 31);
                bits[i >> 3] = (byte)Vector256.ExtractMostSignificantBits(lanes);
            }
        }
#endif
        for (; i + 8 <= n; i += 8)
        {
            int b = 0;
            for (int j = 0; j < 8; j++)
                b |= (Unsafe.Add(ref v, i + j) & 1) << j;
            bits[i >> 3] = (byte)b;
        }

        if (i < n)
        {
            int b = 0;
            for (int j = 0; i + j < n; j++)
                b |= (Unsafe.Add(ref v, i + j) & 1) << j;
            bits[i >> 3] = (byte)b;
        }
    }

    /// <summary>The 8 bits from bit <paramref name="p"/>, LSB first.</summary>
    private static int ReadGroup(byte[] bits, int p)
    {
        int i = p >> 3;
        return ((bits[i] | (bits[i + 1] << 8)) >> (p & 7)) & 0xFF;
    }

    /// <summary>The 64 bits from bit <paramref name="p"/>, LSB first.</summary>
    private static ulong ReadWord(byte[] bits, int p)
    {
        int i = p >> 3, s = p & 7;
        ulong word = BinaryPrimitives.ReadUInt64LittleEndian(bits.AsSpan(i, 8)) >> s;
        return s == 0 ? word : word | ((ulong)bits[i + 8] << (64 - s));
    }

    /// <summary>How many bits from <paramref name="p"/> equal <paramref name="bit"/>, stopping at <paramref name="n"/>.</summary>
    private static int RunLength(byte[] bits, int p, int n, int bit)
    {
        ulong flip = bit != 0 ? ulong.MaxValue : 0;
        int q = p;
        while (q < n)
        {
            ulong differ = ReadWord(bits, q) ^ flip;
            if (differ != 0)
                return Math.Min(q + BitOperations.TrailingZeroCount(differ), n) - p;
            q += 64;
        }

        return n - p;
    }

    private static int VarintLength(uint value)
    {
        int length = 1;
        while (value > 0x7F)
        {
            value >>= 7;
            length++;
        }

        return length;
    }

    /// <summary>
    /// Encodes a sequence already known as runs — <paramref name="values"/>[r] repeated
    /// <paramref name="lengths"/>[r] times — without materializing the values it stands for.
    ///
    /// <para>Byte-identical to <see cref="Encode"/> over the expanded sequence, which is what makes it a
    /// substitution rather than a second encoding: <see cref="Encode"/>'s only use of the expanded form is
    /// to REDISCOVER these runs. Adjacent runs holding the same value are the one case where the two part
    /// company — they encode as two RLE runs here and one there — so callers that can merge them should
    /// (<see cref="DictionaryEncoder"/> does). The other is width 1 with batching, where <see cref="Encode"/>
    /// takes its packed path; dictionary indices are width 1 only for a dictionary of two values.</para>
    /// </summary>
    public void EncodeRuns(ReadOnlySpan<int> values, ReadOnlySpan<int> lengths)
    {
        if (values.Length != lengths.Length)
        {
            throw new ArgumentException(
                $"Got {values.Length} run values and {lengths.Length} run lengths.", nameof(lengths));
        }

        BeginRuns();
        for (int r = 0; r < values.Length; r++)
            AppendRun(values[r], lengths[r]);
        EndRuns();
    }

    /// <summary>
    /// Starts a run-at-a-time encode, for a caller that discovers its runs as it goes rather than holding
    /// them all. Pair with <see cref="AppendRun"/> and <see cref="EndRuns"/>; nothing is complete until
    /// <see cref="EndRuns"/> flushes the trailing partial group.
    /// </summary>
    public void BeginRuns()
    {
        _pending ??= new int[_maxLiteralValues];
        _pendingCount = 0;
    }

    /// <summary>Finishes a <see cref="BeginRuns"/> sequence.</summary>
    public void EndRuns() => FlushLiterals();

    /// <summary>
    /// Emits one run of identical values: as an RLE run where that is legal and cheaper, otherwise through
    /// the literal buffer. Shared by every entry point so a run encodes the same way however it was found.
    /// </summary>
    public void AppendRun(int value, int runLength)
    {
        // The bit-width guard matches Encode's: at width 0 every value is 0 and the stream carries nothing,
        // so a run must not emit a header either.
        if (_bitWidth == 0 || runLength <= 0)
            return;

        if (runLength >= 8 && (_pendingCount & 7) == 0)
        {
            // At a group boundary: flush any whole groups pending, then the run is RLE. Unbatched, nothing is
            // ever pending here; batched, earlier groups may be, and taking the run into them instead would
            // bit-pack values depth 1 encodes as RLE, growing the stream.
            FlushLiterals();
            WriteRleRun(value, runLength);
            return;
        }

        // Add run values to pending, emitting whole groups of 8 as they become available
        int runPos = 0;
        while (runPos < runLength)
        {
            _pending![_pendingCount++] = value;
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
