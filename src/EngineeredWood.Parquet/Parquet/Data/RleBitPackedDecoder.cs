// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
    /// Reads <paramref name="destination"/>.Length values into it.
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
                // Guarded here rather than inside the kernel. AggressiveOptimization implies no
                // inlining, so an unguarded call costs a real call on every run that cannot use it —
                // MEASURED at 0.51 -> 0.85 ms per million level values, which is the whole of what
                // the bulk path wins elsewhere. Thirty-two is where the kernel's setup starts paying
                // for itself: EW writes a run header every eight values by default, and at one group
                // per run the kernel MEASURED slower than the loop below.
                if (toCopy >= MinBulkValues && _bitWidth > 1)
                {
                    int bulk = UnpackGroups(destination, offset, toCopy);
                    offset += bulk;
                    toCopy -= bulk;
                }

                // General path for remaining values or other bit widths
                ulong mask = (1UL << _bitWidth) - 1UL;
                for (int i = 0; i < toCopy; i++)
                {
                    int byteIdx = _bitPackedPos + (_bitOffset >> 3);
                    int bitIdx = _bitOffset & 7;
                    _bitOffset += _bitWidth;

                    destination[offset++] = (int)((ReadPackedWord(byteIdx) >> bitIdx) & mask);
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
                // Guarded here rather than inside the kernel. AggressiveOptimization implies no
                // inlining, so an unguarded call costs a real call on every run that cannot use it —
                // MEASURED at 0.51 -> 0.85 ms per million level values, which is the whole of what
                // the bulk path wins elsewhere. Thirty-two is where the kernel's setup starts paying
                // for itself: EW writes a run header every eight values by default, and at one group
                // per run the kernel MEASURED slower than the loop below.
                if (toCopy >= MinBulkValues && _bitWidth > 1)
                {
                    int bulk = UnpackGroups(destination, offset, toCopy);
                    offset += bulk;
                    toCopy -= bulk;
                }

                ulong mask = (1UL << _bitWidth) - 1UL;
                for (int i = 0; i < toCopy; i++)
                {
                    int byteIdx = _bitPackedPos + (_bitOffset >> 3);
                    int bitIdx = _bitOffset & 7;
                    _bitOffset += _bitWidth;

                    destination[offset++] = (byte)((ReadPackedWord(byteIdx) >> bitIdx) & mask);
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
                // Unpacked in bulk, then counted over what was produced: keeping the comparison
                // out of the kernel is what lets it stay a straight line.
                // Guarded here rather than inside the kernel. AggressiveOptimization implies no
                // inlining, so an unguarded call costs a real call on every run that cannot use it —
                // MEASURED at 0.51 -> 0.85 ms per million level values, which is the whole of what
                // the bulk path wins elsewhere. Thirty-two is where the kernel's setup starts paying
                // for itself: EW writes a run header every eight values by default, and at one group
                // per run the kernel MEASURED slower than the loop below.
                if (toCopy >= MinBulkValues && _bitWidth > 1)
                {
                    int bulk = UnpackGroups(destination, offset, toCopy);
                    for (int i = 0; i < bulk; i++)
                    {
                        if (destination[offset + i] == matchValue)
                            matchCount++;
                    }

                    offset += bulk;
                    toCopy -= bulk;
                }

                ulong mask = (1UL << _bitWidth) - 1UL;
                for (int i = 0; i < toCopy; i++)
                {
                    int byteIdx = _bitPackedPos + (_bitOffset >> 3);
                    int bitIdx = _bitOffset & 7;
                    _bitOffset += _bitWidth;

                    byte val = (byte)((ReadPackedWord(byteIdx) >> bitIdx) & mask);
                    destination[offset++] = val;
                    if (val == matchValue) matchCount++;
                    _remaining--;
                }
            }
        }
    }

    /// <summary>
    /// Reads <paramref name="destination"/>.Length values into it and simultaneously
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
                // See the byte overload: unpacked in bulk, counted afterwards.
                // Guarded here rather than inside the kernel. AggressiveOptimization implies no
                // inlining, so an unguarded call costs a real call on every run that cannot use it —
                // MEASURED at 0.51 -> 0.85 ms per million level values, which is the whole of what
                // the bulk path wins elsewhere. Thirty-two is where the kernel's setup starts paying
                // for itself: EW writes a run header every eight values by default, and at one group
                // per run the kernel MEASURED slower than the loop below.
                if (toCopy >= MinBulkValues && _bitWidth > 1)
                {
                    int bulk = UnpackGroups(destination, offset, toCopy);
                    for (int i = 0; i < bulk; i++)
                    {
                        if (destination[offset + i] == matchValue)
                            matchCount++;
                    }

                    offset += bulk;
                    toCopy -= bulk;
                }

                ulong mask = (1UL << _bitWidth) - 1UL;
                for (int i = 0; i < toCopy; i++)
                {
                    int byteIdx = _bitPackedPos + (_bitOffset >> 3);
                    int bitIdx = _bitOffset & 7;
                    _bitOffset += _bitWidth;

                    int val = (int)((ReadPackedWord(byteIdx) >> bitIdx) & mask);
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

        // A 64-bit read, and a 64-bit mask. The comment that used to sit here said the 32-bit
        // version was "safe when bitWidth <= 24" and then used it at every width: at 27, 29, 30 and
        // 31 some values need more than 32 bits from their starting byte and were truncated, and at
        // 32 the mask itself was wrong, because shifting an int by 32 wraps to a shift by zero.
        ulong mask = (1UL << _bitWidth) - 1UL;
        return (int)((ReadPackedWord(byteIndex) >> bitIndex) & mask);
    }

    /// <summary>
    /// Values a bit-packed run must have left before the bulk path is worth entering. See the call
    /// sites: below this the kernel's setup costs more than it saves.
    /// </summary>
    private const int MinBulkValues = 32;

    /// <summary>
    /// Unpacks whole groups of eight values from the current bit-packed run, returning how many it
    /// produced. The caller finishes the remainder one value at a time.
    /// </summary>
    /// <remarks>
    /// <para>A bit-packed run is always a whole number of groups of eight, and eight values at
    /// <c>bitWidth</c> bits occupy exactly <c>bitWidth</c> bytes — so every group realigns to a byte
    /// boundary and the eight byte offsets and shifts within one depend only on the width. Hoisting
    /// them out of the loop turns a value into one unaligned read, one shift and one mask, where the
    /// loop below recomputes the address and re-checks the buffer for each.</para>
    /// <para>MEASURED at 1.78x the per-value loop on a real dictionary column read — but only where
    /// the literal runs are long. A bit-packed run holds as few as eight values, and how many it
    /// holds is the writer's choice: parquet-mr and arrow-rs batch up to 63 groups into one run,
    /// while EW itself emits a run header every eight values unless
    /// <c>ParquetWriteOptions.BatchBitPackedRuns</c> is set. At eight values a run there is exactly
    /// one group to unpack and the setup below does not pay for itself, which is what
    /// <see cref="MinBulkValues"/> keeps it out of.</para>
    /// <para>Deliberately NOT <c>AggressiveOptimization</c>, unlike the ALP unpacker this is modelled
    /// on. That attribute implies no inlining, and MEASURED, the resulting call in the middle of
    /// ReadBatch costs more on the runs that decline it than the kernel wins on the runs that use it
    /// — level decoding at width 1, which never enters here at all, slowed by 25%. Without it the
    /// win is identical (0.55 vs 0.56 ms per million values) and that cost is gone. An isolated
    /// harness said the opposite, measuring the same body at 13.6 GB/s with the attribute against
    /// 9.0 without; it had no surrounding method for the call to disturb.</para>
    /// </remarks>
    private int UnpackGroups(Span<int> destination, int offset, int count)
    {
        int groups = PlanGroups(count, out int baseByte);
        if (groups == 0)
            return 0;

        int width = _bitWidth;
        ulong mask = (1UL << width) - 1UL;
        int o1 = width >> 3, o2 = (2 * width) >> 3, o3 = (3 * width) >> 3;
        int o4 = (4 * width) >> 3, o5 = (5 * width) >> 3;
        int o6 = (6 * width) >> 3, o7 = (7 * width) >> 3;
        int s1 = width & 7, s2 = (2 * width) & 7, s3 = (3 * width) & 7;
        int s4 = (4 * width) & 7, s5 = (5 * width) & 7;
        int s6 = (6 * width) & 7, s7 = (7 * width) & 7;

        ref byte source = ref Unsafe.Add(ref MemoryMarshal.GetReference(_data), baseByte);

        for (int g = 0; g < groups; g++)
        {
            int b = g * width;
            int o = offset + (g * 8);
            destination[o] = (int)(Word(ref source, b) & mask);
            destination[o + 1] = (int)((Word(ref source, b + o1) >> s1) & mask);
            destination[o + 2] = (int)((Word(ref source, b + o2) >> s2) & mask);
            destination[o + 3] = (int)((Word(ref source, b + o3) >> s3) & mask);
            destination[o + 4] = (int)((Word(ref source, b + o4) >> s4) & mask);
            destination[o + 5] = (int)((Word(ref source, b + o5) >> s5) & mask);
            destination[o + 6] = (int)((Word(ref source, b + o6) >> s6) & mask);
            destination[o + 7] = (int)((Word(ref source, b + o7) >> s7) & mask);
        }

        int produced = groups * 8;
        _bitOffset += produced * width;
        _remaining -= produced;
        return produced;
    }

    /// <summary>Byte-destination counterpart of <see cref="UnpackGroups(Span{int}, int, int)"/>.</summary>
    private int UnpackGroups(Span<byte> destination, int offset, int count)
    {
        int groups = PlanGroups(count, out int baseByte);
        if (groups == 0)
            return 0;

        int width = _bitWidth;
        ulong mask = (1UL << width) - 1UL;
        int o1 = width >> 3, o2 = (2 * width) >> 3, o3 = (3 * width) >> 3;
        int o4 = (4 * width) >> 3, o5 = (5 * width) >> 3;
        int o6 = (6 * width) >> 3, o7 = (7 * width) >> 3;
        int s1 = width & 7, s2 = (2 * width) & 7, s3 = (3 * width) & 7;
        int s4 = (4 * width) & 7, s5 = (5 * width) & 7;
        int s6 = (6 * width) & 7, s7 = (7 * width) & 7;

        ref byte source = ref Unsafe.Add(ref MemoryMarshal.GetReference(_data), baseByte);

        for (int g = 0; g < groups; g++)
        {
            int b = g * width;
            int o = offset + (g * 8);
            destination[o] = (byte)(Word(ref source, b) & mask);
            destination[o + 1] = (byte)((Word(ref source, b + o1) >> s1) & mask);
            destination[o + 2] = (byte)((Word(ref source, b + o2) >> s2) & mask);
            destination[o + 3] = (byte)((Word(ref source, b + o3) >> s3) & mask);
            destination[o + 4] = (byte)((Word(ref source, b + o4) >> s4) & mask);
            destination[o + 5] = (byte)((Word(ref source, b + o5) >> s5) & mask);
            destination[o + 6] = (byte)((Word(ref source, b + o6) >> s6) & mask);
            destination[o + 7] = (byte)((Word(ref source, b + o7) >> s7) & mask);
        }

        int produced = groups * 8;
        _bitOffset += produced * width;
        _remaining -= produced;
        return produced;
    }

    /// <summary>
    /// How many whole groups the bulk path may take, and the byte it starts at. Zero when the run
    /// is not currently sitting on a group boundary, when the width is outside what one 64-bit read
    /// covers, or when the reads would run past the buffer — each of which the per-value loop
    /// handles safely.
    /// </summary>
    private readonly int PlanGroups(int count, out int baseByte)
    {
        baseByte = 0;

        int width = _bitWidth;
        if (width is < 2 or > 32 || count < 8)
            return 0;

        // Every group of eight consumes exactly `width` bytes, so a group boundary is also a byte
        // boundary. Anywhere else the shifts below would not be the ones computed here.
        if (_bitOffset % (width * 8) != 0)
            return 0;

        baseByte = _bitPackedPos + (_bitOffset >> 3);

        // The last value of a group is read as a whole word, which for narrow widths reaches past
        // the group's own bytes.
        int reach = ((7 * width) >> 3) + 8;
        if (_data.Length < baseByte + reach)
            return 0;

        return Math.Min(count / 8, ((_data.Length - baseByte - reach) / width) + 1);
    }

    /// <summary>Reads a little-endian 64-bit word at a byte offset, aligned or not.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Word(ref byte source, int byteOffset)
    {
        ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref source, byteOffset));
        return BitConverter.IsLittleEndian ? value : BinaryPrimitives.ReverseEndianness(value);
    }

    /// <summary>
    /// Reads the 64-bit word holding a value, padding with zeroes past the end of the buffer.
    /// </summary>
    /// <remarks>
    /// Sixty-four bits, not thirty-two. A value starts up to seven bits into its first byte, so a
    /// 32-bit read only covers it while <c>bitIdx + bitWidth &lt;= 32</c> — at widths 27, 29, 30 and
    /// 31 some values in every group need more than that and were silently truncated. Unreachable
    /// through this writer, which abandons dictionary encoding long before an index needs 27 bits,
    /// but reachable by reading a file another implementation wrote.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly ulong ReadPackedWord(int byteIndex)
    {
        int remaining = _data.Length - byteIndex;
        if (remaining >= 8)
            return BinaryPrimitives.ReadUInt64LittleEndian(_data.Slice(byteIndex));

        ulong raw = 0;
        for (int i = 0; i < remaining; i++)
            raw |= (ulong)_data[byteIndex + i] << (i * 8);
        return raw;
    }
}
