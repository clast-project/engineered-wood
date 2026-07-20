// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Parses a single <c>vortex.flat</c> segment into its component pieces:
/// the <see cref="ArrayMessage"/> root FlatBuffer, the per-buffer descriptors
/// (<see cref="BufferDescriptor"/>), and a precomputed offset table giving
/// each buffer's byte position within the segment.
///
/// <para>Wire format (per <c>vortex-array/src/serde.rs</c>):
/// <c>[buf0_padding][buf0_bytes][buf1_padding][buf1_bytes]...[fb_padding?][Array FB bytes][u32 fb_length LE]</c>.
/// The <see cref="BufferDescriptor.Padding"/> field is the gap *before* that
/// buffer; the buffer's own data follows for <see cref="BufferDescriptor.Length"/>
/// bytes.</para>
/// </summary>
internal readonly ref struct SerializedArray
{
    private readonly ReadOnlySpan<byte> _segment;
    private readonly ReadOnlySpan<byte> _flatBuffer;
    private readonly int[] _bufferOffsets;

    public SerializedArray(
        ReadOnlySpan<byte> segment,
        ReadOnlySpan<byte> flatBuffer,
        int[] bufferOffsets)
    {
        _segment = segment;
        _flatBuffer = flatBuffer;
        _bufferOffsets = bufferOffsets;
    }

    /// <summary>The full segment bytes (including data buffers and trailing FB).</summary>
    public ReadOnlySpan<byte> Segment => _segment;

    /// <summary>The Array message FlatBuffer, sliced from the segment.</summary>
    public ReadOnlySpan<byte> FlatBuffer => _flatBuffer;

    /// <summary>Byte offset within the segment of buffer index <paramref name="i"/>'s data.</summary>
    public int BufferOffset(int i) => _bufferOffsets[i];

    public ArrayMessage Message => ArrayMessage.ReadRoot(_flatBuffer);

    public static SerializedArray Parse(ReadOnlySpan<byte> segment)
    {
        if (segment.Length < 4)
            throw new VortexFormatException(
                $"vortex.flat segment is too small ({segment.Length} bytes) to contain the trailing fb_length.");

        var fbLength = (int)BinaryPrimitives.ReadUInt32LittleEndian(segment.Slice(segment.Length - 4));
        if (fbLength <= 0 || fbLength + 4 > segment.Length)
            throw new VortexFormatException(
                $"vortex.flat segment trailing fb_length {fbLength} is invalid (segment is {segment.Length} bytes).");

        var fbStart = segment.Length - 4 - fbLength;
        var fb = segment.Slice(fbStart, fbLength);

        var msg = ArrayMessage.ReadRoot(fb);
        var bufferCount = msg.BufferCount;
        var bufferOffsets = bufferCount == 0 ? Array.Empty<int>() : new int[bufferCount];

        long pos = 0;
        for (int i = 0; i < bufferCount; i++)
        {
            var buf = msg.Buffer(i);
            pos += buf.Padding;
            if (pos > fbStart)
                throw new VortexFormatException(
                    $"Buffer {i} starts at {pos} which is past the FlatBuffer at {fbStart}.");
            bufferOffsets[i] = (int)pos;
            pos += buf.Length;
            if (pos > fbStart)
                throw new VortexFormatException(
                    $"Buffer {i} ends at {pos} which is past the FlatBuffer at {fbStart}.");
        }

        return new SerializedArray(segment, fb, bufferOffsets);
    }

    public ReadOnlySpan<byte> BufferBytes(int i)
    {
        var offset = _bufferOffsets[i];
        var len = checked((int)Message.Buffer(i).Length);
        return _segment.Slice(offset, len);
    }
}
