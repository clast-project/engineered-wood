// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.IO;
using EngineeredWood.Compression;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Writer;

/// <summary>
/// Accumulates data-segment bytes for a Vortex file. Maintains the
/// <see cref="SegmentLocator"/> list that becomes <c>Footer.segment_specs</c>.
/// Each segment is written at its required alignment (2^alignment_exponent
/// bytes) by inserting padding zeros before the segment when necessary.
///
/// <para>Currently only writes uncompressed segments; vortex's writer doesn't
/// emit segment-level compression and the spec lists those fields as reserved
/// for future use.</para>
/// </summary>
internal sealed class SegmentWriter
{
    private readonly Stream _stream;
    private readonly List<SegmentLocator> _segments = new();

    public SegmentWriter(Stream stream)
    {
        _stream = stream;
    }

    public IReadOnlyList<SegmentLocator> SegmentSpecs => _segments;

    /// <summary>Current absolute byte position in the underlying stream.</summary>
    public long Position => _stream.Position;

    /// <summary>
    /// Pads to the requested alignment, writes <paramref name="data"/>, and
    /// records the segment. Returns the segment index (0-based) — the value
    /// used as a SegmentRef in layout segment-id fields.
    /// </summary>
    public uint AppendSegment(byte[] data, byte alignmentExponent)
    {
        AlignTo(alignmentExponent);
        ulong offset = checked((ulong)_stream.Position);
        _stream.Write(data, 0, data.Length);
        _segments.Add(new SegmentLocator(
            offset, checked((uint)data.Length), alignmentExponent,
            CompressionCodec.Uncompressed));
        return checked((uint)(_segments.Count - 1));
    }

    /// <summary>
    /// Writes <paramref name="data"/> as a postscript-resident block (DType /
    /// Layout / Statistics / Footer). Returns the offset where it was written
    /// and the byte length, for inclusion in <c>Postscript.{dtype, layout,
    /// statistics, footer}</c>. Postscript segments are unaligned (vortex's
    /// writer always sets <c>alignment_exponent = 0</c>).
    /// </summary>
    public PostscriptBlock AppendPostscriptBlock(byte[] data)
    {
        ulong offset = checked((ulong)_stream.Position);
        _stream.Write(data, 0, data.Length);
        return new PostscriptBlock(offset, checked((uint)data.Length), AlignmentExponent: 0);
    }

    /// <summary>Writes raw bytes (e.g., the leading/trailing VTXF magic, EndOfFile struct).</summary>
    public void WriteRaw(byte[] bytes) => _stream.Write(bytes, 0, bytes.Length);

    private void AlignTo(byte alignmentExponent)
    {
        if (alignmentExponent == 0) return;
        int alignment = 1 << alignmentExponent;
        long pos = _stream.Position;
        long aligned = (pos + alignment - 1) & ~(long)(alignment - 1);
        long padding = aligned - pos;
        for (long i = 0; i < padding; i++) _stream.WriteByte(0);
    }
}

internal readonly record struct PostscriptBlock(ulong Offset, uint Length, byte AlignmentExponent);
