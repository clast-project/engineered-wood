// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Lance.Format;

/// <summary>
/// The Column Metadata Offset (CMO) and Global Buffer Offset (GBO) tables
/// share a single layout: a flat array of <c>{ position: u64, size: u64 }</c>
/// pairs, 16 bytes per entry, little-endian.
/// </summary>
internal readonly record struct OffsetSizeEntry(long Position, long Size)
{
    public const int Bytes = 16;

    public long End => Position + Size;

    public static OffsetSizeEntry[] ParseTable(ReadOnlySpan<byte> span, int count)
    {
        int expected = checked(count * Bytes);
        if (span.Length < expected)
            throw new LanceFormatException(
                $"Offset table is truncated: need {expected} bytes for {count} entries, got {span.Length}.");

        var result = new OffsetSizeEntry[count];
        for (int i = 0; i < count; i++)
        {
            int off = i * Bytes;
            long position = checked((long)BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(off, 8)));
            long size = checked((long)BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(off + 8, 8)));
            if (position < 0 || size < 0)
                throw new LanceFormatException(
                    $"Offset table entry {i} has out-of-range position/size values.");
            result[i] = new OffsetSizeEntry(position, size);
        }
        return result;
    }

    internal void WriteTo(Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(0, 8), (ulong)Position);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(8, 8), (ulong)Size);
    }
}
