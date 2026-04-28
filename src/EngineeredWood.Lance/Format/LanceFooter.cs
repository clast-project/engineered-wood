// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Lance.Format;

/// <summary>
/// The 40-byte trailer at the end of every Lance v2.x file, little-endian:
/// <code>
/// u64 column_meta_start        // offset of the first ColumnMetadata blob
/// u64 cmo_table_offset         // offset of the Column Metadata Offset table
/// u64 gbo_table_offset         // offset of the Global Buffer Offset table
/// u32 num_global_buffers
/// u32 num_columns
/// u16 major_version
/// u16 minor_version
/// "LANC"                       // 4-byte magic
/// </code>
/// </summary>
internal readonly record struct LanceFooter(
    long ColumnMetaStart,
    long CmoTableOffset,
    long GboTableOffset,
    int NumGlobalBuffers,
    int NumColumns,
    LanceVersion Version)
{
    /// <summary>The footer is a fixed 40 bytes.</summary>
    public const int Size = 40;

    /// <summary>ASCII <c>"LANC"</c>, the last 4 bytes of every valid file.</summary>
    public static ReadOnlySpan<byte> Magic => "LANC"u8;

    /// <summary>
    /// Parses a 40-byte footer from <paramref name="span"/>, which must start at
    /// the footer boundary (not at end-of-file). Throws
    /// <see cref="LanceFormatException"/> if the magic is wrong or any size
    /// field is obviously invalid.
    /// </summary>
    public static LanceFooter Parse(ReadOnlySpan<byte> span)
    {
        if (span.Length != Size)
            throw new ArgumentException(
                $"Footer span must be exactly {Size} bytes, got {span.Length}.",
                nameof(span));

        if (!span.Slice(36, 4).SequenceEqual(Magic))
            throw new LanceFormatException(
                "File does not end with the Lance magic bytes \"LANC\".");

        long columnMetaStart = checked((long)BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(0, 8)));
        long cmoTableOffset = checked((long)BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(8, 8)));
        long gboTableOffset = checked((long)BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(16, 8)));
        uint numGlobalBuffers = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(24, 4));
        uint numColumns = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(28, 4));
        ushort major = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(32, 2));
        ushort minor = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(34, 2));

        if (numGlobalBuffers > int.MaxValue)
            throw new LanceFormatException(
                $"Number of global buffers ({numGlobalBuffers}) exceeds supported limit.");
        if (numColumns > int.MaxValue)
            throw new LanceFormatException(
                $"Number of columns ({numColumns}) exceeds supported limit.");

        return new LanceFooter(
            columnMetaStart,
            cmoTableOffset,
            gboTableOffset,
            (int)numGlobalBuffers,
            (int)numColumns,
            new LanceVersion(major, minor));
    }

    /// <summary>
    /// Serializes this footer to a 40-byte buffer. Primarily used by tests
    /// that hand-craft minimal Lance files.
    /// </summary>
    internal void WriteTo(Span<byte> destination)
    {
        if (destination.Length < Size)
            throw new ArgumentException(
                $"Destination must be at least {Size} bytes.", nameof(destination));

        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(0, 8), (ulong)ColumnMetaStart);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(8, 8), (ulong)CmoTableOffset);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(16, 8), (ulong)GboTableOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(24, 4), (uint)NumGlobalBuffers);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(28, 4), (uint)NumColumns);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(32, 2), Version.Major);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(34, 2), Version.Minor);
        Magic.CopyTo(destination.Slice(36, 4));
    }
}
