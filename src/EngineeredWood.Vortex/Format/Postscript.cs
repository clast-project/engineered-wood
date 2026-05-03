// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Vortex.FlatBuffers;

namespace EngineeredWood.Vortex.Format;

/// <summary>
/// The compression scheme indicated by a per-segment <see cref="CompressionSpec"/>.
/// Mirrors the Vortex <c>CompressionScheme</c> FlatBuffers enum (footer.fbs).
/// </summary>
internal enum CompressionScheme : byte
{
    None = 0,
    LZ4 = 1,
    ZLib = 2,
    ZStd = 3,
}

/// <summary>
/// Reader for the <c>Postscript</c> FlatBuffers table from footer.fbs.
/// Slots: 0=dtype, 1=layout, 2=statistics, 3=footer.
/// </summary>
internal readonly ref struct Postscript
{
    private readonly FlatBufferTable _table;

    public Postscript(FlatBufferTable table) { _table = table; }

    public static Postscript ReadRoot(ReadOnlySpan<byte> buf) =>
        new(FlatBufferTable.ReadRoot(buf));

    public PostscriptSegment DType => new(_table.ReadTable(0));
    public PostscriptSegment Layout => new(_table.ReadTable(1));
    public PostscriptSegment Statistics => new(_table.ReadTable(2));
    public PostscriptSegment Footer => new(_table.ReadTable(3));
}

/// <summary>
/// Reader for the <c>PostscriptSegment</c> FlatBuffers table.
/// Slots: 0=offset, 1=length, 2=alignment_exponent, 3=_compression, 4=_encryption.
/// </summary>
internal readonly ref struct PostscriptSegment
{
    private readonly FlatBufferTable _table;

    public PostscriptSegment(FlatBufferTable table) { _table = table; }

    public bool IsPresent => !_table.IsNull;

    public ulong Offset => _table.ReadUInt64(0);
    public uint Length => _table.ReadUInt32(1);
    public byte AlignmentExponent => _table.ReadByte(2);
    public CompressionSpec Compression => new(_table.ReadTable(3));
    public EncryptionSpec Encryption => new(_table.ReadTable(4));
}

/// <summary>
/// Reader for <c>CompressionSpec</c>. Slot 0 = scheme (u8 enum).
/// </summary>
internal readonly ref struct CompressionSpec
{
    private readonly FlatBufferTable _table;

    public CompressionSpec(FlatBufferTable table) { _table = table; }

    public bool IsPresent => !_table.IsNull;

    public CompressionScheme Scheme =>
        (CompressionScheme)_table.ReadByte(0, (byte)CompressionScheme.None);
}

/// <summary>
/// Reader for the empty <c>EncryptionSpec</c> table. Reserved for future use;
/// readers should reject non-empty specs in phase 1.
/// </summary>
internal readonly ref struct EncryptionSpec
{
    private readonly FlatBufferTable _table;

    public EncryptionSpec(FlatBufferTable table) { _table = table; }

    public bool IsPresent => !_table.IsNull;
}
