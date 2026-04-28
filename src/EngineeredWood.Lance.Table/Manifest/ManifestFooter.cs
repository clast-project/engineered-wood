// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Lance.Table.Manifest;

/// <summary>
/// 16-byte trailer of every Lance manifest file. Layout:
/// <code>
///   [u64 LE: position of the file's Metadata proto]
///   [u16 LE: major version]
///   [u16 LE: minor version]
///   [4 bytes: ASCII "LANC" magic]
/// </code>
///
/// <para>The trailer u64 points to a <c>lance.file.Metadata</c> message,
/// not directly to the Manifest. The Manifest's offset is then read from
/// <c>Metadata.manifest_position</c>. See
/// <see cref="ManifestReader.Parse"/> for the two-hop walk.</para>
///
/// <para>This is similar in shape to <c>EngineeredWood.Lance.Format.LanceFooter</c>
/// (the data-file footer) but smaller — manifest files don't carry a
/// column-metadata offset table or global-buffer table.</para>
/// </summary>
internal readonly record struct ManifestFooter(
    long MetadataPosition,
    ushort MajorVersion,
    ushort MinorVersion)
{
    public const int Size = 16;
    public const uint Magic = 0x434E_414C; // "LANC" little-endian (read byte 0 = 'L')

    public static ManifestFooter Parse(ReadOnlySpan<byte> tail)
    {
        if (tail.Length < Size)
            throw new LanceTableFormatException(
                $"Manifest footer needs {Size} bytes, got {tail.Length}.");

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(tail.Slice(12, 4));
        if (magic != Magic)
            throw new LanceTableFormatException(
                $"Manifest footer magic 0x{magic:X8} does not match expected 0x{Magic:X8} (\"LANC\").");

        ulong metadataPos = BinaryPrimitives.ReadUInt64LittleEndian(tail.Slice(0, 8));
        ushort major = BinaryPrimitives.ReadUInt16LittleEndian(tail.Slice(8, 2));
        ushort minor = BinaryPrimitives.ReadUInt16LittleEndian(tail.Slice(10, 2));

        return new ManifestFooter(checked((long)metadataPos), major, minor);
    }
}
