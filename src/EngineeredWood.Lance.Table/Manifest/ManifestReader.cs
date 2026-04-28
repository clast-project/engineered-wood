// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.Lance.Table.Proto;
using Google.Protobuf;

namespace EngineeredWood.Lance.Table.Manifest;

/// <summary>
/// Reads and parses a single Lance manifest file.
///
/// <para>On-disk layout (verified empirically against pylance 4.x output):</para>
/// <code>
///   [u32 LE: transaction_length]
///   [Transaction proto bytes (transaction_length bytes)]
///   [u32 LE: manifest_length]
///   [Manifest proto bytes (manifest_length bytes)]
///   [u64 LE: position of the u32 manifest_length prefix]
///   [u16 LE: major version]
///   [u16 LE: minor version]
///   [4 bytes: ASCII "LANC" magic]
/// </code>
///
/// <para>We don't currently parse the embedded Transaction (its only Lance-
/// reader-relevant fields — uuid, operation kind — are also reachable via
/// <c>_transactions/</c> files when needed).</para>
/// </summary>
internal static class ManifestReader
{
    public static async ValueTask<Proto.Manifest> ReadAsync(
        ITableFileSystem fs, string path, CancellationToken cancellationToken = default)
    {
        byte[] bytes = await fs.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        return Parse(bytes, path);
    }

    public static Proto.Manifest Parse(ReadOnlySpan<byte> bytes, string pathForErrors)
    {
        if (bytes.Length < ManifestFooter.Size)
            throw new LanceTableFormatException(
                $"Manifest '{pathForErrors}' is too small ({bytes.Length} bytes; minimum {ManifestFooter.Size}).");

        ManifestFooter footer = ManifestFooter.Parse(bytes.Slice(bytes.Length - ManifestFooter.Size));
        int trailerStart = bytes.Length - ManifestFooter.Size;
        long manifestLenPos = footer.MetadataPosition;
        if (manifestLenPos < 0 || manifestLenPos + sizeof(uint) > trailerStart)
        {
            throw new LanceTableFormatException(
                $"Manifest '{pathForErrors}' has out-of-range manifest position {manifestLenPos} " +
                $"(file size {bytes.Length}).");
        }

        uint manifestLength = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
            bytes.Slice((int)manifestLenPos, sizeof(uint)));
        long manifestStart = manifestLenPos + sizeof(uint);
        long manifestEnd = checked(manifestStart + manifestLength);
        if (manifestEnd > trailerStart)
        {
            throw new LanceTableFormatException(
                $"Manifest '{pathForErrors}' length {manifestLength} extends past file " +
                $"(start={manifestStart}, trailer={trailerStart}).");
        }

        ReadOnlySpan<byte> manifestBytes = bytes.Slice((int)manifestStart, (int)manifestLength);
        try
        {
            return Proto.Manifest.Parser.ParseFrom(manifestBytes);
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new LanceTableFormatException(
                $"Failed to parse Manifest proto in '{pathForErrors}'.", ex);
        }
    }
}
