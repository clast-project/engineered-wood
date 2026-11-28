// Copyright (c) clast-project. All rights reserved.
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
///   [u32 LE: index_section_length]      (only when the manifest has indices)
///   [IndexSection proto bytes]
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
/// <para>Each section before the manifest is a length-prefixed message that a manifest field points at by the
/// position of its length prefix: <c>index_section</c> (lance-table 14.0.3 <c>do_write_manifest</c> writes it with
/// <c>write_protobuf</c>, first, so pylance's is at position 0) and <c>transaction_section</c>. A manifest with no
/// <c>index_section</c> has no indices (<c>read_manifest_indexes</c>).</para>
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

    /// <summary>
    /// Reads a manifest and the index section it points at: null when the manifest has none, which lance reads as
    /// "no indices" (#991).
    /// </summary>
    public static async ValueTask<(Proto.Manifest Manifest, IndexSection? Indices)> ReadWithIndicesAsync(
        ITableFileSystem fs, string path, CancellationToken cancellationToken = default)
    {
        byte[] bytes = await fs.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        var manifest = Parse(bytes, path);
        return (manifest, ParseIndexSection(bytes, manifest, path));
    }

    /// <summary>
    /// The index section <paramref name="manifest"/> points at in its file's <paramref name="bytes"/>, or null when it
    /// points at none. The section, with its length prefix, must lie wholly before the manifest's length prefix and
    /// must not overlap any byte of the transaction section (either field, 23 or the deprecated 21), so it can never
    /// be read out of another section's bytes.
    /// </summary>
    public static IndexSection? ParseIndexSection(ReadOnlySpan<byte> bytes, Proto.Manifest manifest, string pathForErrors)
    {
        if (!manifest.HasIndexSection)
            return null;

        // Parse has checked the footer and that this position lies inside the file.
        long manifestLenPos = ManifestFooter.Parse(bytes.Slice(bytes.Length - ManifestFooter.Size)).MetadataPosition;
        ulong position = manifest.IndexSection;
        if (position > (ulong)manifestLenPos || (ulong)manifestLenPos - position < sizeof(uint))
            throw new LanceTableFormatException(
                $"Manifest '{pathForErrors}' has out-of-range index section position {position} " +
                $"(the manifest starts at {manifestLenPos}).");

        int start = (int)position;
        uint length = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(start, sizeof(uint)));
        long end = start + sizeof(uint) + (long)length;
        if (end > manifestLenPos)
            throw new LanceTableFormatException(
                $"Manifest '{pathForErrors}' index section length {length} runs into the manifest " +
                $"(start={start}, manifest={manifestLenPos}).");
        foreach (ulong? transaction in new ulong?[]
                 {
                     manifest.HasTransactionSection ? manifest.TransactionSection : null,
                     manifest.HasTransactionSectionDeprecated ? manifest.TransactionSectionDeprecated : null,
                 })
        {
            if (transaction is not { } t || t >= (ulong)manifestLenPos)
                continue;
            // The transaction's own length prefix and message, cut at the manifest; a prefix that does not fit
            // claims everything up to the manifest.
            long txnStart = (long)t;
            long txnEnd = manifestLenPos;
            if (manifestLenPos - txnStart >= sizeof(uint))
            {
                uint txnLength = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
                    bytes.Slice((int)txnStart, sizeof(uint)));
                txnEnd = System.Math.Min(manifestLenPos, txnStart + sizeof(uint) + (long)txnLength);
            }
            if (start < txnEnd && txnStart < end)
                throw new LanceTableFormatException(
                    $"Manifest '{pathForErrors}' index section [{start}, {end}) overlaps the transaction section " +
                    $"[{txnStart}, {txnEnd}).");
        }

        try
        {
            return IndexSection.Parser.ParseFrom(bytes.Slice(start + sizeof(uint), (int)length));
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new LanceTableFormatException(
                $"Failed to parse the IndexSection proto in '{pathForErrors}'.", ex);
        }
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
