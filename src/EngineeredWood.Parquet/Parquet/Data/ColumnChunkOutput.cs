// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Where <see cref="ColumnChunkOutput.EmitPage(PageHeader, ReadOnlySpan{byte})"/> put a page.
/// </summary>
/// <param name="Offset">Offset of the page's header from the start of the column chunk.</param>
/// <param name="Size">Size of the page in bytes, header included.</param>
/// <param name="Ordinal">
/// The page's position among the chunk's data pages, counting from 0, or null for a dictionary or
/// symbol table page. It is what an OffsetIndex's <c>page_locations</c> is indexed by and what the
/// encryption AAD binds a data page to.
/// </param>
internal readonly record struct EmittedPage(int Offset, int Size, int? Ordinal);

/// <summary>
/// The bytes of one column chunk as its pages are written, and the running totals its
/// <see cref="ColumnMetaData"/> reports.
/// </summary>
/// <remarks>
/// Every page of every kind goes through <see cref="EmitPage(PageHeader, ReadOnlySpan{byte}, ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>,
/// so the page CRC, the header encoding and the placement are decided in one place. Page-index
/// writing reads each page's placement from here, and page encryption will wrap the header and
/// payload here.
/// </remarks>
internal sealed class ColumnChunkOutput
{
    private readonly MemoryStream _stream;
    private readonly bool _pageChecksumEnabled;
    private int _dataPageCount;

    public ColumnChunkOutput(int initialCapacity, ParquetWriteOptions options)
    {
        _stream = new MemoryStream(initialCapacity);
        _pageChecksumEnabled = options.PageChecksumEnabled;
    }

    /// <summary>Header plus uncompressed payload, summed over every page written.</summary>
    public int TotalUncompressedSize { get; private set; }

    /// <summary>Header plus payload as written, summed over every page written.</summary>
    public int TotalCompressedSize { get; private set; }

    /// <summary>The chunk's bytes so far.</summary>
    public ArraySegment<byte> Data
    {
        get
        {
            _stream.TryGetBuffer(out var buffer);
            return buffer;
        }
    }

    /// <summary>Writes a page whose payload is one contiguous span.</summary>
    public EmittedPage EmitPage(PageHeader header, ReadOnlySpan<byte> payload) =>
        EmitPage(header, default, default, payload);

    /// <summary>
    /// Writes a page: its header, then its payload, which is <paramref name="repetitionLevels"/>,
    /// <paramref name="definitionLevels"/> and <paramref name="values"/> in that order.
    /// </summary>
    /// <remarks>
    /// The payload is taken in three parts because a V2 data page stores its levels uncompressed
    /// ahead of its values, and they sit in separate buffers. Every other page passes its whole
    /// payload as <paramref name="values"/>. When checksums are on, the CRC covers the payload
    /// exactly as written, and it is filled into <paramref name="header"/> here.
    /// </remarks>
    public EmittedPage EmitPage(
        PageHeader header,
        ReadOnlySpan<byte> repetitionLevels,
        ReadOnlySpan<byte> definitionLevels,
        ReadOnlySpan<byte> values)
    {
        int payloadLength = repetitionLevels.Length + definitionLevels.Length + values.Length;
        if (payloadLength != header.CompressedPageSize)
        {
            throw new InvalidOperationException(
                $"A {header.Type} page declares {header.CompressedPageSize} payload bytes but carries {payloadLength}.");
        }

        if (_pageChecksumEnabled)
        {
            var crc = new System.IO.Hashing.Crc32();
            crc.Append(repetitionLevels);
            crc.Append(definitionLevels);
            crc.Append(values);
            Span<byte> hash = stackalloc byte[4];
            crc.GetHashAndReset(hash);
            header.Crc = unchecked((int)BinaryPrimitives.ReadUInt32LittleEndian(hash));
        }

        byte[] headerBytes = MetadataEncoder.EncodePageHeader(header);
        int offset = checked((int)_stream.Position);

        Write(headerBytes);
        Write(repetitionLevels);
        Write(definitionLevels);
        Write(values);

        TotalUncompressedSize += headerBytes.Length + header.UncompressedPageSize;
        TotalCompressedSize += headerBytes.Length + payloadLength;

        int? ordinal = header.Type is PageType.DataPage or PageType.DataPageV2 ? _dataPageCount++ : null;
        return new EmittedPage(offset, headerBytes.Length + payloadLength, ordinal);
    }

    private void Write(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
            return;

#if NET8_0_OR_GREATER
        _stream.Write(bytes);
#else
        // netstandard2.0's MemoryStream has no span overload. Growing the stream and copying into its
        // buffer avoids staging the bytes in an array first.
        int position = checked((int)_stream.Position);
        _stream.SetLength(position + bytes.Length);
        bytes.CopyTo(_stream.GetBuffer().AsSpan(position));
        _stream.Position = position + bytes.Length;
#endif
    }
}
