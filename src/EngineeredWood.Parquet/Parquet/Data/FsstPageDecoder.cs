// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using EngineeredWood.Compression;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes the values section of an FSST data page against the column chunk's shared
/// symbol table. Mirror of <see cref="FsstPageEncoder"/>.
/// </summary>
internal static class FsstPageDecoder
{
    /// <summary>
    /// Reads a SYMBOL_TABLE_PAGE body into the table its column chunk's data pages decode
    /// against.
    /// </summary>
    public static FsstSymbolTable ReadSymbolTablePage(
        PageHeader header, ReadOnlySpan<byte> compressedData, ColumnMetaData columnMeta)
    {
        var pageHeader = header.SymbolTablePageHeader
            ?? throw new ParquetFormatException(
                "Symbol table page is missing its symbol_table_page_header.");

        // A page flagged compressed under an Uncompressed codec is contradictory, but there is
        // no codec to invoke: the bytes can only be plain, and rejecting the file would refuse
        // one that is perfectly readable.
        if (!pageHeader.IsCompressed || columnMeta.Codec == CompressionCodec.Uncompressed)
            return FsstSymbolTable.Parse(pageHeader.Type, compressedData);

        int size = header.UncompressedPageSize;
        if (size < 0)
            throw new ParquetFormatException(
                $"Symbol table page declares a negative uncompressed_page_size ({size}).");

        byte[] buffer = ArrayPool<byte>.Shared.Rent(Math.Max(size, 1));
        try
        {
            // The written count is checked rather than assumed: a pooled buffer arrives holding
            // whatever the last renter left in it, so slicing to the DECLARED size after a short
            // decompression would feed Parse stale bytes — which it could conceivably validate
            // as a well-formed table. That is the silent-wrong-data case §3.7 exists to stop.
            int written = Decompressor.Decompress(columnMeta.Codec, compressedData, buffer);
            if (written != size)
                throw new ParquetFormatException(
                    $"Symbol table page declares uncompressed_page_size {size} but decompressed " +
                    $"to {written} bytes.");

            return FsstSymbolTable.Parse(pageHeader.Type, buffer.AsSpan(0, size));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Decodes <paramref name="count"/> values and appends them to <paramref name="state"/>.
    /// </summary>
    public static void Decode(
        ReadOnlySpan<byte> data, FsstSymbolTable table, int count, ColumnBuildState state)
    {
        if (data.Length < FsstPageEncoder.HeaderSize)
            throw new ParquetFormatException(
                $"FSST page is {data.Length} bytes, too small for the " +
                $"{FsstPageEncoder.HeaderSize}-byte header.");

        byte offsetEncodingByte = data[0];
        if (offsetEncodingByte is not ((byte)FsstOffsetEncoding.Plain
            or (byte)FsstOffsetEncoding.DeltaBinaryPacked))
            throw new ParquetFormatException(
                $"FSST page uses offset_encoding {offsetEncodingByte}; only 0 (PLAIN) and " +
                "1 (DELTA_BINARY_PACKED) are defined.");
        var offsetEncoding = (FsstOffsetEncoding)offsetEncodingByte;

        int numValues = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(1, 4));
        int offsetArrayLength = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(5, 4));

        if (numValues != count)
            throw new ParquetFormatException(
                $"FSST page header num_values ({numValues}) does not match the page's " +
                $"non-null value count ({count}).");
        if (offsetArrayLength < 0 ||
            offsetArrayLength > data.Length - FsstPageEncoder.HeaderSize)
            throw new ParquetFormatException(
                $"FSST page offset_array_length ({offsetArrayLength}) does not fit in the " +
                $"remaining {data.Length - FsstPageEncoder.HeaderSize} bytes.");

        var offsetBytes = data.Slice(FsstPageEncoder.HeaderSize, offsetArrayLength);
        var dataSection = data.Slice(FsstPageEncoder.HeaderSize + offsetArrayLength);

        if (count == 0)
            return;

        var endOffsets = ArrayPool<int>.Shared.Rent(count);
        try
        {
            ReadEndOffsets(offsetBytes, offsetEncoding, endOffsets.AsSpan(0, count));
            DecodeValues(dataSection, endOffsets.AsSpan(0, count), table, count, state);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(endOffsets);
        }
    }

    private static void ReadEndOffsets(
        ReadOnlySpan<byte> offsetBytes, FsstOffsetEncoding encoding, Span<int> destination)
    {
        if (encoding == FsstOffsetEncoding.Plain)
        {
            int needed = destination.Length * 4;
            if (offsetBytes.Length < needed)
                throw new ParquetFormatException(
                    $"FSST page has {offsetBytes.Length} bytes of PLAIN offsets but needs {needed} " +
                    $"for {destination.Length} values.");

            for (int i = 0; i < destination.Length; i++)
                destination[i] = BinaryPrimitives.ReadInt32LittleEndian(offsetBytes.Slice(i * 4, 4));
        }
        else
        {
            var decoder = new DeltaBinaryPackedDecoder(offsetBytes);
            decoder.DecodeInt32s(destination);
        }
    }

    private static void DecodeValues(
        ReadOnlySpan<byte> dataSection, ReadOnlySpan<int> endOffsets,
        FsstSymbolTable table, int count, ColumnBuildState state)
    {
        // §10.2: end offsets are only useful if they are sane. Validate monotonicity and the
        // upper bound up front so every slice below is known to be in range.
        int previous = 0;
        for (int i = 0; i < count; i++)
        {
            int end = endOffsets[i];
            if (end < previous)
                throw new ParquetFormatException(
                    $"FSST page end offsets decrease at value {i} ({end} < {previous}).");
            if (end > dataSection.Length)
                throw new ParquetFormatException(
                    $"FSST page end offset {end} at value {i} runs past the " +
                    $"{dataSection.Length}-byte data section.");
            previous = end;
        }

        int compressedTotal = previous;

        // Per-value compressed lengths drive the batch decompress, which hands back the
        // destination offsets — already the shape Arrow wants for its offsets buffer.
        var lengths = ArrayPool<int>.Shared.Rent(count);
        var valueOffsets = ArrayPool<int>.Shared.Rent(count + 1);
        byte[]? decompressed = null;
        try
        {
            previous = 0;
            for (int i = 0; i < count; i++)
            {
                int end = endOffsets[i];
                lengths[i] = end - previous;

                // Validated per value, not over the concatenated stream: an escape marker at the
                // end of one value must not be able to borrow the next value's first byte as its
                // literal (§5.2).
                table.ValidateCodeStream(dataSection.Slice(previous, end - previous));
                previous = end;
            }

            int capacity = table.MaxDecompressedLength(compressedTotal);
            decompressed = ArrayPool<byte>.Shared.Rent(Math.Max(capacity, 1));

            if (!table.TryDecompressBatch(
                    dataSection.Slice(0, compressedTotal), lengths.AsSpan(0, count),
                    decompressed, valueOffsets.AsSpan(0, count + 1), out int totalWritten))
                throw new ParquetFormatException(
                    $"FSST decompression failed for a page of {count} values " +
                    $"({compressedTotal} compressed bytes).");

            state.AddByteArrayValues(
                valueOffsets.AsSpan(0, count + 1), decompressed.AsSpan(0, totalWritten), count);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(lengths);
            ArrayPool<int>.Shared.Return(valueOffsets);
            if (decompressed != null)
                ArrayPool<byte>.Shared.Return(decompressed);
        }
    }
}
