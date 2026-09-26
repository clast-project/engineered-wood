// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet;

/// <summary>
/// Everything about writing a Parquet file that does not depend on how its column chunks were
/// encoded: the leading magic, placing each row group's chunks and bloom filters in the file, and
/// the footer.
/// </summary>
/// <remarks>
/// <see cref="ParquetFileWriter"/> and <see cref="BufferedParquetWriter"/> encode chunks differently
/// but lay the file out the same way, and both do it through this class. Anything added to the
/// layout, such as the page index or encryption, therefore goes here once rather than into each
/// writer.
/// </remarks>
internal sealed class ParquetFileAssembler
{
    private static readonly byte[] Par1Magic = "PAR1"u8.ToArray();

    private readonly ISequentialFile _file;
    private readonly ParquetWriteOptions _options;
    private readonly List<RowGroup> _rowGroups = new();

    public ParquetFileAssembler(ISequentialFile file, ParquetWriteOptions options)
    {
        _file = file;
        _options = options;
    }

    /// <summary>Whether the leading magic has been written.</summary>
    public bool HeaderWritten { get; private set; }

    /// <summary>Writes the leading magic, unless it has been written already.</summary>
    public async ValueTask WriteHeaderAsync(CancellationToken cancellationToken)
    {
        if (HeaderWritten)
            return;

        await _file.WriteAsync(Par1Magic, cancellationToken).ConfigureAwait(false);
        HeaderWritten = true;
    }

    /// <summary>
    /// Writes one row group's encoded column chunks, each followed by its bloom filter if it has one,
    /// and records the row group for the footer. The chunks must be in schema leaf order.
    /// </summary>
    public async ValueTask WriteRowGroupAsync(
        IReadOnlyList<ColumnChunkWriter.ColumnChunkResult> chunks,
        long numRows,
        CancellationToken cancellationToken)
    {
        var columnChunks = new ColumnChunk[chunks.Count];
        long totalByteSize = 0;
        long totalCompressedSize = 0;

        for (int i = 0; i < chunks.Count; i++)
        {
            var result = chunks[i];
            long chunkStart = _file.Position;

            await _file.WriteAsync(result.Data, cancellationToken).ConfigureAwait(false);

            // A dictionary page or an FSST symbol table page precedes the data pages; a chunk
            // has at most one of the two, since FSST is a non-dictionary encoding.
            long dataPageOffset = chunkStart + result.DictionaryPageSize + result.SymbolTablePageSize;
            long? dictionaryPageOffset = result.DictionaryPageSize > 0 ? chunkStart : null;
            long? symbolTablePageOffset = result.SymbolTablePageSize > 0 ? chunkStart : null;

            long? bloomFilterOffset = null;
            int? bloomFilterLength = null;
            if (result.BloomFilterData != null)
            {
                bloomFilterOffset = _file.Position;
                bloomFilterLength = result.BloomFilterData.Length;
                await _file.WriteAsync(result.BloomFilterData, cancellationToken).ConfigureAwait(false);
            }

            // The encoder leaves every offset at zero; only here is the chunk's position known.
            var meta = new ColumnMetaData
            {
                Type = result.MetaData.Type,
                Encodings = result.MetaData.Encodings,
                PathInSchema = result.MetaData.PathInSchema,
                Codec = result.MetaData.Codec,
                NumValues = result.MetaData.NumValues,
                TotalUncompressedSize = result.MetaData.TotalUncompressedSize,
                TotalCompressedSize = result.MetaData.TotalCompressedSize,
                DataPageOffset = dataPageOffset,
                DictionaryPageOffset = dictionaryPageOffset,
                Statistics = result.MetaData.Statistics,
                BloomFilterOffset = bloomFilterOffset,
                BloomFilterLength = bloomFilterLength,
                SymbolTablePageOffset = symbolTablePageOffset,
                SymbolTablePageLength = result.MetaData.SymbolTablePageLength,
            };

            columnChunks[i] = new ColumnChunk { FileOffset = chunkStart, MetaData = meta };
            totalByteSize += result.MetaData.TotalUncompressedSize;
            totalCompressedSize += result.MetaData.TotalCompressedSize;
        }

        _rowGroups.Add(new RowGroup
        {
            Columns = columnChunks,
            TotalByteSize = totalByteSize,
            NumRows = numRows,
            TotalCompressedSize = totalCompressedSize,
            Ordinal = checked((short)_rowGroups.Count),
        });
    }

    /// <summary>
    /// Writes the footer, its length and the trailing magic, then flushes. The leading magic must
    /// already have been written.
    /// </summary>
    public async ValueTask WriteFooterAsync(
        IReadOnlyList<SchemaElement> schema,
        IReadOnlyList<KeyValue>? keyValueMetadata,
        CancellationToken cancellationToken)
    {
        long totalRows = 0;
        foreach (var rg in _rowGroups)
            totalRows += rg.NumRows;

        var fileMetaData = new FileMetaData
        {
            Version = 2,
            Schema = schema,
            NumRows = totalRows,
            RowGroups = _rowGroups,
            CreatedBy = _options.CreatedBy,
            KeyValueMetadata = keyValueMetadata,
            ColumnOrders = ColumnOrderBuilder.Build(schema, _options.FloatColumnOrder),
        };

#pragma warning disable EWPARQUET0002 // Honoring the caller's opt-in; the experimental signal lives on the option itself.
        byte[] footerBytes = MetadataEncoder.EncodeFileMetaData(fileMetaData, writePathInSchema: !_options.OmitPathInSchema);
#pragma warning restore EWPARQUET0002
        await _file.WriteAsync(footerBytes, cancellationToken).ConfigureAwait(false);

        var footerLengthBytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(footerLengthBytes, footerBytes.Length);
        await _file.WriteAsync(footerLengthBytes, cancellationToken).ConfigureAwait(false);

        await _file.WriteAsync(Par1Magic, cancellationToken).ConfigureAwait(false);
        await _file.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
}
