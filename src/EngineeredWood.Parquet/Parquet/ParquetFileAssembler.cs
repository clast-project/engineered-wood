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

    // Page indexes wait for the footer: they are laid out together after the last row group, so that a
    // reader can fetch every index of a row group in one read. Each is a few bytes per page.
    private readonly List<PendingPageIndex> _pageIndexes = new();

    private readonly record struct PendingPageIndex(int RowGroup, int Column, long ChunkStart, ChunkPageIndex Index);

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
            if (result.PageIndex is { } pageIndex)
                _pageIndexes.Add(new PendingPageIndex(_rowGroups.Count, i, chunkStart, pageIndex));
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
    /// <param name="schema">The Parquet schema the row groups were encoded against.</param>
    /// <param name="arrowSchema">
    /// The Arrow schema as the caller declared it, before any unit rescaling, recorded under
    /// <c>ARROW:schema</c> when <see cref="ParquetWriteOptions.WriteArrowSchema"/> is set. Null when
    /// the writer never saw one.
    /// </param>
    /// <param name="cancellationToken">Cancels the write.</param>
    public async ValueTask WriteFooterAsync(
        IReadOnlyList<SchemaElement> schema,
        Apache.Arrow.Schema? arrowSchema,
        CancellationToken cancellationToken)
    {
        var keyValueMetadata = BuildKeyValueMetadata(arrowSchema);
        var rowGroups = _pageIndexes.Count == 0
            ? _rowGroups
            : await WritePageIndexesAsync(cancellationToken).ConfigureAwait(false);

        long totalRows = 0;
        foreach (var rg in rowGroups)
            totalRows += rg.NumRows;

        var fileMetaData = new FileMetaData
        {
            Version = 2,
            Schema = schema,
            NumRows = totalRows,
            RowGroups = rowGroups,
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

    /// <summary>
    /// Adds the <c>ARROW:schema</c> entry to whatever the caller supplied, so that units and zone
    /// names Parquet cannot express survive a round trip the way they do through PyArrow and Polars.
    /// A caller who sets the key explicitly keeps their own value.
    /// </summary>
    private IReadOnlyList<KeyValue>? BuildKeyValueMetadata(Apache.Arrow.Schema? arrowSchema)
    {
        var supplied = _options.KeyValueMetadata;
        if (!_options.WriteArrowSchema || arrowSchema is null)
            return supplied;

        if (supplied is not null)
        {
            foreach (var entry in supplied)
            {
                if (string.Equals(entry.Key, ArrowSchemaMetadata.Key, StringComparison.Ordinal))
                    return supplied;
            }
        }

        var merged = new List<KeyValue>(supplied?.Count + 1 ?? 1);
        if (supplied is not null)
            merged.AddRange(supplied);

        merged.Add(new KeyValue
        {
            Key = ArrowSchemaMetadata.Key,
            Value = ArrowSchemaMetadata.Encode(arrowSchema),
        });
        return merged;
    }

    /// <summary>
    /// Writes every ColumnIndex, then every OffsetIndex, each in row-group and column order (parquet-mr's
    /// layout), and returns the row groups with ColumnChunk fields 4-7 filled in.
    /// </summary>
    private async ValueTask<List<RowGroup>> WritePageIndexesAsync(CancellationToken cancellationToken)
    {
        var columnIndexes = new Dictionary<(int, int), (long Offset, int Length)>();
        foreach (var pending in _pageIndexes)
        {
            if (pending.Index.ColumnIndex is not { } columnIndex)
                continue;

            columnIndexes[(pending.RowGroup, pending.Column)] = await WritePageIndexStructureAsync(
                MetadataEncoder.EncodeColumnIndex(columnIndex), cancellationToken).ConfigureAwait(false);
        }

        var offsetIndexes = new Dictionary<(int, int), (long Offset, int Length)>();
        foreach (var pending in _pageIndexes)
        {
            // The encoder recorded page offsets from the start of the chunk; only now is that known.
            var locations = new PageLocation[pending.Index.PageLocations.Count];
            for (int p = 0; p < locations.Length; p++)
            {
                var location = pending.Index.PageLocations[p];
                locations[p] = location with { Offset = pending.ChunkStart + location.Offset };
            }

            offsetIndexes[(pending.RowGroup, pending.Column)] = await WritePageIndexStructureAsync(
                MetadataEncoder.EncodeOffsetIndex(new OffsetIndex { PageLocations = locations }),
                cancellationToken).ConfigureAwait(false);
        }

        var rowGroups = new List<RowGroup>(_rowGroups.Count);
        for (int g = 0; g < _rowGroups.Count; g++)
        {
            var rowGroup = _rowGroups[g];
            var columns = new ColumnChunk[rowGroup.Columns.Count];
            for (int c = 0; c < columns.Length; c++)
            {
                var chunk = rowGroup.Columns[c];
                bool hasOffsetIndex = offsetIndexes.TryGetValue((g, c), out var offsetIndex);
                bool hasColumnIndex = columnIndexes.TryGetValue((g, c), out var columnIndex);
                columns[c] = new ColumnChunk
                {
                    FilePath = chunk.FilePath,
                    FileOffset = chunk.FileOffset,
                    MetaData = chunk.MetaData,
                    OffsetIndexOffset = hasOffsetIndex ? offsetIndex.Offset : null,
                    OffsetIndexLength = hasOffsetIndex ? offsetIndex.Length : null,
                    ColumnIndexOffset = hasColumnIndex ? columnIndex.Offset : null,
                    ColumnIndexLength = hasColumnIndex ? columnIndex.Length : null,
                };
            }

            rowGroups.Add(new RowGroup
            {
                Columns = columns,
                TotalByteSize = rowGroup.TotalByteSize,
                NumRows = rowGroup.NumRows,
                SortingColumns = rowGroup.SortingColumns,
                TotalCompressedSize = rowGroup.TotalCompressedSize,
                Ordinal = rowGroup.Ordinal,
            });
        }

        return rowGroups;
    }

    /// <summary>
    /// Writes one ColumnIndex or OffsetIndex and returns where it went. Every page-index structure goes
    /// through here, so that encryption can wrap them (module types 6 and 7) in one place.
    /// </summary>
    private async ValueTask<(long Offset, int Length)> WritePageIndexStructureAsync(
        byte[] bytes, CancellationToken cancellationToken)
    {
        long offset = _file.Position;
        await _file.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        return (offset, bytes.Length);
    }
}
