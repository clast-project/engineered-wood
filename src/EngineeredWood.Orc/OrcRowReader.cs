// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Google.Protobuf;
using EngineeredWood.Orc.ColumnReaders;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.IO;
using EngineeredWood.Orc.Proto;
namespace EngineeredWood.Orc;

/// <summary>
/// Reads row data from an ORC file, producing Apache Arrow RecordBatches.
/// Iterates stripe by stripe, batch by batch.
/// </summary>
public sealed class OrcRowReader : IAsyncEnumerable<RecordBatch>
{
    private readonly OrcReader _orcReader;
    private readonly IRandomAccessFile _file;
    private readonly OrcReaderOptions _options;
    private readonly Schema _arrowSchema;
    private readonly HashSet<int> _selectedColumnIds;
    private readonly List<OrcSchema> _selectedTopLevelColumns;

    internal OrcRowReader(OrcReader orcReader, IRandomAccessFile file, OrcReaderOptions options)
    {
        _orcReader = orcReader;
        _file = file;
        _options = options;
        _selectedColumnIds = orcReader.Schema.GetColumnIds(options.Columns);

        // Build selected top-level columns list
        _selectedTopLevelColumns = [];
        foreach (var child in orcReader.Schema.Children)
        {
            if (_selectedColumnIds.Contains(child.ColumnId))
                _selectedTopLevelColumns.Add(child);
        }

        // Build Arrow schema for selected columns
        var fields = new List<Field>();
        foreach (var col in _selectedTopLevelColumns)
        {
            fields.Add(new Field(col.Name ?? $"col_{col.ColumnId}", col.ToArrowType(), nullable: true));
        }
        _arrowSchema = new Schema(fields, null);
    }

    public Schema ArrowSchema => _arrowSchema;

    public async IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        for (int stripeIdx = 0; stripeIdx < _orcReader.NumberOfStripes; stripeIdx++)
        {
            var stripeInfo = _orcReader.GetStripe(stripeIdx);
            using var stripeData = await ReadStripeAsync(stripeInfo, cancellationToken).ConfigureAwait(false);

            long rowsInStripe = (long)stripeInfo.NumberOfRows;
            long rowsRead = 0;

            // Create column readers once per stripe, reuse across batches
            var columnReaders = CreateColumnReaders(stripeData);

            while (rowsRead < rowsInStripe)
            {
                int batchSize = (int)Math.Min(_options.BatchSize, rowsInStripe - rowsRead);
                var batch = ReadBatchFromReaders(columnReaders, batchSize);
                rowsRead += batchSize;
                yield return batch;
            }
        }
    }

    /// <summary>
    /// Determines whether selective column I/O should be used (only a subset of columns selected).
    /// </summary>
    private bool UseSelectiveRead =>
        _options.Columns is { Count: > 0 };

    private Task<StripeData> ReadStripeAsync(StripeInformation stripeInfo, CancellationToken cancellationToken)
    {
        return UseSelectiveRead
            ? ReadStripeSelectiveAsync(stripeInfo, cancellationToken)
            : ReadStripeFullAsync(stripeInfo, cancellationToken);
    }

    /// <summary>
    /// Fast path: reads the entire stripe as one contiguous read. Used when all columns are selected.
    /// </summary>
    private async Task<StripeData> ReadStripeFullAsync(StripeInformation stripeInfo, CancellationToken cancellationToken)
    {
        long stripeStart = (long)stripeInfo.Offset;
        long indexLength = (long)stripeInfo.IndexLength;
        long dataLength = (long)stripeInfo.DataLength;
        long footerLength = (long)stripeInfo.FooterLength;
        long totalLength = indexLength + dataLength + footerLength;

        // Read the entire stripe into memory
        var stripeOwner = await _file.ReadAsync(new FileRange(stripeStart, totalLength), cancellationToken).ConfigureAwait(false);

        // Get the backing array to avoid copying — fall back to ToArray() if not array-backed
        byte[] array;
        int arrayOffset;
        IMemoryOwner<byte>? owner;
        if (MemoryMarshal.TryGetArray<byte>(stripeOwner.Memory, out var segment))
        {
            array = segment.Array!;
            arrayOffset = segment.Offset;
            owner = stripeOwner;
        }
        else
        {
            array = stripeOwner.Memory.Span.ToArray();
            arrayOffset = 0;
            stripeOwner.Dispose();
            owner = null;
        }

        // Parse stripe footer
        var footerStart = arrayOffset + (int)(indexLength + dataLength);
        StripeFooter stripeFooter;
        if (_orcReader.Compression == CompressionKind.None)
        {
            stripeFooter = StripeFooter.Parser.ParseFrom(
                new ReadOnlySpan<byte>(array, footerStart, (int)footerLength));
        }
        else
        {
            var compressed = new ReadOnlySpan<byte>(array, footerStart, (int)footerLength);
            var decompressed = OrcCompression.Decompress(_orcReader.Compression, compressed, (int)_orcReader.CompressionBlockSize);
            stripeFooter = StripeFooter.Parser.ParseFrom(decompressed);
        }

        return new StripeData(owner, array, arrayOffset, stripeFooter, indexLength, dataLength);
    }

    /// <summary>
    /// Selective path: reads only the stripe footer first, then fetches only streams for selected columns.
    /// </summary>
    private async Task<StripeData> ReadStripeSelectiveAsync(StripeInformation stripeInfo, CancellationToken cancellationToken)
    {
        const long CoalesceGap = 1024 * 1024; // 1 MB

        long stripeStart = (long)stripeInfo.Offset;
        long indexLength = (long)stripeInfo.IndexLength;
        long dataLength = (long)stripeInfo.DataLength;
        long footerLength = (long)stripeInfo.FooterLength;

        // Step 1: Read just the stripe footer (small read at end of stripe)
        long footerFileOffset = stripeStart + indexLength + dataLength;
        var footerOwner = await _file.ReadAsync(new FileRange(footerFileOffset, footerLength), cancellationToken).ConfigureAwait(false);

        StripeFooter stripeFooter;
        try
        {
            if (MemoryMarshal.TryGetArray<byte>(footerOwner.Memory, out var footerSeg))
            {
                if (_orcReader.Compression == CompressionKind.None)
                {
                    stripeFooter = StripeFooter.Parser.ParseFrom(
                        new ReadOnlySpan<byte>(footerSeg.Array!, footerSeg.Offset, footerSeg.Count));
                }
                else
                {
                    var compressed = new ReadOnlySpan<byte>(footerSeg.Array!, footerSeg.Offset, footerSeg.Count);
                    var decompressed = OrcCompression.Decompress(_orcReader.Compression, compressed, (int)_orcReader.CompressionBlockSize);
                    stripeFooter = StripeFooter.Parser.ParseFrom(decompressed);
                }
            }
            else
            {
                var footerBytes = footerOwner.Memory.Span;
                if (_orcReader.Compression == CompressionKind.None)
                {
                    stripeFooter = StripeFooter.Parser.ParseFrom(footerBytes.ToArray());
                }
                else
                {
                    var decompressed = OrcCompression.Decompress(_orcReader.Compression, footerBytes, (int)_orcReader.CompressionBlockSize);
                    stripeFooter = StripeFooter.Parser.ParseFrom(decompressed);
                }
            }
        }
        finally
        {
            footerOwner.Dispose();
        }

        // Step 2: Walk streams, identify which belong to selected columns
        // Collect (stripeOffset, length, streamIndex) for needed streams
        var neededStreams = new List<(long StripeOffset, long Length, int StreamIndex)>();
        long streamOffset = 0;

        for (int i = 0; i < stripeFooter.Streams.Count; i++)
        {
            var stream = stripeFooter.Streams[i];
            int colId = (int)stream.Column;
            long length = (long)stream.Length;

            bool isIndexStream = stream.Kind is Proto.Stream.Types.Kind.RowIndex
                or Proto.Stream.Types.Kind.BloomFilter
                or Proto.Stream.Types.Kind.BloomFilterUtf8;

            if (!isIndexStream && _selectedColumnIds.Contains(colId) && length > 0)
            {
                neededStreams.Add((streamOffset, length, i));
            }

            streamOffset += length;
        }

        // Step 3: Build file ranges and coalesce nearby ones
        if (neededStreams.Count == 0)
        {
            // No data streams needed — return empty stripe data
            return new StripeData(
                new List<IMemoryOwner<byte>>(),
                new Dictionary<long, (byte[] Buffer, int Offset)>(),
                stripeFooter, indexLength, dataLength);
        }

        // Build raw file ranges for each needed stream
        var rawRanges = new List<FileRange>(neededStreams.Count);
        for (int i = 0; i < neededStreams.Count; i++)
        {
            var (sOffset, sLength, _) = neededStreams[i];
            rawRanges.Add(new FileRange(stripeStart + sOffset, sLength));
        }

        // Coalesce ranges that are within the gap threshold
        var coalescedRanges = new List<FileRange> { rawRanges[0] };
        var coalescedGroups = new List<List<int>> { new() { 0 } };

        for (int i = 1; i < rawRanges.Count; i++)
        {
            var last = coalescedRanges[^1];
            if (last.IsWithinGap(rawRanges[i], CoalesceGap))
            {
                coalescedRanges[^1] = last.Merge(rawRanges[i]);
                coalescedGroups[^1].Add(i);
            }
            else
            {
                coalescedRanges.Add(rawRanges[i]);
                coalescedGroups.Add(new List<int> { i });
            }
        }

        // Step 4: Read only the needed ranges
        var readResult = await _file.ReadRangesAsync(coalescedRanges, cancellationToken).ConfigureAwait(false);

        // Step 5: Build the stream map: stripeOffset → (buffer, offsetInBuffer)
        var rangeOwners = new List<IMemoryOwner<byte>>(readResult.Count);
        var streamMap = new Dictionary<long, (byte[] Buffer, int Offset)>(neededStreams.Count);

        for (int ci = 0; ci < coalescedRanges.Count; ci++)
        {
            var coalescedRange = coalescedRanges[ci];
            var memOwner = readResult[ci];
            rangeOwners.Add(memOwner);

            byte[] bufArray;
            int bufBase;
            if (MemoryMarshal.TryGetArray<byte>(memOwner.Memory, out var seg))
            {
                bufArray = seg.Array!;
                bufBase = seg.Offset;
            }
            else
            {
                bufArray = memOwner.Memory.Span.ToArray();
                bufBase = 0;
            }

            // Map each stream in this coalesced group
            foreach (var streamIdx in coalescedGroups[ci])
            {
                var (sOffset, sLength, _) = neededStreams[streamIdx];
                long fileOffset = stripeStart + sOffset;
                int offsetWithinBuffer = bufBase + (int)(fileOffset - coalescedRange.Offset);
                streamMap[sOffset] = (bufArray, offsetWithinBuffer);
            }
        }

        return new StripeData(rangeOwners, streamMap, stripeFooter, indexLength, dataLength);
    }

    private RecordBatch ReadBatchFromReaders(Dictionary<int, ColumnReader> columnReaders, int batchSize)
    {
        var arrays = new List<IArrowArray>();
        foreach (var col in _selectedTopLevelColumns)
        {
            if (columnReaders.TryGetValue(col.ColumnId, out var reader))
            {
                arrays.Add(reader.ReadBatch(batchSize));
            }
        }

        return new RecordBatch(_arrowSchema, arrays, batchSize);
    }

    private Dictionary<int, ColumnReader> CreateColumnReaders(StripeData stripeData)
    {
        var allReaders = new Dictionary<int, ColumnReader>();

        // Recursively create column readers for all selected columns (including nested)
        foreach (var col in _selectedTopLevelColumns)
        {
            ColumnReader.Create(col, stripeData.Footer.Columns, allReaders);
        }

        // Assign streams to column readers
        // All streams (index + data) are listed sequentially in the stripe footer,
        // laid out starting from offset 0 within the stripe bytes.
        long streamOffset = 0;

        foreach (var stream in stripeData.Footer.Streams)
        {
            int colId = (int)stream.Column;
            long length = (long)stream.Length;

            // Skip index streams — we only care about data streams
            bool isIndexStream = stream.Kind is Proto.Stream.Types.Kind.RowIndex
                or Proto.Stream.Types.Kind.BloomFilter
                or Proto.Stream.Types.Kind.BloomFilterUtf8;

            if (!isIndexStream && allReaders.TryGetValue(colId, out var columnReader) && length > 0)
            {
                var dataStream = CreateOrcStream(stripeData, streamOffset, length);
                AssignStream(columnReader, stream.Kind, dataStream, stripeData.Footer, colId);
            }

            streamOffset += length;
        }

        return allReaders;
    }

    private OrcByteStream CreateOrcStream(StripeData stripeData, long streamOffset, long length)
    {
        byte[] array;
        int absoluteOffset;

        if (stripeData.StreamMap != null)
        {
            // Selective read path: look up stream location from map
            var (buf, off) = stripeData.StreamMap[streamOffset];
            array = buf;
            absoluteOffset = off;
        }
        else
        {
            // Full-stripe read path: contiguous array
            array = stripeData.Array;
            absoluteOffset = stripeData.ArrayOffset + (int)streamOffset;
        }

        if (_orcReader.Compression == CompressionKind.None)
        {
            return new OrcByteStream(array, absoluteOffset, (int)length);
        }

        var compressed = new ReadOnlySpan<byte>(array, absoluteOffset, (int)length);
        var (buffer, decompressedLength) = OrcCompression.DecompressPooled(
            _orcReader.Compression, compressed, (int)_orcReader.CompressionBlockSize);
        stripeData.TrackPooledBuffer(buffer);
        return new OrcByteStream(buffer, 0, decompressedLength);
    }

    private static void AssignStream(ColumnReader columnReader, Proto.Stream.Types.Kind streamKind, OrcByteStream stream, StripeFooter footer, int columnId)
    {
        switch (columnReader)
        {
            case BooleanColumnReader b:
                if (streamKind == Proto.Stream.Types.Kind.Present) b.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Data) b.SetDataStream(stream);
                break;

            case ByteColumnReader by:
                if (streamKind == Proto.Stream.Types.Kind.Present) by.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Data) by.SetDataStream(stream);
                break;

            case IntegerColumnReader ic:
                if (streamKind == Proto.Stream.Types.Kind.Present) ic.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Data) ic.SetDataStream(stream);
                break;

            case FloatColumnReader fc:
                if (streamKind == Proto.Stream.Types.Kind.Present) fc.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Data) fc.SetDataStream(stream);
                break;

            case DoubleColumnReader dc:
                if (streamKind == Proto.Stream.Types.Kind.Present) dc.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Data) dc.SetDataStream(stream);
                break;

            case StringColumnReader sc:
                if (streamKind == Proto.Stream.Types.Kind.Present) sc.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Data) sc.SetDataStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Length) sc.SetLengthStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.DictionaryData)
                {
                    sc.SetDictionaryDataStream(stream);
                    // Set dictionary size from column encoding
                    if (columnId < footer.Columns.Count)
                        sc.SetDictionarySize((int)footer.Columns[columnId].DictionarySize);
                }
                break;

            case BinaryColumnReader bc:
                if (streamKind == Proto.Stream.Types.Kind.Present) bc.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Data) bc.SetDataStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Length) bc.SetLengthStream(stream);
                break;

            case DateColumnReader dtc:
                if (streamKind == Proto.Stream.Types.Kind.Present) dtc.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Data) dtc.SetDataStream(stream);
                break;

            case TimestampColumnReader tsc:
                if (streamKind == Proto.Stream.Types.Kind.Present) tsc.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Data) tsc.SetDataStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Secondary) tsc.SetSecondaryStream(stream);
                break;

            case DecimalColumnReader dec:
                if (streamKind == Proto.Stream.Types.Kind.Present) dec.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Data) dec.SetDataStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Secondary) dec.SetSecondaryStream(stream);
                break;

            case StructColumnReader str:
                if (streamKind == Proto.Stream.Types.Kind.Present) str.SetPresentStream(stream);
                break;

            case ListColumnReader lst:
                if (streamKind == Proto.Stream.Types.Kind.Present) lst.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Length) lst.SetLengthStream(stream);
                break;

            case MapColumnReader map:
                if (streamKind == Proto.Stream.Types.Kind.Present) map.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Length) map.SetLengthStream(stream);
                break;

            case UnionColumnReader union:
                if (streamKind == Proto.Stream.Types.Kind.Present) union.SetPresentStream(stream);
                else if (streamKind == Proto.Stream.Types.Kind.Data) union.SetDataStream(stream);
                break;
        }
    }

    private sealed class StripeData : IDisposable
    {
        private readonly IMemoryOwner<byte>? _owner;
        private readonly List<IMemoryOwner<byte>>? _rangeOwners;
        private List<byte[]>? _pooledBuffers;
        public byte[] Array { get; }
        public int ArrayOffset { get; }
        public StripeFooter Footer { get; }
        public long IndexLength { get; }
        public long DataLength { get; }

        /// <summary>
        /// For selective reads: maps a stream's offset (within the stripe) to (array, offset within array).
        /// Null when using the contiguous full-stripe read path.
        /// </summary>
        public Dictionary<long, (byte[] Buffer, int Offset)>? StreamMap { get; }

        public StripeData(IMemoryOwner<byte>? owner, byte[] array, int arrayOffset,
            StripeFooter footer, long indexLength, long dataLength)
        {
            _owner = owner;
            Array = array;
            ArrayOffset = arrayOffset;
            Footer = footer;
            IndexLength = indexLength;
            DataLength = dataLength;
        }

        public StripeData(List<IMemoryOwner<byte>> rangeOwners,
            Dictionary<long, (byte[] Buffer, int Offset)> streamMap,
            StripeFooter footer, long indexLength, long dataLength)
        {
            _rangeOwners = rangeOwners;
            StreamMap = streamMap;
            Array = System.Array.Empty<byte>();
            ArrayOffset = 0;
            Footer = footer;
            IndexLength = indexLength;
            DataLength = dataLength;
        }

        public void TrackPooledBuffer(byte[] buffer)
        {
            _pooledBuffers ??= new List<byte[]>();
            _pooledBuffers.Add(buffer);
        }

        public void Dispose()
        {
            _owner?.Dispose();
            if (_rangeOwners != null)
            {
                foreach (var ro in _rangeOwners)
                    ro.Dispose();
            }
            if (_pooledBuffers != null)
            {
                foreach (var buf in _pooledBuffers)
                    ArrayPool<byte>.Shared.Return(buf);
            }
        }
    }
}
