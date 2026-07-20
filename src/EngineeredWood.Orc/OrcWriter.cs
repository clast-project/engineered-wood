// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Google.Protobuf;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Orc.BloomFilter;
using EngineeredWood.Orc.ColumnWriters;
using EngineeredWood.Orc.Proto;
using Type = EngineeredWood.Orc.Proto.Type;

namespace EngineeredWood.Orc;

/// <summary>
/// Writes Arrow RecordBatches as an ORC file.
/// </summary>
public sealed class OrcWriter : IAsyncDisposable, IDisposable
{
    private static readonly byte[] OrcMagic = "ORC"u8.ToArray();
    private const uint WriterCode = 6; // External writer convention
    private const string SoftwareVersion = "EngineeredWood.Orc 0.1";

    private readonly ISequentialFile _file;
    private readonly bool _ownsFile;
    private readonly OrcWriterOptions _options;
    private readonly Schema _arrowSchema;
    private readonly List<ColumnWriter> _columnWriters;   // all writers, flat, column-id order
    private readonly List<ColumnWriter> _topLevelWriters; // 1:1 with schema fields, for WriteBatch
    private readonly List<StripeInformation> _stripes = [];
    private readonly List<ColumnStatistics> _fileStatistics = [];
    private readonly List<StripeStatistics> _stripeStatistics = [];

    // Row index state: one RowIndex per column per stripe (+ root struct at index 0)
    private List<RowIndex>? _currentStripeRowIndexes;
    private List<ulong>? _pendingPositions; // reusable buffer for position collection
    private bool _needsRowGroupStart; // deferred position recording

    private long _totalRows;
    private long _currentStripeRows;
    private long _currentRowGroupRows;
    private bool _headerWritten;
    private bool _closed;

    private bool RowIndexEnabled => _options.RowIndexStride > 0;

    /// <summary>
    /// Creates an ORC writer that writes to the given <see cref="ISequentialFile"/>.
    /// </summary>
    /// <param name="file">The sequential file to write to.</param>
    /// <param name="arrowSchema">The Arrow schema for the data to be written.</param>
    /// <param name="ownsFile">If true, the file will be disposed when this writer is disposed.</param>
    /// <param name="options">Write options. Defaults to <see cref="OrcWriterOptions"/> defaults.</param>
    public OrcWriter(ISequentialFile file, Schema arrowSchema, bool ownsFile = true, OrcWriterOptions? options = null)
    {
        _file = file;
        _ownsFile = ownsFile;
        _options = options ?? new OrcWriterOptions();
        _arrowSchema = arrowSchema;
        _columnWriters = [];
        _topLevelWriters = [];

        // Create column writers for each top-level field
        // Column 0 is the root struct
        int nextId = 1;
        foreach (var field in arrowSchema.FieldsList)
        {
            int topLevelIndex = _columnWriters.Count;
            var (writers, next) = ColumnWriter.Create(field, nextId, _options);
            _columnWriters.AddRange(writers);
            _topLevelWriters.Add(_columnWriters[topLevelIndex]);
            nextId = next;
        }

        // Set up bloom filters for requested columns
        if (_options.BloomFilterColumns != null)
        {
            var writerKind = _options.BloomFilterHashVariant == OrcBloomHashVariant.Cpp
                ? OrcWriterKind.Cpp : OrcWriterKind.Java;

            for (int i = 0; i < _topLevelWriters.Count; i++)
            {
                var fieldName = arrowSchema.FieldsList[i].Name;
                if (_options.BloomFilterColumns.Contains(fieldName))
                {
                    _topLevelWriters[i].BloomFilter = new OrcBloomFilterBuilder(
                        Math.Max(1, _options.RowIndexStride), _options.BloomFilterFpp, writerKind);
                }
            }
        }
    }

    /// <summary>
    /// Creates an ORC writer that writes to a local file.
    /// </summary>
    public static OrcWriter Create(string path, Schema arrowSchema, OrcWriterOptions? options = null)
    {
        var file = new LocalSequentialFile(path);
        return new OrcWriter(file, arrowSchema, ownsFile: true, options);
    }

    /// <summary>
    /// Writes a RecordBatch to the ORC file. Data is buffered until a stripe is full,
    /// at which point the stripe is flushed to the output using async I/O.
    /// </summary>
    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken cancellationToken = default)
    {
        if (_closed) throw new ObjectDisposedException(nameof(OrcWriter));
        if (batch.Schema.FieldsList.Count != _arrowSchema.FieldsList.Count)
            throw new ArgumentException("Batch schema does not match writer schema.");

        // Lazy header write
        if (!_headerWritten)
        {
            await _file.WriteAsync(OrcMagic, cancellationToken).ConfigureAwait(false);
            _headerWritten = true;

            if (RowIndexEnabled)
                StartNewStripeRowIndex();
        }

        // Record positions at the start of a new row group (deferred from previous boundary)
        if (RowIndexEnabled && _needsRowGroupStart)
        {
            RecordCurrentPositions();
            _needsRowGroupStart = false;
        }

        for (int i = 0; i < _topLevelWriters.Count; i++)
        {
            _topLevelWriters[i].Write(batch.Column(i));
        }

        _currentStripeRows += batch.Length;
        _currentRowGroupRows += batch.Length;

        // Check if we've completed a row group
        if (RowIndexEnabled && _currentRowGroupRows >= _options.RowIndexStride)
        {
            FinishRowGroup();
            _needsRowGroupStart = true; // defer position recording to next WriteBatch
        }

        // Check if we should flush the stripe
        if (ShouldFlushStripe())
            await FlushStripeAsync(cancellationToken).ConfigureAwait(false);
    }

    private bool ShouldFlushStripe()
    {
        long buffered = 0;
        foreach (var w in _columnWriters)
            buffered += w.EstimateBufferedBytes();

        return buffered >= _options.StripeSize;
    }

    /// <summary>
    /// Closes the writer, flushing any remaining data and writing the file footer.
    /// </summary>
    public async ValueTask CloseAsync(CancellationToken cancellationToken = default)
    {
        if (_closed) return;
        _closed = true;

        // If nothing was ever written, still need the header
        if (!_headerWritten)
        {
            await _file.WriteAsync(OrcMagic, cancellationToken).ConfigureAwait(false);
            _headerWritten = true;
        }

        if (_currentStripeRows > 0)
            await FlushStripeAsync(cancellationToken).ConfigureAwait(false);

        await WriteFileTailAsync(cancellationToken).ConfigureAwait(false);
    }

    private void StartNewStripeRowIndex()
    {
        // Create RowIndex for root struct (column 0) + each column writer
        _currentStripeRowIndexes = new List<RowIndex>(_columnWriters.Count + 1);
        for (int i = 0; i <= _columnWriters.Count; i++)
            _currentStripeRowIndexes.Add(new RowIndex());

        _pendingPositions ??= new List<ulong>();
        _needsRowGroupStart = false;

        // Record initial positions for the first row group (all zeros)
        RecordCurrentPositions();
    }

    private void RecordCurrentPositions()
    {
        // Root struct (column 0): no streams, no positions
        _currentStripeRowIndexes![0].Entry.Add(new RowIndexEntry());

        // Each column writer
        for (int i = 0; i < _columnWriters.Count; i++)
        {
            var entry = new RowIndexEntry();
            _pendingPositions!.Clear();
            _columnWriters[i].GetStreamPositions(_pendingPositions);
            entry.Positions.AddRange(_pendingPositions);
            _currentStripeRowIndexes[i + 1].Entry.Add(entry);
        }
    }

    private void FinishRowGroup()
    {
        if (_currentRowGroupRows == 0) return;

        // Flush encoders so positions are accurate
        foreach (var w in _columnWriters)
            w.FlushEncoders();

        // Attach statistics to the current (last added) row index entries
        if (_options.EnableStatistics)
        {
            // Root struct entry
            var rootEntry = _currentStripeRowIndexes![0].Entry[^1];
            rootEntry.Statistics = new ColumnStatistics { NumberOfValues = (ulong)_currentRowGroupRows };

            // Column entries
            for (int i = 0; i < _columnWriters.Count; i++)
            {
                var entry = _currentStripeRowIndexes[i + 1].Entry[^1];
                entry.Statistics = _columnWriters[i].GetStatistics();
            }

            // Reset per-row-group statistics (but not streams)
            foreach (var w in _columnWriters)
                w.ResetStatistics();
        }

        // Finish bloom filter row groups
        foreach (var w in _columnWriters)
            w.FinishBloomFilterRowGroup();

        _currentRowGroupRows = 0;
    }

    private async ValueTask FlushStripeAsync(CancellationToken cancellationToken)
    {
        if (_currentStripeRows == 0) return;

        // Finalize the last row group in this stripe
        if (RowIndexEnabled && _currentRowGroupRows > 0)
            FinishRowGroup();

        long stripeStart = _file.Position;

        // Collect all data streams from all column writers
        var streams = new List<OrcStream>();
        foreach (var w in _columnWriters)
            w.GetStreams(streams);

        // Write index streams (RowIndex per column) before data
        var streamDescriptors = new List<Proto.Stream>();
        long indexLength = 0;

        if (RowIndexEnabled && _currentStripeRowIndexes != null)
        {
            // Translate positions for compressed streams if needed
            if (_options.Compression != CompressionKind.None)
                TranslateRowIndexPositions(streams);

            // Write RowIndex for each column (root struct = col 0, then col writers)
            for (int colIdx = 0; colIdx <= _columnWriters.Count; colIdx++)
            {
                var rowIndex = _currentStripeRowIndexes[colIdx];
                var indexBytes = rowIndex.ToByteArray();
                byte[] indexData;
                if (_options.Compression != CompressionKind.None)
                    indexData = OrcCompression.Compress(_options.Compression, indexBytes, _options.CompressionBlockSize, _options.CompressionLevel, _options.CustomCompressionLevel);
                else
                    indexData = indexBytes;

                await _file.WriteAsync(indexData, cancellationToken).ConfigureAwait(false);
                indexLength += indexData.Length;

                streamDescriptors.Add(new Proto.Stream
                {
                    Column = (uint)colIdx,
                    Kind = Proto.Stream.Types.Kind.RowIndex,
                    Length = (ulong)indexData.Length
                });
            }
        }

        // Write BloomFilterIndex streams for columns that have bloom filters
        foreach (var w in _columnWriters)
        {
            var bfi = w.GetBloomFilterIndex();
            if (bfi == null) continue;

            var bfiBytes = bfi.ToByteArray();
            byte[] bfiData;
            if (_options.Compression != CompressionKind.None)
                bfiData = OrcCompression.Compress(_options.Compression, bfiBytes, _options.CompressionBlockSize, _options.CompressionLevel, _options.CustomCompressionLevel);
            else
                bfiData = bfiBytes;

            await _file.WriteAsync(bfiData, cancellationToken).ConfigureAwait(false);
            indexLength += bfiData.Length;

            streamDescriptors.Add(new Proto.Stream
            {
                Column = (uint)w.ColumnId,
                Kind = Proto.Stream.Types.Kind.BloomFilterUtf8,
                Length = (ulong)bfiData.Length
            });
        }

        // Write data streams and track their sizes
        long dataLength = 0;

        foreach (var s in streams)
        {
            var rawSpan = s.Data.WrittenSpan;
            byte[]? compressedData = null;
            ReadOnlyMemory<byte> writeData;

            if (_options.Compression != CompressionKind.None)
            {
                compressedData = OrcCompression.Compress(_options.Compression, rawSpan, _options.CompressionBlockSize, _options.CompressionLevel, _options.CustomCompressionLevel);
                writeData = compressedData;
            }
            else
            {
                writeData = s.Data.WrittenMemory;
            }

            await _file.WriteAsync(writeData, cancellationToken).ConfigureAwait(false);
            dataLength += writeData.Length;

            var descriptor = new Proto.Stream
            {
                Column = (uint)s.ColumnId,
                Kind = s.Kind,
                Length = (ulong)writeData.Length
            };
            streamDescriptors.Add(descriptor);
        }

        // Build stripe footer
        var stripeFooter = new StripeFooter();
        stripeFooter.Streams.AddRange(streamDescriptors);

        // Add column encodings (column 0 = root struct, then each column)
        stripeFooter.Columns.Add(new ColumnEncoding { Kind = ColumnEncoding.Types.Kind.Direct }); // root struct
        foreach (var w in _columnWriters)
        {
            var encoding = w.GetEncoding();
            if (w.BloomFilter != null)
                encoding.BloomEncoding = 1;
            stripeFooter.Columns.Add(encoding);
        }

        // Serialize and optionally compress the stripe footer
        var footerBytes = stripeFooter.ToByteArray();
        byte[] footerData;
        if (_options.Compression != CompressionKind.None)
            footerData = OrcCompression.Compress(_options.Compression, footerBytes, _options.CompressionBlockSize, _options.CompressionLevel, _options.CustomCompressionLevel);
        else
            footerData = footerBytes;

        await _file.WriteAsync(footerData, cancellationToken).ConfigureAwait(false);

        // Record stripe info
        _stripes.Add(new StripeInformation
        {
            Offset = (ulong)stripeStart,
            IndexLength = (ulong)indexLength,
            DataLength = (ulong)dataLength,
            FooterLength = (ulong)footerData.Length,
            NumberOfRows = (ulong)_currentStripeRows
        });

        _totalRows += _currentStripeRows;

        // Collect statistics before resetting
        if (_options.EnableStatistics)
            AccumulateStatistics();

        _currentStripeRows = 0;
        _currentRowGroupRows = 0;

        // Reset column writers for next stripe
        foreach (var w in _columnWriters)
            w.Reset();

        // Start row index tracking for the next stripe
        if (RowIndexEnabled)
            StartNewStripeRowIndex();
    }

    private void TranslateRowIndexPositions(List<OrcStream> streams)
    {
        // For compressed files, translate uncompressed byte offsets in positions
        // to (compressed_block_start, decompressed_offset_within_block) pairs.

        // Build a lookup of stream data by (columnId, streamKind) for position translation
        var streamDataLookup = new Dictionary<(int, Proto.Stream.Types.Kind), byte[]>();
        foreach (var s in streams)
            streamDataLookup[(s.ColumnId, s.Kind)] = s.Data.WrittenSpan.ToArray();

        var layout = new List<int>();

        for (int colIdx = 0; colIdx < _columnWriters.Count; colIdx++)
        {
            var writer = _columnWriters[colIdx];
            var rowIndex = _currentStripeRowIndexes![colIdx + 1];

            // Get the position layout: how many extra positions after each byte offset
            layout.Clear();
            writer.GetPositionLayout(layout);

            // Get the streams for this column to know what raw data to use
            var colStreams = new List<OrcStream>();
            writer.GetStreams(colStreams);

            foreach (var entry in rowIndex.Entry)
            {
                var oldPositions = new List<ulong>(entry.Positions);
                entry.Positions.Clear();

                int posIdx = 0;
                for (int streamIdx = 0; streamIdx < layout.Count && posIdx < oldPositions.Count; streamIdx++)
                {
                    // First position for this stream is the byte offset
                    ulong uncompOffset = oldPositions[posIdx++];
                    int extras = layout[streamIdx];

                    // Find the raw data for this stream
                    if (streamIdx < colStreams.Count)
                    {
                        var cs = colStreams[streamIdx];
                        if (streamDataLookup.TryGetValue((cs.ColumnId, cs.Kind), out var rawData))
                        {
                            OrcCompression.TranslatePosition(
                                (long)uncompOffset, rawData,
                                _options.Compression, _options.CompressionBlockSize,
                                entry.Positions,
                                _options.CompressionLevel, _options.CustomCompressionLevel);
                        }
                        else
                        {
                            entry.Positions.Add(uncompOffset);
                        }
                    }
                    else
                    {
                        entry.Positions.Add(uncompOffset);
                    }

                    // Copy encoding-specific extras (RLE remaining, bit offset, etc.)
                    for (int e = 0; e < extras && posIdx < oldPositions.Count; e++)
                        entry.Positions.Add(oldPositions[posIdx++]);
                }
            }
        }
    }

    private void AccumulateStatistics()
    {
        // When row indexing is enabled, per-row-group stats have been collected via FinishRowGroup
        // and stats have been reset. We need to build stripe stats by merging row group stats.
        // When row indexing is disabled, column writers still have full-stripe stats.

        var stripeStats = new StripeStatistics();
        stripeStats.ColStats.Add(new ColumnStatistics { NumberOfValues = (ulong)_currentStripeRows });

        if (RowIndexEnabled && _currentStripeRowIndexes != null)
        {
            // Build stripe stats from row index entries
            for (int i = 0; i < _columnWriters.Count; i++)
            {
                var rowIndex = _currentStripeRowIndexes[i + 1];
                ColumnStatistics? merged = null;
                foreach (var entry in rowIndex.Entry)
                {
                    if (entry.Statistics == null) continue;
                    if (merged == null)
                        merged = entry.Statistics.Clone();
                    else
                        MergeStatistics(merged, entry.Statistics);
                }
                stripeStats.ColStats.Add(merged ?? new ColumnStatistics());
            }
        }
        else
        {
            // No row index — get stats directly from column writers
            foreach (var w in _columnWriters)
                stripeStats.ColStats.Add(w.GetStatistics());
        }

        _stripeStatistics.Add(stripeStats);

        if (_fileStatistics.Count == 0)
        {
            for (int i = 0; i < _columnWriters.Count; i++)
                _fileStatistics.Add(stripeStats.ColStats[i + 1].Clone());
            return;
        }

        for (int i = 0; i < _columnWriters.Count; i++)
        {
            MergeStatistics(_fileStatistics[i], stripeStats.ColStats[i + 1]);
        }
    }

    private static void MergeStatistics(ColumnStatistics target, ColumnStatistics source)
    {
        target.NumberOfValues += source.NumberOfValues;
        if (source.HasNull)
            target.HasNull = true;

        if (source.IntStatistics != null)
        {
            if (target.IntStatistics == null)
                target.IntStatistics = source.IntStatistics.Clone();
            else
            {
                if (source.IntStatistics.Minimum < target.IntStatistics.Minimum)
                    target.IntStatistics.Minimum = source.IntStatistics.Minimum;
                if (source.IntStatistics.Maximum > target.IntStatistics.Maximum)
                    target.IntStatistics.Maximum = source.IntStatistics.Maximum;
                target.IntStatistics.Sum += source.IntStatistics.Sum;
            }
        }

        if (source.DoubleStatistics != null)
        {
            if (target.DoubleStatistics == null)
                target.DoubleStatistics = source.DoubleStatistics.Clone();
            else
            {
                if (source.DoubleStatistics.Minimum < target.DoubleStatistics.Minimum)
                    target.DoubleStatistics.Minimum = source.DoubleStatistics.Minimum;
                if (source.DoubleStatistics.Maximum > target.DoubleStatistics.Maximum)
                    target.DoubleStatistics.Maximum = source.DoubleStatistics.Maximum;
                target.DoubleStatistics.Sum += source.DoubleStatistics.Sum;
            }
        }

        if (source.StringStatistics != null)
        {
            if (target.StringStatistics == null)
                target.StringStatistics = source.StringStatistics.Clone();
            else
            {
                if (string.Compare(source.StringStatistics.Minimum, target.StringStatistics.Minimum, StringComparison.Ordinal) < 0)
                    target.StringStatistics.Minimum = source.StringStatistics.Minimum;
                if (string.Compare(source.StringStatistics.Maximum, target.StringStatistics.Maximum, StringComparison.Ordinal) > 0)
                    target.StringStatistics.Maximum = source.StringStatistics.Maximum;
                target.StringStatistics.Sum += source.StringStatistics.Sum;
            }
        }

        if (source.BucketStatistics != null)
        {
            if (target.BucketStatistics == null)
                target.BucketStatistics = source.BucketStatistics.Clone();
            else
            {
                for (int j = 0; j < source.BucketStatistics.Count.Count; j++)
                    target.BucketStatistics.Count[j] += source.BucketStatistics.Count[j];
            }
        }

        if (source.DateStatistics != null)
        {
            if (target.DateStatistics == null)
                target.DateStatistics = source.DateStatistics.Clone();
            else
            {
                if (source.DateStatistics.Minimum < target.DateStatistics.Minimum)
                    target.DateStatistics.Minimum = source.DateStatistics.Minimum;
                if (source.DateStatistics.Maximum > target.DateStatistics.Maximum)
                    target.DateStatistics.Maximum = source.DateStatistics.Maximum;
            }
        }

        if (source.TimestampStatistics != null)
        {
            if (target.TimestampStatistics == null)
                target.TimestampStatistics = source.TimestampStatistics.Clone();
            else
            {
                if (source.TimestampStatistics.Minimum < target.TimestampStatistics.Minimum)
                    target.TimestampStatistics.Minimum = source.TimestampStatistics.Minimum;
                if (source.TimestampStatistics.Maximum > target.TimestampStatistics.Maximum)
                    target.TimestampStatistics.Maximum = source.TimestampStatistics.Maximum;
            }
        }

        if (source.BinaryStatistics != null)
        {
            if (target.BinaryStatistics == null)
                target.BinaryStatistics = source.BinaryStatistics.Clone();
            else
                target.BinaryStatistics.Sum += source.BinaryStatistics.Sum;
        }

        if (source.DecimalStatistics != null)
        {
            if (target.DecimalStatistics == null)
                target.DecimalStatistics = source.DecimalStatistics.Clone();
            else
            {
                if (string.Compare(source.DecimalStatistics.Minimum, target.DecimalStatistics.Minimum, StringComparison.Ordinal) < 0)
                    target.DecimalStatistics.Minimum = source.DecimalStatistics.Minimum;
                if (string.Compare(source.DecimalStatistics.Maximum, target.DecimalStatistics.Maximum, StringComparison.Ordinal) > 0)
                    target.DecimalStatistics.Maximum = source.DecimalStatistics.Maximum;
            }
        }

        if (source.CollectionStatistics != null)
        {
            if (target.CollectionStatistics == null)
                target.CollectionStatistics = source.CollectionStatistics.Clone();
            else
            {
                if (source.CollectionStatistics.MinChildren < target.CollectionStatistics.MinChildren)
                    target.CollectionStatistics.MinChildren = source.CollectionStatistics.MinChildren;
                if (source.CollectionStatistics.MaxChildren > target.CollectionStatistics.MaxChildren)
                    target.CollectionStatistics.MaxChildren = source.CollectionStatistics.MaxChildren;
                target.CollectionStatistics.TotalChildren += source.CollectionStatistics.TotalChildren;
            }
        }
    }

    private async ValueTask WriteFileTailAsync(CancellationToken cancellationToken)
    {
        long contentLength = _file.Position - OrcMagic.Length;

        // Write stripe-level statistics (Metadata section)
        long metadataLength = 0;
        if (_options.EnableStatistics && _stripeStatistics.Count > 0)
        {
            var metadata = new Metadata();
            metadata.StripeStats.AddRange(_stripeStatistics);

            var metadataBytes = metadata.ToByteArray();
            byte[] metadataData;
            if (_options.Compression != CompressionKind.None)
                metadataData = OrcCompression.Compress(_options.Compression, metadataBytes, _options.CompressionBlockSize, _options.CompressionLevel, _options.CustomCompressionLevel);
            else
                metadataData = metadataBytes;

            await _file.WriteAsync(metadataData, cancellationToken).ConfigureAwait(false);
            metadataLength = metadataData.Length;
        }

        // Build ORC type list from Arrow schema
        var types = BuildTypeList(_arrowSchema);

        // Build footer
        var footer = new Footer
        {
            HeaderLength = (ulong)OrcMagic.Length,
            ContentLength = (ulong)contentLength,
            NumberOfRows = (ulong)_totalRows,
            Writer = WriterCode,
            SoftwareVersion = SoftwareVersion,
        };

        if (RowIndexEnabled)
            footer.RowIndexStride = (uint)_options.RowIndexStride;

        footer.Stripes.AddRange(_stripes);
        footer.Types_.AddRange(types);

        // Add column statistics
        if (_options.EnableStatistics)
        {
            // Root struct stats
            footer.Statistics.Add(new ColumnStatistics { NumberOfValues = (ulong)_totalRows });
            // Per-column stats accumulated across stripes
            footer.Statistics.AddRange(_fileStatistics);
        }

        // Add user metadata
        if (_options.UserMetadata is { Count: > 0 })
        {
            foreach (var kvp in _options.UserMetadata)
            {
                footer.Metadata.Add(new UserMetadataItem
                {
                    Name = kvp.Key,
                    Value = Google.Protobuf.ByteString.CopyFrom(kvp.Value)
                });
            }
        }

        var footerBytes = footer.ToByteArray();
        byte[] footerData;
        if (_options.Compression != CompressionKind.None)
            footerData = OrcCompression.Compress(_options.Compression, footerBytes, _options.CompressionBlockSize, _options.CompressionLevel, _options.CustomCompressionLevel);
        else
            footerData = footerBytes;

        await _file.WriteAsync(footerData, cancellationToken).ConfigureAwait(false);

        // Build postscript
        var postScript = new PostScript
        {
            FooterLength = (ulong)footerData.Length,
            Compression = _options.Compression,
            CompressionBlockSize = (ulong)_options.CompressionBlockSize,
            WriterVersion = WriterCode,
            MetadataLength = (ulong)metadataLength,
            Magic = "ORC"
        };
        postScript.Version.Add(0);
        postScript.Version.Add(12);

        var psBytes = postScript.ToByteArray();
        if (psBytes.Length > 255)
            throw new InvalidOperationException("PostScript exceeds maximum size of 255 bytes.");

        await _file.WriteAsync(psBytes, cancellationToken).ConfigureAwait(false);
        await _file.WriteAsync(new[] { (byte)psBytes.Length }, cancellationToken).ConfigureAwait(false);
        await _file.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static List<Type> BuildTypeList(Schema arrowSchema)
    {
        var types = new List<Type>();

        // Type 0: root struct
        var root = new Type { Kind = Type.Types.Kind.Struct };
        types.Add(root);

        int nextId = 1;
        foreach (var field in arrowSchema.FieldsList)
        {
            root.Subtypes.Add((uint)nextId);
            root.FieldNames.Add(field.Name);
            nextId = AddType(field.DataType, types, nextId);
        }

        return types;
    }

    private static int AddType(Apache.Arrow.Types.IArrowType arrowType, List<Type> types, int typeId)
    {
        var type = new Type();
        types.Add(type);

        switch (arrowType)
        {
            case Apache.Arrow.Types.BooleanType:
                type.Kind = Type.Types.Kind.Boolean;
                return typeId + 1;
            case Apache.Arrow.Types.Int8Type:
                type.Kind = Type.Types.Kind.Byte;
                return typeId + 1;
            case Apache.Arrow.Types.Int16Type:
                type.Kind = Type.Types.Kind.Short;
                return typeId + 1;
            case Apache.Arrow.Types.Int32Type:
                type.Kind = Type.Types.Kind.Int;
                return typeId + 1;
            case Apache.Arrow.Types.Int64Type:
                type.Kind = Type.Types.Kind.Long;
                return typeId + 1;
            case Apache.Arrow.Types.FloatType:
                type.Kind = Type.Types.Kind.Float;
                return typeId + 1;
            case Apache.Arrow.Types.DoubleType:
                type.Kind = Type.Types.Kind.Double;
                return typeId + 1;
            case Apache.Arrow.Types.StringType:
                type.Kind = Type.Types.Kind.String;
                return typeId + 1;
            case Apache.Arrow.Types.BinaryType:
                type.Kind = Type.Types.Kind.Binary;
                return typeId + 1;
            case Apache.Arrow.Types.Decimal128Type dt:
                type.Kind = Type.Types.Kind.Decimal;
                type.Precision = (uint)dt.Precision;
                type.Scale = (uint)dt.Scale;
                return typeId + 1;
            case Apache.Arrow.Types.Date32Type:
                type.Kind = Type.Types.Kind.Date;
                return typeId + 1;
            case Apache.Arrow.Types.TimestampType ts:
                type.Kind = ts.Timezone == "UTC"
                    ? Type.Types.Kind.TimestampInstant
                    : Type.Types.Kind.Timestamp;
                return typeId + 1;
            case Apache.Arrow.Types.StructType st:
            {
                type.Kind = Type.Types.Kind.Struct;
                int nextId = typeId + 1;
                foreach (var field in st.Fields)
                {
                    type.Subtypes.Add((uint)nextId);
                    type.FieldNames.Add(field.Name);
                    nextId = AddType(field.DataType, types, nextId);
                }
                return nextId;
            }
            case Apache.Arrow.Types.ListType lt:
            {
                type.Kind = Type.Types.Kind.List;
                int nextId = typeId + 1;
                type.Subtypes.Add((uint)nextId);
                nextId = AddType(lt.ValueDataType, types, nextId);
                return nextId;
            }
            case Apache.Arrow.Types.MapType mt:
            {
                type.Kind = Type.Types.Kind.Map;
                int nextId = typeId + 1;
                type.Subtypes.Add((uint)nextId);
                nextId = AddType(mt.KeyField.DataType, types, nextId);
                type.Subtypes.Add((uint)nextId);
                nextId = AddType(mt.ValueField.DataType, types, nextId);
                return nextId;
            }
            case Apache.Arrow.Types.UnionType ut:
            {
                type.Kind = Type.Types.Kind.Union;
                int nextId = typeId + 1;
                foreach (var field in ut.Fields)
                {
                    type.Subtypes.Add((uint)nextId);
                    type.FieldNames.Add(field.Name);
                    nextId = AddType(field.DataType, types, nextId);
                }
                return nextId;
            }
            default:
                throw new NotSupportedException($"Arrow type {arrowType} is not yet supported for ORC writing.");
        }
    }

    public void Dispose()
    {
        if (!_closed)
        {
            _closed = true;
            // Best-effort sync close — callers should prefer CloseAsync/DisposeAsync
        }
        if (_ownsFile)
            _file.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
        if (_ownsFile)
            await _file.DisposeAsync().ConfigureAwait(false);
    }
}
