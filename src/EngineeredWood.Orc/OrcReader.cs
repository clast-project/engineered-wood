// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections;
using Google.Protobuf;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Orc.BloomFilter;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc;

/// <summary>
/// File-level ORC reader. Reads metadata (postscript, footer, schema) and
/// creates OrcRowReaders for reading actual data.
/// </summary>
public sealed class OrcReader : IAsyncDisposable, IDisposable
{
    private const int MaxPostScriptSize = 255;
    private const int MagicLength = 3; // "ORC"
    private static readonly byte[] MagicBytes = "ORC"u8.ToArray();

    private readonly IRandomAccessFile _reader;
    private readonly bool _ownsReader;
    private readonly long _fileLength;

    public PostScript PostScript { get; }
    public Footer Footer { get; }
    public OrcSchema Schema { get; }
    public CompressionKind Compression => PostScript.Compression;
    public ulong CompressionBlockSize => PostScript.CompressionBlockSize;
    public long NumberOfRows => (long)Footer.NumberOfRows;
    public int NumberOfStripes => Footer.Stripes.Count;

    /// <summary>
    /// Returns user-defined metadata from the ORC file footer.
    /// </summary>
    public IReadOnlyDictionary<string, byte[]> UserMetadata
    {
        get
        {
            var dict = new Dictionary<string, byte[]>(Footer.Metadata.Count);
            foreach (var item in Footer.Metadata)
                dict[item.Name] = item.Value.ToByteArray();
            return dict;
        }
    }

    private OrcReader(IRandomAccessFile reader, bool ownsReader, long fileLength,
        PostScript postScript, Footer footer, OrcSchema schema)
    {
        _reader = reader;
        _ownsReader = ownsReader;
        _fileLength = fileLength;
        PostScript = postScript;
        Footer = footer;
        Schema = schema;
    }

    public static async Task<OrcReader> OpenAsync(string path, CancellationToken cancellationToken = default)
    {
        var reader = new LocalRandomAccessFile(path);
        try
        {
            return await OpenAsync(reader, ownsReader: true, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            reader.Dispose();
            throw;
        }
    }

    public static async Task<OrcReader> OpenAsync(IRandomAccessFile reader, bool ownsReader = false, CancellationToken cancellationToken = default)
    {
        var fileLength = await reader.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        if (fileLength < MagicLength + 1) // minimum: "ORC" header + 1 byte postscript length
            throw new InvalidDataException("File is too small to be a valid ORC file.");

        // Read the file tail: we need the last (MaxPostScriptSize + 1) bytes
        // The last byte is the postscript length, then the postscript precedes it.
        var tailSize = (int)Math.Min(fileLength, MaxPostScriptSize + 1);
        using var tailOwner = await reader.ReadAsync(new FileRange(fileLength - tailSize, tailSize), cancellationToken).ConfigureAwait(false);
        var tailBuffer = tailOwner.Memory.Span;

        // Last byte is the postscript length
        var psLength = tailBuffer[tailSize - 1];
        if (psLength > MaxPostScriptSize)
            throw new InvalidDataException($"Invalid postscript length: {psLength}");

        // Parse the postscript
        var psOffset = tailSize - 1 - psLength;
        if (psOffset < 0)
            throw new InvalidDataException("Postscript length exceeds available tail data.");

        var postScript = PostScript.Parser.ParseFrom(tailBuffer.Slice(psOffset, psLength));

        // Validate magic
        if (postScript.HasMagic)
        {
            if (postScript.Magic != "ORC")
                throw new InvalidDataException($"Invalid ORC magic in postscript: '{postScript.Magic}'");
        }
        else
        {
            // Check header magic for ORC files that don't have magic in postscript (very old files)
            using var headerOwner = await reader.ReadAsync(new FileRange(0, MagicLength), cancellationToken).ConfigureAwait(false);
            if (!headerOwner.Memory.Span.SequenceEqual(MagicBytes))
                throw new InvalidDataException("File does not have valid ORC magic bytes.");
        }

        // Read the footer
        var footerLength = (long)postScript.FooterLength;
        var footerStart = fileLength - 1 - psLength - footerLength;

        byte[] footerBytes;
        if (postScript.Compression == CompressionKind.None)
        {
            using var footerOwner = await reader.ReadAsync(new FileRange(footerStart, footerLength), cancellationToken).ConfigureAwait(false);
            footerBytes = footerOwner.Memory.Span.ToArray();
        }
        else
        {
            // Footer is compressed — read and decompress
            using var compressedOwner = await reader.ReadAsync(new FileRange(footerStart, footerLength), cancellationToken).ConfigureAwait(false);
            footerBytes = OrcCompression.Decompress(postScript.Compression, compressedOwner.Memory.Span, (int)postScript.CompressionBlockSize);
        }

        var footer = Footer.Parser.ParseFrom(footerBytes);

        // Build the schema from the type list
        var schema = OrcSchema.FromFooter(footer);

        return new OrcReader(reader, ownsReader, fileLength, postScript, footer, schema);
    }

    /// <summary>
    /// Creates a row reader for reading data from the file.
    /// </summary>
    public OrcRowReader CreateRowReader(OrcReaderOptions? options = null)
    {
        return new OrcRowReader(this, _reader, options ?? new OrcReaderOptions());
    }

    public StripeInformation GetStripe(int index) => Footer.Stripes[index];

    /// <summary>
    /// Reads the stripe-level statistics metadata section.
    /// Returns null if the file has no metadata section.
    /// </summary>
    public async Task<Metadata?> ReadMetadataAsync(CancellationToken cancellationToken = default)
    {
        var metadataLength = (long)PostScript.MetadataLength;
        if (metadataLength == 0) return null;

        var metadataStart = (long)Footer.HeaderLength + (long)Footer.ContentLength;

        byte[] metadataBytes;
        if (PostScript.Compression == CompressionKind.None)
        {
            using var owner = await _reader.ReadAsync(new FileRange(metadataStart, metadataLength), cancellationToken).ConfigureAwait(false);
            metadataBytes = owner.Memory.Span.ToArray();
        }
        else
        {
            using var owner = await _reader.ReadAsync(new FileRange(metadataStart, metadataLength), cancellationToken).ConfigureAwait(false);
            metadataBytes = OrcCompression.Decompress(PostScript.Compression, owner.Memory.Span, (int)PostScript.CompressionBlockSize);
        }

        return Metadata.Parser.ParseFrom(metadataBytes);
    }

    /// <summary>
    /// Reads the row index entries for a given stripe.
    /// Returns a dictionary of column ID → RowIndex.
    /// </summary>
    public async Task<Dictionary<int, RowIndex>> ReadRowIndexAsync(int stripeIndex, CancellationToken cancellationToken = default)
    {
        var stripe = Footer.Stripes[stripeIndex];
        var indexLength = (long)stripe.IndexLength;
        if (indexLength == 0) return new Dictionary<int, RowIndex>();

        var stripeStart = (long)stripe.Offset;
        var dataLength = (long)stripe.DataLength;
        var footerLength = (long)stripe.FooterLength;
        var footerOffset = stripeStart + indexLength + dataLength;

        // Read index and stripe footer in parallel
        var ranges = new FileRange[]
        {
            new(stripeStart, indexLength),
            new(footerOffset, footerLength),
        };
        using var results = new DisposableList<System.Buffers.IMemoryOwner<byte>>(
            await _reader.ReadRangesAsync(ranges, cancellationToken).ConfigureAwait(false));

        var indexSpan = results[0].Memory.Span;
        var footerSpan = results[1].Memory.Span;

        StripeFooter stripeFooter;
        if (Compression == CompressionKind.None)
            stripeFooter = StripeFooter.Parser.ParseFrom(footerSpan);
        else
            stripeFooter = StripeFooter.Parser.ParseFrom(
                OrcCompression.Decompress(Compression, footerSpan, (int)CompressionBlockSize));

        var result = new Dictionary<int, RowIndex>();
        int offset = 0;
        foreach (var stream in stripeFooter.Streams)
        {
            if (stream.Kind == Proto.Stream.Types.Kind.RowIndex)
            {
                var streamBytes = indexSpan.Slice(offset, (int)stream.Length);
                byte[] decompressed;
                if (Compression == CompressionKind.None)
                    decompressed = streamBytes.ToArray();
                else
                    decompressed = OrcCompression.Decompress(Compression, streamBytes, (int)CompressionBlockSize);

                result[(int)stream.Column] = RowIndex.Parser.ParseFrom(decompressed);
                offset += (int)stream.Length;
            }
            else
            {
                break; // Index streams come first
            }
        }

        return result;
    }

    /// <summary>
    /// Returns a <see cref="BitArray"/> indicating which stripes might contain the given value
    /// in the specified column, based on Bloom filter data. Stripes without a Bloom filter
    /// for the column are conservatively marked as candidates.
    /// </summary>
    /// <param name="column">Column name (case-insensitive, top-level only).</param>
    /// <param name="value">The value to probe for. Must be type-compatible with the column's ORC type.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A <see cref="BitArray"/> of length equal to the number of stripes.
    /// A <c>true</c> bit means the stripe might contain the value;
    /// a <c>false</c> bit means it definitely does not.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The column name is not found or the value type is incompatible with the column's ORC type.
    /// </exception>
    public Task<BitArray> GetCandidateStripesAsync(
        string column, object value,
        CancellationToken cancellationToken = default)
    {
        return GetCandidateStripesAsync(column, new[] { value }, cancellationToken);
    }

    /// <summary>
    /// Returns a <see cref="BitArray"/> indicating which stripes might contain any of the given
    /// values in the specified column, based on Bloom filter data. Stripes without a Bloom filter
    /// for the column are conservatively marked as candidates.
    /// </summary>
    /// <param name="column">Column name (case-insensitive, top-level only).</param>
    /// <param name="values">
    /// The values to probe for. A stripe is marked as a candidate if its Bloom filter indicates
    /// it might contain <em>any</em> of the values. All values must be type-compatible with the
    /// column's ORC type.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A <see cref="BitArray"/> of length equal to the number of stripes.
    /// A <c>true</c> bit means the stripe might contain at least one of the values;
    /// a <c>false</c> bit means it definitely does not contain any of them.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The column name is not found, no values are provided, or a value type is incompatible
    /// with the column's ORC type.
    /// </exception>
    public async Task<BitArray> GetCandidateStripesAsync(
        string column, IReadOnlyList<object> values,
        CancellationToken cancellationToken = default)
    {
        if (column is null) throw new ArgumentNullException(nameof(column));
        if (values is null) throw new ArgumentNullException(nameof(values));
        if (values.Count == 0)
            throw new ArgumentException("At least one value must be provided.", nameof(values));

        // Resolve column name to column ID and ORC type kind.
        var (columnId, typeKind) = ResolveColumn(column);
        var hashKind = OrcBloomFilterValueEncoder.GetHashKind(typeKind);

        // Pre-convert values to the appropriate form for probing.
        long[]? longValues = null;
        double[]? doubleValues = null;
        byte[][]? byteValues = null;

        switch (hashKind)
        {
            case OrcBloomHashKind.Long:
                longValues = new long[values.Count];
                for (int i = 0; i < values.Count; i++)
                    longValues[i] = OrcBloomFilterValueEncoder.ToLong(values[i]);
                break;
            case OrcBloomHashKind.Double:
                doubleValues = new double[values.Count];
                for (int i = 0; i < values.Count; i++)
                    doubleValues[i] = OrcBloomFilterValueEncoder.ToDouble(values[i]);
                break;
            case OrcBloomHashKind.Bytes:
                byteValues = new byte[values.Count][];
                for (int i = 0; i < values.Count; i++)
                    byteValues[i] = OrcBloomFilterValueEncoder.ToBytes(values[i]);
                break;
        }

        int numStripes = NumberOfStripes;
        var result = new BitArray(numStripes, true); // default to candidate

        for (int s = 0; s < numStripes; s++)
        {
            var bloomFilters = await ReadBloomFilterIndexAsync(s, cancellationToken).ConfigureAwait(false);
            if (!bloomFilters.TryGetValue(columnId, out var filters) || filters.Count == 0)
                continue; // no bloom filter for this column in this stripe → conservatively include

            // A stripe is excluded only if NONE of its row groups contain any of the values.
            bool stripeCandidate = false;
            foreach (var filter in filters)
            {
                stripeCandidate = hashKind switch
                {
                    OrcBloomHashKind.Long => AnyMatchLong(filter, longValues!),
                    OrcBloomHashKind.Double => AnyMatchDouble(filter, doubleValues!),
                    OrcBloomHashKind.Bytes => AnyMatchBytes(filter, byteValues!),
                    _ => true,
                };
                if (stripeCandidate) break;
            }

            if (!stripeCandidate)
                result[s] = false;
        }

        return result;
    }

    /// <summary>
    /// Reads the bloom filter indices for a given stripe.
    /// Returns a dictionary of column ID → list of <see cref="OrcBloomFilter"/> (one per row group).
    /// Handles both standard ORC format and old Hive BloomKFilter format.
    /// </summary>
    internal async Task<Dictionary<int, List<OrcBloomFilter>>> ReadBloomFilterIndexAsync(
        int stripeIndex, CancellationToken cancellationToken = default)
    {
        var stripe = Footer.Stripes[stripeIndex];
        var indexLength = (long)stripe.IndexLength;
        if (indexLength == 0) return new Dictionary<int, List<OrcBloomFilter>>();

        var stripeStart = (long)stripe.Offset;
        var dataLength = (long)stripe.DataLength;
        var footerLength = (long)stripe.FooterLength;
        var footerOffset = stripeStart + indexLength + dataLength;

        // Read index section and stripe footer in parallel.
        var ranges = new FileRange[]
        {
            new(stripeStart, indexLength),
            new(footerOffset, footerLength),
        };
        using var results = new DisposableList<System.Buffers.IMemoryOwner<byte>>(
            await _reader.ReadRangesAsync(ranges, cancellationToken).ConfigureAwait(false));

        var indexSpan = results[0].Memory.Span;
        var footerSpan = results[1].Memory.Span;

        StripeFooter stripeFooter;
        if (Compression == CompressionKind.None)
            stripeFooter = StripeFooter.Parser.ParseFrom(footerSpan);
        else
            stripeFooter = StripeFooter.Parser.ParseFrom(
                OrcCompression.Decompress(Compression, footerSpan, (int)CompressionBlockSize));

        var result = new Dictionary<int, List<OrcBloomFilter>>();
        int offset = 0;
        foreach (var stream in stripeFooter.Streams)
        {
            bool isIndex = stream.Kind is Proto.Stream.Types.Kind.RowIndex
                or Proto.Stream.Types.Kind.BloomFilter
                or Proto.Stream.Types.Kind.BloomFilterUtf8;

            if (!isIndex) break; // past index section

            if (stream.Kind is Proto.Stream.Types.Kind.BloomFilter
                or Proto.Stream.Types.Kind.BloomFilterUtf8)
            {
                var streamBytes = indexSpan.Slice(offset, (int)stream.Length);
                byte[] decompressed;
                if (Compression == CompressionKind.None)
                    decompressed = streamBytes.ToArray();
                else
                    decompressed = OrcCompression.Decompress(Compression, streamBytes, (int)CompressionBlockSize);

                var bfi = BloomFilterIndex.Parser.ParseFrom(decompressed);
                var writerKind = GetWriterKind();
                var filters = new List<OrcBloomFilter>(bfi.BloomFilter.Count);
                foreach (var bf in bfi.BloomFilter)
                {
                    var filter = OrcBloomFilter.TryCreate(bf, writerKind);
                    if (filter != null)
                        filters.Add(filter);
                }
                if (filters.Count > 0)
                    result[(int)stream.Column] = filters;
            }

            offset += (int)stream.Length;
        }

        return result;
    }

    /// <summary>
    /// Determines the writer kind from the footer's <c>writer</c> field
    /// (ORC spec: 0=Java, 1=C++, 2=Presto, 3=Go, 4=Trino, 5=CUDF).
    /// The field was added to the ORC spec after the initial release;
    /// older files (e.g., Hive-era) may not have it set, in which case
    /// we default to <see cref="OrcWriterKind.Java"/> since those files
    /// were almost always written by Java Hive.
    /// </summary>
    private OrcWriterKind GetWriterKind()
    {
        if (!Footer.HasWriter)
            return OrcWriterKind.Java;

        return Footer.Writer switch
        {
            1 or 5 or 6 => OrcWriterKind.Cpp,
            _ => OrcWriterKind.Java,
        };
    }

    /// <summary>
    /// Resolves a column name to its column ID and ORC type kind.
    /// </summary>
    private (int ColumnId, Proto.Type.Types.Kind Kind) ResolveColumn(string column)
    {
        foreach (var child in Schema.Children)
        {
            if (string.Equals(child.Name, column, StringComparison.OrdinalIgnoreCase))
                return (child.ColumnId, child.Kind);
        }
        throw new ArgumentException($"Column '{column}' not found in schema.", nameof(column));
    }

    private static bool AnyMatchLong(OrcBloomFilter filter, long[] values)
    {
        foreach (var v in values)
            if (filter.MightContainLong(v)) return true;
        return false;
    }

    private static bool AnyMatchDouble(OrcBloomFilter filter, double[] values)
    {
        foreach (var v in values)
            if (filter.MightContainDouble(v)) return true;
        return false;
    }

    private static bool AnyMatchBytes(OrcBloomFilter filter, byte[][] values)
    {
        foreach (var v in values)
            if (filter.MightContain(v)) return true;
        return false;
    }

    public ValueTask DisposeAsync()
    {
        if (_ownsReader)
            return _reader.DisposeAsync();
        return default;
    }

    public void Dispose()
    {
        if (_ownsReader)
            _reader.Dispose();
    }
}

/// <summary>
/// Helper to dispose a list of disposable items.
/// </summary>
internal sealed class DisposableList<T> : IDisposable where T : IDisposable
{
    private readonly IReadOnlyList<T> _items;

    public DisposableList(IReadOnlyList<T> items) => _items = items;

    public T this[int index] => _items[index];
    public int Count => _items.Count;

    public void Dispose()
    {
        foreach (var item in _items)
            item.Dispose();
    }
}
