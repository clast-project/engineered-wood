// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using EngineeredWood.Compression;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.Parquet.BloomFilter;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet;

/// <summary>What a <see cref="MembershipPredicateEvaluator"/> asks whether a value is present.</summary>
internal enum MembershipSource
{
    /// <summary>The column chunk's Bloom filter: probabilistic, so a hit proves nothing.</summary>
    BloomFilter,

    /// <summary>
    /// The column chunk's dictionary page: exact, but only for a chunk whose every data page is
    /// dictionary-encoded, since a chunk that fell back holds values its dictionary does not.
    /// </summary>
    Dictionary,
}

/// <summary>
/// Walks a <see cref="Predicate"/> tree and asks a per-chunk membership source (a Bloom filter or a
/// dictionary page) whether equality and IN sub-predicates can match, to derive
/// <see cref="FilterResult.AlwaysFalse"/> when their values are absent.
/// </summary>
/// <remarks>
/// Either source is used here only to prove absence, so this evaluator returns
/// <see cref="FilterResult.AlwaysFalse"/> or <see cref="FilterResult.Unknown"/> for a leaf. Other
/// predicate kinds (range, IS NULL, function calls) are treated as Unknown and contribute nothing.
///
/// Compose with <see cref="StatisticsEvaluator"/>: run statistics first, then fall back to a
/// membership source only for row groups still marked Unknown.
/// </remarks>
internal static class MembershipPredicateEvaluator
{
    /// <summary>
    /// A dictionary page larger than this is not read for pruning. Writers cap dictionaries near
    /// 1 MiB by default; a page far past that is either unusual or not a dictionary at all, and
    /// reading it would cost more than the scan it might save.
    /// </summary>
    internal const int MaxDictionaryPageBytes = 16 * 1024 * 1024;

    /// <summary>
    /// Evaluates the predicate against the row group's membership sources, reading them lazily from
    /// <paramref name="file"/>. Returns <see cref="FilterResult.AlwaysFalse"/> only when one or more
    /// equality/IN sub-predicates definitively miss.
    /// </summary>
    public static async ValueTask<FilterResult> EvaluateAsync(
        Predicate predicate,
        MembershipSource source,
        int rowGroupIndex,
        FileMetaData metadata,
        SchemaDescriptor schema,
        IRandomAccessFile file,
        long fileLength,
        ColumnChunkFilePathKind filePath,
        bool validateChecksums,
        CancellationToken ct,
        IReadOnlyDictionary<int, IValueSet?>? prefetched = null,
        int? maxPageUncompressedSize = null)
    {
        var ctx = new Context(source, rowGroupIndex, metadata, schema, file, fileLength, filePath, validateChecksums)
        {
            MaxPageUncompressedSize = maxPageUncompressedSize,
        };
        if (prefetched is not null)
        {
            // Read ahead by a MembershipPrefetch for this source: a column present here is never read
            // again, including a null entry, which is a chunk already found unable to answer.
            foreach (var entry in prefetched)
                ctx.Sets[entry.Key] = entry.Value;
        }
        return await EvaluateAsync(predicate, ctx, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The columns whose dictionaries or Bloom filters <paramref name="predicate"/> will actually ask, anywhere in the
    /// tree (a NOT still asks its child): those with an equality or IN leaf the probe would not decline
    /// before reading. That is the probe's own test, so a prefetch never reads a page no leaf can use: a
    /// NULL literal, an IN list holding a non-literal (<c>x IN (a, b)</c>), and a literal that cannot
    /// be encoded for the column (an integer against a DECIMAL stored as INT32) are all declined.
    /// </summary>
    internal static List<(int Index, ColumnDescriptor Descriptor)> MembershipColumns(
        Predicate predicate, SchemaDescriptor schema)
    {
        var columns = new List<(int, ColumnDescriptor)>();
        var seen = new HashSet<int>();
        Collect(predicate);
        return columns;

        void Collect(Predicate p)
        {
            switch (p)
            {
                case AndPredicate and:
                    foreach (var child in and.Children) Collect(child);
                    break;
                case OrPredicate or:
                    foreach (var child in or.Children) Collect(child);
                    break;
                case NotPredicate not:
                    Collect(not.Child);
                    break;
                case ComparisonPredicate cmp when IsEquality(cmp.Op):
                    if ((TryGetColumnAndLiteral(cmp.Left, cmp.Right, out string? column, out var value)
                         || TryGetColumnAndLiteral(cmp.Right, cmp.Left, out column, out value))
                        && !value.IsNull)
                        Add(column!, [value]);
                    break;
                case SetPredicate set when set.Op == SetOperator.In:
                    if (TryGetColumnName(set.Operand, out string? operand)
                        && set.Values.Count > 0
                        && set.TryGetLiteralValues(out var values))
                        Add(operand!, values);
                    break;
            }
        }

        void Add(string name, IReadOnlyList<LiteralValue> values)
        {
            if (!FindColumn(schema, name, out int index, out var descriptor) || seen.Contains(index))
                return;
            foreach (var value in values)
            {
                if (!TryEncodeForProbe(value, descriptor!, out _))
                    return;
            }
            seen.Add(index);
            columns.Add((index, descriptor!));
        }
    }

    private static async ValueTask<FilterResult> EvaluateAsync(
        Predicate predicate, Context ctx, CancellationToken ct)
    {
        switch (predicate)
        {
            case TruePredicate:
                return FilterResult.AlwaysTrue;
            case FalsePredicate:
                return FilterResult.AlwaysFalse;

            case AndPredicate and:
            {
                bool allTrue = true;
                foreach (var child in and.Children)
                {
                    var r = await EvaluateAsync(child, ctx, ct).ConfigureAwait(false);
                    if (r == FilterResult.AlwaysFalse) return FilterResult.AlwaysFalse;
                    if (r != FilterResult.AlwaysTrue) allTrue = false;
                }
                return allTrue ? FilterResult.AlwaysTrue : FilterResult.Unknown;
            }

            case OrPredicate or:
            {
                bool allFalse = true;
                foreach (var child in or.Children)
                {
                    var r = await EvaluateAsync(child, ctx, ct).ConfigureAwait(false);
                    if (r == FilterResult.AlwaysTrue) return FilterResult.AlwaysTrue;
                    if (r != FilterResult.AlwaysFalse) allFalse = false;
                }
                return allFalse ? FilterResult.AlwaysFalse : FilterResult.Unknown;
            }

            case NotPredicate not:
                return (await EvaluateAsync(not.Child, ctx, ct).ConfigureAwait(false)) switch
                {
                    FilterResult.AlwaysTrue => FilterResult.AlwaysFalse,
                    FilterResult.AlwaysFalse => FilterResult.AlwaysTrue,
                    _ => FilterResult.Unknown,
                };

            case ComparisonPredicate cmp when IsEquality(cmp.Op):
                return await EvaluateEqualityAsync(cmp, ctx, ct).ConfigureAwait(false);

            case SetPredicate set when set.Op == SetOperator.In:
                return await EvaluateInAsync(set, ctx, ct).ConfigureAwait(false);

            // Range, IS NULL, NOT IN, function calls, etc. — Bloom filters can't help.
            default:
                return FilterResult.Unknown;
        }
    }

    private static bool IsEquality(ComparisonOperator op) =>
        op == ComparisonOperator.Equal || op == ComparisonOperator.NullSafeEqual;

    private static async ValueTask<FilterResult> EvaluateEqualityAsync(
        ComparisonPredicate cmp, Context ctx, CancellationToken ct)
    {
        if (!TryGetColumnAndLiteral(cmp.Left, cmp.Right, out string? column, out var value)
            && !TryGetColumnAndLiteral(cmp.Right, cmp.Left, out column, out value))
            return FilterResult.Unknown;

        if (value.IsNull)
            return FilterResult.Unknown;

        return await ProbeAsync(column!, [value], ctx, ct).ConfigureAwait(false);
    }

    private static async ValueTask<FilterResult> EvaluateInAsync(
        SetPredicate set, Context ctx, CancellationToken ct)
    {
        if (!TryGetColumnName(set.Operand, out string? column))
            return FilterResult.Unknown;
        if (set.Values.Count == 0)
            return FilterResult.AlwaysFalse;

        // A Bloom filter answers "does this VALUE appear", so a list naming other columns --
        // `x IN (a, b)` -- is not a question it can be asked.
        if (!set.TryGetLiteralValues(out var values))
            return FilterResult.Unknown;

        return await ProbeAsync(column!, values, ctx, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Asks the membership source about every provided value. Returns AlwaysFalse if every value is
    /// absent; Unknown if any might be present, or if the source cannot be asked.
    /// </summary>
    private static async ValueTask<FilterResult> ProbeAsync(
        string column, IReadOnlyList<LiteralValue> values, Context ctx, CancellationToken ct)
    {
        if (!ctx.TryFindColumn(column, out int columnIndex, out var descriptor))
            return FilterResult.Unknown;

        var chunk = ctx.Metadata.RowGroups[ctx.RowGroupIndex].Columns[columnIndex];
        // A chunk stored in another file has its filter and dictionary there too (#405); reading this file
        // at that offset could rule the row group out, so it would never reach the read that refuses it.
        if (chunk.FilePath is not null && ctx.FilePath == ColumnChunkFilePathKind.Refuse)
            return FilterResult.Unknown;

        // Encode before any I/O: a value the source cannot be asked about decides the answer alone.
        var encoded = new List<byte[]>(values.Count);
        foreach (var v in values)
        {
            if (!TryEncodeForProbe(v, descriptor!, out var encodings))
                return FilterResult.Unknown; // can't encode → can't decide
            encoded.AddRange(encodings);
        }

        // One read per column per row group, however many leaves of the predicate name the column.
        if (!ctx.Sets.TryGetValue(columnIndex, out var set))
        {
            set = await ReadSetAsync(ctx.Source, chunk, descriptor!, ctx, ct).ConfigureAwait(false);
            ctx.Sets[columnIndex] = set;
        }

        if (set is null)
            return FilterResult.Unknown;

        foreach (byte[] bytes in encoded)
        {
            if (set.MightContain(bytes))
                return FilterResult.Unknown; // (maybe) present
        }

        return FilterResult.AlwaysFalse;
    }

    /// <summary>A column chunk's answer to "might this value, in its plain encoding, be present?"</summary>
    internal interface IValueSet
    {
        bool MightContain(byte[] value);
    }

    /// <summary>A dictionary page: exact, since only a wholly dictionary-encoded chunk is asked.</summary>
    private sealed class DictionaryValues : IValueSet
    {
        private readonly HashSet<byte[]> _values;

        public DictionaryValues(HashSet<byte[]> values) => _values = values;

        public bool MightContain(byte[] value) => _values.Contains(value);
    }

    /// <summary>A Bloom filter: a hit proves nothing, a miss proves absence.</summary>
    private sealed class BloomFilterValues : IValueSet
    {
        private readonly Clast.BloomFilter.SplitBlockBloomFilter _filter;

        public BloomFilterValues(Clast.BloomFilter.SplitBlockBloomFilter filter) => _filter = filter;

        public bool MightContain(byte[] value) => _filter.MightContain(value);
    }

    private static async ValueTask<IValueSet?> ReadSetAsync(
        MembershipSource source, ColumnChunk chunk, ColumnDescriptor descriptor, Context ctx, CancellationToken ct)
    {
        if (!TryGetRange(source, chunk, descriptor, ctx.FileLength, ctx.FilePath, out var range))
            return null;

        using var buffer = (await ctx.File.ReadRangesAsync(new[] { range }, ct).ConfigureAwait(false))[0];
        return Decode(
            source, buffer.Memory.Span, chunk.MetaData!.Codec, descriptor, ctx.ValidateChecksums,
            ctx.MaxPageUncompressedSize);
    }

    /// <summary>
    /// Where the chunk's <paramref name="source"/> is stored, when the chunk can be answered from it at
    /// all. Shared by the single read above and by <see cref="MembershipPrefetch"/>, so the two cannot
    /// disagree about which chunks are asked.
    /// </summary>
    internal static bool TryGetRange(
        MembershipSource source, ColumnChunk chunk, ColumnDescriptor descriptor, long fileLength,
        ColumnChunkFilePathKind filePath, out FileRange range) =>
        source == MembershipSource.Dictionary
            ? TryGetDictionaryRange(chunk, descriptor, fileLength, filePath, out range)
            : TryGetBloomFilterRange(chunk, fileLength, filePath, out range);

    /// <summary>
    /// Decodes what <see cref="TryGetRange"/> located, or returns null when it cannot be trusted: pruning
    /// declines on a page or filter it cannot read, and the ordinary read of the row group reports it.
    /// </summary>
    /// <param name="maxPageUncompressedSize">
    /// The caller's <see cref="ParquetReadOptions.MaxPageUncompressedSize"/>: a compressed dictionary page
    /// declaring more is declined here, as the read would refuse it, rather than decompressed.
    /// </param>
    internal static IValueSet? Decode(
        MembershipSource source, ReadOnlySpan<byte> bytes, CompressionCodec codec, ColumnDescriptor descriptor,
        bool validateChecksums, int? maxPageUncompressedSize = null)
    {
        if (source == MembershipSource.Dictionary)
        {
            return DecodeDictionary(bytes, codec, descriptor, validateChecksums, maxPageUncompressedSize) is { } values
                ? new DictionaryValues(values)
                : null;
        }

        try
        {
            return new BloomFilterValues(BloomFilterReader.Parse(bytes));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// A Bloom filter larger than this is not read for pruning: well past any filter a writer sizes by
    /// default, and reading it would cost more than the scan it might save.
    /// </summary>
    internal const int MaxBloomFilterBytes = 16 * 1024 * 1024;

    /// <summary>
    /// Where the chunk's Bloom filter is, when it has one in this file. Without a recorded length the
    /// first 4 KiB are read, as before this was shared: the header gives the filter's size.
    /// </summary>
    internal static bool TryGetBloomFilterRange(
        ColumnChunk chunk, long fileLength, ColumnChunkFilePathKind filePath, out FileRange range)
    {
        range = default;
        var meta = chunk.MetaData;
        if (meta?.BloomFilterOffset is not long offset || offset <= 0 || offset >= fileLength)
            return false;

        // A chunk stored in another file has its filter there (#405).
        if (chunk.FilePath is not null && filePath == ColumnChunkFilePathKind.Refuse)
            return false;

        long length = meta.BloomFilterLength ?? Math.Min(4096, fileLength - offset);
        if (length <= 0 || length > MaxBloomFilterBytes || offset > fileLength - length)
            return false;

        range = new FileRange(offset, length);
        return true;
    }

    /// <summary>
    /// True when <paramref name="meta"/>'s <c>encoding_stats</c> prove that every value in the chunk is in
    /// its dictionary: exactly one dictionary page, and no data page in any other encoding.
    /// </summary>
    /// <remarks>
    /// <para>The stats are the only proof. <see cref="ColumnMetaData.Encodings"/> cannot give it, because
    /// writers list PLAIN for the dictionary page of a chunk that never fell back, so it reads the same
    /// as one that did (see <see cref="PageEncodingStats"/>). Without stats, the answer is no.</para>
    /// <para>The two data-page types are one here: parquet-cpp counts a V2 data page as DATA_PAGE. A page
    /// type this reader does not know is a no: it may carry values the dictionary does not, and a pruned
    /// row group is never read, so nothing would ever meet the unknown page and refuse it.</para>
    /// </remarks>
    internal static bool IsWhollyDictionaryEncoded(ColumnMetaData meta)
    {
        if (meta.EncodingStats is not { Count: > 0 } stats)
            return false;

        int dictionaryPages = 0;
        foreach (var entry in stats)
        {
            switch (entry.PageType)
            {
                case PageType.DictionaryPage:
                    dictionaryPages += entry.Count;
                    break;
                case PageType.DataPage or PageType.DataPageV2:
                    if (entry.Count > 0 && entry.Encoding is not (Encoding.RleDictionary or Encoding.PlainDictionary))
                        return false;
                    break;
                case PageType.IndexPage:
                    break; // carries no values
                default:
                    return false;
            }
        }

        return dictionaryPages == 1;
    }

    /// <summary>
    /// Where the chunk's dictionary page is, when the chunk can be answered from it at all: every data
    /// page dictionary-encoded, a physical type with a PLAIN layout a literal can meet, a stored
    /// extent within bounds, and the chunk in this file.
    /// </summary>
    internal static bool TryGetDictionaryRange(
        ColumnChunk chunk, ColumnDescriptor descriptor, long fileLength, ColumnChunkFilePathKind filePath,
        out FileRange range)
    {
        range = default;
        var meta = chunk.MetaData;
        if (meta is null || !IsWhollyDictionaryEncoded(meta))
            return false;

        // A chunk stored in another file has its dictionary there (#405).
        if (chunk.FilePath is not null && filePath == ColumnChunkFilePathKind.Refuse)
            return false;

        // A BOOLEAN dictionary would be bit-packed, and an INT96 value has no literal to meet it.
        if (descriptor.PhysicalType is PhysicalType.Boolean or PhysicalType.Int96)
            return false;

        // The dictionary page opens the chunk and ends where the first data page begins.
        if (meta.DictionaryPageOffset is not long start || start <= 0 || start >= meta.DataPageOffset)
            return false;
        long length = meta.DataPageOffset - start;
        if (length > MaxDictionaryPageBytes || start > fileLength - length)
            return false;

        range = new FileRange(start, length);
        return true;
    }

    /// <summary>
    /// Decodes a dictionary page read from <see cref="TryGetDictionaryRange"/>'s extent, or returns null.
    /// </summary>
    internal static HashSet<byte[]>? DecodeDictionary(
        ReadOnlySpan<byte> page, CompressionCodec codec, ColumnDescriptor descriptor, bool validateChecksums,
        int? maxPageUncompressedSize = null)
    {
        try
        {
            return DecodeDictionaryPage(page, codec, descriptor, validateChecksums, maxPageUncompressedSize);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Whatever the page or its codec throws: ZstdSharp and Snappier signal corrupt input with
            // their own exception types (ZstdException, InvalidOperationException), so no list of
            // expected types is complete. Only the DECODE is guarded; a failure to read the file itself
            // surfaces above, as it would from the read this pruning stands in front of.
            return null;
        }
    }

    private static HashSet<byte[]>? DecodeDictionaryPage(
        ReadOnlySpan<byte> page, CompressionCodec codec, ColumnDescriptor descriptor, bool validateChecksums,
        int? maxPageUncompressedSize)
    {
        // A payload that overruns the extent throws here, which DecodeDictionary turns into null.
        var reader = new PageReader(page, descriptor);
        if (!reader.TryRead(out var read))
            return null;
        var header = read.Header;
        if (header.Type != PageType.DictionaryPage || header.DictionaryPageHeader is not { } dictionaryHeader)
            return null;
        if (dictionaryHeader.Encoding is not (Encoding.Plain or Encoding.PlainDictionary))
            return null;
        if (header.UncompressedPageSize < 0 || header.UncompressedPageSize > MaxDictionaryPageBytes
            || dictionaryHeader.NumValues < 0)
            return null;
        // As PageReader applies it: to a page that is decompressed.
        if (codec != CompressionCodec.Uncompressed && header.UncompressedPageSize > maxPageUncompressedSize)
            return null;

        var payload = read.Payload;
        if (validateChecksums)
            ColumnChunkReader.ValidateCrc(header.Crc, payload, descriptor);

        byte[]? rented = null;
        try
        {
            ReadOnlySpan<byte> plain = payload;
            if (codec != CompressionCodec.Uncompressed)
            {
                // A byte past the declared size, so that Gzip, which stops when its destination is full,
                // shows a page that decompresses to more.
                rented = ArrayPool<byte>.Shared.Rent(header.UncompressedPageSize + 1);
                int written = Decompressor.Decompress(codec, payload, rented.AsSpan(0, header.UncompressedPageSize + 1));
                if (written != header.UncompressedPageSize)
                    return null;
                plain = rented.AsSpan(0, written);
            }

            return PlainValues(plain, dictionaryHeader.NumValues, descriptor);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>The <paramref name="count"/> PLAIN-encoded values of <paramref name="plain"/>, or null when
    /// they do not fill it exactly.</summary>
    private static HashSet<byte[]>? PlainValues(ReadOnlySpan<byte> plain, int count, ColumnDescriptor descriptor)
    {
        int width = descriptor.PhysicalType switch
        {
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.FixedLenByteArray => descriptor.TypeLength ?? -1,
            PhysicalType.ByteArray => 0, // length-prefixed
            _ => -1,
        };
        if (width < 0)
            return null;

        var values = new HashSet<byte[]>(ByteSequenceComparer.Instance);
        int position = 0;
        for (int i = 0; i < count; i++)
        {
            int size = width;
            if (width == 0)
            {
                if (plain.Length - position < 4)
                    return null;
                size = BinaryPrimitives.ReadInt32LittleEndian(plain.Slice(position, 4));
                position += 4;
            }
            if (size < 0 || plain.Length - position < size)
                return null;
            values.Add(plain.Slice(position, size).ToArray());
            position += size;
        }

        return position == plain.Length ? values : null;
    }

    private sealed class ByteSequenceComparer : IEqualityComparer<byte[]>
    {
        public static readonly ByteSequenceComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y) =>
            ReferenceEquals(x, y) || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));

        public int GetHashCode(byte[] bytes)
        {
            // FNV-1a: dictionaries are small and this is only a bucket choice.
            unchecked
            {
                int hash = (int)2166136261;
                foreach (byte b in bytes)
                    hash = (hash ^ b) * 16777619;
                return hash;
            }
        }
    }

    // ── Helpers ──

    private static bool TryGetColumnAndLiteral(
        Expression maybeRef, Expression maybeLit,
        out string? column, out LiteralValue value)
    {
        if (TryGetColumnName(maybeRef, out column) && maybeLit is LiteralExpression lit)
        {
            value = lit.Value;
            return true;
        }
        column = null;
        value = LiteralValue.Null;
        return false;
    }

    private static bool TryGetColumnName(Expression expr, out string? name)
    {
        switch (expr)
        {
            case UnboundReference u: name = u.Name; return true;
            case BoundReference b: name = b.Name; return true;
            default: name = null; return false;
        }
    }

    /// <summary>
    /// Encodes a typed <see cref="LiteralValue"/> into every byte representation a value EQUAL to it
    /// could be stored as (see <see cref="BloomFilterValueEncoder.TryEncodeEquivalents"/>, which holds
    /// the floating-point rule), or returns false when the source cannot be asked.
    /// </summary>
    internal static bool TryEncodeForProbe(
        LiteralValue value, ColumnDescriptor descriptor, out byte[][] encodings)
    {
        try
        {
            object? boxed = ToObjectForColumn(value, descriptor);
            if (boxed is null)
            {
                encodings = [];
                return false;
            }
            return BloomFilterValueEncoder.TryEncodeEquivalents(boxed, descriptor.PhysicalType, out encodings);
        }
        catch (ArgumentException)
        {
            encodings = [];
            return false;
        }
    }

    /// <summary>
    /// Converts a <see cref="LiteralValue"/> to the boxed .NET value the column's bytes would hold.
    /// </summary>
    /// <remarks>
    /// Temporal literals are decided by the LOGICAL type, not the physical one, so they are handled
    /// before the physical dispatch below. Until this existed, a predicate on a DATE, TIME or TIMESTAMP
    /// column could never probe a bloom filter at all: the statistics layer hands those over as
    /// DateOnly / TimeOnly / DateTimeOffset, and every one of them fell through to null.
    /// </remarks>
    private static object? ToObjectForColumn(LiteralValue v, ColumnDescriptor desc)
        => v.Type is LiteralValue.Kind.DateTimeOffset or LiteralValue.Kind.DateOnly
            or LiteralValue.Kind.TimeOnly
            ? TemporalToObject(v, desc)
            : StoresPlainValues(desc.SchemaElement) ? ToObjectForPhysicalType(v, desc.PhysicalType) : null;

    /// <summary>
    /// True when a stored value's bytes ARE the value a non-temporal literal compares against: no
    /// annotation, or one that only names the bytes (a string, an enum, JSON/BSON, an integer width).
    /// </summary>
    /// <remarks>
    /// Anything else stores a representation the literal's bytes do not match. A DECIMAL(9,2) held as
    /// INT32 stores 5.00 as 500, so probing <c>d = 5</c> with the bytes of 5 would report 5.00 absent and
    /// prune a row group that matches; a UUID, FLOAT16 or TIMESTAMP compared to an integer or string
    /// literal is the same shape. Declining costs a pruning opportunity and nothing else. Files that
    /// carry only the legacy <c>converted_type</c> are judged by it.
    /// </remarks>
    internal static bool StoresPlainValues(SchemaElement element)
    {
        if (element.LogicalType is { } logical)
        {
            return logical is LogicalType.StringType or LogicalType.EnumType or LogicalType.JsonType
                or LogicalType.BsonType or LogicalType.IntType;
        }

        return element.ConvertedType is null or ConvertedType.Utf8 or ConvertedType.Enum
            or ConvertedType.Json or ConvertedType.Bson
            or ConvertedType.Uint8 or ConvertedType.Uint16 or ConvertedType.Uint32 or ConvertedType.Uint64
            or ConvertedType.Int8 or ConvertedType.Int16 or ConvertedType.Int32 or ConvertedType.Int64;
    }

    /// <summary>Ticks (100 ns) from .NET's epoch (0001-01-01) to the Unix epoch.</summary>
    private const long UnixEpochTicks = 621_355_968_000_000_000L;

    /// <summary>
    /// Converts a temporal literal to the exact value stored in the column, or null when no stored
    /// value could equal it.
    /// </summary>
    /// <remarks>
    /// <para>EXACTNESS IS THE WHOLE RULE. A bloom filter answers "these bytes, or definitely nothing",
    /// so a literal is only worth probing with if it converts to the column's unit without a remainder.
    /// A DateTimeOffset of 1.5 ms against a MILLIS column does not, and rounding it would probe for a
    /// value the caller never asked about. Declining costs a pruning opportunity and nothing else.</para>
    ///
    /// <para>Returning null is always safe here: it means the filter is not consulted, so the row group
    /// is read. The unsafe direction would be probing with the wrong bytes and being told "absent".</para>
    /// </remarks>
    private static object? TemporalToObject(LiteralValue v, ColumnDescriptor desc)
    {
        var logical = desc.SchemaElement.LogicalType;

        switch (v.Type)
        {
            case LiteralValue.Kind.DateTimeOffset when logical is LogicalType.TimestampType ts:
            {
                // Normalising by the offset is right for an isAdjustedToUTC column, and a no-op for a
                // naive one -- this reader only ever produces those with a zero offset.
                long ticks = v.AsDateTimeOffset.ToUniversalTime().Ticks - UnixEpochTicks;
                if (!ExtendedTimestamp.TryTicksToUnit(ticks, ts.Unit, out Int128 count))
                    return null;

                // Computed in 128 bits because NANOS does not fit 64: year 9999 is 2.5e20 nanoseconds.
                // An INT64 column cannot hold such a value at all, so a literal that big matches nothing
                // there and there is no probe to make.
                if (desc.PhysicalType == PhysicalType.Int64)
                    return ExtendedTimestamp.TryToInt64(count, out long narrowed) ? narrowed : (object?)null;

                if (desc.PhysicalType == PhysicalType.FixedLenByteArray
                    && desc.TypeLength == ExtendedTimestamp.ByteWidth)
                {
                    // The filter holds the hash of the bytes as they sit in the file, so the literal has
                    // to become those same twelve little-endian bytes. The carrier holds +/-2^95, so a
                    // value it cannot represent is likewise not in the column.
                    if (!ExtendedTimestamp.IsRepresentable(count))
                        return null;

                    var carrier = new byte[ExtendedTimestamp.ByteWidth];
                    ExtendedTimestamp.Write(count, carrier);
                    return carrier;
                }

                return null;
            }

#if NET6_0_OR_GREATER
            case LiteralValue.Kind.DateOnly
                when logical is LogicalType.DateType && desc.PhysicalType == PhysicalType.Int32:
                return v.AsDateOnly.DayNumber - EpochDays;

            case LiteralValue.Kind.TimeOnly when logical is LogicalType.TimeType time:
            {
                long ticks = v.AsTimeOnly.Ticks;
                return time.Unit switch
                {
                    Metadata.TimeUnit.Millis when desc.PhysicalType == PhysicalType.Int32
                        => ticks % 10_000 == 0 ? (object)(int)(ticks / 10_000) : null,
                    Metadata.TimeUnit.Micros when desc.PhysicalType == PhysicalType.Int64
                        => ticks % 10 == 0 ? (object)(ticks / 10) : null,
                    // Safe in 64 bits, unlike the TIMESTAMP case: a time of day is at most 8.64e13
                    // nanoseconds, nowhere near where a long runs out.
                    Metadata.TimeUnit.Nanos when desc.PhysicalType == PhysicalType.Int64
                        => ticks * 100,
                    _ => null,
                };
            }
#endif
            default:
                return null;
        }
    }

    /// <summary>Days from .NET's epoch (0001-01-01) to the Unix epoch (1970-01-01).</summary>
    private const int EpochDays = 719_162;

    /// <summary>
    /// Converts a <see cref="LiteralValue"/> to the boxed .NET type that
    /// <see cref="BloomFilterValueEncoder"/> expects for the column's physical
    /// type. Returns null when no safe conversion exists.
    /// </summary>
    private static object? ToObjectForPhysicalType(LiteralValue v, PhysicalType pt) => pt switch
    {
        PhysicalType.Boolean => v.Type == LiteralValue.Kind.Boolean ? (object)v.AsBoolean : null,
        PhysicalType.Int32 => v.Type switch
        {
            LiteralValue.Kind.Int32 => v.AsInt32,
            LiteralValue.Kind.Int64 => v.AsInt64 is >= int.MinValue and <= int.MaxValue
                ? (object)(int)v.AsInt64 : null,
            _ => null,
        },
        PhysicalType.Int64 => v.Type switch
        {
            LiteralValue.Kind.Int64 => (object)v.AsInt64,
            LiteralValue.Kind.Int32 => (long)v.AsInt32,
            _ => null,
        },
        PhysicalType.Float => v.Type == LiteralValue.Kind.Float ? (object)v.AsFloat : null,
        PhysicalType.Double => v.Type switch
        {
            LiteralValue.Kind.Double => v.AsDouble,
            LiteralValue.Kind.Float => (double)v.AsFloat,
            _ => null,
        },
        PhysicalType.ByteArray or PhysicalType.FixedLenByteArray => v.Type switch
        {
            LiteralValue.Kind.String => v.AsString,
            LiteralValue.Kind.Binary => v.AsBinary,
            _ => null,
        },
        _ => null,
    };

    private sealed class Context
    {
        public Context(MembershipSource source, int rgIndex, FileMetaData metadata, SchemaDescriptor schema,
            IRandomAccessFile file, long fileLength, ColumnChunkFilePathKind filePath, bool validateChecksums)
        {
            Source = source;
            ValidateChecksums = validateChecksums;
            RowGroupIndex = rgIndex;
            Metadata = metadata;
            Schema = schema;
            File = file;
            FileLength = fileLength;
            FilePath = filePath;
        }

        public MembershipSource Source { get; }
        public bool ValidateChecksums { get; }
        public int? MaxPageUncompressedSize { get; init; }

        /// <summary>Each column's values from this source once read, or null when it cannot answer.</summary>
        public Dictionary<int, IValueSet?> Sets { get; } = new();

        public int RowGroupIndex { get; }
        public FileMetaData Metadata { get; }
        public SchemaDescriptor Schema { get; }
        public IRandomAccessFile File { get; }
        public long FileLength { get; }
        public ColumnChunkFilePathKind FilePath { get; }

        public bool TryFindColumn(string name, out int index, out ColumnDescriptor? descriptor) =>
            FindColumn(Schema, name, out index, out descriptor);
    }

    /// <summary>A leaf by dotted path, or by bare name for a top-level leaf.</summary>
    internal static bool FindColumn(
        SchemaDescriptor schema, string name, out int index, out ColumnDescriptor? descriptor)
    {
        for (int i = 0; i < schema.Columns.Count; i++)
        {
            if (schema.Columns[i].DottedPath == name
                || (schema.Columns[i].Path.Count == 1
                    && schema.Columns[i].Path[0] == name))
            {
                index = i;
                descriptor = schema.Columns[i];
                return true;
            }
        }
        index = -1;
        descriptor = null;
        return false;
    }
}
