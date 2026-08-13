// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics.CodeAnalysis;
using EngineeredWood.Compression;

namespace EngineeredWood.Parquet;

/// <summary>
/// Parquet data page version.
/// </summary>
public enum DataPageVersion
{
    /// <summary>Data page V1: levels and values are concatenated and compressed together.</summary>
    V1 = 1,

    /// <summary>Data page V2: levels are stored separately (uncompressed); only values are compressed.</summary>
    V2 = 2,
}

/// <summary>
/// Controls the non-dictionary fallback encoding for FLOAT and DOUBLE columns
/// when using V2 data pages.
/// </summary>
public enum FloatingPointEncoding
{
    /// <summary>
    /// BYTE_STREAM_SPLIT: byte-interleaving encoding that pairs well with a generic compressor.
    /// Supported by parquet-mr ≥ 1.12 (mid-2021) and contemporaneous parquet-cpp/Arrow/DuckDB
    /// builds. This is the default.
    /// </summary>
    ByteStreamSplit,

    /// <summary>
    /// ADAPTIVE_LOSSLESS_FLOATING_POINT (ALP, encoding 10): decimal-aware integer encoding plus
    /// frame-of-reference and bit-packing. Strong for monetary, sensor, and scientific data with
    /// limited decimal precision; comparable to plain values for high-precision irrational data.
    /// New in parquet-format; older readers will not be able to decode it.
    /// </summary>
    [Experimental("EWPARQUET0001")]
    Alp,

    /// <summary>
    /// PLAIN: write IEEE-754 values uncompressed. Universal lowest-common-denominator —
    /// readable by every Parquet implementation. Choose this for maximum reader compatibility
    /// or when an outer codec (e.g. Zstd) already exploits byte-level correlation.
    /// </summary>
    Plain,
}

/// <summary>
/// Controls the non-dictionary fallback encoding for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY columns
/// when using V2 data pages.
/// </summary>
public enum ByteArrayEncoding
{
    /// <summary>
    /// DELTA_LENGTH_BYTE_ARRAY: delta-encodes value lengths, concatenates raw bytes.
    /// Good general-purpose encoding for variable-length data. This is the default.
    /// </summary>
    DeltaLengthByteArray,

    /// <summary>
    /// DELTA_BYTE_ARRAY: delta-encodes prefix lengths and suffix lengths, then stores suffixes.
    /// Most effective for sorted or prefix-heavy data (URLs, file paths, dictionary-like keys).
    /// </summary>
    DeltaByteArray,

    /// <summary>
    /// FSST (encoding 11): trains a per-column-chunk symbol table that maps frequent 1-8 byte
    /// substrings to single-byte codes, then stores each value as a sequence of codes. Strong
    /// for high-cardinality machine-generated text — URLs, UUIDs, log lines, identifiers —
    /// where a dictionary cannot help but the values still share substrings.
    /// </summary>
    /// <remarks>
    /// <para>BYTE_ARRAY columns only; a FIXED_LEN_BYTE_ARRAY column falls back to its usual
    /// encoding. The writer also falls back per column chunk when FSST does not actually
    /// shrink the data, so this setting cannot make a file bigger.</para>
    /// <para>New in parquet-format and <b>not yet ratified</b>; older readers cannot decode it,
    /// and this library writes the encoding as 11 rather than the proposal's 10 — see
    /// <see cref="Encoding.Fsst"/>.</para>
    /// </remarks>
    [Experimental("EWPARQUET0003")]
    Fsst,

    /// <summary>
    /// FSST_16 (encoding 11 with a 16-bit symbol table): as <see cref="Fsst"/>, but codes are
    /// two bytes, so the table can hold up to 65,535 symbols instead of 255.
    /// </summary>
    /// <remarks>
    /// <para>Which width wins is data-dependent and there is no universally right answer, so
    /// this is an explicit choice rather than something the writer infers. The proposal's
    /// Appendix C measures FSST_16 ahead on compression ratio, while noting FSST may still be
    /// preferred for encode time on low-cardinality data; two bytes per code also means the
    /// symbols have to be longer than two bytes to pay for themselves. <b>Measure on your own
    /// data</b> — for short, name-like values the 8-bit variant is usually the better default.</para>
    /// <para>Every caveat on <see cref="Fsst"/> applies unchanged: BYTE_ARRAY columns only, V2
    /// data pages only, per-chunk fallback when it does not shrink the data, and a wire format
    /// that is <b>not yet ratified</b>.</para>
    /// </remarks>
    [Experimental("EWPARQUET0003")]
    Fsst16,
}

/// <summary>
/// Sort order recorded for FLOAT, DOUBLE, and FLOAT16 columns' min/max
/// statistics (the Parquet <c>ColumnOrder</c> union, PARQUET-2249).
/// </summary>
public enum FloatingPointColumnOrder
{
    /// <summary>
    /// <c>TypeDefinedOrder</c> — the historical ordering. NaN is excluded from
    /// min/max and a <c>nan_count</c> is recorded. Readable by every Parquet
    /// implementation; this is the default.
    /// </summary>
    TypeDefined,

    /// <summary>
    /// <c>IEEE754TotalOrder</c> — the spec-recommended total order
    /// (<c>-0 &lt; +0</c>; NaN bounds only when every non-null value is NaN).
    /// Readers that do not recognize this union member will ignore min/max for
    /// the affected columns, losing predicate pushdown on them.
    /// </summary>
    Ieee754TotalOrder,
}

/// <summary>
/// Options that control how Arrow data is written to Parquet files.
/// </summary>
public sealed record ParquetWriteOptions
{
    /// <summary>Default options with sensible defaults for general-purpose use.</summary>
    public static readonly ParquetWriteOptions Default = new();

    /// <summary>
    /// Compression codec applied to data pages. Default is <see cref="CompressionCodec.Snappy"/>.
    /// </summary>
    public CompressionCodec Compression { get; init; } = CompressionCodec.Snappy;

    /// <summary>
    /// Codec-agnostic compression level applied to data and dictionary pages.
    /// <see langword="null"/> (the default) preserves each codec's historical default.
    /// Codecs without a tunable level (Snappy, Lz4Hadoop) ignore this setting.
    /// </summary>
    public BlockCompressionLevel? CompressionLevel { get; init; }

    /// <summary>
    /// Optional explicit native compression level. When set, overrides <see cref="CompressionLevel"/>.
    /// Honored by Zstd (1..22), Brotli (0..11), and Lz4 (LZ4Level enum value).
    /// Ignored by Gzip/Deflate (BCL exposes only an enum-valued level).
    /// </summary>
    public int? CustomCompressionLevel { get; init; }

    /// <summary>
    /// Data page version. Default is <see cref="DataPageVersion.V2"/>.
    /// </summary>
    public DataPageVersion DataPageVersion { get; init; } = DataPageVersion.V2;

    /// <summary>
    /// Target uncompressed size of a data page in bytes. Default is 1 MB.
    /// </summary>
    public int DataPageSize { get; init; } = 1024 * 1024;

    /// <summary>
    /// Maximum byte size of a dictionary page before dictionary encoding is abandoned
    /// for that column. Default is 1 MB.
    /// </summary>
    public int DictionaryPageSizeLimit { get; init; } = 1024 * 1024;

    /// <summary>
    /// Whether dictionary encoding is enabled. When enabled, columns are analyzed
    /// before writing and dictionary encoding is used if the cardinality is sufficiently low.
    /// Default is <see langword="true"/>.
    /// </summary>
    public bool DictionaryEnabled { get; init; } = true;

    /// <summary>
    /// Maximum number of rows per row group. Default is 1,000,000.
    /// </summary>
    public int RowGroupMaxRows { get; init; } = 1_000_000;

    /// <summary>
    /// Maximum uncompressed byte size per row group. Default is 128 MB.
    /// </summary>
    public long RowGroupMaxBytes { get; init; } = 128L * 1024 * 1024;

    /// <summary>
    /// Non-dictionary fallback encoding for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY columns
    /// when using V2 data pages. Default is <see cref="ByteArrayEncoding.DeltaLengthByteArray"/>.
    /// Set to <see cref="ByteArrayEncoding.DeltaByteArray"/> for sorted or prefix-heavy data.
    /// </summary>
    public ByteArrayEncoding ByteArrayEncoding { get; init; } = ByteArrayEncoding.DeltaLengthByteArray;

    /// <summary>
    /// Non-dictionary fallback encoding for FLOAT and DOUBLE columns when using V2 data pages.
    /// Default is <see cref="FloatingPointEncoding.ByteStreamSplit"/>; set to
    /// <see cref="FloatingPointEncoding.Alp"/> for decimal-like floating-point data.
    /// </summary>
    public FloatingPointEncoding FloatingPointEncoding { get; init; } = FloatingPointEncoding.ByteStreamSplit;

    /// <summary>
    /// Per-column compression codec overrides, keyed by dotted column path (e.g. "col1" or "struct1.field1").
    /// Columns not listed use <see cref="Compression"/>.
    /// </summary>
    public IReadOnlyDictionary<string, CompressionCodec>? ColumnCodecs { get; init; }

    /// <summary>
    /// Per-column compression-level overrides, keyed by dotted column path.
    /// Columns not listed use <see cref="CompressionLevel"/>.
    /// </summary>
    public IReadOnlyDictionary<string, BlockCompressionLevel>? ColumnCompressionLevels { get; init; }

    /// <summary>
    /// Per-column encoding overrides for BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY columns,
    /// keyed by dotted column path. Columns not listed use <see cref="ByteArrayEncoding"/>.
    /// </summary>
    public IReadOnlyDictionary<string, ByteArrayEncoding>? ColumnEncodings { get; init; }

    /// <summary>
    /// Per-column dictionary-encoding overrides, keyed by dotted column path. Columns not listed use
    /// <see cref="DictionaryEnabled"/>.
    ///
    /// <para>For a producer that already knows a column's cardinality. Deciding a column is unsuitable
    /// otherwise costs a full analysis pass that ends in nothing: the encoder hashes its way to a fifth of
    /// the row count in distinct values before giving up, and the column is then read again and written
    /// PLAIN. Measured on a 1M-row column of distinct strings, that attempt cost 17.5 MB and 13 ms and
    /// produced a byte-identical file. Setting <see langword="false"/> for such a column skips it.</para>
    ///
    /// <para>A map rather than a list of exclusions, so it overrides in both directions: dictionary
    /// encoding is on by default, and a column set <see langword="true"/> here is still analyzed when
    /// <see cref="DictionaryEnabled"/> is <see langword="false"/> globally.</para>
    ///
    /// <para>This is a switch, not an assertion about the data — <see langword="true"/> means "analyze it",
    /// not "it is low-cardinality". The cardinality threshold and
    /// <see cref="DictionaryPageSizeLimit"/> still decide the outcome, so a column left on cannot produce
    /// an oversized dictionary however it is annotated.</para>
    /// </summary>
    public IReadOnlyDictionary<string, bool>? ColumnDictionaryEnabled { get; init; }

    /// <summary>
    /// Enables VARIANT shredding on write, using these inference thresholds. Default
    /// <see langword="null"/>: variant columns are written in the canonical <c>struct&lt;metadata,
    /// value&gt;</c> form, which is spec-legal and what every reader accepts.
    /// </summary>
    /// <remarks>
    /// <para>Shredding hoists part of each value into a typed <c>typed_value</c> child, which is what
    /// buys per-field statistics, predicate pruning and better encoding than an opaque variant blob —
    /// and matches the layout Spark 4.x and DuckDB emit by default. It is an optimisation, not a
    /// compatibility requirement, which is why it is opt-in.</para>
    ///
    /// <para><b>The shape is decided by the FIRST row group.</b> A parquet file has one schema, so
    /// the writer infers each variant column's layout from the first batch it sees and reuses it for
    /// the rest of the file; later rows that do not fit fall to the residual <c>value</c> rather than
    /// failing. An unrepresentative first batch therefore costs space, not correctness. Supply
    /// <see cref="VariantShredSchemas"/> when the shape is known ahead of the data.</para>
    ///
    /// <para>Top-level variant columns only. A variant nested inside a struct, list or map is written
    /// unshredded regardless.</para>
    /// </remarks>
    public Apache.Arrow.Operations.Shredding.ShredOptions? ShredVariants { get; init; }

    /// <summary>
    /// Explicit shred layout per top-level variant column, keyed by column name. Bypasses inference
    /// for the columns it names, and applies whether or not <see cref="ShredVariants"/> is set.
    /// </summary>
    /// <remarks>
    /// For a producer that knows the shape — a table format carrying a declared variant schema, or a
    /// host writing many files that must agree on one layout. Build one with
    /// <see cref="Apache.Arrow.Operations.Shredding.ShredSchema.ForObject"/> and friends, or capture
    /// one from <see cref="VariantShredding.InferSchema(Apache.Arrow.VariantArray, Apache.Arrow.Operations.Shredding.ShredOptions)"/>
    /// over a representative sample. A column named here is shredded even if inference would have
    /// declined it.
    /// </remarks>
    public IReadOnlyDictionary<string, Apache.Arrow.Operations.Shredding.ShredSchema>? VariantShredSchemas { get; init; }

    /// <summary>
    /// Prototype. When <see langword="true"/>, consecutive bit-packed groups in RLE/bit-packed
    /// hybrid streams — definition levels, repetition levels, and dictionary indices — are batched
    /// into a single literal run (up to 63 groups) instead of emitting a run header for every
    /// 8 values. Default: <see langword="false"/>.
    /// </summary>
    /// <remarks>
    /// <para>The output stays fully spec-conformant either way; multi-group literal runs are what
    /// parquet-mr and arrow already emit, and every reader handles them. Batching shrinks
    /// bit-packed-heavy streams (most visibly V2 repetition levels, which are stored uncompressed)
    /// and gives decoders a contiguous block instead of header-interleaved single bytes.</para>
    /// <para>Prototype flag: this changes the bytes of every file written, so it is opt-in until the
    /// size and decode-speed trade-off has been measured across representative data.</para>
    /// </remarks>
    public bool BatchBitPackedRuns { get; init; }

    /// <summary>
    /// Number of bit-packed groups to batch per literal run, derived from
    /// <see cref="BatchBitPackedRuns"/>.
    /// </summary>
    internal int MaxLiteralGroups =>
        BatchBitPackedRuns ? Data.RleBitPackedEncoder.MaxLiteralGroups : 1;

    /// <summary>
    /// Application identifier written to the file footer's <c>created_by</c> field.
    /// </summary>
    public string CreatedBy { get; init; } = "EngineeredWood";

    /// <summary>
    /// Optional key-value metadata to include in the file footer.
    /// </summary>
    public IReadOnlyList<Metadata.KeyValue>? KeyValueMetadata { get; init; }

    /// <summary>
    /// Whether to compute and write CRC-32C checksums for each data and dictionary page.
    /// When enabled, each page header includes a <c>crc</c> field covering the compressed
    /// page data (excluding the header itself). Default is <see langword="false"/>.
    /// </summary>
    public bool PageChecksumEnabled { get; init; }

    /// <summary>
    /// Column names (dotted paths) for which Bloom filters should be written.
    /// <c>null</c> (the default) disables Bloom filter writing for all columns.
    /// Use a <see cref="HashSet{T}"/> for efficient lookup.
    /// </summary>
    public IReadOnlyCollection<string>? BloomFilterColumns { get; init; }

    /// <summary>
    /// Target false positive probability for Bloom filters. Default is 0.05 (5%).
    /// Lower values produce larger filters with fewer false positives.
    /// </summary>
    public double BloomFilterFpp { get; init; } = 0.05;

    /// <summary>
    /// Maximum Bloom filter size in bytes per column per row group. Default is 1 MB.
    /// </summary>
    public int BloomFilterMaxBytes { get; init; } = 1024 * 1024;

    /// <summary>
    /// <para>Whether to omit the <c>path_in_schema</c> field from each column chunk's metadata.
    /// Default is <see langword="false"/> — the field IS written.</para>
    ///
    /// <para><b>Setting this to <see langword="true"/> produces files that no other Parquet
    /// implementation can read.</b> <c>path_in_schema</c> is required by the current Parquet spec.
    /// Although row group columns are matched to the schema by ordinal position (making the field
    /// redundant in principle) and an in-progress spec change would make it optional, that change is
    /// not ratified. Readers in the field — pyarrow, ParquetSharp, delta-kernel-rs — fail to
    /// deserialize the thrift footer entirely, reporting the file as corrupt rather than as using an
    /// unsupported feature.</para>
    ///
    /// <para>EngineeredWood's own reader tolerates the omission, so a round-trip through this library
    /// will not detect the incompatibility. Only enable this for size experiments on files that stay
    /// inside EngineeredWood.</para>
    /// </summary>
    [Experimental("EWPARQUET0002")]
    public bool OmitPathInSchema { get; init; }

    /// <summary>
    /// Sort order recorded for FLOAT/DOUBLE columns in the footer's
    /// <c>column_orders</c> field. Default is
    /// <see cref="FloatingPointColumnOrder.TypeDefined"/>, which every reader
    /// understands; a <c>nan_count</c> is written for floating-point columns
    /// under either setting. Use
    /// <see cref="FloatingPointColumnOrder.Ieee754TotalOrder"/> for the
    /// spec-recommended total order at the cost of pushdown on readers that do
    /// not recognize it.
    /// </summary>
    public FloatingPointColumnOrder FloatingPointOrder { get; init; } = FloatingPointColumnOrder.TypeDefined;

    /// <summary>
    /// Maps <see cref="FloatingPointOrder"/> to the metadata-level
    /// <see cref="Metadata.ColumnOrder"/> recorded for FLOAT/DOUBLE columns.
    /// </summary>
    internal Metadata.ColumnOrder FloatColumnOrder =>
        FloatingPointOrder == FloatingPointColumnOrder.Ieee754TotalOrder
            ? Metadata.ColumnOrder.Ieee754TotalOrder
            : Metadata.ColumnOrder.TypeDefined;

    /// <summary>
    /// Returns whether the given column should have a Bloom filter.
    /// </summary>
    internal bool HasBloomFilter(IReadOnlyList<string> pathInSchema)
    {
        if (BloomFilterColumns == null) return false;
        var dottedPath = string.Join(".", pathInSchema);
        // Use efficient Contains for HashSet, linear scan for other collections.
        if (BloomFilterColumns is HashSet<string> hs)
            return hs.Contains(dottedPath);
        foreach (var col in BloomFilterColumns)
            if (col == dottedPath) return true;
        return false;
    }

    /// <summary>
    /// Resolves the compression codec for a column, checking per-column overrides first.
    /// </summary>
    internal CompressionCodec GetCodec(IReadOnlyList<string> pathInSchema) =>
        ColumnCodecs != null && ColumnCodecs.TryGetValue(string.Join(".", pathInSchema), out var codec)
            ? codec
            : Compression;

    /// <summary>
    /// Resolves the compression level for a column, checking per-column overrides first.
    /// </summary>
    internal BlockCompressionLevel? GetCompressionLevel(IReadOnlyList<string> pathInSchema) =>
        ColumnCompressionLevels != null &&
        ColumnCompressionLevels.TryGetValue(string.Join(".", pathInSchema), out var level)
            ? level
            : CompressionLevel;

    /// <summary>
    /// Resolves the byte-array encoding for a column, checking per-column overrides first.
    /// </summary>
    internal ByteArrayEncoding GetByteArrayEncoding(IReadOnlyList<string> pathInSchema) =>
        ColumnEncodings != null && ColumnEncodings.TryGetValue(string.Join(".", pathInSchema), out var enc)
            ? enc
            : ByteArrayEncoding;

    /// <summary>
    /// Resolves whether a column is dictionary-analyzed at all, checking per-column overrides first.
    /// </summary>
    internal bool GetDictionaryEnabled(IReadOnlyList<string> pathInSchema) =>
        ColumnDictionaryEnabled != null &&
        ColumnDictionaryEnabled.TryGetValue(string.Join(".", pathInSchema), out bool enabled)
            ? enabled
            : DictionaryEnabled;
}
