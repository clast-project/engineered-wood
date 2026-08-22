// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using EngineeredWood.Expressions;

namespace EngineeredWood.Parquet;

/// <summary>
/// Controls the Arrow output type for BYTE_ARRAY (string/binary) columns.
/// </summary>
public enum ByteArrayOutputKind
{
    /// <summary>
    /// Default: UTF8-annotated columns produce <c>StringType</c>; all others produce <c>BinaryType</c>.
    /// Uses 32-bit offsets (max 2 GB of string data per column per row group).
    /// </summary>
    Default,

    /// <summary>
    /// Produces <c>StringViewType</c> or <c>BinaryViewType</c>.
    /// Values ≤12 bytes are stored inline in the 16-byte view entry (no overflow copy).
    /// Longer values share a single overflow buffer. Best for short-string or prefix-scan workloads.
    /// </summary>
    ViewType,

    /// <summary>
    /// Produces <c>LargeStringType</c> or <c>LargeBinaryType</c> with 64-bit offsets.
    /// Removes the 2 GB per-column limit. Decode path is otherwise identical to <see cref="Default"/>.
    /// </summary>
    LargeOffsets,
}

/// <summary>
/// Controls the Arrow output type for DECIMAL columns.
/// </summary>
public enum DecimalOutputKind
{
    /// <summary>
    /// Default: the narrowest Arrow decimal that fits — <c>Decimal32Type</c> for an INT32-backed column,
    /// <c>Decimal64Type</c> for INT64, and by precision for FIXED_LEN_BYTE_ARRAY. Preserves the physical
    /// width, at the cost of producing types some consumers do not handle (see <see cref="Decimal128"/>).
    /// </summary>
    Default,

    /// <summary>
    /// Always produce the classic <c>Decimal128Type</c> (<c>Decimal256Type</c> when precision &gt; 38),
    /// whatever the parquet physical width. The narrow <c>Decimal32</c>/<c>Decimal64</c> Arrow types are
    /// newer and not reliably handled by Arrow C-data-interface consumers — DuckDB, for one, reads the
    /// exported format string as 128-bit over the 4/8-byte buffer and corrupts the values. The decoders
    /// sign-extend to any target width, so widening is lossless and precision/scale are preserved.
    /// Choose this when the batches cross the C data interface or feed a consumer of unknown vintage.
    /// </summary>
    Decimal128,
}

/// <summary>
/// Controls the Arrow output type for INT96 columns.
/// </summary>
/// <remarks>
/// INT96 is deprecated in the Parquet format and nothing writes it today, but Hive, Impala and
/// Spark before 3.0 wrote it for every timestamp, so it is common in files that already exist.
/// The physical layout is settled — 8 bytes of nanoseconds-within-day little-endian, then a
/// 4-byte Julian day — so only the Arrow unit is a choice.
/// </remarks>
public enum Int96OutputKind
{
    /// <summary>
    /// Default: <c>timestamp[us]</c> with no timezone. Reads every date a writer can express,
    /// including the far-future values Spark overflowed on write. DuckDB reads INT96 this way.
    /// </summary>
    /// <remarks>
    /// The conversion floors: an INT96 carrying sub-microsecond precision — Impala wrote true
    /// nanoseconds — loses the last three digits without warning. Choose
    /// <see cref="TimestampNanoseconds"/> when that matters.
    /// </remarks>
    TimestampMicroseconds = 0,

    /// <summary>
    /// <c>timestamp[ns]</c> with no timezone, preserving everything INT96 can express.
    /// PyArrow and Polars read INT96 this way, so this is the choice that matches them value
    /// for value.
    /// </summary>
    /// <remarks>
    /// <c>timestamp[ns]</c> only spans roughly 1677-09-21 to 2262-04-11, and INT96's Julian day
    /// reaches well past both ends — 9999-12-31 is an everyday Spark value. A timestamp outside
    /// that window throws <see cref="ParquetFormatException"/> naming the row and this option,
    /// rather than wrapping into a plausible-looking date the way PyArrow does. The same file read
    /// as <see cref="TimestampMicroseconds"/> gives the right answer.
    /// </remarks>
    TimestampNanoseconds,

    /// <summary>
    /// The undecoded 12 bytes as <c>FixedSizeBinaryType(12)</c> — the behaviour before INT96 was
    /// interpreted. Nothing is lost either way; this only declines to say what the bytes mean.
    /// </summary>
    FixedSizeBinary,
}

/// <summary>
/// Controls the Arrow output type for a TIMESTAMP-annotated <c>FIXED_LEN_BYTE_ARRAY(12)</c> column —
/// the extended-precision carrier proposed in apache/parquet-format#600.
/// </summary>
/// <remarks>
/// <para>The carrier holds a signed 96-bit little-endian count of the column's declared
/// <c>TimeUnit</c> since the Unix epoch, which spans the whole ANSI SQL TIMESTAMP(9) range.
/// INT64 nanoseconds does not: it stops at 1677-09-21 and 2262-04-11.</para>
///
/// <para>This is deliberately a sibling of <see cref="Int96OutputKind"/> rather than the same enum.
/// INT96 carries no logical annotation, so its unit is the reader's choice; here the file declares
/// MILLIS, MICROS or NANOS and reading at any other unit would be a rescale, not an output kind.</para>
///
/// <para><b>The byte order is not settled upstream.</b> The proposal, the parquet-java reference
/// implementation and the proposed conformance fixture are all little-endian, but the choice was
/// still open on the spec PR when this was written. Nothing on the wire distinguishes the two
/// orders, so if it flips, files already written become silently wrong-valued rather than
/// unreadable.</para>
/// </remarks>
[Experimental("EWPARQUET0004")]
public enum ExtendedTimestampOutputKind
{
    /// <summary>
    /// Default: <c>timestamp[us]</c>, rescaled from the unit the file declares, UTC when
    /// <c>isAdjustedToUTC</c> is set and naive otherwise. Microseconds span roughly ±292,000 years,
    /// so this reads the whole ANSI SQL range and a long way past it.
    /// </summary>
    /// <remarks>
    /// <para>A NANOS column loses its last three digits, and the conversion floors rather than
    /// truncating toward zero so that a pre-epoch value rounds the same way as a post-epoch one.
    /// Choose <see cref="Timestamp"/> when those digits matter.</para>
    ///
    /// <para>This is the default for the same reason
    /// <see cref="Int96OutputKind.TimestampMicroseconds"/> is INT96's: it is the mode that produces
    /// an answer for every value a writer would sensibly emit. It is not unconditional — the carrier
    /// holds ±2^95 units, so a value outside ±292,000 years still overflows int64 and is reported —
    /// but nothing representing a date can reach that.</para>
    /// </remarks>
    TimestampMicroseconds = 0,

    /// <summary>
    /// <c>timestamp</c> at the unit the file declares, keeping every digit that was written.
    /// </summary>
    /// <remarks>
    /// Arrow timestamps are int64, so a value the carrier holds but int64 cannot — year 9999 in
    /// nanoseconds is the ordinary case here, not a corrupt file — throws
    /// <see cref="ParquetFormatException"/> naming the row and this option rather than wrapping into
    /// a plausible-looking date. That refusal is the point of the mode: it is for callers who would
    /// rather be told than lose precision silently. Use the default
    /// <see cref="TimestampMicroseconds"/> to read such a column anyway, or
    /// <see cref="FixedSizeBinary"/> for the raw bytes.
    /// </remarks>
    Timestamp,

    /// <summary>
    /// The undecoded 12 bytes as <c>FixedSizeBinaryType(12)</c>. Nothing is lost; this only declines
    /// to say what the bytes mean.
    /// </summary>
    FixedSizeBinary,
}

/// <summary>
/// Options that control how Parquet data is read and mapped to Apache Arrow types.
/// </summary>
/// <remarks>
/// A <c>record</c> (like its <see cref="ParquetWriteOptions"/> sibling) so callers can derive one set
/// of options from another with <c>with</c>. That matters for correctness, not just convenience: a
/// layer that needs to adjust ONE option — e.g. the Delta table registering the
/// <c>arrow.parquet.variant</c> extension — gets a compiler-generated copy of every other member, so
/// adding an option here can never silently drop it on the derived path.
/// </remarks>
public sealed record ParquetReadOptions
{
    /// <summary>Default options: all features disabled, producing standard Arrow types.</summary>
    public static readonly ParquetReadOptions Default = new();

    /// <summary>
    /// Controls the Arrow output type for BYTE_ARRAY (string/binary) columns.
    /// </summary>
    public ByteArrayOutputKind ByteArrayOutput { get; init; } = ByteArrayOutputKind.Default;

    /// <summary>
    /// Controls the Arrow output type for DECIMAL columns.
    /// </summary>
    public DecimalOutputKind DecimalOutput { get; init; } = DecimalOutputKind.Default;

    /// <summary>
    /// Controls the Arrow output type for INT96 columns. Defaults to
    /// <see cref="Int96OutputKind.TimestampMicroseconds"/> — an INT96 column reads back as a
    /// naive <c>timestamp[us]</c>, not as raw bytes.
    /// </summary>
    public Int96OutputKind Int96Output { get; init; } = Int96OutputKind.TimestampMicroseconds;

    /// <summary>
    /// Controls the Arrow output type for a TIMESTAMP-annotated <c>FIXED_LEN_BYTE_ARRAY(12)</c>
    /// column. Defaults to <see cref="ExtendedTimestampOutputKind.TimestampMicroseconds"/> — the
    /// column reads back as <c>timestamp[us]</c>, which spans every date the carrier exists to hold,
    /// so a generic read of a conforming file never fails.
    /// </summary>
    /// <remarks>
    /// The carrier is an unratified parquet-format proposal (apache/parquet-format#600) whose byte
    /// order was still under discussion when this shipped; see
    /// <see cref="ExtendedTimestampOutputKind"/>.
    /// </remarks>
    [Experimental("EWPARQUET0004")]
    public ExtendedTimestampOutputKind ExtendedTimestampOutput { get; init; }
        = ExtendedTimestampOutputKind.TimestampMicroseconds;

    /// <summary>
    /// Maximum number of rows per <see cref="Apache.Arrow.RecordBatch"/>. When set, row groups
    /// larger than this limit are split across multiple batches. When <see langword="null"/>
    /// (the default), each row group produces exactly one batch.
    /// </summary>
    public int? BatchSize { get; init; }

    /// <summary>
    /// Approximate maximum uncompressed size (in bytes) of a single <see cref="Apache.Arrow.RecordBatch"/>.
    /// The budget is measured as the sum of uncompressed Parquet page sizes across all columns;
    /// the actual Arrow representation may be somewhat larger due to validity bitmaps, offset
    /// arrays, and alignment padding. When both <see cref="BatchSize"/> and
    /// <see cref="MaxBatchByteSize"/> are set, the more restrictive limit wins.
    /// When <see langword="null"/> (the default), no size limit is applied.
    /// </summary>
    public long? MaxBatchByteSize { get; init; }

    /// <summary>
    /// Optional row group filter predicate. When set, the reader evaluates the
    /// predicate against each row group's column statistics; row groups that
    /// can be proven empty of matching rows (per <see cref="StatisticsEvaluator"/>)
    /// are skipped without reading data pages.
    /// </summary>
    /// <remarks>
    /// Predicates that statistics can't evaluate (function calls, two-column
    /// comparisons, missing stats) are conservatively kept. The reader does not
    /// re-apply the predicate to rows; callers wanting exact row-level
    /// filtering must do that on the returned batches themselves.
    /// </remarks>
    public Predicate? Filter { get; init; }

    /// <summary>
    /// When <see langword="true"/> and <see cref="Filter"/> is set, the reader
    /// also probes Bloom filters for equality and IN predicates that the
    /// statistics evaluator could not decide. Requires extra I/O per candidate
    /// row group (one read per column with a Bloom filter), so this is opt-in.
    /// Default: <see langword="false"/>.
    /// </summary>
    public bool FilterUseBloomFilters { get; init; }

    /// <summary>
    /// Whether to validate CRC-32C checksums when present in page headers.
    /// When enabled and a page header contains a <c>crc</c> field, the compressed
    /// page data is verified before decompression. Mismatches throw
    /// <see cref="ParquetFormatException"/>. Default is <see langword="false"/>.
    /// </summary>
    public bool PageChecksumValidation { get; init; }

    /// <summary>
    /// When <see langword="true"/>, the reader probes the encoded definition and repetition level
    /// streams of single-level list columns for the fixed-length, fully-defined shape that vector
    /// data (embeddings, coordinates, fixed feature rows) takes. On a match, the levels are never
    /// decoded or materialised and the list offsets are computed arithmetically.
    /// Default: <see langword="false"/>.
    /// </summary>
    /// <remarks>
    /// <para>The probe is cheap — it walks RLE/bit-packed runs, not values — but a column that is only
    /// ragged part-way through a chunk is read twice, so this stays opt-in.
    /// Results are identical either way; only the decode cost changes.</para>
    /// <para>Prototype. The option is read-only and output-equivalent — it never changes the bytes on
    /// disk or the Arrow data produced — so it is safe to enable, but its shape may still change: it
    /// currently applies file-wide (no per-column control) and only on the whole-row-group read
    /// entry points, not the batched <see cref="BatchSize"/> / <see cref="MaxBatchByteSize"/> path.</para>
    /// </remarks>
    public bool FixedListFastPath { get; init; }

    /// <summary>
    /// Optional registry of Arrow extension types. When supplied, columns whose
    /// Parquet logical type matches a registered extension are materialised as
    /// the corresponding <see cref="Apache.Arrow.ExtensionArray"/> rather than
    /// the default storage type. For example, registering
    /// <c>GuidExtensionDefinition</c> causes <c>UUID</c>-annotated columns to
    /// produce <see cref="GuidArray"/> instead of <see cref="Apache.Arrow.Arrays.FixedSizeBinaryArray"/>.
    /// When <see langword="null"/> (the default), the reader produces the
    /// underlying storage types and ignores extension annotations.
    /// </summary>
    public ExtensionTypeRegistry? ExtensionRegistry { get; init; }

    /// <summary>
    /// Shorthand for <c>ByteArrayOutput == ByteArrayOutputKind.ViewType</c>.
    /// When set to <see langword="true"/>, sets <see cref="ByteArrayOutput"/> to
    /// <see cref="ByteArrayOutputKind.ViewType"/>; setting to <see langword="false"/>
    /// reverts to <see cref="ByteArrayOutputKind.Default"/>.
    /// </summary>
    public bool UseViewTypes
    {
        get => ByteArrayOutput == ByteArrayOutputKind.ViewType;
        init => ByteArrayOutput = value ? ByteArrayOutputKind.ViewType : ByteArrayOutputKind.Default;
    }
}
