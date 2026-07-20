// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

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
    /// Optional registry of Arrow extension types. When supplied, columns whose
    /// Parquet logical type matches a registered extension are materialised as
    /// the corresponding <see cref="Apache.Arrow.ExtensionArray"/> rather than
    /// the default storage type. For example, registering
    /// <c>GuidExtensionDefinition</c> causes <c>UUID</c>-annotated columns to
    /// produce <see cref="GuidArray"/> instead of <see cref="Apache.Arrow.FixedSizeBinaryArray"/>.
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
