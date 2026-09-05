// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics.CodeAnalysis;

namespace EngineeredWood.Parquet;

/// <summary>
/// Physical (primitive) types defined in the Parquet format specification.
/// </summary>
public enum PhysicalType
{
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Int96 = 3,
    Float = 4,
    Double = 5,
    ByteArray = 6,
    FixedLenByteArray = 7,
}

/// <summary>
/// Encodings supported by the Parquet format.
/// </summary>
public enum Encoding
{
    Plain = 0,
    PlainDictionary = 2,
    Rle = 3,
    BitPacked = 4,
    DeltaBinaryPacked = 5,
    DeltaLengthByteArray = 6,
    DeltaByteArray = 7,
    RleDictionary = 8,
    ByteStreamSplit = 9,

    /// <summary>
    /// Adaptive Lossless floating-Point Compression (ALP). The Parquet spec proposal is
    /// not yet finalized; the wire format may change before the spec is ratified.
    /// </summary>
    [Experimental("EWPARQUET0001")]
    Alp = 10,

    /// <summary>
    /// Patched Frame of Reference (PFOR) for INT32 and INT64 columns: frame of reference plus
    /// bit-packing, with the values that do not fit the chosen width stored as exceptions, and
    /// an optional per-vector delta mode. The Parquet spec proposal is not yet finalized; the
    /// wire format may change before the spec is ratified.
    /// </summary>
    /// <remarks>
    /// Proposed in apache/parquet-format#617, and implemented by apache/parquet-java#3775 and
    /// apache/arrow-rs#10977, both of which write 11. Unlike the FSST proposal below it, PFOR's
    /// number is not in dispute.
    /// </remarks>
    [Experimental("EWPARQUET0005")]
    Pfor = 11,

    /// <summary>
    /// Fast Static Symbol Table (FSST) substring compression for BYTE_ARRAY columns.
    /// The Parquet spec proposal is not yet finalized; the wire format may change
    /// before the spec is ratified.
    /// </summary>
    /// <remarks>
    /// <para><b>This is 12, but the FSST proposal says 10.</b> FSST has been bumped twice
    /// because it keeps losing the slot it asks for to a proposal further along. It claimed
    /// 10; ALP took 10 and has since been merged into <c>parquet.thrift</c> on
    /// parquet-format <c>main</c>, so 10 is settled and not FSST's. FSST then sat on 11 here
    /// — which is what the arrow-rs proof-of-concept predicted ("may become 11 once ALP
    /// encoding lands") — until PFOR claimed 11, in a spec PR backed by both a parquet-java
    /// and an arrow-rs implementation. So FSST moves again, to the next free slot.</para>
    /// <para>Files written by this library are self-consistent, but will not interoperate
    /// with an implementation that has settled on a different number for FSST until the
    /// spec picks one. Moving it is cheap precisely because the encoding is experimental;
    /// it will move again if the ratified spec says something else.</para>
    /// </remarks>
    [Experimental("EWPARQUET0003")]
    Fsst = 12,
}

/// <summary>
/// Type of a symbol table carried by a <see cref="PageType.SymbolTablePage"/>.
/// </summary>
/// <remarks>
/// From the FSST proposal's <c>SymbolTableType</c> enum. Both widths are read and written.
/// The data pages are identical either way — the type recorded here is what tells a reader
/// how wide the codes in them are (§2.3).
/// </remarks>
[Experimental("EWPARQUET0003")]
public enum SymbolTableType
{
    /// <summary>8-bit codes: 0-254 are symbols, 255 is the escape marker.</summary>
    Fsst = 0,

    /// <summary>16-bit codes: 0-65534 are symbols, 65535 is the escape marker.</summary>
    Fsst16 = 1,
}

/// <summary>
/// Converted types (deprecated in favor of LogicalType, but still present in many files).
/// </summary>
public enum ConvertedType
{
    Utf8 = 0,
    Map = 1,
    MapKeyValue = 2,
    List = 3,
    Enum = 4,
    Decimal = 5,
    Date = 6,
    TimeMillis = 7,
    TimeMicros = 8,
    TimestampMillis = 9,
    TimestampMicros = 10,
    Uint8 = 11,
    Uint16 = 12,
    Uint32 = 13,
    Uint64 = 14,
    Int8 = 15,
    Int16 = 16,
    Int32 = 17,
    Int64 = 18,
    Json = 19,
    Bson = 20,
    Interval = 21,
}

/// <summary>
/// Repetition type for schema fields.
/// </summary>
public enum FieldRepetitionType
{
    Required = 0,
    Optional = 1,
    Repeated = 2,
}

/// <summary>
/// Types of pages within a column chunk.
/// </summary>
public enum PageType
{
    DataPage = 0,
    IndexPage = 1,
    DictionaryPage = 2,
    DataPageV2 = 3,

    /// <summary>
    /// Symbol table shared by every FSST-encoded data page in a column chunk.
    /// Part of the unratified FSST proposal; see <see cref="Encoding.Fsst"/>.
    /// </summary>
    SymbolTablePage = 4,
}

/// <summary>
/// Sort order for column index boundary values.
/// </summary>
public enum BoundaryOrder
{
    Unordered = 0,
    Ascending = 1,
    Descending = 2,
}
