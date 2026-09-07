// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Centralizes encoding selection logic for the write pipeline.
/// Determines which Parquet encoding to use for a given column type and options.
/// </summary>
internal static class EncodingStrategyResolver
{
    /// <summary>
    /// Resolves the initial encoding for a column: dictionary if enabled and cardinality is low,
    /// otherwise falls back to <see cref="GetFallbackEncoding"/>.
    /// </summary>
    public static bool ShouldAttemptDictionary(PhysicalType physicalType, ParquetWriteOptions options) =>
        options.DictionaryEnabled && physicalType != PhysicalType.Boolean;

    /// <summary>
    /// Gets the non-dictionary encoding for a data page based on the physical type and the caller's
    /// encoding options. The answer does not depend on the data page version: the values section of a
    /// V1 page and of a V2 page hold the same bytes, and the format carries the encoding in the page
    /// header either way. Only the level framing and the compression boundary differ.
    /// </summary>
    public static Encoding GetValueEncoding(
        PhysicalType physicalType,
        ByteArrayEncoding byteArrayEncoding,
        FloatingPointEncoding floatingPointEncoding,
        IntegerEncoding integerEncoding) =>
        physicalType switch
        {
            PhysicalType.Boolean => Encoding.Rle,
            PhysicalType.Int32 or PhysicalType.Int64 => integerEncoding switch
            {
#pragma warning disable EWPARQUET0005 // PFOR is intentionally selectable; the experimental signal lives on the enum value, not internal dispatch.
                IntegerEncoding.Pfor => Encoding.Pfor,
#pragma warning restore EWPARQUET0005
                IntegerEncoding.Plain => Encoding.Plain,
                _ => Encoding.DeltaBinaryPacked,
            },
            PhysicalType.Float or PhysicalType.Double => floatingPointEncoding switch
            {
#pragma warning disable EWPARQUET0001 // ALP is intentionally selectable; the experimental signal lives on the enum value, not internal dispatch.
                FloatingPointEncoding.Alp => Encoding.Alp,
#pragma warning restore EWPARQUET0001
                FloatingPointEncoding.ByteStreamSplit => Encoding.ByteStreamSplit,
                FloatingPointEncoding.Plain => Encoding.Plain,
                // Every arm above is named, and the catch-all is PLAIN rather than
                // BYTE_STREAM_SPLIT. BSS is still the enum's ZERO value, so a fall-through to it
                // would hand `default(FloatingPointEncoding)` an encoding Spark's vectorized reader
                // rejects; PLAIN is the safe answer for an enum value added later too.
                _ => Encoding.Plain,
            },
            PhysicalType.ByteArray => byteArrayEncoding switch
            {
                ByteArrayEncoding.Plain => Encoding.Plain,
                ByteArrayEncoding.DeltaByteArray => Encoding.DeltaByteArray,
#pragma warning disable EWPARQUET0003 // FSST is intentionally selectable; the experimental signal lives on the enum values, not internal dispatch.
                ByteArrayEncoding.Fsst or ByteArrayEncoding.Fsst16 => Encoding.Fsst,
#pragma warning restore EWPARQUET0003
                _ => Encoding.DeltaLengthByteArray,
            },
            PhysicalType.FixedLenByteArray when byteArrayEncoding == ByteArrayEncoding.DeltaByteArray
                => Encoding.DeltaByteArray,
            _ => Encoding.Plain,
        };

    /// <summary>
    /// Resolves the fallback encoding for a column whose dictionary was abandoned
    /// (e.g. cardinality too high).
    /// </summary>
    public static Encoding GetFallbackEncoding(PhysicalType physicalType, ParquetWriteOptions options) =>
        GetValueEncoding(physicalType, options.ByteArrayEncoding, options.FloatingPointEncoding,
            options.IntegerEncoding);
}
