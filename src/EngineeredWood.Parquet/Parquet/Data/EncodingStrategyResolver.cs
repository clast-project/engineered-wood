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
    /// Gets the non-dictionary encoding for a V2 data page based on the physical type.
    /// </summary>
    public static Encoding GetV2Encoding(
        PhysicalType physicalType,
        ByteArrayEncoding byteArrayEncoding,
        FloatingPointEncoding floatingPointEncoding) =>
        physicalType switch
        {
            PhysicalType.Boolean => Encoding.Rle,
            PhysicalType.Int32 or PhysicalType.Int64 => Encoding.DeltaBinaryPacked,
            PhysicalType.Float or PhysicalType.Double => floatingPointEncoding switch
            {
#pragma warning disable EWPARQUET0001 // ALP is intentionally selectable; the experimental signal lives on the enum value, not internal dispatch.
                FloatingPointEncoding.Alp => Encoding.Alp,
#pragma warning restore EWPARQUET0001
                FloatingPointEncoding.Plain => Encoding.Plain,
                _ => Encoding.ByteStreamSplit,
            },
            PhysicalType.ByteArray => byteArrayEncoding == ByteArrayEncoding.DeltaByteArray
                ? Encoding.DeltaByteArray
                : Encoding.DeltaLengthByteArray,
            PhysicalType.FixedLenByteArray when byteArrayEncoding == ByteArrayEncoding.DeltaByteArray
                => Encoding.DeltaByteArray,
            _ => Encoding.Plain,
        };

    /// <summary>
    /// Gets the encoding for V1 data pages (always PLAIN).
    /// </summary>
    public static Encoding GetV1Encoding() => Encoding.Plain;

    /// <summary>
    /// Resolves the fallback encoding for a column whose dictionary was abandoned
    /// (e.g. cardinality too high). Returns the appropriate V2 encoding for the type.
    /// </summary>
    public static Encoding GetFallbackEncoding(PhysicalType physicalType, ParquetWriteOptions options) =>
        options.DataPageVersion == DataPageVersion.V2
            ? GetV2Encoding(physicalType, options.ByteArrayEncoding, options.FloatingPointEncoding)
            : GetV1Encoding();
}
