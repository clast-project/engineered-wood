// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// The ceiling on how many bytes of BYTE_ARRAY data can land in one Arrow array, and the diagnostics
/// for exceeding it.
/// </summary>
/// <remarks>
/// <para>Arrow's <c>StringArray</c>/<c>BinaryArray</c> address their data buffer with 32-bit offsets, and
/// the buffer itself is handled as a <see cref="Span{T}"/>, whose length is an <see cref="int"/>. So the
/// limit is <see cref="int.MaxValue"/> and it is structural, not a tunable — <c>LargeOffsets</c> output
/// widens the OFFSETS to 64 bits but the data buffer is still spanned, so it does not raise this
/// ceiling.</para>
/// <para>Before this existed, both the decoder's running total and the builder's write offset were
/// <see cref="int"/> and simply wrapped negative, which surfaced as
/// <c>ArgumentOutOfRangeException</c> from a span slice — telling the caller nothing about which
/// column was too large, or that size was the problem at all (issue #157).</para>
/// </remarks>
internal static class ByteArrayCapacity
{
    /// <summary>Maximum decoded BYTE_ARRAY bytes addressable by one Arrow array.</summary>
    internal const long MaxBytes = int.MaxValue;

    /// <summary>
    /// A single value larger than one Arrow array can hold. Nothing can split this: it is one value, so
    /// no batching makes it fit. Parquet's own 4-byte length prefix makes this hard to reach for a
    /// well-formed PLAIN page, which is why it names the value rather than suggesting a remedy.
    /// </summary>
    internal static Exception ValueTooLarge(string? columnPath, int valueIndex, long valueLength) =>
        new NotSupportedException(
            $"{Describe(columnPath)} contains a value of length {valueLength:N0} bytes at index {valueIndex:N0}, " +
            $"which is not supported: a single BYTE_ARRAY value cannot exceed {MaxBytes:N0} bytes, the most " +
            "one Arrow array can address with 32-bit offsets.");

    /// <summary>
    /// More decoded bytes than one Arrow array can hold, across several values. Splitting the read into
    /// smaller batches is the remedy, so the message says so and names the option.
    /// </summary>
    internal static Exception ChunkTooLarge(string? columnPath, long neededBytes) =>
        new NotSupportedException(
            $"{Describe(columnPath)} decodes to at least {neededBytes:N0} bytes, over the {MaxBytes:N0}-byte " +
            "limit for one Arrow array. Read the file in smaller batches so each stays under the limit — set " +
            $"{nameof(ParquetReadOptions)}.{nameof(ParquetReadOptions.MaxBatchByteSize)} and read with " +
            "ReadAllAsync or ReadRowGroupBatchesAsync; flat columns are split automatically when a chunk is " +
            "this large. A nested (list/map/struct) column cannot be split this way yet, and a single row " +
            "larger than the limit cannot be split at all.");

    private static string Describe(string? columnPath) =>
        columnPath is null ? "This BYTE_ARRAY column" : $"Column '{columnPath}'";
}
