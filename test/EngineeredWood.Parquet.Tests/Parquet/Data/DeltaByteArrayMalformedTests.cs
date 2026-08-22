// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// DELTA_BYTE_ARRAY reconstructs each value as the first <c>prefix_length</c> bytes of the PREVIOUS
/// value, followed by a suffix. Nothing checked that the prefix length actually fit inside the
/// previous value.
///
/// It does not read out of bounds — the output buffer is sized from the same lengths — so it read
/// forward into the zero-filled region reserved for the value being reconstructed, and produced a
/// value that is neither what was encoded nor an error. A first value with a nonzero prefix is the
/// same bug at index 0, where there is no previous value at all.
///
/// Malformed input has to be rejected rather than reconstructed into something plausible. These
/// payloads are hand-built, because no encoder here can produce them.
/// </summary>
public class DeltaByteArrayMalformedTests
{
    /// <summary>
    /// Builds a DELTA_BYTE_ARRAY page from raw parts: prefix lengths, suffix lengths, suffix bytes.
    /// </summary>
    private static byte[] Page(int[] prefixLengths, int[] suffixLengths, byte[] suffixBytes)
    {
        var prefixes = new DeltaBinaryPackedEncoder(64);
        prefixes.EncodeInt32s(prefixLengths);
        var suffixes = new DeltaBinaryPackedEncoder(64);
        suffixes.EncodeInt32s(suffixLengths);

        var page = new byte[prefixes.Length + suffixes.Length + suffixBytes.Length];
        prefixes.WrittenSpan.CopyTo(page);
        suffixes.WrittenSpan.CopyTo(page.AsSpan(prefixes.Length));
        suffixBytes.CopyTo(page.AsSpan(prefixes.Length + suffixes.Length));
        return page;
    }

    private static ParquetFormatException Decode(byte[] page, int count)
    {
        using var state = new ColumnBuildState(PhysicalType.ByteArray, 0, 0, capacity: 16);
        return Assert.Throws<ParquetFormatException>(() => DeltaByteArrayDecoder.Decode(page, count, state));
    }

    [Fact]
    public void APrefixLongerThanThePreviousValueIsRejected()
    {
        // Value 0 is two bytes. Value 1 claims to share five of them — there are only two, so the
        // reconstruction would take three bytes of whatever follows.
        var page = Page([0, 5], [2, 1], [0xAA, 0xBB, 0xCC]);

        var error = Decode(page, 2);
        Assert.Contains("prefix", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void APrefixOnTheFirstValueIsRejected()
    {
        // There is no previous value at index 0, so any nonzero prefix is meaningless.
        var page = Page([3], [1], [0xAA]);

        var error = Decode(page, 1);
        Assert.Contains("prefix", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ANegativePrefixIsRejected()
    {
        var page = Page([0, -1], [2, 2], [0xAA, 0xBB, 0xCC, 0xDD]);

        Decode(page, 2);
    }

    [Fact]
    public void ANegativeSuffixIsRejected()
    {
        var page = Page([0, 0], [2, -1], [0xAA, 0xBB]);

        Decode(page, 2);
    }

    [Fact]
    public void ASuffixRunningPastTheEndOfThePageIsRejected()
    {
        // The suffix bytes claimed are longer than the page actually carries.
        var page = Page([0, 0], [2, 99], [0xAA, 0xBB]);

        Decode(page, 2);
    }

    [Fact]
    public void APrefixExactlyTheLengthOfThePreviousValueIsFine()
    {
        // The boundary the check must not reject: sharing the whole previous value is legal, and is
        // what an encoder emits for a repeated value.
        var page = Page([0, 2], [2, 0], [0xAA, 0xBB]);

        using var state = new ColumnBuildState(PhysicalType.ByteArray, 0, 0, capacity: 16);
        DeltaByteArrayDecoder.Decode(page, 2, state);

        Assert.Equal(2, state.ValueCount);
    }

    [Fact]
    public void AWellFormedPageStillDecodes()
    {
        // "AB", then "AC" — one shared prefix byte, which is the ordinary case.
        var page = Page([0, 1], [2, 1], [0x41, 0x42, 0x43]);

        using var state = new ColumnBuildState(PhysicalType.ByteArray, 0, 0, capacity: 16);
        DeltaByteArrayDecoder.Decode(page, 2, state);

        Assert.Equal(2, state.ValueCount);
    }
}
