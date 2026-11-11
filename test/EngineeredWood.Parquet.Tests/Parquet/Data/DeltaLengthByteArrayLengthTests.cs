// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// #788: a DELTA_LENGTH_BYTE_ARRAY page whose lengths are impossible (negative, or adding up past the page) is refused,
/// whichever output kind it decodes to: every length is checked before any value is written. Unchecked, the
/// offset-buffer outputs wrote such offsets as given, non-monotonic or wrapped past <c>int.MaxValue</c>, without
/// complaint. Found, and fixed, on <c>feature/row-level-filtering</c> (#787), where
/// <c>DeltaLengthByteArrayValidationTests</c> also crosses these pages with a row selection; ported to <c>main</c>
/// ahead of that branch.
/// </summary>
public class DeltaLengthByteArrayLengthTests
{
    public static IEnumerable<object[]> Cases()
    {
        foreach (var output in new[] { ByteArrayOutputKind.Default, ByteArrayOutputKind.ViewType, ByteArrayOutputKind.LargeOffsets })
        {
            yield return [output, new[] { 1, -1, 1 }, "A"];        // negative
            yield return [output, new[] { 2, -1, 0 }, "AB"];       // negative, the total still in the page
            yield return [output, new[] { 1, 5, 1 }, "ABC"];       // past the page's bytes
            yield return [output, new[] { int.MaxValue, int.MaxValue, 2 }, "AB"]; // wraps as int
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void ImpossibleLengthsAreRefused(ByteArrayOutputKind output, int[] lengths, string payload)
    {
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt32s(lengths);
        byte[] page = [.. encoder.WrittenSpan.ToArray(), .. System.Text.Encoding.ASCII.GetBytes(payload)];

        using var state = new ColumnBuildState(PhysicalType.ByteArray, maxDefLevel: 0, maxRepLevel: 0, capacity: lengths.Length, output);
        Assert.Throws<ParquetFormatException>(() => DeltaLengthByteArrayDecoder.Decode(page, lengths.Length, state));
    }

    [Fact]
    public void SoundLengthsDecode()
    {
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt32s([1, 0, 2]);
        byte[] page = [.. encoder.WrittenSpan.ToArray(), .. "ABC"u8.ToArray()];
        using var state = new ColumnBuildState(PhysicalType.ByteArray, maxDefLevel: 0, maxRepLevel: 0, capacity: 3);
        DeltaLengthByteArrayDecoder.Decode(page, 3, state);
        Assert.Equal(3, state.ValueCount);
    }
}
