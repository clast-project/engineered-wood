// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// A DELTA_LENGTH_BYTE_ARRAY page whose lengths are impossible (negative, or adding up past the page) is refused,
/// whichever output kind it decodes to and whether or not a row selection drops the bad values (#787 review): every
/// length is checked before any value is written.
/// </summary>
public class DeltaLengthByteArrayValidationTests
{
    public static IEnumerable<object[]> Cases()
    {
        foreach (var output in new[] { ByteArrayOutputKind.Default, ByteArrayOutputKind.ViewType, ByteArrayOutputKind.LargeOffsets })
        {
            foreach (bool selected in new[] { false, true })
            {
                yield return [output, selected, new[] { 1, -1, 1 }, "A"];        // negative, dropped by the selection
                yield return [output, selected, new[] { 2, -1, 0 }, "AB"];       // negative, the total still in the page
                yield return [output, selected, new[] { 1, 5, 1 }, "ABC"];       // past the page's bytes
                yield return [output, selected, new[] { int.MaxValue, int.MaxValue, 2 }, "AB"]; // wraps as int
            }
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void ImpossibleLengthsAreRefused(ByteArrayOutputKind output, bool selected, int[] lengths, string payload)
    {
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt32s(lengths);
        byte[] page = [.. encoder.WrittenSpan.ToArray(), .. System.Text.Encoding.ASCII.GetBytes(payload)];

        using var state = new ColumnBuildState(PhysicalType.ByteArray, maxDefLevel: 0, maxRepLevel: 0, capacity: lengths.Length, output);
        if (selected)
        {
            // The middle value, the bad one in the first case, is dropped.
            state.SetRowSelection([1, 0, 1]);
            state.BeginPage(lengths.Length, lengths.Length);
        }

        Assert.Throws<ParquetFormatException>(() => DeltaLengthByteArrayDecoder.Decode(page, lengths.Length, state, selected));
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
