// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Parquet.Tests.Parquet.Data;

/// <summary>
/// <c>PresentValues.Scatter</c> takes a final partial byte a row at a time, then whole bytes three ways
/// (all present as one block, mixed, all null), and stops early when the read index meets the row or runs
/// out. These check each path against a per-bit reference at row counts that end at every position in a
/// byte, over patterns built to reach each path and each stop: random shares, leading and trailing nulls,
/// and nulls only at the top or only at the bottom.
/// </summary>
public class PresentValuesScatterTests
{
    public static TheoryData<int> Rows =>
    [
        0, 1, 2, 7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33, 63, 64, 65, 100, 127, 128, 129, 1_003,
    ];

    private static IEnumerable<(string Name, bool[] Valid)> Patterns(int rows)
    {
        foreach (double share in new[] { 0.0, 0.02, 1.0 / 7, 0.5, 0.9, 1.0 })
        {
            var random = new Random(rows * 31 + (int)(share * 1000));
            yield return ($"random {share:F2}", Enumerable.Range(0, rows).Select(_ => random.NextDouble() >= share).ToArray());
        }

        foreach (int k in new[] { 1, 3, 8, 9, rows / 2 })
        {
            // Nulls only at the bottom: every value moves, read runs out at the top of the null block.
            yield return ($"{k} leading nulls", Enumerable.Range(0, rows).Select(i => i >= k).ToArray());
            // Nulls only at the top: nothing moves, the first byte below them already lines up.
            yield return ($"{k} trailing nulls", Enumerable.Range(0, rows).Select(i => i < rows - k).ToArray());
            // One null block in the middle: whole bytes move above it, the early stop fires below it.
            yield return ($"{k} nulls at the middle", Enumerable.Range(0, rows).Select(i => i < rows / 3 || i >= rows / 3 + k).ToArray());
        }

        yield return ("alternating", Enumerable.Range(0, rows).Select(i => i % 2 == 0).ToArray());
        yield return ("one null per byte", Enumerable.Range(0, rows).Select(i => i % 8 != 5).ToArray());
    }

    [Theory]
    [MemberData(nameof(Rows))]
    public void MatchesPerBitReference(int rows)
    {
        foreach (var (name, valid) in Patterns(rows))
        {
            var bitmap = new byte[(rows + 7) / 8];
            int present = 0;
            for (int i = 0; i < rows; i++)
            {
                if (valid[i])
                {
                    bitmap[i >> 3] |= (byte)(1 << (i & 7));
                    present++;
                }
            }

            // Distinct values, so a value landing one row off cannot match by accident. Room past the rows, as
            // the reader's buffer has, holding a sentinel that must survive.
            const long Sentinel = -77;
            var values = new long[rows + 9];
            values.AsSpan().Fill(Sentinel);
            for (int i = 0; i < present; i++)
                values[i] = 1_000 + i;

            PresentValues.Scatter<long>(values, bitmap, rows, present);

            int next = 0;
            for (int i = 0; i < rows; i++)
            {
                if (valid[i])
                    Assert.True(values[i] == 1_000 + next++, $"{name}, {rows} rows: row {i} holds {values[i]}");
            }

            for (int i = rows; i < values.Length; i++)
                Assert.True(values[i] == Sentinel, $"{name}, {rows} rows: wrote past the rows at {i}");
        }
    }

    [Fact]
    public void FourByteValues_MatchPerBitReference()
    {
        // The block move is in elements, not bytes: a narrower T must move the same rows.
        const int rows = 77;
        var random = new Random(5);
        var bitmap = new byte[(rows + 7) / 8];
        random.NextBytes(bitmap);
        bitmap[bitmap.Length - 1] &= (1 << (rows & 7)) - 1;
        bitmap[2] = 0xFF;
        bitmap[3] = 0xFF;
        int present = Enumerable.Range(0, rows).Count(i => (bitmap[i >> 3] & (1 << (i & 7))) != 0);

        var values = new float[rows];
        for (int i = 0; i < present; i++)
            values[i] = i + 0.5f;

        PresentValues.Scatter<float>(values, bitmap, rows, present);

        int next = 0;
        for (int i = 0; i < rows; i++)
        {
            if ((bitmap[i >> 3] & (1 << (i & 7))) != 0)
                Assert.Equal(next++ + 0.5f, values[i]);
        }
    }

    [Fact]
    public void NothingPresent_AcceptsAnUnallocatedBuffer()
    {
        // An all-null page may never have allocated its value buffer.
        PresentValues.Scatter(Span<double>.Empty, new byte[2], rows: 13, present: 0);
    }

    [Fact]
    public void RefusesCountsOutsideTheRows()
    {
        var bitmap = new byte[] { 0xFF, 0xFF };
        Assert.Throws<ArgumentOutOfRangeException>(() => PresentValues.Scatter(new long[16], bitmap, rows: 16, present: 17));
        Assert.Throws<ArgumentOutOfRangeException>(() => PresentValues.Scatter(new long[16], bitmap, rows: -1, present: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => PresentValues.Scatter(new long[15], bitmap, rows: 16, present: 3));
        Assert.Throws<ArgumentOutOfRangeException>(() => PresentValues.Scatter(new long[16], new byte[1], rows: 16, present: 3));
    }
}
