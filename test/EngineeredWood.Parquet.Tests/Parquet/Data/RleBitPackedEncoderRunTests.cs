// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The encoder can be driven by runs instead of by values. The contract is byte equality with the
/// value-driven form over the same sequence — which is what makes it a substitution rather than a second
/// encoding, and the reason a run-end encoded column can skip materializing its indices at all.
/// </summary>
public class RleBitPackedEncoderRunTests
{
    private static byte[] FromValues(int bitWidth, ReadOnlySpan<int> values, int maxLiteralGroups = 1)
    {
        var encoder = new RleBitPackedEncoder(bitWidth, maxLiteralGroups: maxLiteralGroups);
        encoder.Encode(values);
        return encoder.ToArray();
    }

    private static byte[] FromRuns(
        int bitWidth, ReadOnlySpan<int> values, ReadOnlySpan<int> lengths, int maxLiteralGroups = 1)
    {
        var encoder = new RleBitPackedEncoder(bitWidth, maxLiteralGroups: maxLiteralGroups);
        encoder.EncodeRuns(values, lengths);
        return encoder.ToArray();
    }

    private static int[] Expand(ReadOnlySpan<int> values, ReadOnlySpan<int> lengths)
    {
        var expanded = new List<int>();
        for (int r = 0; r < values.Length; r++)
        {
            for (int i = 0; i < lengths[r]; i++)
                expanded.Add(values[r]);
        }

        return expanded.ToArray();
    }

    public static TheoryData<int[], int[]> RunShapes => new()
    {
        // A single long run — the constant column.
        { [0], [1000] },
        // Runs long enough to be RLE-encoded on both sides of the 8-value threshold.
        { [0, 1, 0], [100, 3, 100] },
        // Every run below the threshold, so the whole thing bit-packs.
        { [1, 2, 3, 1, 2], [1, 1, 1, 1, 1] },
        // A run that ends mid-group, forcing a literal flush before an RLE run.
        { [1, 0], [5, 40] },
        // Exactly the threshold.
        { [3, 4], [8, 8] },
        // A long run following a partial group.
        { [2, 5, 2], [3, 200, 7] },
    };

    [Theory]
    [MemberData(nameof(RunShapes))]
    public void EncodeRuns_MatchesEncodeOverTheExpandedSequence(int[] values, int[] lengths)
    {
        int bitWidth = 3;

        Assert.Equal(
            FromValues(bitWidth, Expand(values, lengths)),
            FromRuns(bitWidth, values, lengths));
    }

    [Theory]
    [MemberData(nameof(RunShapes))]
    public void EncodeRuns_MatchesEncode_WithLiteralBatching(int[] values, int[] lengths)
    {
        // Batching changes when literals are flushed, which is exactly where a run-driven encoder could
        // diverge from a value-driven one.
        int bitWidth = 3;

        Assert.Equal(
            FromValues(bitWidth, Expand(values, lengths), maxLiteralGroups: 8),
            FromRuns(bitWidth, values, lengths, maxLiteralGroups: 8));
    }

    [Fact]
    public void AppendRun_OneRunAtATime_MatchesTheSpanForm()
    {
        int[] values = [1, 0, 2];
        int[] lengths = [30, 4, 9];

        var encoder = new RleBitPackedEncoder(2);
        encoder.BeginRuns();
        for (int r = 0; r < values.Length; r++)
            encoder.AppendRun(values[r], lengths[r]);
        encoder.EndRuns();

        Assert.Equal(FromRuns(2, values, lengths), encoder.ToArray());
    }

    [Fact]
    public void AppendRun_SplitInTwo_EncodesAsTwoRuns_NotAsOne()
    {
        // Pinned because the page loop DOES split a run across pages, and each page is its own stream:
        // the halves must each stand alone rather than depend on being rejoined.
        var split = new RleBitPackedEncoder(2);
        split.BeginRuns();
        split.AppendRun(1, 20);
        split.AppendRun(1, 20);
        split.EndRuns();

        var whole = new RleBitPackedEncoder(2);
        whole.BeginRuns();
        whole.AppendRun(1, 40);
        whole.EndRuns();

        // Two RLE run headers against one — larger, and deliberately not silently merged, since a caller
        // that wanted them merged has to say so (DictionaryEncoder does).
        Assert.NotEqual(whole.ToArray(), split.ToArray());
        Assert.True(split.Length > whole.Length);
    }

    [Fact]
    public void EmptyRuns_EncodeToNothing()
    {
        Assert.Empty(FromRuns(2, [], []));
    }

    [Fact]
    public void ZeroLengthRuns_ContributeNothing()
    {
        Assert.Equal(
            FromRuns(2, [1, 2], [10, 10]),
            FromRuns(2, [1, 3, 2], [10, 0, 10]));
    }

    [Fact]
    public void MismatchedRunArrays_AreRejected()
    {
        var encoder = new RleBitPackedEncoder(2);

        Assert.Throws<ArgumentException>(() => encoder.EncodeRuns([1, 2], [5]));
    }
}
