// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary><see cref="SelectionMask"/> against a <c>bool[]</c> per row.</summary>
public class SelectionMaskTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(63)]
    [InlineData(64)]
    [InlineData(65)]
    [InlineData(1000)]
    public void SelectAllSelectsExactlyTheRows(int rows)
    {
        var words = Noise(SelectionMask.WordCount(rows), new Random(rows));
        SelectionMask.SelectAll(words, rows);
        Assert.Equal(Enumerable.Repeat(true, rows), ToBools(words, rows));
        Assert.Equal(rows, SelectionMask.Count(words)); // nothing past the last row
    }

    [Theory]
    [InlineData(int.MaxValue, 33554432)]
    [InlineData(int.MaxValue - 63, 33554431)]
    [InlineData(int.MaxValue - 64, 33554431)]
    public void WordCountDoesNotOverflow(int rows, int words) => Assert.Equal(words, SelectionMask.WordCount(rows));

    [Fact]
    public void ClearRangeMatchesPerRowClearing()
    {
        var rng = new Random(1);
        for (int trial = 0; trial < 2000; trial++)
        {
            int rows = rng.Next(0, 300);
            var words = new ulong[SelectionMask.WordCount(rows) + 1];
            SelectionMask.SelectAll(words, rows);
            var expected = Enumerable.Repeat(true, rows).ToArray();
            for (int r = 0; r < 4; r++)
            {
                int from = rng.Next(0, rows + 1), to = rng.Next(0, rows + 1);
                SelectionMask.ClearRange(words, from, to);
                for (int i = from; i < to; i++) expected[i] = false;
            }
            Assert.Equal(expected, ToBools(words, rows));
            Assert.Equal(expected.Count(b => b), SelectionMask.Count(words));
            Assert.Equal(0UL, words[SelectionMask.WordCount(rows)]); // the guard word is untouched
        }
    }

    [Fact]
    public void ToIndicesListsSelectedRowsInOrder()
    {
        var rng = new Random(2);
        for (int trial = 0; trial < 500; trial++)
        {
            int rows = rng.Next(0, 400);
            var bools = Enumerable.Range(0, rows).Select(_ => rng.Next(3) != 0).ToArray();
            var words = FromBools(bools);
            var indices = new int[rows];
            int n = SelectionMask.ToIndices(words, indices);
            Assert.Equal(Enumerable.Range(0, rows).Where(i => bools[i]).ToArray(), indices.AsSpan(0, n).ToArray());
            Assert.Equal(n, SelectionMask.Count(words));
        }
    }

    [Fact]
    public void AndKeepsRowsWhereTheMaskIsTrue()
    {
        var rng = new Random(3);
        for (int trial = 0; trial < 500; trial++)
        {
            int rows = rng.Next(0, 300);
            bool?[] mask = Enumerable.Range(0, rows)
                .Select(_ => rng.Next(4) switch { 0 => (bool?)null, 1 => false, _ => true }).ToArray();
            var selected = Enumerable.Range(0, rows).Select(_ => rng.Next(5) != 0).ToArray();

            var words = FromBools(selected);
            SelectionMask.And(words, rows, Build(mask));

            var expected = selected.Select((s, i) => s && mask[i] == true).ToArray();
            Assert.Equal(expected, ToBools(words, rows));
            Assert.Equal(expected.Count(b => b), SelectionMask.Count(words));
        }
    }

    [Fact]
    public void AndReadsASlicedMaskFromItsOffset()
    {
        var rng = new Random(4);
        for (int trial = 0; trial < 500; trial++)
        {
            int total = rng.Next(1, 400);
            bool?[] mask = Enumerable.Range(0, total)
                .Select(_ => rng.Next(4) switch { 0 => (bool?)null, 1 => false, _ => true }).ToArray();
            int offset = rng.Next(0, total), length = rng.Next(0, total - offset + 1);
            var sliced = (BooleanArray)Build(mask).Slice(offset, length);

            var words = new ulong[SelectionMask.WordCount(length)];
            SelectionMask.SelectAll(words, length);
            SelectionMask.And(words, length, sliced);

            Assert.Equal(Enumerable.Range(offset, length).Select(i => mask[i] == true), ToBools(words, length));
            Assert.Equal(mask.Skip(offset).Take(length).Count(b => b == true), SelectionMask.Count(words));
        }
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(9, 0)]
    [InlineData(70, 0)]
    [InlineData(70, 5)]
    [InlineData(130, 3)]
    public void AndToleratesBuffersNotPaddedToAWord(int rows, int offset)
    {
        // Exactly ceil((offset + rows) / 8) bytes, as a gather produces them: the last word reads past the end.
        int bytes = (offset + rows + 7) / 8;
        var values = new byte[bytes];
        var validity = new byte[bytes];
        var rng = new Random(rows * 31 + offset);
        rng.NextBytes(values);
        rng.NextBytes(validity);
        int nulls = 0;
        for (int i = offset; i < offset + rows; i++)
            if ((validity[i >> 3] >> (i & 7) & 1) == 0) nulls++;
        var mask = new BooleanArray(new ArrowBuffer(values), new ArrowBuffer(validity), rows, nulls, offset);

        var words = new ulong[SelectionMask.WordCount(rows)];
        SelectionMask.SelectAll(words, rows);
        SelectionMask.And(words, rows, mask);

        var expected = Enumerable.Range(offset, rows)
            .Select(i => (values[i >> 3] >> (i & 7) & 1) != 0 && (validity[i >> 3] >> (i & 7) & 1) != 0);
        Assert.Equal(expected, ToBools(words, rows));
        Assert.Equal(expected.Count(b => b), SelectionMask.Count(words));
    }

    [Fact]
    public void AndRefusesAMaskOfAnotherLength()
    {
        var words = new ulong[1];
        Assert.Throws<ArgumentException>(() => SelectionMask.And(words, 10, Build(new bool?[9])));
        Assert.Throws<ArgumentNullException>(() => SelectionMask.And(words, 10, null!));
    }

    private static BooleanArray Build(bool?[] values)
    {
        var builder = new BooleanArray.Builder();
        foreach (var v in values)
        {
            if (v is null) builder.AppendNull();
            else builder.Append(v.Value);
        }
        return builder.Build();
    }

    private static ulong[] Noise(int words, Random rng)
    {
        var result = new ulong[words];
        for (int i = 0; i < words; i++)
            result[i] = ((ulong)(uint)rng.Next() << 33) ^ ((ulong)(uint)rng.Next() << 2) ^ (ulong)(uint)rng.Next(4);
        return result;
    }

    private static ulong[] FromBools(bool[] bools)
    {
        var words = new ulong[SelectionMask.WordCount(bools.Length)];
        for (int i = 0; i < bools.Length; i++)
            if (bools[i]) words[i >> 6] |= 1UL << (i & 63);
        return words;
    }

    private static bool[] ToBools(ulong[] words, int rows) =>
        Enumerable.Range(0, rows).Select(i => (words[i >> 6] >> (i & 63) & 1) != 0).ToArray();
}
