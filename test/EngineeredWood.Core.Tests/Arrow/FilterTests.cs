// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// <see cref="ArrowCompute.SelectedIndices"/> reads the mask a 64-bit word at a time and finishes the partial
/// last word bit by bit; a sliced mask that starts inside a byte takes a row-by-row path. Every case is checked
/// against the row-by-row definition: the rows whose value is true, a null selecting nothing.
/// </summary>
public class FilterTests
{
    /// <summary>Lengths around the 64-bit word boundaries, where a mis-sized tail would show.</summary>
    public static TheoryData<int> Lengths => [0, 1, 7, 8, 9, 63, 64, 65, 127, 128, 129, 200, 1000];

    private enum Pattern
    {
        AllTrue,
        AllFalse,
        Random,
        RandomWithNulls,
        AllNull,
    }

    private static BooleanArray Mask(int length, Pattern pattern, int seed = 1)
    {
        var random = new Random(seed);
        var builder = new BooleanArray.Builder();
        for (int i = 0; i < length; i++)
        {
            switch (pattern)
            {
                case Pattern.AllTrue: builder.Append(true); break;
                case Pattern.AllFalse: builder.Append(false); break;
                case Pattern.Random: builder.Append(random.Next(3) == 0); break;
                case Pattern.AllNull: builder.AppendNull(); break;
                default:
                    int r = random.Next(4);
                    if (r == 0)
                        builder.AppendNull();
                    else
                        builder.Append(r == 1);
                    break;
            }
        }
        return builder.Build();
    }

    private static int[] Reference(BooleanArray mask)
    {
        var expected = new List<int>();
        for (int i = 0; i < mask.Length; i++)
        {
            if (mask.GetValue(i) == true)
                expected.Add(i);
        }
        return expected.ToArray();
    }

    [Theory]
    [MemberData(nameof(Lengths))]
    public void SelectedIndices_MatchesTheRowByRowDefinition(int length)
    {
        foreach (Pattern pattern in Enum.GetValues(typeof(Pattern)))
        {
            var mask = Mask(length, pattern);
            Assert.Equal(Reference(mask), ArrowCompute.SelectedIndices(mask));
        }
    }

    [Fact]
    public void SelectedIndices_NullsSelectNothingEvenWhereTheValueBitIsSet()
    {
        // A null slot's value bit is undefined; build one whose value bit IS set, so only the validity bitmap
        // can drop it.
        int length = 70;
        var valueBits = new byte[(length + 7) / 8];
        var validityBits = new byte[(length + 7) / 8];
        for (int i = 0; i < length; i++)
        {
            valueBits[i >> 3] |= (byte)(1 << (i & 7));       // every value bit set
            if (i % 3 != 0)
                validityBits[i >> 3] |= (byte)(1 << (i & 7)); // every third row null
        }
        int nulls = (length + 2) / 3;
        var mask = new BooleanArray(
            new ArrowBuffer(valueBits), new ArrowBuffer(validityBits), length, nulls, 0);

        var expected = Enumerable.Range(0, length).Where(i => i % 3 != 0).ToArray();
        Assert.Equal(expected, ArrowCompute.SelectedIndices(mask));
    }

    [Theory]
    [InlineData(0, 200)]
    [InlineData(8, 150)]     // starts on a byte
    [InlineData(64, 100)]    // starts on a word
    [InlineData(3, 150)]     // starts inside a byte
    [InlineData(67, 70)]
    public void SelectedIndices_OfASlicedMask_CountsFromTheSlice(int offset, int length)
    {
        foreach (var pattern in new[] { Pattern.Random, Pattern.RandomWithNulls })
        {
            var whole = Mask(offset + length + 10, pattern, seed: offset + length);
            var slice = (BooleanArray)ArrowArrayFactory.Slice(whole, offset, length);
            Assert.Equal(Reference(slice), ArrowCompute.SelectedIndices(slice));
        }
    }

    [Fact]
    public void Filter_KeepsTheSelectedRowsOfEveryColumn_InOrder()
    {
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        for (int i = 0; i < 130; i++)
        {
            ids.Append(i);
            if (i % 5 == 0)
                names.AppendNull();
            else
                names.Append($"n{i}");
        }
        var schema = new Schema.Builder()
            .Field(new Field("id", Apache.Arrow.Types.Int64Type.Default, nullable: false))
            .Field(new Field("name", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Build();
        using var batch = new RecordBatch(schema, [ids.Build(), names.Build()], 130);
        var mask = Mask(130, Pattern.RandomWithNulls, seed: 7);
        var expected = Reference(mask);

        using var filtered = ArrowCompute.Filter(batch, mask);

        Assert.Same(schema, filtered.Schema);
        Assert.Equal(expected.Length, filtered.Length);
        var keptIds = (Int64Array)filtered.Column(0);
        var keptNames = (StringArray)filtered.Column(1);
        for (int k = 0; k < expected.Length; k++)
        {
            int row = expected[k];
            Assert.Equal(row, keptIds.GetValue(k));
            Assert.Equal(row % 5 == 0 ? null : $"n{row}", keptNames.GetString(k));
        }
    }

    [Fact]
    public void Filter_RefusesAMaskOfAnotherLength()
    {
        var schema = new Schema.Builder()
            .Field(new Field("id", Apache.Arrow.Types.Int64Type.Default, nullable: false)).Build();
        using var batch = new RecordBatch(schema, [new Int64Array.Builder().AppendRange([1, 2, 3]).Build()], 3);

        var ex = Assert.Throws<ArgumentException>(() => ArrowCompute.Filter(batch, Mask(4, Pattern.AllTrue)));
        Assert.Equal("mask", ex.ParamName);
    }
}
