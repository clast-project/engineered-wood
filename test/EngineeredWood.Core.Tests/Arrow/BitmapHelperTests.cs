// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// <c>BitmapHelper</c> picks between a Vector256 loop, a Vector128 loop and a scalar loop based on the
/// element count, then finishes with a scalar tail. Which branch runs is invisible from the outside, so
/// these cross-check every path against an independent scalar reference across counts that straddle the
/// 16- and 32-element thresholds and their immediate neighbours — where a mis-sized tail or a partial
/// final byte would show up.
/// </summary>
public class BitmapHelperTests
{
    /// <summary>
    /// Counts around each dispatch boundary: below the Vector128 threshold, at and around 16, at and
    /// around 32, one whole vector plus a partial, and a count that is a multiple of neither.
    /// </summary>
    public static TheoryData<int> Counts =>
    [
        0, 1, 2, 7, 8, 9, 15, 16, 17, 23, 24, 31, 32, 33,
        39, 40, 47, 48, 63, 64, 65, 96, 100, 127, 128, 129, 200,
    ];

    private static byte[] ScalarReference(ReadOnlySpan<byte> values, int count, byte target)
    {
        var expected = new byte[(count + 7) / 8];
        for (int i = 0; i < count; i++)
        {
            if (values[i] == target)
                expected[i >> 3] |= (byte)(1 << (i & 7));
        }
        return expected;
    }

    /// <summary>
    /// Compares only the bits below <paramref name="count"/>. Bits in the final partial byte past the end
    /// are padding, which Arrow leaves undefined, so asserting on them would be testing an accident.
    /// </summary>
    private static void AssertBitsEqual(byte[] expected, byte[] actual, int count)
    {
        for (int i = 0; i < count; i++)
        {
            bool e = (expected[i >> 3] & (1 << (i & 7))) != 0;
            bool a = (actual[i >> 3] & (1 << (i & 7))) != 0;
            Assert.True(e == a, $"bit {i} of {count}: expected {e}, got {a}");
        }
    }

    private static byte[] Values(int count, Func<int, byte> pick)
    {
        // Over-allocate: the vector loops read in 16/32-byte strides and the helper is only promised a
        // buffer at least as long as the count.
        var values = new byte[Math.Max(count + 32, 32)];
        for (int i = 0; i < count; i++) values[i] = pick(i);
        return values;
    }

    [Theory]
    [MemberData(nameof(Counts))]
    public void BuildFromEquality_MatchesScalarReference_MixedValues(int count)
    {
        // A pattern that is not periodic on 8, 16 or 32 so a misaligned lane cannot coincidentally agree.
        byte[] values = Values(count, i => (byte)(i % 5 == 0 ? 1 : (i % 7 == 0 ? 1 : 2)));
        var bitmap = new byte[(count + 7) / 8];

        BitmapHelper.BuildFromEquality(values, bitmap, count, target: 1);

        AssertBitsEqual(ScalarReference(values, count, 1), bitmap, count);
    }

    [Theory]
    [MemberData(nameof(Counts))]
    public void BuildFromEquality_MatchesScalarReference_AllMatch(int count)
    {
        byte[] values = Values(count, _ => 1);
        var bitmap = new byte[(count + 7) / 8];

        BitmapHelper.BuildFromEquality(values, bitmap, count, target: 1);

        AssertBitsEqual(ScalarReference(values, count, 1), bitmap, count);
        for (int i = 0; i < count; i++)
            Assert.True((bitmap[i >> 3] & (1 << (i & 7))) != 0, $"bit {i} should be set");
    }

    [Theory]
    [MemberData(nameof(Counts))]
    public void BuildFromEquality_MatchesScalarReference_NoneMatch(int count)
    {
        byte[] values = Values(count, _ => 9);
        var bitmap = new byte[(count + 7) / 8];

        BitmapHelper.BuildFromEquality(values, bitmap, count, target: 1);

        for (int i = 0; i < count; i++)
            Assert.True((bitmap[i >> 3] & (1 << (i & 7))) == 0, $"bit {i} should be clear");
    }

    [Theory]
    [MemberData(nameof(Counts))]
    public void BuildFromEquality_OverwritesDirtyBitmap(int count)
    {
        // Every path either overwrites its bytes or clears before OR-ing, so a reused buffer must not
        // leak stale set bits into the result. Nothing in Core promises a zeroed buffer on entry.
        byte[] values = Values(count, i => (byte)(i % 4 == 0 ? 1 : 2));
        var bitmap = new byte[(count + 7) / 8];
        bitmap.AsSpan().Fill(0xFF);

        BitmapHelper.BuildFromEquality(values, bitmap, count, target: 1);

        AssertBitsEqual(ScalarReference(values, count, 1), bitmap, count);
    }

    [Theory]
    [MemberData(nameof(Counts))]
    public void BuildFromEquality_TargetOtherThanOne(int count)
    {
        byte[] values = Values(count, i => (byte)(i % 3));
        var bitmap = new byte[(count + 7) / 8];

        BitmapHelper.BuildFromEquality(values, bitmap, count, target: 2);

        AssertBitsEqual(ScalarReference(values, count, 2), bitmap, count);
    }

    [Theory]
    [MemberData(nameof(Counts))]
    public void BuildFromBooleans_MatchesScalarReference(int count)
    {
        var values = new bool[Math.Max(count + 32, 32)];
        for (int i = 0; i < count; i++) values[i] = i % 5 == 0 || i % 7 == 0;

        var bitmap = new byte[(count + 7) / 8];
        BitmapHelper.BuildFromBooleans(values, bitmap, count);

        var expected = new byte[(count + 7) / 8];
        for (int i = 0; i < count; i++)
        {
            if (values[i])
                expected[i >> 3] |= (byte)(1 << (i & 7));
        }

        AssertBitsEqual(expected, bitmap, count);
    }

    [Theory]
    [MemberData(nameof(Counts))]
    public void BuildFromBooleans_AllTrueAndAllFalse(int count)
    {
        var allTrue = new bool[Math.Max(count + 32, 32)];
        for (int i = 0; i < count; i++) allTrue[i] = true;

        var bitmap = new byte[(count + 7) / 8];
        BitmapHelper.BuildFromBooleans(allTrue, bitmap, count);
        for (int i = 0; i < count; i++)
            Assert.True((bitmap[i >> 3] & (1 << (i & 7))) != 0, $"bit {i} should be set");

        var allFalse = new bool[Math.Max(count + 32, 32)];
        var bitmap2 = new byte[(count + 7) / 8];
        BitmapHelper.BuildFromBooleans(allFalse, bitmap2, count);
        for (int i = 0; i < count; i++)
            Assert.True((bitmap2[i >> 3] & (1 << (i & 7))) == 0, $"bit {i} should be clear");
    }

    [Fact]
    public void SetBit_SetsExactlyOneBit()
    {
        var bitmap = new byte[4];

        BitmapHelper.SetBit(bitmap, 0);
        BitmapHelper.SetBit(bitmap, 7);
        BitmapHelper.SetBit(bitmap, 8);
        BitmapHelper.SetBit(bitmap, 31);

        Assert.Equal(0b1000_0001, bitmap[0]);
        Assert.Equal(0b0000_0001, bitmap[1]);
        Assert.Equal(0, bitmap[2]);
        Assert.Equal(0b1000_0000, bitmap[3]);
    }

    [Fact]
    public void SetBit_IsIdempotent()
    {
        var bitmap = new byte[1];

        BitmapHelper.SetBit(bitmap, 3);
        BitmapHelper.SetBit(bitmap, 3);

        Assert.Equal(0b0000_1000, bitmap[0]);
    }
}
