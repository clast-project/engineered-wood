// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.TypeWidening;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The scale-changing decimal widenings (decimal → decimal with a larger scale, integer → decimal) multiply in
/// <c>Int128</c> over the raw slots. These check them against a <see cref="BigInteger"/> oracle at the edges a
/// span loop can get wrong: sign, the largest magnitude the source precision allows, nulls, and a sliced input
/// whose offset the loop has to apply itself.
/// </summary>
public class DecimalRescaleWideningTests
{
    private static BigInteger Pow10(int e) => BigInteger.Pow(10, e);

    private static byte[] ToLE16(BigInteger v)
    {
        var dest = new byte[16];
        dest.AsSpan().Fill(v.Sign < 0 ? (byte)0xFF : (byte)0x00);
        var bytes = v.ToByteArray();
        Assert.True(bytes.Length <= 16, $"{v} does not fit in 128 bits");
        bytes.CopyTo(dest, 0);
        return dest;
    }

    private static BigInteger FromLE16(ReadOnlySpan<byte> bytes) => new(bytes.ToArray());

    private static Decimal128Array Decimals(Decimal128Type type, params BigInteger?[] unscaled)
    {
        var bytes = new byte[unscaled.Length * 16];
        var validity = new ArrowBuffer.BitmapBuilder();
        for (int i = 0; i < unscaled.Length; i++)
        {
            validity.Append(unscaled[i] is not null);
            // Garbage under a null — the widening must not care what's there.
            var v = unscaled[i] ?? BigInteger.MinusOne << 120;
            ToLE16(v).CopyTo(bytes, i * 16);
        }
        int nulls = unscaled.Count(v => v is null);
        return new Decimal128Array(new ArrayData(
            type, unscaled.Length, nulls, 0, [validity.Build(), new ArrowBuffer(bytes)]));
    }

    private static void AssertUnscaled(BigInteger?[] expected, Decimal128Array actual, Decimal128Type type)
    {
        Assert.Equal(type.Precision, ((Decimal128Type)actual.Data.DataType).Precision);
        Assert.Equal(type.Scale, ((Decimal128Type)actual.Data.DataType).Scale);
        Assert.Equal(expected.Length, actual.Length);
        Assert.Equal(expected.Count(v => v is null), actual.NullCount);
        for (int i = 0; i < expected.Length; i++)
        {
            if (expected[i] is null)
                Assert.True(actual.IsNull(i), $"row {i} should be null");
            else
            {
                Assert.True(actual.IsValid(i), $"row {i} should be valid");
                Assert.Equal(expected[i]!.Value, FromLE16(actual.GetBytes(i)));
            }
        }
    }

    public static TheoryData<int, int, int, int> DecimalRescales() => new()
    {
        { 9, 2, 12, 4 },
        { 18, 2, 22, 4 },
        { 30, 2, 34, 4 },
        { 10, 0, 38, 28 },  // the largest multiplier a legal widening can need
        { 1, 0, 38, 37 },
    };

    [Theory]
    [MemberData(nameof(DecimalRescales))]
    public void DecimalRescale_MatchesBigInteger(int p, int s, int p2, int s2)
    {
        var max = Pow10(p) - 1;
        BigInteger?[] values = [0, 1, -1, max, -max, null, 12345 % Pow10(p), null, -(7 % Pow10(p))];
        var source = Decimals(new Decimal128Type(p, s), values);
        var target = new Decimal128Type(p2, s2);

        var result = Assert.IsType<Decimal128Array>(ValueWidener.WidenArray(source, target));

        var mult = Pow10(s2 - s);
        AssertUnscaled(values.Select(v => v * mult).ToArray(), result, target);
    }

    [Fact]
    public void DecimalRescale_SlicedInput_AppliesTheOffset()
    {
        var max = Pow10(18) - 1;
        BigInteger?[] values = [11, null, -max, 22, null, max, -33, 44, null, 55, 66];
        var full = Decimals(new Decimal128Type(18, 2), values);
        var target = new Decimal128Type(22, 4);

        // Offset 3 lands mid-byte in the bitmap, so a shared (unshifted) bitmap would misread every row.
        var sliced = (Decimal128Array)full.Slice(3, 7);
        var result = Assert.IsType<Decimal128Array>(ValueWidener.WidenArray(sliced, target));

        Assert.Equal(0, result.Data.Offset);
        AssertUnscaled(values.Skip(3).Take(7).Select(v => v * 100).ToArray(), result, target);
    }

    [Fact]
    public void DecimalRescale_NoNulls_HasNoBitmap()
    {
        var source = Decimals(new Decimal128Type(9, 2), 1, 2, 3);
        var unmasked = new Decimal128Array(new ArrayData(
            source.Data.DataType, 3, 0, 0, [ArrowBuffer.Empty, source.Data.Buffers[1]]));

        var result = (Decimal128Array)ValueWidener.WidenArray(unmasked, new Decimal128Type(12, 4));

        Assert.Equal(0, result.NullCount);
        AssertUnscaled([100, 200, 300], result, new Decimal128Type(12, 4));
    }

    [Fact]
    public void DecimalScaleDecrease_IsRefused()
    {
        var source = Decimals(new Decimal128Type(10, 4), 12345);

        Assert.Throws<NotSupportedException>(
            () => ValueWidener.WidenArray(source, new Decimal128Type(10, 2)));
    }

    private static IArrowArray Integers<T>(IArrowType type, T[] values, bool[] valid)
        where T : struct
    {
        var b = new ArrowBuffer.Builder<T>(values.Length);
        foreach (var v in values) b.Append(v);
        var validity = new ArrowBuffer.BitmapBuilder();
        foreach (bool v in valid) validity.Append(v);
        return ArrowArrayFactory.BuildArray(new ArrayData(
            type, values.Length, valid.Count(v => !v), 0, [validity.Build(), b.Build()]));
    }

    private static void AssertIntegerWidening<T>(
        IArrowType type, T[] values, Func<T, long> toLong, Decimal128Type target)
        where T : struct
    {
        // Every fourth row null; the values under them stay in the buffer and must not leak through.
        var valid = values.Select((_, i) => i % 4 != 2).ToArray();
        var full = Integers(type, values, valid);
        var expected = values
            .Select((v, i) => valid[i] ? (BigInteger?)(new BigInteger(toLong(v)) * Pow10(target.Scale)) : null)
            .ToArray();

        AssertUnscaled(expected, Assert.IsType<Decimal128Array>(ValueWidener.WidenArray(full, target)), target);

        var sliced = ArrowArrayFactory.Slice(full, 1, values.Length - 1);
        var slicedResult = Assert.IsType<Decimal128Array>(ValueWidener.WidenArray(sliced, target));
        Assert.Equal(0, slicedResult.Data.Offset);
        AssertUnscaled(expected.Skip(1).ToArray(), slicedResult, target);
    }

    [Fact]
    public void Int8ToDecimal_MatchesBigInteger() => AssertIntegerWidening<sbyte>(
        Int8Type.Default, [sbyte.MinValue, -1, 0, 1, sbyte.MaxValue, 42, -42], v => v, new Decimal128Type(5, 2));

    [Fact]
    public void Int16ToDecimal_MatchesBigInteger() => AssertIntegerWidening<short>(
        Int16Type.Default, [short.MinValue, -1, 0, 1, short.MaxValue, 4242, -4242], v => v,
        new Decimal128Type(7, 2));

    [Fact]
    public void Int32ToDecimal_MatchesBigInteger() => AssertIntegerWidening<int>(
        Int32Type.Default, [int.MinValue, -1, 0, 1, int.MaxValue, 424242, -424242], v => v,
        new Decimal128Type(12, 2));

    [Fact]
    public void Int64ToDecimal_MatchesBigInteger() => AssertIntegerWidening<long>(
        Int64Type.Default, [long.MinValue, -1, 0, 1, long.MaxValue, 42424242424242, -42424242424242], v => v,
        new Decimal128Type(38, 18));
}
