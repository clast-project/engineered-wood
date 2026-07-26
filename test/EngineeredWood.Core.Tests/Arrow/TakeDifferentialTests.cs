// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// Cross-checks the buffer-level gather against the typed-array-builder gather it replaced, for the types
/// where the builder round-trip is lossless. Those are the cases where the old four implementations were
/// correct, so agreement here is what says the consolidation preserved behaviour.
///
/// <para>The lossy types are deliberately absent: a builder puts a timestamp through
/// <see cref="DateTimeOffset"/> and a decimal through <see cref="decimal"/>, so pinning the new code
/// against the old on those would encode the data loss as the expectation. Their real behaviour is
/// asserted directly in <see cref="TakeTypeMatrixTests"/> instead.</para>
/// </summary>
public class TakeDifferentialTests
{
    private static readonly int[] Indices = [4, 0, 2, 2, 7, 1];

    /// <summary>Deterministic nulls at every third row, so both paths see the same mix.</summary>
    private static bool[] Validity(int count)
    {
        var valid = new bool[count];
        for (int i = 0; i < count; i++) valid[i] = i % 3 != 0;
        return valid;
    }

    private static void AssertSameAs<TArray>(
        IArrowArray viaTake, TArray viaBuilder, Func<TArray, int, object?> read)
        where TArray : IArrowArray
    {
        Assert.Equal(viaBuilder.Length, viaTake.Length);
        var takeTyped = Assert.IsType<TArray>(viaTake, exactMatch: false);

        for (int i = 0; i < viaBuilder.Length; i++)
            Assert.Equal(read(viaBuilder, i), read(takeTyped, i));
    }

    [Fact]
    public void Int32_MatchesBuilderGather()
    {
        int[] values = [0, 11, 22, 33, 44, 55, 66, 77];
        bool[] valid = Validity(values.Length);
        var source = (Int32Array)RawArrays.Fixed(Int32Type.Default, values, valid);

        var builder = new Int32Array.Builder();
        foreach (int r in Indices)
        {
            if (source.IsNull(r)) builder.AppendNull();
            else builder.Append(source.GetValue(r)!.Value);
        }

        AssertSameAs(
            ArrowCompute.Take(source, Indices), builder.Build(),
            (Int32Array a, int i) => a.GetValue(i));
    }

    [Fact]
    public void Int64_MatchesBuilderGather()
    {
        long[] values = [0, 11, 22, 33, 44, 55, 66, long.MaxValue];
        bool[] valid = Validity(values.Length);
        var source = (Int64Array)RawArrays.Fixed(Int64Type.Default, values, valid);

        var builder = new Int64Array.Builder();
        foreach (int r in Indices)
        {
            if (source.IsNull(r)) builder.AppendNull();
            else builder.Append(source.GetValue(r)!.Value);
        }

        AssertSameAs(
            ArrowCompute.Take(source, Indices), builder.Build(),
            (Int64Array a, int i) => a.GetValue(i));
    }

    [Fact]
    public void UInt64_MatchesBuilderGather()
    {
        ulong[] values = [0, 11, 22, 33, 44, 55, 66, ulong.MaxValue];
        bool[] valid = Validity(values.Length);
        var source = (UInt64Array)RawArrays.Fixed(UInt64Type.Default, values, valid);

        var builder = new UInt64Array.Builder();
        foreach (int r in Indices)
        {
            if (source.IsNull(r)) builder.AppendNull();
            else builder.Append(source.GetValue(r)!.Value);
        }

        AssertSameAs(
            ArrowCompute.Take(source, Indices), builder.Build(),
            (UInt64Array a, int i) => a.GetValue(i));
    }

    [Fact]
    public void Double_MatchesBuilderGather()
    {
        double[] values =
            [0.5, -1.25, double.MaxValue, double.Epsilon, 0, double.NaN,
             double.NegativeInfinity, 3.14159];
        bool[] valid = Validity(values.Length);
        var source = (DoubleArray)RawArrays.Fixed(DoubleType.Default, values, valid);

        var builder = new DoubleArray.Builder();
        foreach (int r in Indices)
        {
            if (source.IsNull(r)) builder.AppendNull();
            else builder.Append(source.GetValue(r)!.Value);
        }

        var viaBuilder = builder.Build();
        var viaTake = Assert.IsType<DoubleArray>(ArrowCompute.Take(source, Indices));

        // Compared as raw bits so NaN counts as equal to itself and -0.0 stays distinct from 0.0.
        for (int i = 0; i < viaBuilder.Length; i++)
        {
            Assert.Equal(viaBuilder.IsNull(i), viaTake.IsNull(i));
            if (!viaBuilder.IsNull(i))
            {
                Assert.Equal(
                    BitConverter.DoubleToInt64Bits(viaBuilder.GetValue(i)!.Value),
                    BitConverter.DoubleToInt64Bits(viaTake.GetValue(i)!.Value));
            }
        }
    }

    [Fact]
    public void Float_MatchesBuilderGather()
    {
        float[] values = [0.5f, -1.25f, float.MaxValue, float.Epsilon, 0f, 1f, 2f, 3f];
        bool[] valid = Validity(values.Length);
        var source = (FloatArray)RawArrays.Fixed(FloatType.Default, values, valid);

        var builder = new FloatArray.Builder();
        foreach (int r in Indices)
        {
            if (source.IsNull(r)) builder.AppendNull();
            else builder.Append(source.GetValue(r)!.Value);
        }

        AssertSameAs(
            ArrowCompute.Take(source, Indices), builder.Build(),
            (FloatArray a, int i) => a.GetValue(i));
    }

    [Fact]
    public void Boolean_MatchesBuilderGather()
    {
        bool[] values = [true, false, true, true, false, false, true, false];
        bool[] valid = Validity(values.Length);
        var source = (BooleanArray)RawArrays.Boolean(values, valid);

        var builder = new BooleanArray.Builder();
        foreach (int r in Indices)
        {
            if (source.IsNull(r)) builder.AppendNull();
            else builder.Append(source.GetValue(r)!.Value);
        }

        AssertSameAs(
            ArrowCompute.Take(source, Indices), builder.Build(),
            (BooleanArray a, int i) => a.GetValue(i));
    }

    [Fact]
    public void String_MatchesBuilderGather()
    {
        string[] values = ["", "alpha", "bravo", "charlie", "d", "ee", "fff", "gggg"];
        bool[] valid = Validity(values.Length);
        var source = (StringArray)RawArrays.VarBinary(
            StringType.Default, values, large: false, valid);

        var builder = new StringArray.Builder();
        foreach (int r in Indices)
        {
            if (source.IsNull(r)) builder.AppendNull();
            else builder.Append(source.GetString(r));
        }

        AssertSameAs(
            ArrowCompute.Take(source, Indices), builder.Build(),
            (StringArray a, int i) => a.GetString(i));
    }

    [Fact]
    public void Binary_MatchesBuilderGather()
    {
        string[] values = ["", "ab", "cde", "f", "gh", "ijk", "l", "mn"];
        bool[] valid = Validity(values.Length);
        var source = (BinaryArray)RawArrays.VarBinary(
            BinaryType.Default, values, large: false, valid);

        var builder = new BinaryArray.Builder();
        foreach (int r in Indices)
        {
            if (source.IsNull(r)) builder.AppendNull();
            else builder.Append(source.GetBytes(r));
        }

        var viaBuilder = builder.Build();
        var viaTake = Assert.IsType<BinaryArray>(ArrowCompute.Take(source, Indices));

        Assert.Equal(viaBuilder.Length, viaTake.Length);
        for (int i = 0; i < viaBuilder.Length; i++)
        {
            Assert.Equal(viaBuilder.IsNull(i), viaTake.IsNull(i));
            if (!viaBuilder.IsNull(i))
                Assert.Equal(viaBuilder.GetBytes(i).ToArray(), viaTake.GetBytes(i).ToArray());
        }
    }
}
