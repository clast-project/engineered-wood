// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// <c>ArrowCompute.Scatter</c> — placing a short answer back into the rows it was computed for.
/// </summary>
/// <remarks>
/// The properties asserted here are the ones its caller relies on: a row nothing was computed for reads
/// as NULL rather than as another row's value, the type survives intact, and gathering then scattering
/// the same rows is the identity on those rows. The last one is why most cases are written as a round
/// trip against <c>Take</c> — the two are inverses, and a differential test needs no separate oracle.
/// </remarks>
public class ScatterTests
{
    [Theory]
    [MemberData(nameof(FixedWidthTypes))]
    public void FixedWidth_RoundTripsThroughTakeAndBlanksTheRest(string name)
    {
        var (type, width) = FixedWidthCases.Resolve(name);
        var bytes = new byte[6 * width];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)(i * 11 + 3);
        var source = RawArrays.Fixed(type, width, bytes);

        int[] rows = [4, 1, 5];
        var result = ArrowCompute.Scatter(ArrowCompute.Take(source, rows), rows, source.Length);

        Assert.Equal(source.Length, result.Length);
        Assert.Same(type, result.Data.DataType);

        for (int row = 0; row < source.Length; row++)
        {
            if (System.Array.IndexOf(rows, row) < 0)
            {
                Assert.True(result.IsNull(row), $"row {row} was not scattered and must be null");
                continue;
            }

            Assert.False(result.IsNull(row));
            Assert.Equal(
                bytes.AsSpan(row * width, width).ToArray(),
                ValueBytes(result, row, width));
        }
    }

    public static TheoryData<string> FixedWidthTypes
    {
        get
        {
            var cases = new TheoryData<string>();
            foreach (string name in FixedWidthCases.All)
            {
#if !NET6_0_OR_GREATER
                // No HalfFloatArray can be constructed on netstandard2.0, so there is nothing to scatter.
                if (name == "halffloat") continue;
#endif
                cases.Add(name);
            }
            return cases;
        }
    }

    [Fact]
    public void Strings_RoundTrip()
    {
        var builder = new StringArray.Builder();
        builder.Append("alpha").AppendNull().Append("gamma").Append("delta");
        var source = builder.Build();

        int[] rows = [3, 0];
        var result = (StringArray)ArrowCompute.Scatter(ArrowCompute.Take(source, rows), rows, 4);

        Assert.Equal("alpha", result.GetString(0));
        Assert.True(result.IsNull(1));
        Assert.True(result.IsNull(2));
        Assert.Equal("delta", result.GetString(3));
        Assert.Equal(2, result.NullCount);
    }

    /// <summary>A null the values already carried stays a null, and is counted as one.</summary>
    [Fact]
    public void ANullValueScattersAsANull()
    {
        var builder = new Int32Array.Builder();
        builder.Append(1).AppendNull().Append(3);
        var source = builder.Build();

        int[] rows = [0, 1, 2];
        var result = (Int32Array)ArrowCompute.Scatter(source, rows, 5);

        Assert.Equal(1, result.GetValue(0));
        Assert.True(result.IsNull(1));
        Assert.Equal(3, result.GetValue(2));
        Assert.True(result.IsNull(3));
        Assert.True(result.IsNull(4));
        Assert.Equal(3, result.NullCount);
    }

    /// <summary>
    /// Nothing to place. The type still has to come back exactly, which is the case the caller leans on:
    /// a branch no row selected still has to type the conditional it sits in.
    /// </summary>
    [Fact]
    public void NothingToPlace_IsAllNullOfTheSourceType()
    {
        var type = new Decimal128Type(38, 10);
        var source = ArrowCompute.MakeNullArray(type, 0);

        var result = ArrowCompute.Scatter(source, System.Array.Empty<int>(), 4);

        Assert.Equal(4, result.Length);
        Assert.Equal(4, result.NullCount);
        Assert.Equal(type, result.Data.DataType);
    }

    [Fact]
    public void ScatteringNowhere_IsAnEmptyArrayOfTheSourceType()
    {
        var result = ArrowCompute.Scatter(ArrowCompute.MakeNullArray(Int64Type.Default, 0), System.Array.Empty<int>(), 0);

        Assert.Equal(0, result.Length);
        Assert.Equal(Int64Type.Default, result.Data.DataType);
    }

    [Fact]
    public void RowsOutOfOrderAndSparse_LandWhereTheyWereNamed()
    {
        var builder = new Int32Array.Builder();
        builder.Append(70).Append(20).Append(90);
        var source = builder.Build();

        int[] rows = [7, 2, 9];
        var result = (Int32Array)ArrowCompute.Scatter(source, rows, 10);

        Assert.Equal(70, result.GetValue(7));
        Assert.Equal(20, result.GetValue(2));
        Assert.Equal(90, result.GetValue(9));
        Assert.Equal(7, result.NullCount);
    }

    [Fact]
    public void Nested_RoundTrips()
    {
        var lists = new ListArray.Builder(Int32Type.Default);
        var values = (Int32Array.Builder)lists.ValueBuilder;
        lists.Append(); values.Append(1); values.Append(2);
        lists.Append(); values.Append(3);
        lists.Append(); values.Append(4);
        var source = lists.Build();

        int[] rows = [2, 0];
        var result = (ListArray)ArrowCompute.Scatter(ArrowCompute.Take(source, rows), rows, 3);

        Assert.Equal(3, result.Length);
        Assert.True(result.IsNull(1));
        Assert.Equal([1, 2], Values(result, 0));
        Assert.Equal([4], Values(result, 2));
    }

    [Fact]
    public void MoreRowsThanValues_IsRefused()
    {
        var builder = new Int32Array.Builder();
        builder.Append(1);
        Assert.Throws<ArgumentException>(() => ArrowCompute.Scatter(builder.Build(), TwoRows, 4));
    }

    /// <summary>
    /// A repeated target row is refused rather than resolved by last-write.
    /// </summary>
    /// <remarks>
    /// It would otherwise discard the value placed there first, silently — and this is only
    /// Take's inverse while the rows are distinct, since Take allows duplicates and a row read
    /// twice has no inverse.
    /// </remarks>
    [Fact]
    public void ARepeatedRow_IsRefused()
    {
        var builder = new Int32Array.Builder();
        builder.Append(10).Append(20);

        var error = Assert.Throws<ArgumentException>(
            () => ArrowCompute.Scatter(builder.Build(), RepeatedRow, 3));
        Assert.Contains("more than once", error.Message);
    }

    private static readonly int[] RepeatedRow = [1, 1];

    [Fact]
    public void ARowOutsideTheArray_IsRefused()
    {
        var builder = new Int32Array.Builder();
        builder.Append(1);
        Assert.Throws<ArgumentOutOfRangeException>(() => ArrowCompute.Scatter(builder.Build(), PastTheEnd, 4));
    }

    private static readonly int[] TwoRows = [0, 1];

    private static readonly int[] PastTheEnd = [4];

    private static int[] Values(ListArray lists, int row)
    {
        var child = (Int32Array)lists.Values;
        int start = lists.ValueOffsets[row];
        int end = lists.ValueOffsets[row + 1];
        var values = new int[end - start];
        for (int i = 0; i < values.Length; i++) values[i] = child.GetValue(start + i)!.Value;
        return values;
    }

    private static byte[] ValueBytes(IArrowArray array, int row, int width) =>
        array.Data.Buffers[1].Span.Slice((array.Data.Offset + row) * width, width).ToArray();
}
