// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// <see cref="ArrowCompute.Concatenate"/> joins string, binary and struct arrays itself, into buffers sized once
/// (#667). Arrow's own concatenator is the oracle: the rows must come out the same.
/// </summary>
public class ConcatenateSizedTests
{
    public static TheoryData<int> Seeds => new() { 1, 2, 3, 4, 5, 6, 7, 8 };

    [Theory]
    [MemberData(nameof(Seeds))]
    public void String_MatchesArrow(int seed) =>
        AssertMatchesArrow(SlicedInputs(seed, rnd => Strings(rnd, rnd.Next(1, 40))));

    [Theory]
    [MemberData(nameof(Seeds))]
    public void Binary_MatchesArrow(int seed) =>
        AssertMatchesArrow(SlicedInputs(seed, rnd => Binaries(rnd, rnd.Next(1, 40))));

    [Theory]
    [MemberData(nameof(Seeds))]
    public void LargeString_MatchesArrow(int seed) =>
        AssertMatchesArrow(SlicedInputs(seed, rnd => LargeStrings(rnd, rnd.Next(1, 40))));

    [Theory]
    [MemberData(nameof(Seeds))]
    public void Struct_WithStringChild_MatchesArrow(int seed) =>
        AssertMatchesArrow(SlicedInputs(seed, rnd => Structs(rnd, rnd.Next(1, 40))));

    [Fact]
    public void String_WithoutNulls_HasNoBitmap()
    {
        var result = (StringArray)ArrowCompute.Concatenate([Of("a", "bc"), Of("", "def")]);

        Assert.Equal(0, result.NullCount);
        Assert.Equal(0, result.Data.Buffers[0].Length);
        Assert.Equal(new[] { "a", "bc", "", "def" }, Enumerable.Range(0, 4).Select(i => result.GetString(i)));
    }

    [Fact]
    public void AllEmptyStrings_HaveNoValueBytes()
    {
        var result = (StringArray)ArrowCompute.Concatenate([Of("", ""), Of("")]);

        Assert.Equal(3, result.Length);
        Assert.All(Enumerable.Range(0, 3), i => Assert.Equal("", result.GetString(i)));
    }

    [Fact]
    public void StringOntoBinary_IsRefused()
    {
        var binary = new BinaryArray.Builder().Append(new byte[] { 1 }).Build();
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([Of("a"), binary]));
    }

#if NET
    /// <summary>
    /// The point of #667: Arrow's concatenator allocated several times a string column's size to join it.
    /// </summary>
    [Fact]
    public void String_AllocatesAboutItsOwnSize()
    {
        var rnd = new Random(42);
        var parts = Enumerable.Range(0, 16).Select(_ => (IArrowArray)Strings(rnd, 20_000)).ToList();
        long size = parts.Sum(p => p.Data.Buffers.Sum(b => (long)b.Length));
        ArrowCompute.Concatenate(parts); // warm up

        long before = GC.GetAllocatedBytesForCurrentThread();
        ArrowCompute.Concatenate(parts);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allocated < size * 1.2, $"allocated {allocated} bytes to join {size}");
    }
#endif

    private static void AssertMatchesArrow(List<IArrowArray> inputs)
    {
        var expected = ArrowArrayConcatenator.Concatenate(inputs);
        var actual = ArrowCompute.Concatenate(inputs);

        Assert.Equal(expected.Data.DataType.TypeId, actual.Data.DataType.TypeId);
        Assert.Equal(0, actual.Data.Offset);
        AssertSameRows(expected, actual);
    }

    private static void AssertSameRows(IArrowArray expected, IArrowArray actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        Assert.Equal(expected.NullCount, actual.NullCount);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected.IsNull(i), actual.IsNull(i));

        switch (expected)
        {
            case BinaryArray e:
                var a = (BinaryArray)actual;
                for (int i = 0; i < e.Length; i++)
                {
                    if (!e.IsNull(i))
                        Assert.Equal(e.GetBytes(i).ToArray(), a.GetBytes(i).ToArray());
                    GC.KeepAlive(e);
                    GC.KeepAlive(a);
                }
                break;
            case LargeStringArray e:
                var ls = (LargeStringArray)actual;
                for (int i = 0; i < e.Length; i++)
                    Assert.Equal(e.GetString(i), ls.GetString(i));
                break;
            case Int32Array e:
                Assert.Equal(e.ToArray(), ((Int32Array)actual).ToArray());
                break;
            case StructArray e:
                var s = (StructArray)actual;
                for (int f = 0; f < e.Fields.Count; f++)
                    AssertSameRows(e.Fields[f], s.Fields[f]);
                break;
            default:
                throw new InvalidOperationException($"No comparison for {expected.GetType().Name}.");
        }
    }

    /// <summary>
    /// Three to six arrays, most of them sliced at an odd offset, so the bitmaps are read and written off byte
    /// boundaries. One of them has no nulls, and so no bitmap.
    /// </summary>
    private static List<IArrowArray> SlicedInputs(int seed, Func<Random, IArrowArray> make)
    {
        var rnd = new Random(seed);
        int count = rnd.Next(3, 7);
        var inputs = new List<IArrowArray>(count);
        for (int i = 0; i < count; i++)
        {
            var array = make(rnd);
            if (i == 1)
                array = ArrowCompute.Take(array, Enumerable.Range(0, array.Length).Where(r => !array.IsNull(r)).ToArray());
            if (array.Length > 2 && rnd.Next(4) != 0)
            {
                int offset = rnd.Next(1, array.Length - 1);
                array = ArrowArrayFactory.Slice(array, offset, rnd.Next(1, array.Length - offset + 1));
            }
            inputs.Add(array);
        }
        return inputs;
    }

    private static StringArray Strings(Random rnd, int length)
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < length; i++)
        {
            if (rnd.Next(5) == 0)
                builder.AppendNull();
            else
                builder.Append(new string((char)('a' + rnd.Next(26)), rnd.Next(0, 12)));
        }
        return builder.Build();
    }

    private static LargeStringArray LargeStrings(Random rnd, int length)
    {
        var builder = new LargeStringArray.Builder();
        for (int i = 0; i < length; i++)
        {
            if (rnd.Next(5) == 0)
                builder.AppendNull();
            else
                builder.Append(new string((char)('a' + rnd.Next(26)), rnd.Next(0, 12)));
        }
        return builder.Build();
    }

    private static BinaryArray Binaries(Random rnd, int length)
    {
        var builder = new BinaryArray.Builder();
        for (int i = 0; i < length; i++)
        {
            if (rnd.Next(5) == 0)
            {
                builder.AppendNull();
            }
            else
            {
                var bytes = new byte[rnd.Next(0, 12)];
                rnd.NextBytes(bytes);
                builder.Append(bytes);
            }
        }
        return builder.Build();
    }

    private static StructArray Structs(Random rnd, int length)
    {
        var strings = Strings(rnd, length);
        var ints = new Int32Array.Builder();
        var validity = new ArrowBuffer.BitmapBuilder(length);
        int nulls = 0;
        for (int i = 0; i < length; i++)
        {
            ints.Append(rnd.Next());
            bool valid = rnd.Next(6) != 0;
            validity.Append(valid);
            if (!valid)
                nulls++;
        }

        var type = new StructType(
        [
            new Field("s", StringType.Default, nullable: true),
            new Field("n", Int32Type.Default, nullable: false),
        ]);
        return new StructArray(type, length, new IArrowArray[] { strings, ints.Build() }, validity.Build(), nulls);
    }

    private static StringArray Of(params string[] values)
    {
        var builder = new StringArray.Builder();
        foreach (var value in values)
            builder.Append(value);
        return builder.Build();
    }
}
