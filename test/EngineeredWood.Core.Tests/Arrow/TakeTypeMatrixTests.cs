// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// One case per width in <c>ArrowCompute.FixedWidthBytes</c>, plus every non-fixed-width arm. Each asserts
/// two things the typed array builders got wrong: the gathered bytes are the source's bytes, and the
/// gathered column reports the source's exact type — same unit, timezone, precision, scale and width.
/// </summary>
public class TakeTypeMatrixTests
{
    /// <summary>
    /// Every fixed-width type the gather claims to support, from the shared width table in
    /// <see cref="FixedWidthCases"/>.
    /// </summary>
    public static TheoryData<string> FixedWidthTypes
    {
        get
        {
            var cases = new TheoryData<string>();
            foreach (string name in FixedWidthCases.All)
            {
                // Apache.Arrow's ArrowArrayFactory throws "Half-float arrays are not supported by this target
                // framework" on netstandard2.0, so no HalfFloatArray can exist there to gather FROM — there is
                // no input to construct, which is why this is a skip here rather than an expected throw.
#if !NET6_0_OR_GREATER
                if (name == "halffloat") continue;
#endif
                cases.Add(name);
            }
            return cases;
        }
    }

    private static (IArrowType Type, int Width) Resolve(string name) => FixedWidthCases.Resolve(name);

    /// <summary>Distinct, non-zero bytes so a mis-slotted copy cannot coincidentally match.</summary>
    private static byte[] Pattern(int rows, int width)
    {
        var bytes = new byte[rows * width];
        for (int i = 0; i < bytes.Length; i++)
            bytes[i] = (byte)(i * 7 + 1);
        return bytes;
    }

    [Theory]
    [MemberData(nameof(FixedWidthTypes))]
    public void FixedWidth_GathersExactBytesAndPreservesType(string name)
    {
        var (type, width) = Resolve(name);
        byte[] source = Pattern(rows: 5, width);
        var array = RawArrays.Fixed(type, width, source);

        int[] indices = [3, 0, 4, 1];
        var result = ArrowCompute.Take(array, indices);

        Assert.Equal(indices.Length, result.Length);

        // The gathered type must be the SOURCE's type instance, not a rebuilt default. Reference equality
        // is the strongest form of "nothing was re-tagged" and holds because the gather reuses DataType.
        Assert.Same(type, result.Data.DataType);

        for (int i = 0; i < indices.Length; i++)
        {
            Assert.Equal(
                source.AsSpan(indices[i] * width, width).ToArray(),
                RawArrays.Slot(result, i, width));
        }

        // No null was gathered, so there must be no validity buffer to check at read time.
        Assert.Equal(0, result.Data.NullCount);
        Assert.Equal(0, result.Data.Buffers[0].Length);
    }

    [Theory]
    [MemberData(nameof(FixedWidthTypes))]
    public void FixedWidth_GathersNullsAndPreservesType(string name)
    {
        var (type, width) = Resolve(name);
        byte[] source = Pattern(rows: 5, width);
        bool[] valid = [true, false, true, true, false];
        var array = RawArrays.Fixed(type, width, source, valid);

        int[] indices = [1, 2, 4, 0];
        var result = ArrowCompute.Take(array, indices);

        Assert.Same(type, result.Data.DataType);
        Assert.Equal(2, result.Data.NullCount);

        var asArray = Assert.IsAssignableFrom<Apache.Arrow.Array>(result);
        Assert.True(asArray.IsNull(0));   // source row 1
        Assert.False(asArray.IsNull(1));  // source row 2
        Assert.True(asArray.IsNull(2));   // source row 4
        Assert.False(asArray.IsNull(3));  // source row 0

        // Non-null slots keep their bytes; null slots are not asserted (Arrow leaves them undefined).
        Assert.Equal(
            source.AsSpan(2 * width, width).ToArray(), RawArrays.Slot(result, 1, width));
        Assert.Equal(
            source.AsSpan(0, width).ToArray(), RawArrays.Slot(result, 3, width));
    }

    [Fact]
    public void Boolean_GathersBitPackedValues()
    {
        // Deliberately longer than one bitmap byte so the gather cannot get away with a byte copy.
        bool[] values = [true, false, true, true, false, false, true, false, true, true];
        var array = RawArrays.Boolean(values);

        int[] indices = [9, 0, 4, 6, 1, 8];
        var result = Assert.IsType<BooleanArray>(ArrowCompute.Take(array, indices));

        Assert.Equal(indices.Length, result.Length);
        for (int i = 0; i < indices.Length; i++)
            Assert.Equal(values[indices[i]], result.GetValue(i));
    }

    [Fact]
    public void Boolean_GathersNulls()
    {
        bool[] values = [true, true, false, true];
        bool[] valid = [true, false, true, false];
        var array = RawArrays.Boolean(values, valid);

        var result = Assert.IsType<BooleanArray>(ArrowCompute.Take(array, (int[])[3, 0, 1, 2]));

        Assert.Null(result.GetValue(0));
        Assert.True(result.GetValue(1));
        Assert.Null(result.GetValue(2));
        Assert.False(result.GetValue(3));
        Assert.Equal(2, result.NullCount);
    }

    [Fact]
    public void String_GathersValuesAndPreservesType()
    {
        string[] values = ["alpha", "", "bravo", "charlie", "d"];
        var array = RawArrays.VarBinary(StringType.Default, values, large: false);

        var result = Assert.IsType<StringArray>(ArrowCompute.Take(array, (int[])[3, 1, 0, 4]));

        Assert.Equal("charlie", result.GetString(0));
        Assert.Equal("", result.GetString(1));
        Assert.Equal("alpha", result.GetString(2));
        Assert.Equal("d", result.GetString(3));
    }

    [Fact]
    public void LargeString_IsNotNarrowedToString()
    {
        string[] values = ["alpha", "bravo", "charlie"];
        var array = RawArrays.VarBinary(LargeStringType.Default, values, large: true);

        var result = ArrowCompute.Take(array, (int[])[2, 0]);

        // Narrowing to StringType would produce a column contradicting the schema it lands in, and would
        // overflow past 2 GiB of values.
        Assert.IsType<LargeStringArray>(result);
        Assert.Equal(ArrowTypeId.LargeString, result.Data.DataType.TypeId);
        Assert.Same(LargeStringType.Default, result.Data.DataType);

        var large = (LargeStringArray)result;
        Assert.Equal("charlie", large.GetString(0));
        Assert.Equal("alpha", large.GetString(1));
    }

    [Fact]
    public void Binary_GathersValues()
    {
        string[] values = ["", "xyz", "", "q"];
        var array = RawArrays.VarBinary(BinaryType.Default, values, large: false);

        var result = Assert.IsType<BinaryArray>(ArrowCompute.Take(array, (int[])[1, 3, 2]));

        Assert.Equal(System.Text.Encoding.UTF8.GetBytes("xyz"), result.GetBytes(0).ToArray());
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes("q"), result.GetBytes(1).ToArray());
        Assert.Empty(result.GetBytes(2).ToArray());
    }

    [Fact]
    public void LargeBinary_IsNotNarrowedToBinary()
    {
        var array = RawArrays.VarBinary(
            LargeBinaryType.Default, ["aa", "bb", "cc"], large: true);

        var result = ArrowCompute.Take(array, (int[])[2, 1]);

        Assert.IsType<LargeBinaryArray>(result);
        Assert.Same(LargeBinaryType.Default, result.Data.DataType);
    }

    [Fact]
    public void VarBinary_GathersNullsWithMonotonicOffsets()
    {
        string[] values = ["alpha", "bravo", "charlie", "delta"];
        bool[] valid = [true, false, true, false];
        var array = RawArrays.VarBinary(StringType.Default, values, large: false, valid);

        var result = Assert.IsType<StringArray>(ArrowCompute.Take(array, (int[])[1, 2, 3, 0]));

        Assert.Null(result.GetString(0));
        Assert.Equal("charlie", result.GetString(1));
        Assert.Null(result.GetString(2));
        Assert.Equal("alpha", result.GetString(3));
        Assert.Equal(2, result.NullCount);

        // A null contributes a zero-length slot; the offsets must still never go backwards.
        var offsets = System.Runtime.InteropServices.MemoryMarshal
            .Cast<byte, int>(result.Data.Buffers[1].Span);
        for (int i = 0; i < result.Length; i++)
            Assert.True(offsets[i + 1] >= offsets[i], $"offset {i + 1} went backwards");
    }

    [Fact]
    public void Struct_GathersChildrenAndOwnValidity()
    {
        var childType = new StructType(
        [
            new Field("n", Int32Type.Default, true),
            new Field("s", StringType.Default, true),
        ]);

        var n = RawArrays.Fixed(Int32Type.Default, new[] { 10, 20, 30, 40 });
        var s = RawArrays.VarBinary(StringType.Default, ["w", "x", "y", "z"], large: false);
        var array = RawArrays.Struct(childType, 4, [n, s], physicalValid: [true, true, false, true]);

        var result = Assert.IsType<StructArray>(ArrowCompute.Take(array, (int[])[3, 2, 0]));

        Assert.Equal(3, result.Length);
        Assert.Equal(1, result.NullCount);
        Assert.False(result.IsNull(0));
        Assert.True(result.IsNull(1));
        Assert.False(result.IsNull(2));

        var gotN = Assert.IsType<Int32Array>(result.Fields[0]);
        var gotS = Assert.IsType<StringArray>(result.Fields[1]);
        Assert.Equal(40, gotN.GetValue(0));
        Assert.Equal(30, gotN.GetValue(1));
        Assert.Equal(10, gotN.GetValue(2));
        Assert.Equal("z", gotS.GetString(0));
        Assert.Equal("y", gotS.GetString(1));
        Assert.Equal("w", gotS.GetString(2));
    }

    [Fact]
    public void Extension_GathersThroughStorageAndKeepsAnnotation()
    {
        // GuidType is a real Arrow extension type over FixedSizeBinary(16) — no test-only subclass needed.
        var guidType = new GuidType();
        var guids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

        var bytes = new byte[guids.Length * 16];
        for (int i = 0; i < guids.Length; i++)
            guids[i].ToByteArray().CopyTo(bytes, i * 16);

        var storage = RawArrays.Fixed(guidType.StorageType, 16, bytes);
        var extArray = guidType.CreateArray(storage);

        var result = ArrowCompute.Take(extArray, (int[])[2, 0]);

        // Gathering must not strip the annotation down to bare FixedSizeBinary — the batch's schema would
        // still claim the extension type.
        var ext = Assert.IsAssignableFrom<ExtensionArray>(result);
        var extType = Assert.IsAssignableFrom<ExtensionType>(ext.Data.DataType);
        Assert.Equal(guidType.Name, extType.Name);

        Assert.Equal(bytes.AsSpan(32, 16).ToArray(), RawArrays.Slot(ext.Storage, 0, 16));
        Assert.Equal(bytes.AsSpan(0, 16).ToArray(), RawArrays.Slot(ext.Storage, 1, 16));
    }

    [Fact]
    public void Null_GathersToRequestedLength()
    {
        var result = ArrowCompute.Take(new NullArray(5), (int[])[4, 4, 0]);

        Assert.IsType<NullArray>(result);
        Assert.Equal(3, result.Length);
    }

    [Fact]
    public void UnsupportedType_Throws()
    {
        // A dictionary-encoded column is genuinely not gatherable here. It must throw rather than pass the
        // column through unfiltered — an unfiltered column has the wrong length for the batch it lands in.
        var indices = new Int32Array.Builder().Append(0).Append(1).Append(0).Build();
        var dictionary = new StringArray.Builder().Append("alpha").Append("bravo").Build();
        var dict = new DictionaryArray(
            new DictionaryType(Int32Type.Default, StringType.Default, ordered: false),
            indices, dictionary);

        var ex = Assert.Throws<NotSupportedException>(() => ArrowCompute.Take(dict, (int[])[1, 0]));
        Assert.Contains("ArrowCompute", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Dictionary", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Interval_ThrowsRatherThanGuessingItsWidth()
    {
        // No caller produces interval columns, so no width table for them is exercised by any test. This
        // pins the decision to throw: if a caller ever needs intervals, this test is what should change.
        // Built through Arrow's own builder so the array is genuinely well-formed — otherwise the expected
        // exception could come from Arrow rejecting malformed data rather than from the gather.
        var builder = new MonthDayNanosecondIntervalArray.Builder();
        builder.Append(new MonthDayNanosecondInterval(1, 2, 3));
        builder.Append(new MonthDayNanosecondInterval(4, 5, 6));
        var interval = builder.Build();

        var ex = Assert.Throws<NotSupportedException>(
            () => ArrowCompute.Take(interval, (int[])[1, 0]));

        // Must be the gather refusing, identifiable by its own message, not Arrow refusing something else.
        Assert.Contains("ArrowCompute", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Interval", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
