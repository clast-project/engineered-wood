// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// The cases that are about the SHAPE of the input rather than its type: a non-zero logical offset,
/// repeated indices, an empty selection, and a declared-but-unknown null count.
///
/// <para>These are exactly the cases the callers cannot construct — a Delta write or a Lance scan always
/// hands over offset-zero arrays with distinct ascending indices — which is why they went uncovered while
/// four separate implementations of this gather existed.</para>
/// </summary>
public class TakeDimensionTests
{
    [Fact]
    public void Offset_ReadsFromLogicalRowNotPhysicalSlot()
    {
        // Eight physical values; the array's logical view starts at slot 3, so logical row 0 is 300.
        int[] physical = [0, 100, 200, 300, 400, 500, 600, 700];
        var array = RawArrays.Fixed(Int32Type.Default, physical, offset: 3);
        Assert.Equal(5, array.Length);

        var result = Assert.IsType<Int32Array>(ArrowCompute.Take(array, (int[])[0, 2, 4]));

        // An offset-blind gather returns the values at physical slots 0/2/4 — 0, 200, 400 — instead.
        Assert.Equal(300, result.GetValue(0));
        Assert.Equal(500, result.GetValue(1));
        Assert.Equal(700, result.GetValue(2));
    }

    [Fact]
    public void Offset_ReadsCorrectValidityBits()
    {
        int[] physical = [0, 1, 2, 3, 4, 5];
        // Physical slots 1 and 4 are null; with offset 2, that is logical rows -1 (invisible) and 2.
        bool[] valid = [true, false, true, true, false, true];
        var array = RawArrays.Fixed(Int32Type.Default, physical, valid, offset: 2);
        Assert.Equal(4, array.Length);

        var result = Assert.IsType<Int32Array>(ArrowCompute.Take(array, (int[])[0, 2, 3]));

        Assert.Equal(2, result.GetValue(0));   // logical 0 → physical 2
        Assert.Null(result.GetValue(1));       // logical 2 → physical 4, null
        Assert.Equal(5, result.GetValue(2));   // logical 3 → physical 5
        Assert.Equal(1, result.NullCount);
    }

    [Fact]
    public void Offset_OnVarBinaryReadsCorrectSlices()
    {
        string[] physical = ["skip0", "skip1", "alpha", "bravo", "charlie"];
        var array = RawArrays.VarBinary(StringType.Default, physical, large: false, offset: 2);
        Assert.Equal(3, array.Length);

        var result = Assert.IsType<StringArray>(ArrowCompute.Take(array, (int[])[2, 0]));

        Assert.Equal("charlie", result.GetString(0));
        Assert.Equal("alpha", result.GetString(1));
    }

    [Fact]
    public void Offset_OnLargeVarBinaryReadsCorrectSlices()
    {
        string[] physical = ["skip", "alpha", "bravo"];
        var array = RawArrays.VarBinary(LargeStringType.Default, physical, large: true, offset: 1);

        var result = Assert.IsType<LargeStringArray>(ArrowCompute.Take(array, (int[])[1, 0]));

        Assert.Equal("bravo", result.GetString(0));
        Assert.Equal("alpha", result.GetString(1));
    }

    [Fact]
    public void Offset_OnBooleanReadsCorrectBits()
    {
        bool[] physical = [true, true, false, true, false, false, true];
        var array = RawArrays.Boolean(physical, offset: 3);
        Assert.Equal(4, array.Length);

        var result = Assert.IsType<BooleanArray>(ArrowCompute.Take(array, (int[])[0, 1, 3]));

        Assert.True(result.GetValue(0));    // logical 0 → physical 3
        Assert.False(result.GetValue(1));   // logical 1 → physical 4
        Assert.True(result.GetValue(2));    // logical 3 → physical 6
    }

    [Fact]
    public void Offset_OnStructParentShiftsChildRows()
    {
        // A struct's children are indexed by PHYSICAL slot and are not pre-shifted by the parent's offset,
        // so gathering logical row r must read child slot parentOffset + r. Getting this wrong silently
        // pairs a row's struct validity with a different row's field values.
        var type = new StructType(
        [
            new Field("n", Int32Type.Default, true),
            new Field("s", StringType.Default, true),
        ]);

        var n = RawArrays.Fixed(Int32Type.Default, new[] { 0, 10, 20, 30, 40 });
        var s = RawArrays.VarBinary(StringType.Default, ["v", "w", "x", "y", "z"], large: false);
        var array = RawArrays.Struct(type, physicalCount: 5, [n, s], offset: 2);
        Assert.Equal(3, array.Length);

        var result = Assert.IsType<StructArray>(ArrowCompute.Take(array, (int[])[2, 0]));

        var gotN = Assert.IsType<Int32Array>(result.Fields[0]);
        var gotS = Assert.IsType<StringArray>(result.Fields[1]);

        // Logical rows 2 and 0 → physical slots 4 and 2.
        Assert.Equal(40, gotN.GetValue(0));
        Assert.Equal(20, gotN.GetValue(1));
        Assert.Equal("z", gotS.GetString(0));
        Assert.Equal("x", gotS.GetString(1));
    }

    [Fact]
    public void NestedStruct_GathersRecursively()
    {
        var innerType = new StructType([new Field("deep", Int64Type.Default, true)]);
        var outerType = new StructType(
        [
            new Field("inner", innerType, true),
            new Field("flat", Int32Type.Default, true),
        ]);

        var deep = RawArrays.Fixed(Int64Type.Default, new[] { 1L, 2L, 3L, 4L });
        var inner = RawArrays.Struct(innerType, 4, [deep]);
        var flat = RawArrays.Fixed(Int32Type.Default, new[] { 11, 22, 33, 44 });
        var outer = RawArrays.Struct(outerType, 4, [inner, flat]);

        var result = Assert.IsType<StructArray>(ArrowCompute.Take(outer, (int[])[3, 1]));

        var gotInner = Assert.IsType<StructArray>(result.Fields[0]);
        var gotDeep = Assert.IsType<Int64Array>(gotInner.Fields[0]);
        var gotFlat = Assert.IsType<Int32Array>(result.Fields[1]);

        Assert.Equal(4L, gotDeep.GetValue(0));
        Assert.Equal(2L, gotDeep.GetValue(1));
        Assert.Equal(44, gotFlat.GetValue(0));
        Assert.Equal(22, gotFlat.GetValue(1));
    }

    [Fact]
    public void DuplicateIndices_EmitOneRowEach()
    {
        // The XML docs promise duplicates are allowed, so that promise gets a test.
        var array = RawArrays.Fixed(Int32Type.Default, new[] { 7, 8, 9 });

        var result = Assert.IsType<Int32Array>(ArrowCompute.Take(array, (int[])[1, 1, 1, 0, 1]));

        Assert.Equal(5, result.Length);
        Assert.Equal(8, result.GetValue(0));
        Assert.Equal(8, result.GetValue(1));
        Assert.Equal(8, result.GetValue(2));
        Assert.Equal(7, result.GetValue(3));
        Assert.Equal(8, result.GetValue(4));
    }

    [Fact]
    public void DuplicateIndices_OnVarBinaryCopyValueEachTime()
    {
        var array = RawArrays.VarBinary(StringType.Default, ["ab", "cde"], large: false);

        var result = Assert.IsType<StringArray>(ArrowCompute.Take(array, (int[])[1, 1, 0]));

        Assert.Equal("cde", result.GetString(0));
        Assert.Equal("cde", result.GetString(1));
        Assert.Equal("ab", result.GetString(2));

        // Total value bytes must account for the duplicate, not alias one slice.
        Assert.Equal(3 + 3 + 2, result.Data.Buffers[2].Length);
    }

    [Fact]
    public void OutOfOrderIndices_PreserveRequestedOrder()
    {
        var array = RawArrays.Fixed(Int32Type.Default, new[] { 0, 1, 2, 3, 4 });

        var result = Assert.IsType<Int32Array>(ArrowCompute.Take(array, (int[])[4, 3, 2, 1, 0]));

        for (int i = 0; i < 5; i++)
            Assert.Equal(4 - i, result.GetValue(i));
    }

    [Fact]
    public void EmptySelection_YieldsTypedZeroLengthColumn()
    {
        // This is the path DeletionVectorFilter's all-rows-deleted case now takes, replacing a per-type
        // construction that fell back to StringArray and produced a column contradicting its own schema.
        var tsType = new TimestampType(TimeUnit.Microsecond, "UTC");
        var array = RawArrays.Fixed(tsType, new[] { 1L, 2L, 3L });

        var result = ArrowCompute.Take(array, System.Array.Empty<int>());

        Assert.Equal(0, result.Length);
        Assert.Same(tsType, result.Data.DataType);
        Assert.IsType<TimestampArray>(result);
    }

    [Theory]
    [InlineData("string")]
    [InlineData("largestring")]
    [InlineData("boolean")]
    [InlineData("struct")]
    public void EmptySelection_YieldsTypedZeroLengthColumnForNonFixedWidth(string kind)
    {
        IArrowArray array = kind switch
        {
            "string" => RawArrays.VarBinary(StringType.Default, ["a", "b"], large: false),
            "largestring" => RawArrays.VarBinary(LargeStringType.Default, ["a", "b"], large: true),
            "boolean" => RawArrays.Boolean([true, false]),
            "struct" => RawArrays.Struct(
                new StructType([new Field("n", Int32Type.Default, true)]), 2,
                [RawArrays.Fixed(Int32Type.Default, new[] { 1, 2 })]),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };

        var result = ArrowCompute.Take(array, System.Array.Empty<int>());

        Assert.Equal(0, result.Length);
        Assert.Equal(array.Data.DataType.TypeId, result.Data.DataType.TypeId);
    }

    [Fact]
    public void AllNullSource_GathersAllNulls()
    {
        var array = RawArrays.Fixed(
            Int32Type.Default, new[] { 1, 2, 3 }, physicalValid: [false, false, false]);

        var result = Assert.IsType<Int32Array>(ArrowCompute.Take(array, (int[])[2, 0, 1]));

        Assert.Equal(3, result.NullCount);
        for (int i = 0; i < 3; i++)
            Assert.Null(result.GetValue(i));
    }

    [Fact]
    public void NoNullSource_ProducesNoValidityBuffer()
    {
        // A source with no nulls cannot produce one whichever rows are gathered, so the result should carry
        // no validity buffer at all — that is what lets Arrow skip the per-element null check downstream.
        var array = RawArrays.Fixed(Int64Type.Default, new[] { 1L, 2L, 3L, 4L });

        var result = ArrowCompute.Take(array, (int[])[3, 3, 0]);

        Assert.Equal(0, result.Data.NullCount);
        Assert.Equal(0, result.Data.Buffers[0].Length);
    }

    [Fact]
    public void UnknownNullCount_IsTreatedAsMayContainNulls()
    {
        // Arrow uses a null count of -1 to mean "not computed". The gather must fall back to probing each
        // row rather than trusting the count and skipping the validity read entirely.
        int[] physical = [10, 20, 30, 40];
        bool[] valid = [true, false, true, false];
        var array = RawArrays.Fixed(
            Int32Type.Default, physical, valid, nullCountOverride: -1);

        var result = Assert.IsType<Int32Array>(ArrowCompute.Take(array, (int[])[0, 1, 3, 2]));

        Assert.Equal(10, result.GetValue(0));
        Assert.Null(result.GetValue(1));
        Assert.Null(result.GetValue(2));
        Assert.Equal(30, result.GetValue(3));
        Assert.Equal(2, result.NullCount);
    }

    [Fact]
    public void SingleRowAndSingleByteBitmapBoundaries()
    {
        // Lengths straddling a bitmap byte, where a validity writer that mishandles the final partial byte
        // shows up.
        foreach (int count in new[] { 1, 7, 8, 9, 15, 16, 17 })
        {
            var values = new int[count];
            var valid = new bool[count];
            for (int i = 0; i < count; i++)
            {
                values[i] = i * 3;
                valid[i] = i % 3 != 0; // some nulls, never all
            }

            var array = RawArrays.Fixed(Int32Type.Default, values, valid);
            var indices = new int[count];
            for (int i = 0; i < count; i++) indices[i] = count - 1 - i;

            var result = Assert.IsType<Int32Array>(ArrowCompute.Take(array, indices));

            Assert.Equal(count, result.Length);
            for (int i = 0; i < count; i++)
            {
                int src = count - 1 - i;
                if (valid[src])
                    Assert.Equal(values[src], result.GetValue(i));
                else
                    Assert.Null(result.GetValue(i));
            }
        }
    }

    [Fact]
    public void RecordBatchOverload_GathersEveryColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int32Type.Default, true))
            .Field(new Field("s", StringType.Default, true))
            .Build();

        var batch = new RecordBatch(
            schema,
            [
                RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3 }),
                RawArrays.VarBinary(StringType.Default, ["x", "y", "z"], large: false),
            ],
            3);

        var result = ArrowCompute.Take(batch, schema, (int[])[2, 0]);

        Assert.Equal(2, result.Length);
        Assert.Equal(2, result.ColumnCount);
        Assert.Equal(3, ((Int32Array)result.Column(0)).GetValue(0));
        Assert.Equal("z", ((StringArray)result.Column(1)).GetString(0));
    }

    [Fact]
    public void RecordBatchListOverload_MatchesSpanOverload()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int32Type.Default, true))
            .Field(new Field("s", StringType.Default, true))
            .Build();

        RecordBatch Build() => new(
            schema,
            [
                RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3, 4 }),
                RawArrays.VarBinary(StringType.Default, ["w", "x", "y", "z"], large: false),
            ],
            4);

        int[] indices = [3, 1, 1];

        var viaSpan = ArrowCompute.Take(Build(), schema, indices);
        var viaList = ArrowCompute.Take(Build(), schema, new List<int>(indices));

        Assert.Equal(viaSpan.Length, viaList.Length);
        Assert.Equal(viaSpan.ColumnCount, viaList.ColumnCount);
        for (int i = 0; i < viaSpan.Length; i++)
        {
            Assert.Equal(
                ((Int32Array)viaSpan.Column(0)).GetValue(i),
                ((Int32Array)viaList.Column(0)).GetValue(i));
            Assert.Equal(
                ((StringArray)viaSpan.Column(1)).GetString(i),
                ((StringArray)viaList.Column(1)).GetString(i));
        }
    }

    [Fact]
    public void ListOverload_MatchesSpanOverload()
    {
        // The List<int> convenience overload takes a different route on netstandard2.0 (ToArray) than on
        // modern targets (CollectionsMarshal.AsSpan), so both must agree.
        var array = RawArrays.Fixed(Int32Type.Default, new[] { 5, 6, 7, 8 });
        int[] indices = [3, 1, 1, 0];

        var viaSpan = Assert.IsType<Int32Array>(ArrowCompute.Take(array, indices));
        var viaList = Assert.IsType<Int32Array>(ArrowCompute.Take(array, new List<int>(indices)));

        Assert.Equal(viaSpan.Length, viaList.Length);
        for (int i = 0; i < viaSpan.Length; i++)
            Assert.Equal(viaSpan.GetValue(i), viaList.GetValue(i));
    }
}
