// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Layouts;
using EngineeredWood.Vortex.Tests.TestData;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// A <c>vortex.dict</c> layout can carry a column's nulls in its dictionary values: a null
/// entry that null rows point at, with non-nullable codes. vortex 0.86's default writer does
/// this for low-cardinality nullable columns (<c>write_dict_nullable_values</c> in
/// <c>Rust/src/main.rs</c>).
/// </summary>
public class VortexDictNullTests
{
    private const string Fixture = "dict_nullable_values_20000rows.vortex";
    private const int Rows = 20_000;

    [Fact]
    public async Task BothColumnsUseADictLayout()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Fixture));

        Assert.All(reader.ColumnPlans, plan => Assert.IsType<DictColumnPlan>(plan));
    }

    [Fact]
    public async Task NullDictionaryEntryReadsAsNullNumber()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Fixture));

        var u = Assert.IsType<UInt16Array>(await reader.ReadColumnAsync(0));
        Assert.Equal(Rows, u.Length);
        Assert.Equal(Enumerable.Range(0, Rows).Count(i => i % 7 == 0), u.NullCount);
        for (int i = 0; i < Rows; i++)
            Assert.Equal(i % 7 == 0 ? null : (ushort)(i % 50), u.GetValue(i));
    }

    [Fact]
    public async Task NullDictionaryEntryReadsAsNullString()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Fixture));

        var tag = Assert.IsType<StringArray>(await reader.ReadColumnAsync(1));
        Assert.Equal(Rows, tag.Length);
        Assert.Equal(Enumerable.Range(0, Rows).Count(i => i % 11 == 0), tag.NullCount);
        for (int i = 0; i < Rows; i++)
        {
            if (i % 11 == 0)
                Assert.True(tag.IsNull(i), $"row {i}");
            else
                Assert.Equal($"t{i % 30}", tag.GetString(i));
        }
    }

    [Fact]
    public async Task StreamedBatchesCarryTheNulls()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Fixture));

        long row = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            var u = (UInt16Array)batch.Column(0);
            var tag = (StringArray)batch.Column(1);
            for (int i = 0; i < batch.Length; i++, row++)
            {
                Assert.Equal(row % 7 == 0, u.IsNull(i));
                Assert.Equal(row % 11 == 0, tag.IsNull(i));
            }
        }
        Assert.Equal(Rows, row);
    }

    [Theory]
    [InlineData(-2)]
    [InlineData(-1)] // a stored -1 is not the null sentinel
    public void NegativeSignedCodeIsRejected(int code)
    {
        var values = new StringArray.Builder().Append("a").Append("b").Build();
        var codes = new Int32Array.Builder().Append(0).Append(code).Build();

        var ex = Assert.Throws<VortexFormatException>(
            () => DictReconstructor.Reconstruct(StringType.Default, values, codes));
        Assert.Contains("negative", ex.Message);
    }

    [Fact]
    public void NullSignedCodeIsANullRowWhateverItStores()
    {
        var values = new Int64Array.Builder().Append(10).Append(20).Build();
        // Row 1 is null; the value under it is garbage and must not be read.
        var data = new byte[12];
        BitConverter.GetBytes(1).CopyTo(data, 0);
        BitConverter.GetBytes(-7).CopyTo(data, 4);
        BitConverter.GetBytes(0).CopyTo(data, 8);
        var validity = new ArrowBuffer.BitmapBuilder(3).Append(true).Append(false).Append(true).Build();
        var codes = new Int32Array(new ArrowBuffer(data), validity, 3, nullCount: 1, offset: 0);

        var result = Assert.IsType<Int64Array>(
            DictReconstructor.Reconstruct(Int64Type.Default, values, codes));
        Assert.Equal(new long?[] { 20, null, 10 }, Enumerable.Range(0, 3).Select(i => result.GetValue(i)));
    }

    [Fact]
    public void NarrowSignedCodesAreAccepted()
    {
        var values = new StringArray.Builder().Append("a").AppendNull().Append("c").Build();
        var i8 = new Int8Array.Builder().Append(2).Append(1).Append(0).Build();
        var i16 = new Int16Array.Builder().Append(2).Append(1).Append(0).Build();

        foreach (IArrowArray codes in new IArrowArray[] { i8, i16 })
        {
            var result = Assert.IsType<StringArray>(
                DictReconstructor.Reconstruct(StringType.Default, values, codes));
            Assert.Equal("c", result.GetString(0));
            Assert.True(result.IsNull(1));
            Assert.Equal("a", result.GetString(2));
        }
    }

    [Fact]
    public void OutOfRangeCodeIsRejectedForEveryType()
    {
        var values = new DoubleArray.Builder().Append(1.5).Build();
        var codes = new UInt8Array.Builder().Append(0).Append(1).Build();

        var ex = Assert.Throws<VortexFormatException>(
            () => DictReconstructor.Reconstruct(DoubleType.Default, values, codes));
        Assert.Contains("out of range", ex.Message);
    }
}
