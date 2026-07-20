// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class DictionaryEncoderTests
{
    private static readonly ParquetWriteOptions DefaultOptions = new()
    {
        DictionaryEnabled = true,
        DictionaryPageSizeLimit = 1024 * 1024,
    };

    [Fact]
    public void TryEncode_LowCardinality_ReturnsDictionary()
    {
        // 100 values with 3 unique → 3.3% cardinality, under 20% threshold
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i % 3);
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int32, 0, null, 100, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(3, result.Value.DictionaryCount);
        Assert.Equal(100, result.Value.Indices.Length);
        Assert.Equal(3 * 4, result.Value.DictionaryPageData.Length); // 3 int32s
    }

    [Fact]
    public void TryEncode_HighCardinality_ReturnsNull()
    {
        // 100 unique values out of 100 → 100% cardinality
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i);
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int32, 0, null, 100, DefaultOptions);

        Assert.Null(result);
    }

    [Fact]
    public void TryEncode_Boolean_ReturnsNull()
    {
        var builder = new BooleanArray.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i % 2 == 0);
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Boolean, 0, null, 100, DefaultOptions);

        Assert.Null(result);
    }

    [Fact]
    public void TryEncode_DictionaryDisabled_ReturnsNull()
    {
        var options = new ParquetWriteOptions { DictionaryEnabled = false };
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i % 3);
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int32, 0, null, 100, options);

        Assert.Null(result);
    }

    [Fact]
    public void TryEncode_WithNulls_IndexesOnlyNonNull()
    {
        // 50 rows: every 5th is null, values cycle through 10 and 20
        // → 40 non-null values, 2 unique → 5% cardinality, under threshold
        var builder = new Int32Array.Builder();
        var defLevels = new int[50];
        int nonNullCount = 0;
        for (int i = 0; i < 50; i++)
        {
            if (i % 5 == 0)
            {
                builder.AppendNull();
                defLevels[i] = 0;
            }
            else
            {
                builder.Append(i % 2 == 0 ? 10 : 20);
                defLevels[i] = 1;
                nonNullCount++;
            }
        }

        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int32, 0, defLevels, nonNullCount, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(2, result.Value.DictionaryCount); // 10 and 20
        Assert.Equal(nonNullCount, result.Value.Indices.Length);
    }

    [Fact]
    public void TryEncode_StringColumn_LowCardinality()
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i % 4 == 0 ? "alpha" : i % 4 == 1 ? "beta" : i % 4 == 2 ? "gamma" : "delta");
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, null, 100, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(4, result.Value.DictionaryCount);
        Assert.Equal(100, result.Value.Indices.Length);
    }

    [Fact]
    public void TryEncode_DictionaryPageSizeLimit_Exceeded()
    {
        // Very small page size limit that can't fit the dictionary
        var options = new ParquetWriteOptions
        {
            DictionaryEnabled = true,
            DictionaryPageSizeLimit = 4, // only room for 1 int32
        };

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i % 5); // 5 unique values = 20 bytes
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int32, 0, null, 100, options);

        Assert.Null(result);
    }

    [Fact]
    public void TryEncode_AllSameValue_SingleEntry()
    {
        var builder = new Int64Array.Builder();
        for (int i = 0; i < 100; i++) builder.Append(42);
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int64, 0, null, 100, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal(8, result.Value.DictionaryPageData.Length); // 1 int64
        Assert.All(result.Value.Indices, idx => Assert.Equal(0, idx));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(255)]
    public void GetIndexBitWidth_CorrectForDictionarySize(int dictCount)
    {
        int bitWidth = DictionaryEncoder.GetIndexBitWidth(dictCount);
        Assert.True(bitWidth >= 1);
        // Verify all indices 0..(dictCount-1) fit in bitWidth bits
        int maxIndex = dictCount - 1;
        Assert.True(maxIndex < (1 << bitWidth),
            $"dictCount={dictCount}, bitWidth={bitWidth}, maxIndex={maxIndex}");
    }
}
