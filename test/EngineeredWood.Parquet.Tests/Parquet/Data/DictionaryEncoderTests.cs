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

    // ── The constant-column fast path ──
    //
    // A column holding one value in every row skips the per-row hashing entirely: uniform value length is
    // checked in O(1), then the value buffer is compared against itself shifted by one value. The result
    // must be indistinguishable from what the hashing loop would have produced — one dictionary entry and an
    // index of 0 for every row — because the encoding is what lands in the file.

    private static StringArray Strings(params string[] values)
    {
        var b = new StringArray.Builder();
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    [Fact]
    public void TryEncode_ConstantStringColumn_MatchesWhatHashingWouldProduce()
    {
        var array = Strings(Enumerable.Repeat("update_postimage", 64).ToArray());

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, null, 64, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal(64, result.Value.Indices.Length);
        Assert.All(result.Value.Indices, idx => Assert.Equal(0, idx));
        // PLAIN dictionary page: 4-byte LE length prefix, then the value.
        Assert.Equal(4 + 16, result.Value.DictionaryPageData.Length);
        Assert.Equal("update_postimage",
            System.Text.Encoding.UTF8.GetString(result.Value.DictionaryPageData, 4, 16));
    }

    [Fact]
    public void TryEncode_VaryingLengthsOverRepeatingBytes_IsNotMistakenForConstant()
    {
        // The case the offset walk exists for. Lengths 2,1,3 over a run of 'a's hold THREE different values,
        // yet the two cheap checks both pass: the lengths average out so uniform-length is not refuted, and
        // the value buffer is trivially periodic in any period. Without the walk this encodes as one value
        // repeated — a corrupt file rather than a failure.
        //
        // Repeated to 30 rows so the 3 distinct values stay under the cardinality threshold; at 3 rows
        // TryEncode bails on cardinality before reaching any of this.
        var pattern = new[] { "aa", "a", "aaa" };
        var array = Strings(Enumerable.Range(0, 30).Select(i => pattern[i % 3]).ToArray());

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, null, 30, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(3, result.Value.DictionaryCount);
        Assert.Equal([0, 1, 2, 0, 1, 2], result.Value.Indices.Take(6));
    }

    [Fact]
    public void TryEncode_SameLengthDifferentValues_IsNotMistakenForConstant()
    {
        // Uniform length, so the O(1) check cannot refute it; the periodicity compare is what does, at the
        // first differing byte. Repeated to 40 rows to clear the cardinality threshold.
        var pattern = new[] { "aaa", "bbb", "aaa", "ccc" };
        var array = Strings(Enumerable.Range(0, 40).Select(i => pattern[i % 4]).ToArray());

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, null, 40, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(3, result.Value.DictionaryCount);
        Assert.Equal([0, 1, 0, 2], result.Value.Indices.Take(4));
    }

    [Fact]
    public void TryEncode_AllEmptyStrings_IsConstant()
    {
        // Value length 0 — the buffer is empty, so periodicity is vacuous and only the offset walk speaks.
        var array = Strings("", "", "");

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, null, 3, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal(4, result.Value.DictionaryPageData.Length); // length prefix only
        Assert.Equal([0, 0, 0], result.Value.Indices);
    }

    [Fact]
    public void TryEncode_ConstantColumnWithNulls_FallsBackToHashing()
    {
        // The probe reads every row, so it is only valid with no nulls. With def levels present the column
        // must still encode correctly — through the hashing loop, which skips null rows.
        var array = Strings("x", "x", "x", "x");
        int[] defLevels = [1, 0, 1, 1];

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, defLevels, 3, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal(3, result.Value.Indices.Length); // one per NON-NULL row
    }

    [Fact]
    public void TryEncode_ConstantFixedWidthColumn_MatchesWhatHashingWouldProduce()
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < 64; i++) b.Append(7L);

        var result = DictionaryEncoder.TryEncode(
            b.Build(), PhysicalType.Int64, 0, null, 64, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal(8, result.Value.DictionaryPageData.Length);
        Assert.Equal(7L, BitConverter.ToInt64(result.Value.DictionaryPageData, 0));
        Assert.All(result.Value.Indices, idx => Assert.Equal(0, idx));
    }

    [Fact]
    public void TryEncode_SingleRow_IsConstant()
    {
        var result = DictionaryEncoder.TryEncode(
            Strings("only"), PhysicalType.ByteArray, 0, null, 1, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal([0], result.Value.Indices);
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
