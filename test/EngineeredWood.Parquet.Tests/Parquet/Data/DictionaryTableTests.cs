// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The byte-keyed dictionary (BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, run-end encoded) against the plain answer: the
/// distinct values in order of first appearance, laid out as the PLAIN page, and each value's index into them.
/// The keys aim at the table's short-key words (lengths 0 to 16 that share all but one byte, zero bytes
/// against a shorter key) and its long-key path, with enough distinct values to grow the table and the page
/// several times; and the two limits, each hit exactly and passed by one.
/// </summary>
public class DictionaryTableTests
{
    private static readonly ParquetWriteOptions Options = new() { DictionaryEnabled = true, DictionaryPageSizeLimit = 1 << 20 };

    public static TheoryData<int> Seeds()
    {
        var data = new TheoryData<int>();
        for (int seed = 0; seed < 12; seed++)
            data.Add(seed);
        return data;
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void ByteArray_MatchesFirstAppearance(int seed)
    {
        var random = new Random(seed);
        var pool = KeyPool(random, 400);
        var values = Enumerable.Range(0, 5000).Select(_ => pool[random.Next(pool.Length)]).ToArray();
        bool[] present = Enumerable.Range(0, values.Length).Select(_ => seed % 2 == 0 || random.Next(5) != 0).ToArray();

        var builder = new BinaryArray.Builder();
        for (int i = 0; i < values.Length; i++)
        {
            if (present[i]) builder.Append(values[i].AsSpan());
            else builder.AppendNull();
        }

        int[]? levels = seed % 2 == 0 ? null : present.Select(p => p ? 1 : 0).ToArray();
        int nonNull = present.Count(p => p);
        var result = DictionaryEncoder.TryEncode(builder.Build(), PhysicalType.ByteArray, 0, levels, nonNull, Options);

        var (page, count, indices) = Expected(values.Where((_, i) => present[i]), lengthPrefixed: true);
        Assert.NotNull(result);
        Assert.Equal(count, result.Value.DictionaryCount);
        Assert.Equal(page, result.Value.DictionaryPageData);
        Assert.Equal(indices, result.Value.Indices);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(33)]
    public void FixedLenByteArray_MatchesFirstAppearance(int length)
    {
        var random = new Random(length);
        // Keys that differ in one byte only, at every position, so every byte must reach the comparison.
        var pool = new List<byte[]>();
        for (int at = 0; at < length; at++)
        {
            for (int v = 0; v < 3; v++)
            {
                var key = new byte[length];
                key[at] = (byte)v;
                pool.Add(key);
            }
        }

        var values = Enumerable.Range(0, 20 * pool.Count).Select(_ => pool[random.Next(pool.Count)]).ToArray();
        var data = values.SelectMany(v => v).ToArray();
        var array = new Apache.Arrow.Arrays.FixedSizeBinaryArray(new ArrayData(
            new FixedSizeBinaryType(length), values.Length, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(data)]));

        var result = DictionaryEncoder.TryEncode(array, PhysicalType.FixedLenByteArray, length, null, values.Length, Options);

        var (page, count, indices) = Expected(values, lengthPrefixed: false);
        Assert.NotNull(result);
        Assert.Equal(count, result.Value.DictionaryCount);
        Assert.Equal(page, result.Value.DictionaryPageData);
        Assert.Equal(indices, result.Value.Indices);
    }

    [Fact]
    public void RunEndEncoded_MatchesFirstAppearance()
    {
        var random = new Random(5);
        var pool = KeyPool(random, 60).Select(k => new string(k.Select(b => (char)('a' + b % 26)).ToArray())).Distinct().ToArray();
        var runs = Enumerable.Range(0, 2000).Select(_ => (Value: pool[random.Next(pool.Length)], Length: 1 + random.Next(5))).ToArray();

        var values = new StringArray.Builder();
        var ends = new Int32Array.Builder();
        int end = 0;
        foreach (var (value, length) in runs)
        {
            values.Append(value);
            ends.Append(end += length);
        }

        var result = DictionaryEncoder.TryEncode(
            new RunEndEncodedArray(ends.Build(), values.Build()), PhysicalType.ByteArray, 0, null, end, Options);

        var (page, count, _) = Expected(runs.Select(r => System.Text.Encoding.UTF8.GetBytes(r.Value)), lengthPrefixed: true);
        Assert.NotNull(result);
        Assert.Equal(count, result.Value.DictionaryCount);
        Assert.Equal(page, result.Value.DictionaryPageData);
    }

    [Fact]
    public void ZeroBytesAgainstShorterKeys_AreDistinct()
    {
        // Zero-filled to a word, "a", "a\0" and "a\0\0" read alike; their lengths tell them apart.
        byte[][] keys = [[0x61], [0x61, 0], [0x61, 0, 0], [], [0], [0, 0, 0, 0, 0, 0, 0, 0], new byte[16], new byte[17]];
        var builder = new BinaryArray.Builder();
        for (int i = 0; i < 100; i++)
            builder.Append(keys[i % keys.Length].AsSpan());

        var result = DictionaryEncoder.TryEncode(builder.Build(), PhysicalType.ByteArray, 0, null, 100, Options);
        Assert.NotNull(result);
        Assert.Equal(keys.Length, result.Value.DictionaryCount);
        Assert.Equal(Expected(Enumerable.Range(0, 100).Select(i => keys[i % keys.Length]), true).Page, result.Value.DictionaryPageData);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void CardinalityCap_ExactlyAndOneOver(int over)
    {
        // A fifth of the non-null values may be distinct: 1000 rows allow 200.
        var builder = new StringArray.Builder();
        for (int i = 0; i < 1000; i++)
            builder.Append("k" + (i % (200 + over)));

        var result = DictionaryEncoder.TryEncode(builder.Build(), PhysicalType.ByteArray, 0, null, 1000, Options);
        Assert.Equal(over == 0, result is not null);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void PageSizeLimit_ExactlyAndOneOver(int over)
    {
        // 10 distinct 6-byte values: a 100-byte page (4 + 6 each). A 100-byte limit takes it; 99 does not.
        var builder = new StringArray.Builder();
        for (int i = 0; i < 100; i++)
            builder.Append("value" + (i % 10));

        var options = Options with { DictionaryPageSizeLimit = 100 - over };
        var result = DictionaryEncoder.TryEncode(builder.Build(), PhysicalType.ByteArray, 0, null, 100, options);
        Assert.Equal(over == 0, result is not null);
    }

    /// <summary>The PLAIN page of the distinct values in order of first appearance, their count, and each
    /// value's index.</summary>
    private static (byte[] Page, int Count, int[] Indices) Expected(IEnumerable<byte[]> values, bool lengthPrefixed)
    {
        var order = new List<byte[]>();
        var index = new Dictionary<string, int>();
        var indices = new List<int>();
        foreach (var v in values)
        {
            string k = Convert.ToBase64String(v);
            if (!index.TryGetValue(k, out int i))
            {
                index[k] = i = order.Count;
                order.Add(v);
            }

            indices.Add(i);
        }

        var page = new List<byte>();
        foreach (var v in order)
        {
            if (lengthPrefixed)
            {
                var len = new byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(len, v.Length);
                page.AddRange(len);
            }

            page.AddRange(v);
        }

        return (page.ToArray(), order.Count, indices.ToArray());
    }

    /// <summary>
    /// Keys of length 0 to 40: a shared prefix up to 20 bytes long and a tail from a three-byte alphabet with zero
    /// in it, so many keys differ in one byte and short ones read alike once zero-filled.
    /// </summary>
    private static byte[][] KeyPool(Random random, int count)
    {
        byte[] prefix = "category-of-things-x"u8.ToArray();
        var keys = new byte[count][];
        for (int k = 0; k < count; k++)
        {
            int p = random.Next(prefix.Length + 1);
            int tail = random.Next(21);
            var key = new byte[p + tail];
            prefix.AsSpan(0, p).CopyTo(key);
            for (int i = p; i < key.Length; i++)
                key[i] = (byte)(random.Next(3) switch { 0 => 0, 1 => 1, _ => 0xFF });
            keys[k] = key;
        }

        return keys;
    }
}
