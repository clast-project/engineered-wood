// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Encodings;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class AdvancedEncoderTests
{
    // ── DeltaBinaryPacked ──

    [Fact]
    public void DeltaBinaryPacked_Int32_RoundTrip()
    {
        var values = new int[] { 1, 2, 3, 4, 5, 100, 200, 300 };
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt32s(values);
        var encoded = encoder.ToArray();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[values.Length];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void DeltaBinaryPacked_Int32_SingleValue()
    {
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt32s([42]);
        var encoded = encoder.ToArray();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[1];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(42, decoded[0]);
    }

    [Fact]
    public void DeltaBinaryPacked_Int32_Constant()
    {
        // All same value — all deltas are 0
        var values = Enumerable.Repeat(7, 200).ToArray();
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt32s(values);
        var encoded = encoder.ToArray();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[values.Length];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void DeltaBinaryPacked_Int32_NegativeDeltas()
    {
        var values = new int[] { 100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0, -10 };
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt32s(values);
        var encoded = encoder.ToArray();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[values.Length];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void DeltaBinaryPacked_Int32_LargeDataset()
    {
        var values = new int[10_000];
        for (int i = 0; i < values.Length; i++)
            values[i] = i * 7 - 3000;

        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt32s(values);
        var encoded = encoder.ToArray();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[values.Length];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(values, decoded);

        // Delta encoding should be much smaller than PLAIN for sequential data
        Assert.True(encoded.Length < values.Length * 4,
            $"Encoded={encoded.Length}, PLAIN={values.Length * 4}");
    }

    [Fact]
    public void DeltaBinaryPacked_Int64_RoundTrip()
    {
        var values = new long[] { long.MinValue, -1, 0, 1, long.MaxValue };
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt64s(values);
        var encoded = encoder.ToArray();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new long[values.Length];
        decoder.DecodeInt64s(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void DeltaBinaryPacked_Int64_Sequential()
    {
        var values = new long[500];
        for (int i = 0; i < values.Length; i++)
            values[i] = (long)i * 1000000;

        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt64s(values);
        var encoded = encoder.ToArray();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new long[values.Length];
        decoder.DecodeInt64s(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void DeltaBinaryPacked_Int32_MixedDeltas()
    {
        // Mix of positive and negative deltas, plus some constants
        var values = new int[] { 0, 10, 5, 100, 100, 100, -50, -100, 0, 1, 2, 3 };
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt32s(values);
        var encoded = encoder.ToArray();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[values.Length];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(values, decoded);
    }

    // ── ByteStreamSplit ──

    [Fact]
    public void ByteStreamSplit_Float_RoundTrip()
    {
        var values = new float[] { 1.0f, 2.5f, -3.14f, 0f, float.MaxValue };
        var encoded = ByteStreamSplitEncoder.EncodeFloats(values);

        var decoded = new float[values.Length];
        ByteStreamSplitDecoder.DecodeFloats(encoded, decoded, values.Length);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void ByteStreamSplit_Double_RoundTrip()
    {
        var values = new double[] { 1.0, 2.718281828, -1e100, 0.0, double.MaxValue };
        var encoded = ByteStreamSplitEncoder.EncodeDoubles(values);

        var decoded = new double[values.Length];
        ByteStreamSplitDecoder.DecodeDoubles(encoded, decoded, values.Length);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void ByteStreamSplit_Float_OutputSize()
    {
        var values = new float[100];
        var encoded = ByteStreamSplitEncoder.EncodeFloats(values);
        Assert.Equal(400, encoded.Length); // 100 * 4 bytes
    }

    // ── DeltaLengthByteArray ──

    [Fact]
    public void DeltaLengthByteArray_RoundTrip()
    {
        var strings = new[] { "hello", "world", "", "parquet", "test" };
        // Build Arrow-style offsets and data
        var offsets = new int[strings.Length + 1];
        offsets[0] = 0;
        for (int i = 0; i < strings.Length; i++)
            offsets[i + 1] = offsets[i] + strings[i].Length;

        var data = new byte[offsets[strings.Length]];
        for (int i = 0; i < strings.Length; i++)
            System.Text.Encoding.UTF8.GetBytes(strings[i]).CopyTo(data.AsSpan(offsets[i]));

        var encoded = DeltaLengthByteArrayEncoder.Encode(offsets, data, strings.Length);

        // Decode using the read-path decoder
        var state = new ColumnBuildState(
            PhysicalType.ByteArray, maxDefLevel: 0, maxRepLevel: 0, capacity: strings.Length,
            ByteArrayOutputKind.Default);
        DeltaLengthByteArrayDecoder.Decode(encoded, strings.Length, state);

        // Build the array and verify
        var field = new Apache.Arrow.Field("test", Apache.Arrow.Types.StringType.Default, false);
        var array = ArrowArrayBuilder.Build(state, field, strings.Length);
        var stringArray = (Apache.Arrow.StringArray)array;

        for (int i = 0; i < strings.Length; i++)
            Assert.Equal(strings[i], stringArray.GetString(i));

        state.Dispose();
    }

    [Fact]
    public void DeltaLengthByteArray_EmptyStrings()
    {
        var offsets = new int[] { 0, 0, 0, 0 }; // 3 empty strings
        var data = System.Array.Empty<byte>();

        var encoded = DeltaLengthByteArrayEncoder.Encode(offsets, data, 3);

        var state = new ColumnBuildState(
            PhysicalType.ByteArray, maxDefLevel: 0, maxRepLevel: 0, capacity: 3,
            ByteArrayOutputKind.Default);
        DeltaLengthByteArrayDecoder.Decode(encoded, 3, state);

        var field = new Apache.Arrow.Field("test", Apache.Arrow.Types.StringType.Default, false);
        var array = ArrowArrayBuilder.Build(state, field, 3);
        var stringArray = (Apache.Arrow.StringArray)array;

        for (int i = 0; i < 3; i++)
            Assert.Equal("", stringArray.GetString(i));

        state.Dispose();
    }

    // ── DeltaByteArray ──

    [Fact]
    public void DeltaByteArray_RoundTrip_SharedPrefixes()
    {
        var strings = new[] { "prefix_abc", "prefix_abd", "prefix_xyz", "other_123" };
        AssertDbaRoundTrip(strings);
    }

    [Fact]
    public void DeltaByteArray_RoundTrip_NoSharedPrefixes()
    {
        var strings = new[] { "alpha", "bravo", "charlie", "delta" };
        AssertDbaRoundTrip(strings);
    }

    [Fact]
    public void DeltaByteArray_RoundTrip_EmptyStrings()
    {
        var strings = new[] { "", "", "", "" };
        AssertDbaRoundTrip(strings);
    }

    [Fact]
    public void DeltaByteArray_RoundTrip_MixedEmpty()
    {
        var strings = new[] { "hello", "", "hello", "" };
        AssertDbaRoundTrip(strings);
    }

    [Fact]
    public void DeltaByteArray_RoundTrip_SingleValue()
    {
        AssertDbaRoundTrip(["only"]);
    }

    [Fact]
    public void DeltaByteArray_RoundTrip_IdenticalValues()
    {
        var strings = Enumerable.Repeat("same", 200).ToArray();
        AssertDbaRoundTrip(strings);
    }

    [Fact]
    public void DeltaByteArray_RoundTrip_SortedUrls()
    {
        var strings = Enumerable.Range(0, 500)
            .Select(i => $"https://example.com/path/to/resource/{i:D5}")
            .ToArray();
        AssertDbaRoundTrip(strings);
    }

    [Fact]
    public void DeltaByteArray_SmallerThanDlba_ForSortedPrefixData()
    {
        // Sorted strings with long common prefixes — DBA should be much smaller
        var strings = Enumerable.Range(0, 1000)
            .Select(i => $"https://cdn.example.com/assets/images/product/{i:D6}.jpg")
            .ToArray();

        // Encode with DBA
        var (offsets, data) = BuildOffsetsAndData(strings);
        int maxSize = DeltaByteArrayEncoder.EstimateMaxSize(strings.Length, data.Length);
        var dbaDest = new byte[maxSize];
        int dbaLen = DeltaByteArrayEncoder.Encode(offsets, data, 0, strings.Length, strings.Length, null, dbaDest);

        // Encode with DLBA
        var dlbaEncoded = DeltaLengthByteArrayEncoder.Encode(offsets, data, strings.Length);

        Assert.True(dbaLen < dlbaEncoded.Length,
            $"DBA ({dbaLen}) should be smaller than DLBA ({dlbaEncoded.Length}) for sorted prefix data");
    }

    private static void AssertDbaRoundTrip(string[] strings)
    {
        var (offsets, data) = BuildOffsetsAndData(strings);

        int maxSize = DeltaByteArrayEncoder.EstimateMaxSize(strings.Length, data.Length);
        var dest = new byte[maxSize];
        int encodedLen = DeltaByteArrayEncoder.Encode(
            offsets, data, 0, strings.Length, strings.Length, null, dest);

        // Decode using the read-path decoder
        var state = new ColumnBuildState(
            PhysicalType.ByteArray, maxDefLevel: 0, maxRepLevel: 0, capacity: strings.Length,
            ByteArrayOutputKind.Default);
        DeltaByteArrayDecoder.Decode(dest.AsSpan(0, encodedLen), strings.Length, state);

        var field = new Apache.Arrow.Field("test", Apache.Arrow.Types.StringType.Default, false);
        var array = ArrowArrayBuilder.Build(state, field, strings.Length);
        var stringArray = (Apache.Arrow.StringArray)array;

        for (int i = 0; i < strings.Length; i++)
            Assert.Equal(strings[i], stringArray.GetString(i));

        state.Dispose();
    }

    private static (int[] Offsets, byte[] Data) BuildOffsetsAndData(string[] strings)
    {
        var offsets = new int[strings.Length + 1];
        offsets[0] = 0;
        for (int i = 0; i < strings.Length; i++)
            offsets[i + 1] = offsets[i] + System.Text.Encoding.UTF8.GetByteCount(strings[i]);

        var data = new byte[offsets[strings.Length]];
        for (int i = 0; i < strings.Length; i++)
            System.Text.Encoding.UTF8.GetBytes(strings[i]).CopyTo(data.AsSpan(offsets[i]));

        return (offsets, data);
    }
}
