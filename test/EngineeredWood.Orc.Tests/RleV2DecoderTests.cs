// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Buffers;
using EngineeredWood.Orc.Encodings;

namespace EngineeredWood.Orc.Tests;

public class RleV2DecoderTests
{
    [Fact]
    public void ShortRepeat_SpecExample()
    {
        // [10000, 10000, 10000, 10000, 10000] encodes as [0x0a, 0x27, 0x10]
        var data = new byte[] { 0x0a, 0x27, 0x10 };
        var decoder = new RleDecoderV2(new OrcByteStream(data, 0, data.Length), signed: false);
        var values = new long[5];
        decoder.ReadValues(values);
        Assert.All(values, v => Assert.Equal(10000, v));
    }

    [Fact]
    public void Direct_SpecExample()
    {
        // [23713, 43806, 57005, 48879] encodes as [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef]
        var data = new byte[] { 0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef };
        var decoder = new RleDecoderV2(new OrcByteStream(data, 0, data.Length), signed: false);
        var values = new long[4];
        decoder.ReadValues(values);
        Assert.Equal(23713, values[0]);
        Assert.Equal(43806, values[1]);
        Assert.Equal(57005, values[2]);
        Assert.Equal(48879, values[3]);
    }

    [Fact]
    public void Delta_SpecExample()
    {
        // [2, 3, 5, 7, 11, 13, 17, 19, 23, 29] encodes as [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46]
        var data = new byte[] { 0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46 };
        var decoder = new RleDecoderV2(new OrcByteStream(data, 0, data.Length), signed: false);
        var values = new long[10];
        decoder.ReadValues(values);
        long[] expected = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29];
        for (int i = 0; i < 10; i++)
            Assert.Equal(expected[i], values[i]);
    }

    [Fact]
    public void MultipleBatches_Direct()
    {
        // Create a Direct encoding with 4 values, read 2 at a time
        var data = new byte[] { 0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef };
        var stream = new OrcByteStream(data, 0, data.Length);

        // First batch: read 2 values
        var decoder1 = new RleDecoderV2(stream, signed: false);
        var values1 = new long[2];
        decoder1.ReadValues(values1);
        Assert.Equal(23713, values1[0]);
        Assert.Equal(43806, values1[1]);

        // Stream position should be past the full run (all 4 values consumed)
        // So the next read should fail (no more data)
    }

    [Fact]
    public void EncoderDecoder_PatchedBase_RoundTrip()
    {
        // Data with mostly small values and a few large outliers
        var rng = new Random(123);
        var original = new long[100];
        for (int i = 0; i < 100; i++)
        {
            if (i == 10 || i == 50 || i == 90)
                original[i] = 1_000_000L + rng.Next(1000);
            else
                original[i] = rng.Next(50);
        }

        var buf = new GrowableBuffer();
        var encoder = new RleEncoderV2(buf, signed: true);
        encoder.WriteValues(original);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new RleDecoderV2(new OrcByteStream(decoderBytes, 0, decoderBytes.Length), signed: true);
        var decoded = new long[100];
        decoder.ReadValues(decoded);

        for (int i = 0; i < 100; i++)
            Assert.Equal(original[i], decoded[i]);
    }

    [Fact]
    public void EncoderDecoder_ShortRepeat_RoundTrip()
    {
        foreach (int count in new[] { 3, 5, 7, 10 })
        {
            var original = new long[count];
#if NET8_0_OR_GREATER
            Array.Fill(original, 42L);
#else
            for (int i = 0; i < original.Length; i++) original[i] = 42L;
#endif

            var buf = new GrowableBuffer();
            var encoder = new RleEncoderV2(buf, signed: true);
            encoder.WriteValues(original);
            encoder.Flush();

            var decoderBytes = buf.WrittenSpan.ToArray();
            var decoder = new RleDecoderV2(new OrcByteStream(decoderBytes, 0, decoderBytes.Length), signed: true);
            var decoded = new long[count];
            decoder.ReadValues(decoded);

            for (int i = 0; i < count; i++)
                Assert.Equal(42L, decoded[i]);
        }
    }

    [Fact]
    public void EncoderDecoder_StringLengths_RoundTrip()
    {
        // Simulate string lengths: 10 * 6 then 40 * 7 (like "item_0" through "item_49")
        // Regression test: delta encoding with 1-bit deltas was misinterpreted as constant
        var original = new long[50];
        for (int i = 0; i < 10; i++) original[i] = 6;
        for (int i = 10; i < 50; i++) original[i] = 7;

        var buf = new GrowableBuffer();
        var encoder = new RleEncoderV2(buf, signed: false);
        encoder.WriteValues(original);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new RleDecoderV2(new OrcByteStream(decoderBytes, 0, decoderBytes.Length), signed: false);
        var decoded = new long[50];
        decoder.ReadValues(decoded);

        for (int i = 0; i < 50; i++)
            Assert.Equal(original[i], decoded[i]);
    }

    [Fact]
    public void EncoderDecoder_ShortRepeat_Negative()
    {
        var original = new long[] { -100, -100, -100, -100, -100 };
        var buf = new GrowableBuffer();
        var encoder = new RleEncoderV2(buf, signed: true);
        encoder.WriteValues(original);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new RleDecoderV2(new OrcByteStream(decoderBytes, 0, decoderBytes.Length), signed: true);
        var decoded = new long[5];
        decoder.ReadValues(decoded);

        for (int i = 0; i < 5; i++)
            Assert.Equal(-100L, decoded[i]);
    }
}
