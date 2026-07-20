// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Orc.Encodings;

namespace EngineeredWood.Orc.Tests;

public class RleV1DecoderTests
{
    /// <summary>
    /// Helper to write an RLE v1 run: control byte (runLength-3), delta byte, then base value as varint.
    /// </summary>
    private static void WriteRun(MemoryStream ms, int runLength, sbyte delta, long baseValue, bool signed)
    {
        ms.WriteByte((byte)(runLength - 3)); // control: 0..127
        ms.WriteByte((byte)delta);
        if (signed)
            WriteSignedVarInt(ms, baseValue);
        else
            WriteUnsignedVarInt(ms, baseValue);
    }

    /// <summary>
    /// Helper to write RLE v1 literals: control byte (-(count) as sbyte), then values as varints.
    /// </summary>
    private static void WriteLiterals(MemoryStream ms, long[] values, bool signed)
    {
        ms.WriteByte((byte)(-(sbyte)values.Length)); // control: 128..255
        foreach (var v in values)
        {
            if (signed)
                WriteSignedVarInt(ms, v);
            else
                WriteUnsignedVarInt(ms, v);
        }
    }

    private static void WriteUnsignedVarInt(Stream output, long value)
    {
        ulong v = (ulong)value;
        while (v > 0x7F)
        {
            output.WriteByte((byte)(0x80 | (v & 0x7F)));
            v >>= 7;
        }
        output.WriteByte((byte)v);
    }

    private static void WriteSignedVarInt(Stream output, long value)
    {
        ulong zz = (ulong)((value << 1) ^ (value >> 63));
        while (zz > 0x7F)
        {
            output.WriteByte((byte)(0x80 | (zz & 0x7F)));
            zz >>= 7;
        }
        output.WriteByte((byte)zz);
    }

    [Fact]
    public void Run_ConstantValues()
    {
        // Run of 5 identical values (delta=0, base=42)
        var ms = new MemoryStream();
        WriteRun(ms, 5, 0, 42, signed: false);

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: false);
        var values = new long[5];
        decoder.ReadValues(values);

        Assert.All(values, v => Assert.Equal(42L, v));
    }

    [Fact]
    public void Run_WithDelta()
    {
        // Run of 5 values starting at 10 with delta=3: [10, 13, 16, 19, 22]
        var ms = new MemoryStream();
        WriteRun(ms, 5, 3, 10, signed: false);

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: false);
        var values = new long[5];
        decoder.ReadValues(values);

        Assert.Equal([10L, 13L, 16L, 19L, 22L], values);
    }

    [Fact]
    public void Run_WithNegativeDelta()
    {
        // Run of 4 values starting at 100 with delta=-5: [100, 95, 90, 85]
        var ms = new MemoryStream();
        WriteRun(ms, 4, -5, 100, signed: true);

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: true);
        var values = new long[4];
        decoder.ReadValues(values);

        Assert.Equal([100L, 95L, 90L, 85L], values);
    }

    [Fact]
    public void Run_MinLength()
    {
        // Minimum run length is 3 (control=0)
        var ms = new MemoryStream();
        WriteRun(ms, 3, 1, 0, signed: false);

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: false);
        var values = new long[3];
        decoder.ReadValues(values);

        Assert.Equal([0L, 1L, 2L], values);
    }

    [Fact]
    public void Run_MaxLength()
    {
        // Maximum run length is 130 (control=127)
        var ms = new MemoryStream();
        WriteRun(ms, 130, 0, 7, signed: false);

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: false);
        var values = new long[130];
        decoder.ReadValues(values);

        Assert.All(values, v => Assert.Equal(7L, v));
    }

    [Fact]
    public void Literals_Unsigned()
    {
        var ms = new MemoryStream();
        WriteLiterals(ms, [100, 200, 300, 400], signed: false);

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: false);
        var values = new long[4];
        decoder.ReadValues(values);

        Assert.Equal([100L, 200L, 300L, 400L], values);
    }

    [Fact]
    public void Literals_Signed()
    {
        var ms = new MemoryStream();
        WriteLiterals(ms, [-10, 20, -30, 40], signed: true);

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: true);
        var values = new long[4];
        decoder.ReadValues(values);

        Assert.Equal([-10L, 20L, -30L, 40L], values);
    }

    [Fact]
    public void Mixed_RunThenLiterals()
    {
        var ms = new MemoryStream();
        WriteRun(ms, 3, 0, 5, signed: false);       // [5, 5, 5]
        WriteLiterals(ms, [10, 20, 30], signed: false); // [10, 20, 30]

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: false);
        var values = new long[6];
        decoder.ReadValues(values);

        Assert.Equal([5L, 5L, 5L, 10L, 20L, 30L], values);
    }

    [Fact]
    public void Mixed_LiteralsThenRun()
    {
        var ms = new MemoryStream();
        WriteLiterals(ms, [7, 14], signed: false);
        WriteRun(ms, 4, 2, 100, signed: false); // [100, 102, 104, 106]

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: false);
        var values = new long[6];
        decoder.ReadValues(values);

        Assert.Equal([7L, 14L, 100L, 102L, 104L, 106L], values);
    }

    [Fact]
    public void ReadInSmallBatches()
    {
        // Write a run of 6 values, read 2 at a time
        var ms = new MemoryStream();
        WriteRun(ms, 6, 1, 10, signed: false); // [10, 11, 12, 13, 14, 15]

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: false);

        var batch1 = new long[2];
        decoder.ReadValues(batch1);
        Assert.Equal([10L, 11L], batch1);

        var batch2 = new long[2];
        decoder.ReadValues(batch2);
        Assert.Equal([12L, 13L], batch2);

        var batch3 = new long[2];
        decoder.ReadValues(batch3);
        Assert.Equal([14L, 15L], batch3);
    }

    [Fact]
    public void ReadAcrossRunBoundary()
    {
        // Two runs of 3, read 4 then 2 (spans the boundary)
        var ms = new MemoryStream();
        WriteRun(ms, 3, 0, 1, signed: false);  // [1, 1, 1]
        WriteRun(ms, 3, 0, 2, signed: false);  // [2, 2, 2]

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: false);

        var batch1 = new long[4];
        decoder.ReadValues(batch1);
        Assert.Equal([1L, 1L, 1L, 2L], batch1);

        var batch2 = new long[2];
        decoder.ReadValues(batch2);
        Assert.Equal([2L, 2L], batch2);
    }

    [Fact]
    public void LargeValues_Unsigned()
    {
        var ms = new MemoryStream();
        WriteLiterals(ms, [long.MaxValue, 0, 1_000_000_000_000L], signed: false);

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: false);
        var values = new long[3];
        decoder.ReadValues(values);

        Assert.Equal(long.MaxValue, values[0]);
        Assert.Equal(0L, values[1]);
        Assert.Equal(1_000_000_000_000L, values[2]);
    }

    [Fact]
    public void LargeValues_Signed()
    {
        var ms = new MemoryStream();
        WriteLiterals(ms, [long.MinValue, long.MaxValue, -1], signed: true);

        var bytes = ms.ToArray();
        var decoder = new RleDecoderV1(new OrcByteStream(bytes, 0, bytes.Length), signed: true);
        var values = new long[3];
        decoder.ReadValues(values);

        Assert.Equal(long.MinValue, values[0]);
        Assert.Equal(long.MaxValue, values[1]);
        Assert.Equal(-1L, values[2]);
    }
}
