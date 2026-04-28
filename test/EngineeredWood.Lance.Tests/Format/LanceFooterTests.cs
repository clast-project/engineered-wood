// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Lance.Format;

namespace EngineeredWood.Lance.Tests.Format;

public class LanceFooterTests
{
    [Fact]
    public void Parse_RoundTripsAllFields()
    {
        var original = new LanceFooter(
            ColumnMetaStart: 0x1122_3344_5566_7788L,
            CmoTableOffset: 0x2233_4455_6677_8899L,
            GboTableOffset: 0x3344_5566_7788_99AAL,
            NumGlobalBuffers: 3,
            NumColumns: 7,
            Version: LanceVersion.V2_1);

        Span<byte> bytes = stackalloc byte[LanceFooter.Size];
        original.WriteTo(bytes);

        LanceFooter parsed = LanceFooter.Parse(bytes);
        Assert.Equal(original, parsed);
    }

    [Fact]
    public void Parse_RejectsBadMagic()
    {
        Span<byte> bytes = stackalloc byte[LanceFooter.Size];
        new LanceFooter(0, 0, 0, 0, 0, LanceVersion.V2_1).WriteTo(bytes);
        // Corrupt the magic.
        bytes[36] = (byte)'X';

        byte[] copy = bytes.ToArray();
        var ex = Assert.Throws<LanceFormatException>(() => LanceFooter.Parse(copy));
        Assert.Contains("LANC", ex.Message);
    }

    [Fact]
    public void Parse_RejectsWrongLength()
    {
        byte[] tooShort = new byte[LanceFooter.Size - 1];
        Assert.Throws<ArgumentException>(() => LanceFooter.Parse(tooShort));
    }

    [Theory]
    [InlineData(0, 2)]   // legacy v0.1
    [InlineData(2, 2)]   // future v2.2
    [InlineData(3, 0)]   // unknown future
    public void Version_IsSupported_OnlyForV20AndV21(int major, int minor)
    {
        var version = new LanceVersion((ushort)major, (ushort)minor);
        Assert.False(version.IsSupported);
    }

    [Theory]
    [InlineData(2, 0)]
    [InlineData(2, 1)]
    public void Version_IsSupported_ForV20AndV21(int major, int minor)
    {
        var version = new LanceVersion((ushort)major, (ushort)minor);
        Assert.True(version.IsSupported);
    }
}
