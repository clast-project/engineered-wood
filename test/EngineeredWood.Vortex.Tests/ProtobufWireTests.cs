// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Layouts;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Skipping a field the parser doesn't know. A length-delimited skip must count the length
/// varint's own bytes; an earlier form (<c>pos += ReadUnsigned(bytes, ref pos)</c>) didn't, and
/// landed mid-message.
/// </summary>
public class ProtobufWireTests
{
    [Theory]
    [InlineData(new byte[] { 0x96, 0x01 }, 0ul, 2)]                     // varint 150
    [InlineData(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, 1ul, 8)]         // fixed64
    [InlineData(new byte[] { 0x03, 0xAA, 0xBB, 0xCC }, 2ul, 4)]         // 1-byte length + 3 bytes
    [InlineData(new byte[] { 1, 2, 3, 4 }, 5ul, 4)]                     // fixed32
    public void SkipsTheWholeValue(byte[] value, ulong wireType, int expectedEnd)
    {
        int pos = 0;
        ProtobufWire.SkipField(value, ref pos, wireType, "test");
        Assert.Equal(expectedEnd, pos);
    }

    [Fact]
    public void SkipsALengthWhoseVarintTakesTwoBytes()
    {
        var value = new byte[2 + 200];
        value[0] = 0xC8; // 200 = 0b1_1001000 -> 0xC8 0x01
        value[1] = 0x01;
        int pos = 0;
        ProtobufWire.SkipField(value, ref pos, 2, "test");
        Assert.Equal(202, pos);
    }

    [Fact]
    public void RejectsAFieldRunningPastTheMessage()
    {
        int pos = 0;
        Assert.Throws<VortexFormatException>(
            () => ProtobufWire.SkipField(new byte[] { 0x05, 0x01 }, ref pos, 2, "test"));
    }

    [Theory]
    [InlineData(1ul)]
    [InlineData(5ul)]
    public void RejectsAFixedWidthFieldPastTheMessage(ulong wireType)
    {
        int pos = 1;
        Assert.Throws<VortexFormatException>(
            () => ProtobufWire.SkipField(new byte[] { 0, 1, 2 }, ref pos, wireType, "test"));
    }

    [Fact]
    public void RejectsALengthThatWouldOverflowTheOffset()
    {
        // Three bytes in, a length of int.MaxValue: added first, it wraps negative and would
        // pass a "past the end" check.
        var bytes = new byte[] { 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0x07, 0 };
        int pos = 3;
        Assert.Throws<VortexFormatException>(
            () => ProtobufWire.SkipField(bytes, ref pos, 2, "test"));
    }

    [Fact]
    public void ZonedMetadataReadsFieldsAfterAnUnknownBytesField()
    {
        // version 1, then: field 9 bytes "xyz" (unknown), field 1 zone_len = 8192,
        // field 2 aggregate { id = "vortex.null_count" }.
        var id = System.Text.Encoding.UTF8.GetBytes(ZonedZoneMap.NullCount);
        var spec = new byte[] { 0x0A, (byte)id.Length }.Concat(id).ToArray();
        var metadata = new byte[] { 1, 0x4A, 3, (byte)'x', (byte)'y', (byte)'z', 0x08, 0x80, 0x40, 0x12, (byte)spec.Length }
            .Concat(spec)
            .ToArray();

        Assert.True(ZonedZoneMap.TryParseMetadata(metadata, out var zoneLen, out var aggregates));
        Assert.Equal(8192, zoneLen);
        Assert.Equal(ZonedZoneMap.NullCount, Assert.Single(aggregates).Id);
    }
}
