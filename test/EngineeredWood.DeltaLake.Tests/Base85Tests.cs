// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake.Tests;

public class Base85Tests
{
    [Fact]
    public void RoundTrip_SimpleData()
    {
        byte[] original = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
        string encoded = Base85.Encode(original);
        byte[] decoded = Base85.Decode(encoded);
        Assert.Equal(original, decoded);
    }

    [Fact]
    public void RoundTrip_AllZeros()
    {
        byte[] original = [0, 0, 0, 0];
        string encoded = Base85.Encode(original);
        Assert.Equal(5, encoded.Length);
        byte[] decoded = Base85.Decode(encoded);
        Assert.Equal(original, decoded);
    }

    [Fact]
    public void RoundTrip_AllOnes()
    {
        byte[] original = [0xFF, 0xFF, 0xFF, 0xFF];
        string encoded = Base85.Encode(original);
        byte[] decoded = Base85.Decode(encoded);
        Assert.Equal(original, decoded);
    }

    [Fact]
    public void Decode_KnownValue()
    {
        // "HelloWorld" in Z85 decodes to specific bytes
        // Z85: "HelloWorld" (10 chars) → 8 bytes
        string encoded = "HelloWorld";
        byte[] decoded = Base85.Decode(encoded);
        Assert.Equal(8, decoded.Length);

        // Verify round-trip
        string reEncoded = Base85.Encode(decoded);
        Assert.Equal(encoded, reEncoded);
    }

    [Fact]
    public void Decode_InvalidLength_Throws()
    {
        Assert.Throws<DeltaFormatException>(() => Base85.Decode("abc"));
    }

    [Fact]
    public void Encode_InvalidLength_Throws()
    {
        Assert.Throws<ArgumentException>(() => Base85.Encode([1, 2, 3]));
    }

    [Fact]
    public void RoundTrip_16Bytes_UuidSize()
    {
        // UUID-sized data (16 bytes → 20 Z85 chars)
        byte[] uuid = Guid.NewGuid().ToByteArray();
        string encoded = Base85.Encode(uuid);
        Assert.Equal(20, encoded.Length);
        byte[] decoded = Base85.Decode(encoded);
        Assert.Equal(uuid, decoded);
    }
}
