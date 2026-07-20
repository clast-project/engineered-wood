// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake.Tests;

public class RoaringBitmapReaderTests
{
    /// <summary>
    /// Builds a minimal RoaringBitmapArray with the Delta Lake magic number
    /// and an array container containing the specified values (all in container 0, i.e., values 0-65535).
    /// </summary>
    private static byte[] BuildSimpleDv(params ushort[] values)
    {
        // Spec RoaringBitmapArray (the only accepted form):
        // 4 bytes:  magic (0x6431544D)
        // 8 bytes:  int64 sub-bitmap count
        // Per sub-bitmap: 4 bytes int32 high-32-bit key, then a portable CRoaring bitmap:
        //   4 bytes: cookie = 12346 (NO_RUNCONTAINER)
        //   4 bytes: size = container count
        //   Per container: 2 bytes key + 2 bytes (cardinality-1)   (descriptive header)
        //   Per container: 4 bytes offset                          (offset header, always present)
        //   Then container data

        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);

        bw.Write((uint)1681511377);   // magic
        bw.Write((long)1);            // one sub-bitmap
        bw.Write((uint)0);            // high-32-bit key = 0

        int containerCount = 1;
        int cardinality = values.Length;

        bw.Write((uint)12346);        // cookie: NO_RUNCONTAINER
        bw.Write((uint)containerCount);

        // Descriptive header: key=0, cardinality-1
        bw.Write((ushort)0);
        bw.Write((ushort)(cardinality - 1));

        // Offset header (always present for the no-run cookie); the reader skips it, so the value is filler.
        bw.Write((uint)0);

        // Array container: sorted uint16 values
        foreach (ushort v in values.OrderBy(v => v))
            bw.Write(v);

        bw.Flush();
        return ms.ToArray();
    }

    [Fact]
    public void Deserialize_SingleValue()
    {
        byte[] data = BuildSimpleDv(42);
        var result = RoaringBitmapReader.Deserialize(data);

        Assert.Single(result);
        Assert.Contains(42L, result);
    }

    [Fact]
    public void Deserialize_MultipleValues()
    {
        byte[] data = BuildSimpleDv(1, 5, 10, 100);
        var result = RoaringBitmapReader.Deserialize(data);

        Assert.Equal(4, result.Count);
        Assert.Contains(1L, result);
        Assert.Contains(5L, result);
        Assert.Contains(10L, result);
        Assert.Contains(100L, result);
    }

    [Fact]
    public void Deserialize_InvalidMagic_Throws()
    {
        byte[] data = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(data));
    }

    [Fact]
    public void Deserialize_TooShort_Throws()
    {
        byte[] data = [0x01, 0x02];
        Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(data));
    }

    [Fact]
    public void Deserialize_ConsecutiveValues()
    {
        // Values 0, 1, 2, 3, 4
        byte[] data = BuildSimpleDv(0, 1, 2, 3, 4);
        var result = RoaringBitmapReader.Deserialize(data);

        Assert.Equal(5, result.Count);
        for (long i = 0; i < 5; i++)
            Assert.Contains(i, result);
    }
}
