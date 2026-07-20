// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Vortex.FlatBuffers;
using EngineeredWood.Vortex.Tests.TestHelpers;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Tests for the hand-rolled FlatBuffers reader. The fixtures are built by
/// <see cref="BackwardsFlatBufferBuilder"/> so we can be specific about exactly
/// which bytes the reader is consuming.
/// </summary>
public class FlatBufferReaderTests
{
    /// <summary>
    /// Logical content under test:
    ///   table {
    ///     slot 0: u64    = 0x0123_4567_89AB_CDEF
    ///     slot 1: u32    = 0xDEAD_BEEF
    ///     slot 2:        absent
    ///     slot 3: string = "hi"
    ///     slot 4: vector&lt;u32&gt; = [1, 2, 3]
    ///   }
    /// </summary>
    [Fact]
    public void ReadsPrimitivesAndIndirects()
    {
        var b = new BackwardsFlatBufferBuilder();

        var stringTicket = b.WriteString("hi");
        var vectorTicket = b.WriteUInt32Vector(new uint[] { 1, 2, 3 });

        // Vtable layout (offsets within the table's inline data, after the soffset_t at offset 0):
        //   slot 0 (u64) at inline offset 4   (size 8)
        //   slot 1 (u32) at inline offset 12  (size 4)
        //   slot 2       absent (offset = 0)
        //   slot 3 (uoffset_t) at inline offset 16 (size 4)
        //   slot 4 (uoffset_t) at inline offset 20 (size 4)
        // vtable: [u16 vt_size=14][u16 inline_size=24][u16 s0=4][u16 s1=12][u16 s2=0][u16 s3=16][u16 s4=20]
        var vtableTicket = b.WriteUInt16s(new ushort[] { 14, 24, 4, 12, 0, 16, 20 });

        // Table contains a u64 field — alignment 8. inline_size = 24.
        var tableTicket = b.StartTable(alignment: 8, inlineSize: 24)
            .EmitUOffset(vectorTicket)            // slot 4
            .EmitUOffset(stringTicket)            // slot 3
            .EmitU32(0xDEAD_BEEFu)                // slot 1
            .EmitU64(0x0123_4567_89AB_CDEFUL)     // slot 0
            .EmitSOffsetTo(vtableTicket);

        var buf = b.Finish(tableTicket);

        var t = FlatBufferTable.ReadRoot(buf);

        Assert.Equal(0x0123_4567_89AB_CDEFUL, t.ReadUInt64(0));
        Assert.Equal(0xDEAD_BEEFu, t.ReadUInt32(1));
        Assert.Equal(0u, t.ReadUInt32(2));
        Assert.Equal(42u, t.ReadUInt32(2, 42u));
        Assert.Equal("hi", t.ReadString(3));

        var vec = t.ReadVector(4);
        Assert.Equal(3, vec.Length);
        Assert.Equal(1u, vec.UInt32(0));
        Assert.Equal(2u, vec.UInt32(1));
        Assert.Equal(3u, vec.UInt32(2));
    }

    [Fact]
    public void NestedTableIsReadable()
    {
        // table outer { slot 0: u32 = 7; slot 1: table inner { slot 0: u32 = 99 } }
        var b = new BackwardsFlatBufferBuilder();

        // Inner table.
        // vtable_inner: [u16 vt=6][u16 inline=8][u16 s0=4]
        var innerVt = b.WriteUInt16s(new ushort[] { 6, 8, 4 });
        var innerTable = b.StartTable(alignment: 4, inlineSize: 8)
            .EmitU32(99u)
            .EmitSOffsetTo(innerVt);

        // Outer table.
        // vtable_outer: [u16 vt=8][u16 inline=12][u16 s0=4][u16 s1=8]
        var outerVt = b.WriteUInt16s(new ushort[] { 8, 12, 4, 8 });
        var outerTable = b.StartTable(alignment: 4, inlineSize: 12)
            .EmitUOffset(innerTable)
            .EmitU32(7u)
            .EmitSOffsetTo(outerVt);

        var buf = b.Finish(outerTable);
        var t = FlatBufferTable.ReadRoot(buf);

        Assert.Equal(7u, t.ReadUInt32(0));
        var inner = t.ReadTable(1);
        Assert.False(inner.IsNull);
        Assert.Equal(99u, inner.ReadUInt32(0));
    }

    [Fact]
    public void EmptyBufferThrows()
    {
        Assert.Throws<VortexFormatException>(() =>
            FlatBufferTable.ReadRoot(new byte[3]));
    }
}
