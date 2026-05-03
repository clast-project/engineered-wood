// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Vortex.Layouts;
using EngineeredWood.Vortex.Writer;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Round-trips a struct-of-flat layout through LayoutSerializer →
/// VortexLayoutParser, verifying the resulting tree matches the input shape.
/// </summary>
public class LayoutSerializerTests
{
    [Fact]
    public void StructOfFlat_Roundtrip()
    {
        // layout_specs registry: [vortex.flat=0, vortex.struct=1]
        var specs = new[] { VortexLayoutEncodings.Flat, VortexLayoutEncodings.Struct };

        // Three columns, each with its own segment.
        var bytes = LayoutSerializer.SerializeStructFlat(
            structEncodingIdx: 1,
            flatEncodingIdx: 0,
            totalRows: 100,
            perColumnSegmentIdx: new uint[] { 7, 3, 11 });

        var root = VortexLayoutParser.Parse(bytes, specs);
        Assert.Equal(VortexLayoutEncodings.Struct, root.EncodingId);
        Assert.Equal(100UL, root.RowCount);
        Assert.Equal(3, root.Children.Count);
        Assert.Empty(root.SegmentRefs);

        Assert.Equal(VortexLayoutEncodings.Flat, root.Children[0].EncodingId);
        Assert.Equal(100UL, root.Children[0].RowCount);
        Assert.Equal(new uint[] { 7 }, root.Children[0].SegmentRefs);

        Assert.Equal(new uint[] { 3 }, root.Children[1].SegmentRefs);
        Assert.Equal(new uint[] { 11 }, root.Children[2].SegmentRefs);
    }
}
