// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Vortex.FlatBuffers;
using EngineeredWood.Vortex.Format;
using EngineeredWood.Vortex.Tests.TestData;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Ground-truth tests against a real <c>.vortex</c> file produced by the Rust
/// fixture-generator crate at <c>test/EngineeredWood.Vortex.Tests/Rust</c>.
/// These exercise the FlatBuffers reader and the typed accessors against bytes
/// emitted by the canonical Vortex implementation, catching endian / offset /
/// vtable bugs that mutual self-tests can't.
/// </summary>
public class VortexFixtureTests
{
    private static readonly byte[] VtxfMagic = "VTXF"u8.ToArray();

    [Fact]
    public void StructIntFixture_HasMagicBytesAtBothEnds()
    {
        var bytes = File.ReadAllBytes(TestDataPath.Resolve("struct_int_3rows.vortex"));

        Assert.True(bytes.Length >= 16, $"file too small ({bytes.Length} bytes)");
        Assert.Equal(VtxfMagic, bytes.AsSpan(0, 4).ToArray());
        Assert.Equal(VtxfMagic, bytes.AsSpan(bytes.Length - 4, 4).ToArray());
    }

    [Fact]
    public void StructIntFixture_PostscriptParses()
    {
        var bytes = File.ReadAllBytes(TestDataPath.Resolve("struct_int_3rows.vortex"));

        // EndOfFile struct (8 bytes at the very tail):
        //   version: u16   (LE)
        //   postscript_len: u16 (LE)
        //   magic: 4 bytes 'VTXF'
        var version = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(bytes.Length - 8, 2));
        var postscriptLen = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(bytes.Length - 6, 2));

        Assert.Equal(1, version);
        Assert.True(postscriptLen > 0 && postscriptLen <= 65528,
            $"postscript_len out of range: {postscriptLen}");

        var postscriptStart = bytes.Length - 8 - postscriptLen;
        Assert.True(postscriptStart > 4,
            $"postscript would start at {postscriptStart}, before the leading magic");

        var postscriptBytes = bytes.AsSpan(postscriptStart, postscriptLen);
        var postscript = Postscript.ReadRoot(postscriptBytes);

        // Layout and Footer pointers are required by the spec.
        Assert.True(postscript.Layout.IsPresent, "Postscript.layout missing");
        Assert.True(postscript.Footer.IsPresent, "Postscript.footer missing");

        var layoutSeg = postscript.Layout;
        var footerSeg = postscript.Footer;

        // Sanity: each segment must lie inside the file (between leading magic and EndOfFile struct).
        Assert.InRange((long)layoutSeg.Offset, 4L, bytes.Length - 8);
        Assert.InRange((long)footerSeg.Offset, 4L, bytes.Length - 8);
        Assert.True(layoutSeg.Length > 0, "layout segment empty");
        Assert.True(footerSeg.Length > 0, "footer segment empty");
        Assert.True((long)layoutSeg.Offset + layoutSeg.Length <= bytes.Length - 8,
            "layout segment extends past file");
        Assert.True((long)footerSeg.Offset + footerSeg.Length <= bytes.Length - 8,
            "footer segment extends past file");
    }

    [Fact]
    public void StructIntFixture_FooterParses_AndHasExpectedSpecs()
    {
        var bytes = File.ReadAllBytes(TestDataPath.Resolve("struct_int_3rows.vortex"));
        var postscriptLen = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(bytes.Length - 6, 2));
        var postscriptStart = bytes.Length - 8 - postscriptLen;
        var postscript = Postscript.ReadRoot(bytes.AsSpan(postscriptStart, postscriptLen));

        var footerSeg = postscript.Footer;
        var footerBytes = bytes.AsSpan(checked((int)footerSeg.Offset), checked((int)footerSeg.Length));
        var footer = Footer.ReadRoot(footerBytes);

        // A non-empty file must reference at least one segment, and to interpret
        // it we need at least one layout spec and at least one array spec.
        Assert.True(footer.SegmentSpecs.Length > 0, "footer has no segment_specs");
        Assert.True(footer.LayoutSpecs.Length > 0, "footer has no layout_specs");
        Assert.True(footer.ArraySpecs.Length > 0, "footer has no array_specs");

        // Every layout/array spec id is non-empty and namespaced (vortex.* or similar).
        for (int i = 0; i < footer.LayoutSpecs.Length; i++)
            Assert.False(string.IsNullOrEmpty(footer.LayoutSpecId(i)),
                $"layout_specs[{i}].id is empty");
        for (int i = 0; i < footer.ArraySpecs.Length; i++)
            Assert.False(string.IsNullOrEmpty(footer.ArraySpecId(i)),
                $"array_specs[{i}].id is empty");

        // Every segment_specs entry should lie inside the file.
        for (int i = 0; i < footer.SegmentSpecs.Length; i++)
        {
            var seg = footer.SegmentSpec(i);
            Assert.InRange((long)seg.Offset, 0L, bytes.Length);
            Assert.True((long)seg.Offset + seg.Length <= bytes.Length,
                $"segment_specs[{i}] extends past file");
        }
    }

    [Fact]
    public void StructIntFixture_DTypeIsStructOfPrimitive()
    {
        var bytes = File.ReadAllBytes(TestDataPath.Resolve("struct_int_3rows.vortex"));
        var postscriptLen = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(bytes.Length - 6, 2));
        var postscriptStart = bytes.Length - 8 - postscriptLen;
        var postscript = Postscript.ReadRoot(bytes.AsSpan(postscriptStart, postscriptLen));

        // The DType segment is optional in general, but our fixture has one.
        Assert.True(postscript.DType.IsPresent, "Postscript.dtype missing");
        var dtypeSeg = postscript.DType;
        var dtypeBytes = bytes.AsSpan(checked((int)dtypeSeg.Offset), checked((int)dtypeSeg.Length));
        var dtype = DType.ReadRoot(dtypeBytes);

        Assert.Equal(DTypeKind.Struct, dtype.Kind);
        var s = dtype.AsStruct();
        Assert.Equal(1, s.Names.Length);
        Assert.Equal(1, s.DTypes.Length);
        Assert.Equal("a", s.FieldName(0));

        var fieldDType = s.FieldDType(0);
        Assert.Equal(DTypeKind.Primitive, fieldDType.Kind);
        var prim = fieldDType.AsPrimitive();
        Assert.Equal(PType.I32, prim.PType);
        Assert.False(prim.Nullable, "Validity::NonNullable should map to nullable=false");
    }
}
