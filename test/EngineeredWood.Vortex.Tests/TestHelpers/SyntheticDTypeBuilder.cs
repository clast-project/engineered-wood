// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Vortex.FlatBuffers;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Tests.TestHelpers;

/// <summary>
/// Constructs synthetic Vortex <c>DType</c> FlatBuffer blobs for unit-testing
/// the schema converter without round-tripping through Rust. The layouts here
/// are valid FB-canonical encodings (4-byte alignment for tables of u8/uoffset
/// fields).
/// </summary>
internal static class SyntheticDTypeBuilder
{
    /// <summary>
    /// Build a root <c>DType</c> wrapping a <c>Primitive</c> table.
    /// Primitive inline: soffset(4) + ptype(1@4) + nullable(1@5) = 6 bytes.
    /// DType inline: soffset(4) + type uoffset(4@4) + type_type(1@8) = 9 bytes.
    /// </summary>
    public static byte[] Primitive(PType pType, bool nullable)
        => WrapDType(DTypeKind.Primitive, b =>
        {
            var pVt = b.WriteUInt16s(new ushort[] { 8, 6, 4, 5 });
            return b.StartTable(alignment: 4, inlineSize: 6)
                .EmitBool(nullable)
                .EmitU8((byte)pType)
                .EmitSOffsetTo(pVt);
        });

    /// <summary>Build a root <c>DType</c> wrapping a <c>Bool</c> table.</summary>
    public static byte[] Bool(bool nullable)
        => WrapDType(DTypeKind.Bool, b =>
        {
            // Bool vtable: vt_size=6, inline=5, slot0=4
            var bVt = b.WriteUInt16s(new ushort[] { 6, 5, 4 });
            return b.StartTable(alignment: 4, inlineSize: 5)
                .EmitBool(nullable)
                .EmitSOffsetTo(bVt);
        });

    /// <summary>Build a root <c>DType</c> wrapping a <c>Utf8</c> table.</summary>
    public static byte[] Utf8(bool nullable)
        => WrapDType(DTypeKind.Utf8, b =>
        {
            var vt = b.WriteUInt16s(new ushort[] { 6, 5, 4 });
            return b.StartTable(alignment: 4, inlineSize: 5).EmitBool(nullable).EmitSOffsetTo(vt);
        });

    /// <summary>Build a root <c>DType</c> wrapping a <c>Binary</c> table.</summary>
    public static byte[] Binary(bool nullable)
        => WrapDType(DTypeKind.Binary, b =>
        {
            var vt = b.WriteUInt16s(new ushort[] { 6, 5, 4 });
            return b.StartTable(alignment: 4, inlineSize: 5).EmitBool(nullable).EmitSOffsetTo(vt);
        });

    /// <summary>Build a root <c>DType</c> wrapping an empty <c>Null</c> table.</summary>
    public static byte[] NullType()
        => WrapDType(DTypeKind.Null, b =>
        {
            // Null table: empty body, vtable just the header. inline_size = 4 (just soffset).
            var vt = b.WriteUInt16s(new ushort[] { 4, 4 });
            return b.StartTable(alignment: 4, inlineSize: 4).EmitSOffsetTo(vt);
        });

    /// <summary>
    /// Build a root <c>DType</c> wrapping a <c>Decimal</c> table.
    /// Decimal inline: soffset(4) + precision(1@4) + scale(1@5) + nullable(1@6) = 7 bytes.
    /// </summary>
    public static byte[] Decimal(byte precision, sbyte scale, bool nullable)
        => WrapDType(DTypeKind.Decimal, b =>
        {
            var vt = b.WriteUInt16s(new ushort[] { 10, 7, 4, 5, 6 });
            return b.StartTable(alignment: 4, inlineSize: 7)
                .EmitBool(nullable)
                .EmitU8(unchecked((byte)scale))
                .EmitU8(precision)
                .EmitSOffsetTo(vt);
        });

    /// <summary>Build a root <c>DType</c> with an empty <c>Variant</c> body to exercise the rejection path.</summary>
    public static byte[] Variant(bool nullable)
        => WrapDType(DTypeKind.Variant, b =>
        {
            var vt = b.WriteUInt16s(new ushort[] { 6, 5, 4 });
            return b.StartTable(alignment: 4, inlineSize: 5).EmitBool(nullable).EmitSOffsetTo(vt);
        });

    /// <summary>
    /// Wraps an inner-table emit in a <c>DType</c> table whose union slots are
    /// 0=type_type (u8 tag), 1=type (uoffset_t to inner). Inner builder must
    /// return its inner-table ticket.
    /// </summary>
    private static byte[] WrapDType(DTypeKind kind, Func<BackwardsFlatBufferBuilder, int> emitInner)
    {
        var b = new BackwardsFlatBufferBuilder();
        var innerTable = emitInner(b);

        // DType vtable: slot0 (type_type u8) @ inline 8, slot1 (type uoffset) @ inline 4.
        // Inline_size = 9.
        var dVt = b.WriteUInt16s(new ushort[] { 8, 9, 8, 4 });
        var dTable = b.StartTable(alignment: 4, inlineSize: 9)
            .EmitU8((byte)kind)
            .EmitUOffset(innerTable)
            .EmitSOffsetTo(dVt);
        return b.Finish(dTable);
    }
}
