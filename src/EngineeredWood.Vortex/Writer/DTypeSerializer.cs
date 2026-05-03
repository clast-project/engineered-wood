// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;
using EngineeredWood.Vortex.FlatBuffers;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Writer;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Schema.VortexSchemaConverter"/>:
/// serializes an Apache Arrow <see cref="Apache.Arrow.Schema"/> into a Vortex
/// <c>DType</c> FlatBuffer (root must be a Struct).
///
/// <para>Phase 1 scope mirrors the writer MVP: Struct root with primitive
/// (Int8..Int64, UInt8..UInt64, Float, Double) and Bool fields.
/// Other dtypes (decimal, varbin, list, FSL, extension, struct-of-struct) land
/// in subsequent phases.</para>
/// </summary>
internal static class DTypeSerializer
{
    public static byte[] SerializeSchema(Apache.Arrow.Schema schema)
    {
        var b = new BackwardsFlatBufferBuilder();
        var rootTicket = EmitStructDType(b, schema.FieldsList, nullable: false);
        return b.Finish(rootTicket);
    }

    /// <summary>Emits a root DType wrapping a Struct table. Returns the DType table ticket.</summary>
    private static int EmitStructDType(
        BackwardsFlatBufferBuilder b, IReadOnlyList<Apache.Arrow.Field> fields, bool nullable)
    {
        // 1. Emit each field's DType (recursive). Order doesn't affect ticket validity.
        var dtypeTickets = new int[fields.Count];
        for (int i = 0; i < fields.Count; i++)
            dtypeTickets[i] = EmitDType(b, fields[i]);

        // 2. Emit each field's name as a FlatBuffer string.
        var nameTickets = new int[fields.Count];
        for (int i = 0; i < fields.Count; i++)
            nameTickets[i] = b.WriteString(fields[i].Name);

        // 3. Build the dtypes and names vectors.
        var dtypesVecTicket = b.WriteUOffsetVector(dtypeTickets);
        var namesVecTicket = b.WriteUOffsetVector(nameTickets);

        // 4. Struct table:
        //    vtable: vt_size=10, inline_size=13, slot0(names)@4, slot1(dtypes)@8, slot2(nullable)@12
        //    inline: soffset(4) + names_uoffset(4@4) + dtypes_uoffset(4@8) + nullable(1@12) = 13 bytes
        var structVt = b.WriteUInt16s(new ushort[] { 10, 13, 4, 8, 12 });
        var structTicket = b.StartTable(alignment: 4, inlineSize: 13)
            .EmitBool(nullable)
            .EmitUOffset(dtypesVecTicket)
            .EmitUOffset(namesVecTicket)
            .EmitSOffsetTo(structVt);

        // 5. Wrap in a DType union-table.
        return WrapDType(b, DTypeKind.Struct, structTicket);
    }

    /// <summary>Emits one field's DType (root or inner). Returns the DType table ticket.</summary>
    private static int EmitDType(BackwardsFlatBufferBuilder b, Apache.Arrow.Field field)
    {
        bool nullable = field.IsNullable;
        // Note: Apache.Arrow's Decimal128Type/Decimal256Type derive from
        // FixedSizeBinaryType. They MUST be matched before any future
        // FixedSizeBinaryType case is added here (none today).
        return field.DataType switch
        {
            Int8Type => WrapDType(b, DTypeKind.Primitive, EmitPrimitive(b, PType.I8, nullable)),
            Int16Type => WrapDType(b, DTypeKind.Primitive, EmitPrimitive(b, PType.I16, nullable)),
            Int32Type => WrapDType(b, DTypeKind.Primitive, EmitPrimitive(b, PType.I32, nullable)),
            Int64Type => WrapDType(b, DTypeKind.Primitive, EmitPrimitive(b, PType.I64, nullable)),
            UInt8Type => WrapDType(b, DTypeKind.Primitive, EmitPrimitive(b, PType.U8, nullable)),
            UInt16Type => WrapDType(b, DTypeKind.Primitive, EmitPrimitive(b, PType.U16, nullable)),
            UInt32Type => WrapDType(b, DTypeKind.Primitive, EmitPrimitive(b, PType.U32, nullable)),
            UInt64Type => WrapDType(b, DTypeKind.Primitive, EmitPrimitive(b, PType.U64, nullable)),
            FloatType => WrapDType(b, DTypeKind.Primitive, EmitPrimitive(b, PType.F32, nullable)),
            DoubleType => WrapDType(b, DTypeKind.Primitive, EmitPrimitive(b, PType.F64, nullable)),
            BooleanType => WrapDType(b, DTypeKind.Bool, EmitBool(b, nullable)),
            Decimal128Type d128 => WrapDType(b, DTypeKind.Decimal, EmitDecimalDType(b, d128.Precision, d128.Scale, nullable)),
            Decimal256Type d256 => WrapDType(b, DTypeKind.Decimal, EmitDecimalDType(b, d256.Precision, d256.Scale, nullable)),
            StringType => WrapDType(b, DTypeKind.Utf8, EmitNullableTaggedTable(b, nullable)),
            BinaryType => WrapDType(b, DTypeKind.Binary, EmitNullableTaggedTable(b, nullable)),
            StructType st => EmitStructDType(b, st.Fields, nullable),
            ListType lst => WrapDType(b, DTypeKind.List, EmitListDType(b, lst.ValueField, nullable)),
            FixedSizeListType fsl => WrapDType(b, DTypeKind.FixedSizeList, EmitFixedSizeListDType(b, fsl.ValueField, fsl.ListSize, nullable)),
            _ => throw new NotSupportedException(
                $"Vortex writer Phase 1 doesn't yet support Arrow type {field.DataType} (field '{field.Name}')."),
        };
    }

    /// <summary>
    /// Decimal inner table: { precision: u8 (slot 0), scale: i8 (slot 1), nullable: bool (slot 2) }.
    /// vt_size=10, inline=7 (soffset(4) + precision(1@4) + scale(1@5) + nullable(1@6)).
    /// </summary>
    private static int EmitDecimalDType(BackwardsFlatBufferBuilder b, int precision, int scale, bool nullable)
    {
        if (precision < 1 || precision > 255)
            throw new ArgumentOutOfRangeException(nameof(precision), precision, "Decimal precision must fit in u8.");
        if (scale < sbyte.MinValue || scale > sbyte.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(scale), scale, "Decimal scale must fit in i8.");
        var vt = b.WriteUInt16s(new ushort[] { 10, 7, 4, 5, 6 });
        return b.StartTable(alignment: 4, inlineSize: 7)
            .EmitBool(nullable)
            .EmitU8((byte)(sbyte)scale)  // sbyte → byte preserves bit pattern for FB inline storage
            .EmitU8((byte)precision)
            .EmitSOffsetTo(vt);
    }

    /// <summary>
    /// Primitive table: vtable size 8, inline size 6 (soffset(4) + ptype(1@4) + nullable(1@5)).
    /// </summary>
    private static int EmitPrimitive(BackwardsFlatBufferBuilder b, PType pType, bool nullable)
    {
        var vt = b.WriteUInt16s(new ushort[] { 8, 6, 4, 5 });
        return b.StartTable(alignment: 4, inlineSize: 6)
            .EmitBool(nullable)
            .EmitU8((byte)pType)
            .EmitSOffsetTo(vt);
    }

    /// <summary>Bool table: vtable size 6, inline size 5 (soffset(4) + nullable(1@4)).</summary>
    private static int EmitBool(BackwardsFlatBufferBuilder b, bool nullable)
    {
        var vt = b.WriteUInt16s(new ushort[] { 6, 5, 4 });
        return b.StartTable(alignment: 4, inlineSize: 5).EmitBool(nullable).EmitSOffsetTo(vt);
    }

    /// <summary>
    /// Inner table for dtypes whose only field is <c>nullable: bool</c> at slot 0.
    /// Used by Utf8 and Binary, which have identical wire shapes.
    /// vtable size 6, inline size 5.
    /// </summary>
    private static int EmitNullableTaggedTable(BackwardsFlatBufferBuilder b, bool nullable) =>
        EmitBool(b, nullable);

    /// <summary>
    /// List inner table: { element_type: DType (slot 0), nullable: bool (slot 1) }.
    /// vt_size=8, inline=9 (soffset(4) + element_uoff(4@4) + nullable(1@8)).
    /// </summary>
    private static int EmitListDType(BackwardsFlatBufferBuilder b, Apache.Arrow.Field elementField, bool nullable)
    {
        var elementTicket = EmitDType(b, elementField);
        var vt = b.WriteUInt16s(new ushort[] { 8, 9, 4, 8 });
        return b.StartTable(alignment: 4, inlineSize: 9)
            .EmitBool(nullable)
            .EmitUOffset(elementTicket)
            .EmitSOffsetTo(vt);
    }

    /// <summary>
    /// FixedSizeList inner table: { element_type (slot 0), size: u32 (slot 1), nullable (slot 2) }.
    /// vt_size=10, inline=13 (soffset(4) + element_uoff(4@4) + size(4@8) + nullable(1@12)).
    /// </summary>
    private static int EmitFixedSizeListDType(
        BackwardsFlatBufferBuilder b, Apache.Arrow.Field elementField, int listSize, bool nullable)
    {
        var elementTicket = EmitDType(b, elementField);
        var vt = b.WriteUInt16s(new ushort[] { 10, 13, 4, 8, 12 });
        return b.StartTable(alignment: 4, inlineSize: 13)
            .EmitBool(nullable)
            .EmitU32(checked((uint)listSize))
            .EmitUOffset(elementTicket)
            .EmitSOffsetTo(vt);
    }

    /// <summary>
    /// DType union-table: vtable size 8, inline size 9 (soffset(4) + type_uoffset(4@4) + type_type(1@8)).
    /// Slot 0 is the type_type tag (u8); slot 1 is the uoffset to the inner variant table.
    /// </summary>
    private static int WrapDType(BackwardsFlatBufferBuilder b, DTypeKind kind, int innerTicket)
    {
        var dVt = b.WriteUInt16s(new ushort[] { 8, 9, 8, 4 });
        return b.StartTable(alignment: 4, inlineSize: 9)
            .EmitU8((byte)kind)
            .EmitUOffset(innerTicket)
            .EmitSOffsetTo(dVt);
    }
}
