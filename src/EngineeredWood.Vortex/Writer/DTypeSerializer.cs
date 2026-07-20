// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
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
            HalfFloatType => WrapDType(b, DTypeKind.Primitive, EmitPrimitive(b, PType.F16, nullable)),
            BooleanType => WrapDType(b, DTypeKind.Bool, EmitBool(b, nullable)),
            Decimal128Type d128 => WrapDType(b, DTypeKind.Decimal, EmitDecimalDType(b, d128.Precision, d128.Scale, nullable)),
            Decimal256Type d256 => WrapDType(b, DTypeKind.Decimal, EmitDecimalDType(b, d256.Precision, d256.Scale, nullable)),
            StringType => WrapDType(b, DTypeKind.Utf8, EmitNullableTaggedTable(b, nullable)),
            BinaryType => WrapDType(b, DTypeKind.Binary, EmitNullableTaggedTable(b, nullable)),
            StructType st => EmitStructDType(b, st.Fields, nullable),
            ListType lst => WrapDType(b, DTypeKind.List, EmitListDType(b, lst.ValueField, nullable)),
            FixedSizeListType fsl => WrapDType(b, DTypeKind.FixedSizeList, EmitFixedSizeListDType(b, fsl.ValueField, fsl.ListSize, nullable)),
            TimestampType ts => WrapDType(b, DTypeKind.Extension, EmitTimestampExtension(b, ts, nullable)),
            Date32Type => WrapDType(b, DTypeKind.Extension, EmitDateExtension(b, isDate64: false, nullable)),
            Date64Type => WrapDType(b, DTypeKind.Extension, EmitDateExtension(b, isDate64: true, nullable)),
            Time32Type t32 => WrapDType(b, DTypeKind.Extension, EmitTimeExtension(b, t32.Unit, isWide: false, nullable)),
            Time64Type t64 => WrapDType(b, DTypeKind.Extension, EmitTimeExtension(b, t64.Unit, isWide: true, nullable)),
            FixedSizeBinaryType fsb when fsb.ByteWidth == 16 =>
                WrapDType(b, DTypeKind.Extension, EmitUuidExtension(b, nullable)),
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
    /// Emits a <c>vortex.timestamp</c> Extension DType wrapping I64 storage.
    /// Storage carries the column's nullability (the Extension layer doesn't
    /// have its own nullable slot). Metadata layout per upstream
    /// <c>vortex-array/src/extension/datetime/timestamp.rs</c>:
    /// <c>[unit:u8, tz_len:u16 LE, tz_bytes (UTF-8)]</c>; unit tag is
    /// 0=Ns, 1=Us, 2=Ms, 3=S (Days isn't representable as Arrow TimestampType).
    /// </summary>
    private static int EmitTimestampExtension(
        BackwardsFlatBufferBuilder b, TimestampType ts, bool nullable)
    {
        // Storage = Primitive(I64) wrapped as a DType.
        var storageInner = EmitPrimitive(b, PType.I64, nullable);
        var storageTicket = WrapDType(b, DTypeKind.Primitive, storageInner);

        // Metadata: unit byte + tz_len (u16 LE) + UTF-8 tz bytes.
        byte unitTag = ts.Unit switch
        {
            Apache.Arrow.Types.TimeUnit.Nanosecond => 0,
            Apache.Arrow.Types.TimeUnit.Microsecond => 1,
            Apache.Arrow.Types.TimeUnit.Millisecond => 2,
            Apache.Arrow.Types.TimeUnit.Second => 3,
            _ => throw new NotSupportedException(
                $"vortex.timestamp doesn't support TimeUnit {ts.Unit} (Days maps to vortex.date, not Timestamp)."),
        };
        var tzBytes = ts.Timezone is null
            ? System.Array.Empty<byte>()
            : System.Text.Encoding.UTF8.GetBytes(ts.Timezone);
        if (tzBytes.Length > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(
                nameof(ts), ts.Timezone, $"Timezone string is {tzBytes.Length} bytes; must fit in u16.");
        var metadata = new byte[3 + tzBytes.Length];
        metadata[0] = unitTag;
        BinaryPrimitives.WriteUInt16LittleEndian(metadata.AsSpan(1, 2), (ushort)tzBytes.Length);
        if (tzBytes.Length > 0) Buffer.BlockCopy(tzBytes, 0, metadata, 3, tzBytes.Length);
        var metadataTicket = b.WriteByteVector(metadata);

        var idTicket = b.WriteString("vortex.timestamp");
        return EmitExtensionTable(b, idTicket, storageTicket, metadataTicket);
    }

    /// <summary>
    /// Emits a <c>vortex.date</c> Extension DType. Storage is Primitive(I32) +
    /// unit tag 4 (Days) for Date32, or Primitive(I64) + unit tag 2 (Ms) for
    /// Date64 — matching the reader's <c>ResolveDate</c>. Metadata is a
    /// single unit byte (no tz). Mirrors upstream
    /// <c>vortex-array/src/extension/datetime/date.rs</c>.
    /// </summary>
    private static int EmitDateExtension(
        BackwardsFlatBufferBuilder b, bool isDate64, bool nullable)
    {
        var (ptype, unitTag) = isDate64 ? (PType.I64, (byte)2) : (PType.I32, (byte)4);
        var storageInner = EmitPrimitive(b, ptype, nullable);
        var storageTicket = WrapDType(b, DTypeKind.Primitive, storageInner);
        var metadataTicket = b.WriteByteVector(new[] { unitTag });
        var idTicket = b.WriteString("vortex.date");
        return EmitExtensionTable(b, idTicket, storageTicket, metadataTicket);
    }

    /// <summary>
    /// Emits a <c>vortex.time</c> Extension DType. Storage is Primitive(I32)
    /// for Time32 (Seconds tag 3 / Milliseconds tag 2) and Primitive(I64)
    /// for Time64 (Microseconds tag 1 / Nanoseconds tag 0) — matching the
    /// reader's <c>ResolveTime</c>. Metadata is a single unit byte. Mirrors
    /// upstream <c>vortex-array/src/extension/datetime/time.rs</c>.
    /// </summary>
    private static int EmitTimeExtension(
        BackwardsFlatBufferBuilder b, Apache.Arrow.Types.TimeUnit unit, bool isWide, bool nullable)
    {
        // Time32 supports {S, Ms}; Time64 supports {Us, Ns}. The Apache.Arrow
        // ctor enforces that, so we don't need to re-validate here — but
        // tagging is unit-driven, not width-driven.
        byte unitTag = unit switch
        {
            Apache.Arrow.Types.TimeUnit.Second => 3,
            Apache.Arrow.Types.TimeUnit.Millisecond => 2,
            Apache.Arrow.Types.TimeUnit.Microsecond => 1,
            Apache.Arrow.Types.TimeUnit.Nanosecond => 0,
            _ => throw new NotSupportedException(
                $"vortex.time doesn't support TimeUnit {unit}."),
        };
        var ptype = isWide ? PType.I64 : PType.I32;
        var storageInner = EmitPrimitive(b, ptype, nullable);
        var storageTicket = WrapDType(b, DTypeKind.Primitive, storageInner);
        var metadataTicket = b.WriteByteVector(new[] { unitTag });
        var idTicket = b.WriteString("vortex.time");
        return EmitExtensionTable(b, idTicket, storageTicket, metadataTicket);
    }

    /// <summary>
    /// Emits a <c>vortex.uuid</c> Extension DType wrapping
    /// <c>FixedSizeList(U8, 16)</c> storage. Metadata is empty (we don't
    /// record an explicit UUID version; the reader's
    /// <c>ResolveUuid</c> accepts that). Mirrors upstream
    /// <c>vortex-array/src/extension/uuid/{metadata,vtable}.rs</c>.
    /// </summary>
    private static int EmitUuidExtension(BackwardsFlatBufferBuilder b, bool nullable)
    {
        // Inner element field: Primitive(U8, non-nullable).
        var elemPrimitive = EmitPrimitive(b, PType.U8, nullable: false);
        var elemDType = WrapDType(b, DTypeKind.Primitive, elemPrimitive);
        // Storage: FixedSizeList(elem=U8, size=16, nullable). Reuses the
        // generic FSL DType emitter — no UUID-specific child shape.
        var storageInner = EmitFixedSizeListInner(b, elemDType, listSize: 16, nullable);
        var storageTicket = WrapDType(b, DTypeKind.FixedSizeList, storageInner);
        var metadataTicket = b.WriteByteVector(System.Array.Empty<byte>());
        var idTicket = b.WriteString("vortex.uuid");
        return EmitExtensionTable(b, idTicket, storageTicket, metadataTicket);
    }

    /// <summary>
    /// Emits a FixedSizeList inner-table given a pre-emitted element-DType
    /// ticket. Mirrors <see cref="EmitFixedSizeListDType"/> but takes the
    /// element ticket directly so callers can drive list shape independently
    /// of an Apache.Arrow Field.
    /// </summary>
    private static int EmitFixedSizeListInner(
        BackwardsFlatBufferBuilder b, int elementDTypeTicket, int listSize, bool nullable)
    {
        var vt = b.WriteUInt16s(new ushort[] { 10, 13, 4, 8, 12 });
        return b.StartTable(alignment: 4, inlineSize: 13)
            .EmitBool(nullable)
            .EmitU32(checked((uint)listSize))
            .EmitUOffset(elementDTypeTicket)
            .EmitSOffsetTo(vt);
    }

    /// <summary>
    /// Common Extension table emit for <c>vortex.date</c> / <c>vortex.time</c> /
    /// <c>vortex.timestamp</c> / <c>vortex.uuid</c>. Slots:
    /// 0=id (string), 1=storage_dtype (DType), 2=metadata (vec&lt;u8&gt;).
    /// vt_size = 4 + 3*2 = 10. inline = 16
    /// (soffset(4) + id_uoff(4@4) + storage_uoff(4@8) + metadata_uoff(4@12)).
    /// </summary>
    private static int EmitExtensionTable(
        BackwardsFlatBufferBuilder b, int idTicket, int storageTicket, int metadataTicket)
    {
        var vt = b.WriteUInt16s(new ushort[] { 10, 16, 4, 8, 12 });
        return b.StartTable(alignment: 4, inlineSize: 16)
            .EmitUOffset(metadataTicket)
            .EmitUOffset(storageTicket)
            .EmitUOffset(idTicket)
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
