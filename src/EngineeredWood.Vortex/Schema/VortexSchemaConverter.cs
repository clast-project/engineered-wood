// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Schema;

/// <summary>
/// Converts a Vortex <see cref="DType"/> tree into an Apache Arrow
/// <see cref="Apache.Arrow.Schema"/> or <see cref="Apache.Arrow.Field"/>.
/// The Arrow-facing API requires a Struct root; bare scalar/list roots that
/// Vortex permits are surfaced via lower-level access in a later phase.
/// </summary>
internal static class VortexSchemaConverter
{
    /// <summary>Convert a root <see cref="DType"/> to an Arrow Schema. Requires the root to be a Struct.
    /// When <paramref name="useLargeList"/> is true, Vortex <c>List</c> dtypes map to <see cref="LargeListType"/>
    /// (i64 offsets) instead of the default <see cref="ListType"/> (i32 offsets). Use this for files
    /// whose list columns may have more than 2³¹ total elements.</summary>
    public static Apache.Arrow.Schema ToArrowSchema(DType root, bool useLargeList = false)
    {
        if (root.Kind != DTypeKind.Struct)
            throw new VortexFormatException(
                $"Vortex file root dtype is {root.Kind}, but the Arrow-facing API requires a Struct root.");

        var s = root.AsStruct();
        var fields = new Apache.Arrow.Field[s.Names.Length];
        if (s.DTypes.Length != fields.Length)
            throw new VortexFormatException(
                $"Vortex Struct dtype has {s.Names.Length} names but {s.DTypes.Length} dtypes.");

        for (int i = 0; i < fields.Length; i++)
            fields[i] = ToField(s.FieldName(i), s.FieldDType(i), useLargeList);

        return new Apache.Arrow.Schema(fields, metadata: null);
    }

    /// <summary>Convert one Vortex dtype to an Arrow field with the given name.</summary>
    public static Apache.Arrow.Field ToField(string name, DType d, bool useLargeList = false)
    {
        var (type, nullable) = ToType(d, useLargeList);
        return new Apache.Arrow.Field(name, type, nullable);
    }

    private static (IArrowType Type, bool Nullable) ToType(DType d, bool useLargeList)
    {
        switch (d.Kind)
        {
            case DTypeKind.Null:
                // The Null dtype is the all-null type; conventionally nullable.
                return (NullType.Default, true);

            case DTypeKind.Bool:
                {
                    var b = d.AsBool();
                    return (BooleanType.Default, b.Nullable);
                }

            case DTypeKind.Primitive:
                {
                    var p = d.AsPrimitive();
                    return (PrimitiveType(p.PType), p.Nullable);
                }

            case DTypeKind.Decimal:
                {
                    var dec = d.AsDecimal();
                    // Decimal128 covers precision ≤ 38 (the limit for an i128
                    // unscaled value). Higher precision needs Decimal256.
                    IArrowType type = dec.Precision <= 38
                        ? new Decimal128Type(dec.Precision, dec.Scale)
                        : new Decimal256Type(dec.Precision, dec.Scale);
                    return (type, dec.Nullable);
                }

            case DTypeKind.Utf8:
                {
                    var u = d.AsUtf8();
                    return (StringType.Default, u.Nullable);
                }

            case DTypeKind.Binary:
                {
                    var b = d.AsBinary();
                    return (BinaryType.Default, b.Nullable);
                }

            case DTypeKind.Struct:
                {
                    var s = d.AsStruct();
                    if (s.DTypes.Length != s.Names.Length)
                        throw new VortexFormatException(
                            $"Vortex Struct has {s.Names.Length} names but {s.DTypes.Length} dtypes.");
                    var children = new Apache.Arrow.Field[s.Names.Length];
                    for (int i = 0; i < children.Length; i++)
                        children[i] = ToField(s.FieldName(i), s.FieldDType(i), useLargeList);
                    return (new StructType(children), s.Nullable);
                }

            case DTypeKind.List:
                {
                    var l = d.AsList();
                    var item = ToField("item", l.ElementType, useLargeList);
                    IArrowType type = useLargeList ? new LargeListType(item) : new ListType(item);
                    return (type, l.Nullable);
                }

            case DTypeKind.FixedSizeList:
                {
                    var f = d.AsFixedSizeList();
                    var item = ToField("item", f.ElementType, useLargeList);
                    if (f.Size > int.MaxValue)
                        throw new VortexFormatException(
                            $"FixedSizeList size {f.Size} exceeds Int32 range.");
                    return (new FixedSizeListType(item, checked((int)f.Size)), f.Nullable);
                }

            case DTypeKind.Extension:
                {
                    var ext = d.AsExtension();
                    var resolved = ResolveKnownExtension(ext);
                    if (resolved is { } known) return known;
                    // Unknown extension: fall back to the storage type so we can at
                    // least decode the underlying data.
                    return ToType(ext.StorageDType, useLargeList);
                }

            case DTypeKind.Variant:
                throw new NotSupportedException(
                    "The Vortex 'variant' dtype is not yet supported by EngineeredWood.Vortex.");

            case DTypeKind.None:
                throw new VortexFormatException(
                    "DType union has no value (kind = NONE). The file may be malformed.");

            default:
                throw new VortexFormatException(
                    $"Unknown Vortex DType kind {(int)d.Kind}.");
        }
    }

    /// <summary>
    /// Maps known Vortex extension dtypes to their Arrow equivalents:
    /// <c>vortex.timestamp</c> → <see cref="TimestampType"/>.
    /// Returns null for unknown extension ids so the caller can fall back.
    /// </summary>
    private static (IArrowType Type, bool Nullable)? ResolveKnownExtension(ExtensionDType ext)
    {
        return ext.Id switch
        {
            "vortex.timestamp" => ResolveTimestamp(ext),
            "vortex.date" => ResolveDate(ext),
            "vortex.time" => ResolveTime(ext),
            "vortex.uuid" => ResolveUuid(ext),
            _ => null,
        };
    }

    private static (IArrowType Type, bool Nullable)? ResolveUuid(ExtensionDType ext)
    {
        // Storage: FixedSizeList(U8, 16). Metadata: 0 or 1 byte (optional version).
        var storage = ext.StorageDType;
        if (storage.Kind != DTypeKind.FixedSizeList)
            throw new VortexFormatException(
                $"vortex.uuid storage_dtype is {storage.Kind}, expected FixedSizeList(U8, 16).");
        var fsl = storage.AsFixedSizeList();
        if (fsl.Size != 16)
            throw new VortexFormatException(
                $"vortex.uuid storage FixedSizeList size is {fsl.Size}, expected 16.");
        var elem = fsl.ElementType;
        if (elem.Kind != DTypeKind.Primitive || elem.AsPrimitive().PType != PType.U8)
            throw new VortexFormatException(
                $"vortex.uuid element dtype must be Primitive(U8), got {elem.Kind}.");

        return (new FixedSizeBinaryType(16), fsl.Nullable);
    }

    private static (IArrowType Type, bool Nullable)? ResolveTimestamp(ExtensionDType ext)
    {
        var storage = ext.StorageDType;
        if (storage.Kind != DTypeKind.Primitive)
            throw new VortexFormatException(
                $"vortex.timestamp storage_dtype is {storage.Kind}, expected Primitive(I64).");
        var prim = storage.AsPrimitive();
        if (prim.PType != PType.I64)
            throw new VortexFormatException(
                $"vortex.timestamp storage ptype is {prim.PType}, expected I64.");

        var (unitTag, tz) = ParseUnitAndTzMetadata(ext.Metadata, expectTz: true);
        var arrowUnit = unitTag switch
        {
            0 => Apache.Arrow.Types.TimeUnit.Nanosecond,
            1 => Apache.Arrow.Types.TimeUnit.Microsecond,
            2 => Apache.Arrow.Types.TimeUnit.Millisecond,
            3 => Apache.Arrow.Types.TimeUnit.Second,
            4 => throw new NotSupportedException(
                "vortex.timestamp with TimeUnit::Days maps to Date32, not TimestampType. " +
                "Re-encode as vortex.date if Days are needed."),
            _ => throw new VortexFormatException(
                $"vortex.timestamp metadata has unknown TimeUnit tag {unitTag}."),
        };
        return (new TimestampType(arrowUnit, tz), prim.Nullable);
    }

    private static (IArrowType Type, bool Nullable)? ResolveDate(ExtensionDType ext)
    {
        // Storage: I32 for Days, I64 for Milliseconds. Metadata: 1 byte TimeUnit tag.
        var storage = ext.StorageDType;
        if (storage.Kind != DTypeKind.Primitive)
            throw new VortexFormatException(
                $"vortex.date storage_dtype is {storage.Kind}, expected Primitive.");
        var prim = storage.AsPrimitive();
        var (unitTag, _) = ParseUnitAndTzMetadata(ext.Metadata, expectTz: false);
        return unitTag switch
        {
            4 /*Days*/ when prim.PType == PType.I32 => (Date32Type.Default, prim.Nullable),
            2 /*Ms*/   when prim.PType == PType.I64 => (Date64Type.Default, prim.Nullable),
            _ => throw new VortexFormatException(
                $"vortex.date with TimeUnit tag {unitTag} + storage {prim.PType} is not a valid combination " +
                "(Days/I32 → Date32, Milliseconds/I64 → Date64)."),
        };
    }

    private static (IArrowType Type, bool Nullable)? ResolveTime(ExtensionDType ext)
    {
        // Storage: I32 for Seconds/Milliseconds, I64 for Microseconds/Nanoseconds.
        var storage = ext.StorageDType;
        if (storage.Kind != DTypeKind.Primitive)
            throw new VortexFormatException(
                $"vortex.time storage_dtype is {storage.Kind}, expected Primitive.");
        var prim = storage.AsPrimitive();
        var (unitTag, _) = ParseUnitAndTzMetadata(ext.Metadata, expectTz: false);
        return unitTag switch
        {
            3 /*S*/  when prim.PType == PType.I32 => (new Time32Type(Apache.Arrow.Types.TimeUnit.Second), prim.Nullable),
            2 /*Ms*/ when prim.PType == PType.I32 => (new Time32Type(Apache.Arrow.Types.TimeUnit.Millisecond), prim.Nullable),
            1 /*Us*/ when prim.PType == PType.I64 => (new Time64Type(Apache.Arrow.Types.TimeUnit.Microsecond), prim.Nullable),
            0 /*Ns*/ when prim.PType == PType.I64 => (new Time64Type(Apache.Arrow.Types.TimeUnit.Nanosecond), prim.Nullable),
            _ => throw new VortexFormatException(
                $"vortex.time with TimeUnit tag {unitTag} + storage {prim.PType} is not a valid combination."),
        };
    }

    private static (byte UnitTag, string? Tz) ParseUnitAndTzMetadata(
        FlatBuffers.FlatBufferVector metaVec, bool expectTz)
    {
        if (metaVec.Length < 1)
            throw new VortexFormatException("Extension metadata is empty; need at least 1 byte for TimeUnit.");
        var meta = metaVec.RawBytes(metaVec.Length);
        var unitTag = meta[0];
        if (!expectTz) return (unitTag, null);

        if (meta.Length < 3)
            throw new VortexFormatException(
                $"Timestamp metadata is {meta.Length} bytes; need at least 3 (unit + tz_len).");
        var tzLen = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(meta.Slice(1, 2));
        string? tz = null;
        if (tzLen > 0)
        {
            if (meta.Length < 3 + tzLen)
                throw new VortexFormatException(
                    $"Timestamp metadata says tz_len={tzLen} but only {meta.Length - 3} bytes remain.");
            var tzBytes = meta.Slice(3, tzLen);
#if NET8_0_OR_GREATER
            tz = System.Text.Encoding.UTF8.GetString(tzBytes);
#else
            tz = System.Text.Encoding.UTF8.GetString(tzBytes.ToArray());
#endif
        }
        return (unitTag, tz);
    }

    private static IArrowType PrimitiveType(PType p) => p switch
    {
        PType.U8 => UInt8Type.Default,
        PType.U16 => UInt16Type.Default,
        PType.U32 => UInt32Type.Default,
        PType.U64 => UInt64Type.Default,
        PType.I8 => Int8Type.Default,
        PType.I16 => Int16Type.Default,
        PType.I32 => Int32Type.Default,
        PType.I64 => Int64Type.Default,
        PType.F16 => HalfFloatType.Default,
        PType.F32 => FloatType.Default,
        PType.F64 => DoubleType.Default,
        _ => throw new VortexFormatException($"Unknown Vortex PType {(int)p}."),
    };
}
