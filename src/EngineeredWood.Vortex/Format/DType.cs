// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Vortex.FlatBuffers;

namespace EngineeredWood.Vortex.Format;

/// <summary>
/// Numeric primitive type tags used inside <see cref="Primitive"/> dtypes.
/// Mirrors the Vortex <c>PType</c> FlatBuffers enum (dtype.fbs).
/// </summary>
internal enum PType : byte
{
    U8 = 0, U16 = 1, U32 = 2, U64 = 3,
    I8 = 4, I16 = 5, I32 = 6, I64 = 7,
    F16 = 8, F32 = 9, F64 = 10,
}

/// <summary>
/// Discriminator for the <see cref="DType"/> union (dtype.fbs).
/// Index 0 is the FlatBuffers <c>NONE</c> sentinel.
/// </summary>
internal enum DTypeKind : byte
{
    None = 0,
    Null = 1,
    Bool = 2,
    Primitive = 3,
    Decimal = 4,
    Utf8 = 5,
    Binary = 6,
    Struct = 7,
    List = 8,
    Extension = 9,
    FixedSizeList = 10,
    Variant = 11,
}

/// <summary>
/// Reader for the root <c>DType</c> FlatBuffers table. The single union field
/// occupies two vtable slots: 0=type_type (u8 tag), 1=type (uoffset_t to the
/// variant table).
/// </summary>
internal readonly ref struct DType
{
    private readonly FlatBufferTable _table;

    public DType(FlatBufferTable table) { _table = table; }

    public static DType ReadRoot(ReadOnlySpan<byte> buf) =>
        new(FlatBufferTable.ReadRoot(buf));

    public DTypeKind Kind => (DTypeKind)_table.ReadByte(0);

    private FlatBufferTable Variant => _table.ReadTable(1);

    public BoolDType AsBool() => new(Variant);
    public PrimitiveDType AsPrimitive() => new(Variant);
    public DecimalDType AsDecimal() => new(Variant);
    public Utf8DType AsUtf8() => new(Variant);
    public BinaryDType AsBinary() => new(Variant);
    public StructDType AsStruct() => new(Variant);
    public ListDType AsList() => new(Variant);
    public FixedSizeListDType AsFixedSizeList() => new(Variant);
    public ExtensionDType AsExtension() => new(Variant);
    public VariantDType AsVariant() => new(Variant);
}

/// <summary>Empty Null variant. The Null dtype is implicitly nullable.</summary>
internal readonly ref struct NullDType { }

internal readonly ref struct BoolDType
{
    private readonly FlatBufferTable _t;
    public BoolDType(FlatBufferTable t) { _t = t; }
    public bool Nullable => _t.ReadBool(0);
}

internal readonly ref struct PrimitiveDType
{
    private readonly FlatBufferTable _t;
    public PrimitiveDType(FlatBufferTable t) { _t = t; }
    public PType PType => (PType)_t.ReadByte(0);
    public bool Nullable => _t.ReadBool(1);
}

internal readonly ref struct DecimalDType
{
    private readonly FlatBufferTable _t;
    public DecimalDType(FlatBufferTable t) { _t = t; }
    public byte Precision => _t.ReadByte(0);
    public sbyte Scale => _t.ReadSByte(1);
    public bool Nullable => _t.ReadBool(2);
}

internal readonly ref struct Utf8DType
{
    private readonly FlatBufferTable _t;
    public Utf8DType(FlatBufferTable t) { _t = t; }
    public bool Nullable => _t.ReadBool(0);
}

internal readonly ref struct BinaryDType
{
    private readonly FlatBufferTable _t;
    public BinaryDType(FlatBufferTable t) { _t = t; }
    public bool Nullable => _t.ReadBool(0);
}

internal readonly ref struct StructDType
{
    private readonly FlatBufferTable _t;
    public StructDType(FlatBufferTable t) { _t = t; }

    public FlatBufferVector Names => _t.ReadVector(0);
    public FlatBufferVector DTypes => _t.ReadVector(1);
    public bool Nullable => _t.ReadBool(2);

    public string FieldName(int i) => Names.String(i);
    public DType FieldDType(int i) => new(DTypes.Table(i));
}

internal readonly ref struct ListDType
{
    private readonly FlatBufferTable _t;
    public ListDType(FlatBufferTable t) { _t = t; }
    public DType ElementType => new(_t.ReadTable(0));
    public bool Nullable => _t.ReadBool(1);
}

internal readonly ref struct FixedSizeListDType
{
    private readonly FlatBufferTable _t;
    public FixedSizeListDType(FlatBufferTable t) { _t = t; }
    public DType ElementType => new(_t.ReadTable(0));
    public uint Size => _t.ReadUInt32(1);
    public bool Nullable => _t.ReadBool(2);
}

internal readonly ref struct ExtensionDType
{
    private readonly FlatBufferTable _t;
    public ExtensionDType(FlatBufferTable t) { _t = t; }
    public string? Id => _t.ReadString(0);
    public DType StorageDType => new(_t.ReadTable(1));
    public FlatBufferVector Metadata => _t.ReadVector(2);
}

internal readonly ref struct VariantDType
{
    private readonly FlatBufferTable _t;
    public VariantDType(FlatBufferTable t) { _t = t; }
    public bool Nullable => _t.ReadBool(0);
}
