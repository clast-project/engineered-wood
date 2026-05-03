// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;
using EngineeredWood.Vortex.Schema;
using EngineeredWood.Vortex.Tests.TestHelpers;

namespace EngineeredWood.Vortex.Tests;

public class VortexSchemaConverterTests
{
    [Theory]
    [InlineData((byte)0, typeof(UInt8Type))]    // U8
    [InlineData((byte)1, typeof(UInt16Type))]   // U16
    [InlineData((byte)2, typeof(UInt32Type))]   // U32
    [InlineData((byte)3, typeof(UInt64Type))]   // U64
    [InlineData((byte)4, typeof(Int8Type))]     // I8
    [InlineData((byte)5, typeof(Int16Type))]    // I16
    [InlineData((byte)6, typeof(Int32Type))]    // I32
    [InlineData((byte)7, typeof(Int64Type))]    // I64
    [InlineData((byte)8, typeof(HalfFloatType))] // F16
    [InlineData((byte)9, typeof(FloatType))]    // F32
    [InlineData((byte)10, typeof(DoubleType))]  // F64
    public void Primitive_MapsToArrowType(byte pTypeByte, Type expected)
    {
        var pType = (PType)pTypeByte;
        foreach (var nullable in new[] { false, true })
        {
            var bytes = SyntheticDTypeBuilder.Primitive(pType, nullable);
            var dtype = DType.ReadRoot(bytes);

            Assert.Equal(DTypeKind.Primitive, dtype.Kind);
            var p = dtype.AsPrimitive();
            Assert.Equal(pType, p.PType);
            Assert.Equal(nullable, p.Nullable);

            var field = VortexSchemaConverter.ToField($"x_{pType}", dtype);
            Assert.Equal(expected, field.DataType.GetType());
            Assert.Equal(nullable, field.IsNullable);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Bool_MapsToBooleanType(bool nullable)
    {
        var bytes = SyntheticDTypeBuilder.Bool(nullable);
        var dtype = DType.ReadRoot(bytes);
        Assert.Equal(DTypeKind.Bool, dtype.Kind);

        var field = VortexSchemaConverter.ToField("b", dtype);
        Assert.IsType<BooleanType>(field.DataType);
        Assert.Equal(nullable, field.IsNullable);
    }

    [Fact]
    public void Utf8_MapsToStringType()
    {
        var dtype = DType.ReadRoot(SyntheticDTypeBuilder.Utf8(nullable: true));
        var field = VortexSchemaConverter.ToField("s", dtype);
        Assert.IsType<StringType>(field.DataType);
        Assert.True(field.IsNullable);
    }

    [Fact]
    public void Binary_MapsToBinaryType()
    {
        var dtype = DType.ReadRoot(SyntheticDTypeBuilder.Binary(nullable: false));
        var field = VortexSchemaConverter.ToField("b", dtype);
        Assert.IsType<BinaryType>(field.DataType);
        Assert.False(field.IsNullable);
    }

    [Fact]
    public void Null_MapsToNullType()
    {
        var dtype = DType.ReadRoot(SyntheticDTypeBuilder.NullType());
        var field = VortexSchemaConverter.ToField("n", dtype);
        Assert.IsType<NullType>(field.DataType);
        Assert.True(field.IsNullable);
    }

    [Fact]
    public void Decimal_MapsToDecimal128_PreservingPrecisionAndScale()
    {
        var dtype = DType.ReadRoot(SyntheticDTypeBuilder.Decimal(precision: 18, scale: 4, nullable: true));
        var field = VortexSchemaConverter.ToField("d", dtype);

        var dec = Assert.IsType<Decimal128Type>(field.DataType);
        Assert.Equal(18, dec.Precision);
        Assert.Equal(4, dec.Scale);
        Assert.True(field.IsNullable);
    }

    [Fact]
    public void Variant_Throws()
    {
        // ref struct DType can't be captured in a lambda, so call directly with try/catch.
        var bytes = SyntheticDTypeBuilder.Variant(nullable: true);
        try
        {
            _ = VortexSchemaConverter.ToField("v", DType.ReadRoot(bytes));
            Assert.Fail("Expected NotSupportedException for Variant dtype.");
        }
        catch (NotSupportedException) { }
    }

    [Fact]
    public void NonStructRoot_RejectedByToArrowSchema()
    {
        var bytes = SyntheticDTypeBuilder.Primitive(PType.I32, nullable: false);
        try
        {
            _ = VortexSchemaConverter.ToArrowSchema(DType.ReadRoot(bytes));
            Assert.Fail("Expected VortexFormatException for non-struct root.");
        }
        catch (VortexFormatException ex)
        {
            Assert.Contains("Struct root", ex.Message);
        }
    }
}
