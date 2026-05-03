// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;
using EngineeredWood.Vortex.Schema;
using EngineeredWood.Vortex.Writer;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Round-trips an Arrow Schema through DTypeSerializer → DType.ReadRoot →
/// VortexSchemaConverter, asserting the result matches the input. This exercises
/// every FB layout the writer emits without going through a full file write.
/// </summary>
public class DTypeSerializerTests
{
    [Fact]
    public void RootStruct_Primitives_Roundtrip()
    {
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("a", Int32Type.Default, nullable: false),
            new Field("b", DoubleType.Default, nullable: true),
            new Field("c", BooleanType.Default, nullable: false),
            new Field("d", Int64Type.Default, nullable: false),
            new Field("e", UInt8Type.Default, nullable: true),
        }, metadata: null);

        var bytes = DTypeSerializer.SerializeSchema(schema);
        var dtype = DType.ReadRoot(bytes);
        Assert.Equal(DTypeKind.Struct, dtype.Kind);

        var roundtripped = VortexSchemaConverter.ToArrowSchema(dtype);
        Assert.Equal(schema.FieldsList.Count, roundtripped.FieldsList.Count);
        for (int i = 0; i < schema.FieldsList.Count; i++)
        {
            var orig = schema.FieldsList[i];
            var rt = roundtripped.FieldsList[i];
            Assert.Equal(orig.Name, rt.Name);
            Assert.Equal(orig.DataType.GetType(), rt.DataType.GetType());
            Assert.Equal(orig.IsNullable, rt.IsNullable);
        }
    }

    [Fact]
    public void NestedStruct_Roundtrip()
    {
        var inner = new StructType(new[]
        {
            new Field("ix", Int32Type.Default, nullable: false),
            new Field("iy", FloatType.Default, nullable: true),
        });
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("flag", BooleanType.Default, nullable: false),
            new Field("nested", inner, nullable: true),
        }, metadata: null);

        var bytes = DTypeSerializer.SerializeSchema(schema);
        var dtype = DType.ReadRoot(bytes);
        var rt = VortexSchemaConverter.ToArrowSchema(dtype);

        Assert.Equal(2, rt.FieldsList.Count);
        Assert.IsType<BooleanType>(rt.FieldsList[0].DataType);
        var nested = Assert.IsType<StructType>(rt.FieldsList[1].DataType);
        Assert.Equal(2, nested.Fields.Count);
        Assert.Equal("ix", nested.Fields[0].Name);
        Assert.IsType<Int32Type>(nested.Fields[0].DataType);
        Assert.Equal("iy", nested.Fields[1].Name);
        Assert.IsType<FloatType>(nested.Fields[1].DataType);
        Assert.True(nested.Fields[1].IsNullable);
    }
}
