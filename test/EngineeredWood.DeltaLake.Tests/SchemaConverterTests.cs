// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Tests;

public class SchemaConverterTests
{
    [Fact]
    public void RoundTrip_PrimitiveTypes()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, true))
            .Field(new Field("l", Int64Type.Default, false))
            .Field(new Field("i", Int32Type.Default, true))
            .Field(new Field("sh", Int16Type.Default, true))
            .Field(new Field("b", Int8Type.Default, true))
            .Field(new Field("f", FloatType.Default, true))
            .Field(new Field("d", DoubleType.Default, true))
            .Field(new Field("bool", BooleanType.Default, true))
            .Field(new Field("bin", BinaryType.Default, true))
            .Field(new Field("dt", Date32Type.Default, true))
            .Build();

        var deltaSchema = SchemaConverter.FromArrowSchema(arrowSchema);
        var roundTripped = SchemaConverter.ToArrowSchema(deltaSchema);

        Assert.Equal(arrowSchema.FieldsList.Count, roundTripped.FieldsList.Count);
        for (int i = 0; i < arrowSchema.FieldsList.Count; i++)
        {
            Assert.Equal(arrowSchema.FieldsList[i].Name, roundTripped.FieldsList[i].Name);
            Assert.Equal(arrowSchema.FieldsList[i].IsNullable, roundTripped.FieldsList[i].IsNullable);
        }
    }

    [Fact]
    public void RoundTrip_TimestampTypes()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, (string?)"UTC"), true))
            .Field(new Field("ts_ntz", new TimestampType(TimeUnit.Microsecond, (string?)null), true))
            .Build();

        var deltaSchema = SchemaConverter.FromArrowSchema(arrowSchema);

        Assert.Equal(2, deltaSchema.Fields.Count);
        Assert.Equal("timestamp", ((PrimitiveType)deltaSchema.Fields[0].Type).TypeName);
        Assert.Equal("timestamp_ntz", ((PrimitiveType)deltaSchema.Fields[1].Type).TypeName);
    }

    [Fact]
    public void RoundTrip_DecimalType()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("amount", new Decimal128Type(10, 2), true))
            .Build();

        var deltaSchema = SchemaConverter.FromArrowSchema(arrowSchema);
        Assert.Equal("decimal(10,2)", ((PrimitiveType)deltaSchema.Fields[0].Type).TypeName);

        var roundTripped = SchemaConverter.ToArrowSchema(deltaSchema);
        var decType = (Decimal128Type)roundTripped.FieldsList[0].DataType;
        Assert.Equal(10, decType.Precision);
        Assert.Equal(2, decType.Scale);
    }

    [Fact]
    public void RoundTrip_NestedStruct()
    {
        var innerFields = new List<Field>
        {
            new Field("x", Int32Type.Default, true),
            new Field("y", Int32Type.Default, true),
        };
        var structType = new Apache.Arrow.Types.StructType(innerFields);

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("point", structType, true))
            .Build();

        var deltaSchema = SchemaConverter.FromArrowSchema(arrowSchema);
        Assert.IsType<Schema.StructType>(deltaSchema.Fields[0].Type);

        var innerStruct = (Schema.StructType)deltaSchema.Fields[0].Type;
        Assert.Equal(2, innerStruct.Fields.Count);
        Assert.Equal("x", innerStruct.Fields[0].Name);
        Assert.Equal("integer", ((PrimitiveType)innerStruct.Fields[0].Type).TypeName);
    }

    [Fact]
    public void RoundTrip_ListType()
    {
        var listType = new ListType(new Field("element", Int64Type.Default, true));

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("ids", listType, true))
            .Build();

        var deltaSchema = SchemaConverter.FromArrowSchema(arrowSchema);
        Assert.IsType<Schema.ArrayType>(deltaSchema.Fields[0].Type);

        var arrayType = (Schema.ArrayType)deltaSchema.Fields[0].Type;
        Assert.Equal("long", ((PrimitiveType)arrayType.ElementType).TypeName);
        Assert.True(arrayType.ContainsNull);
    }

    [Fact]
    public void DeltaSchemaSerializer_RoundTrip()
    {
        var deltaSchema = new Schema.StructType
        {
            Fields =
            [
                new Schema.StructField
                {
                    Name = "id", Type = new PrimitiveType { TypeName = "long" },
                    Nullable = false,
                },
                new Schema.StructField
                {
                    Name = "name", Type = new PrimitiveType { TypeName = "string" },
                    Nullable = true,
                },
                new Schema.StructField
                {
                    Name = "amount",
                    Type = new PrimitiveType { TypeName = "decimal(10,2)" },
                    Nullable = true,
                },
            ],
        };

        string json = DeltaSchemaSerializer.Serialize(deltaSchema);
        var parsed = DeltaSchemaSerializer.Parse(json);

        Assert.Equal(3, parsed.Fields.Count);
        Assert.Equal("id", parsed.Fields[0].Name);
        Assert.Equal("long", ((PrimitiveType)parsed.Fields[0].Type).TypeName);
        Assert.False(parsed.Fields[0].Nullable);
        Assert.Equal("name", parsed.Fields[1].Name);
        Assert.Equal("string", ((PrimitiveType)parsed.Fields[1].Type).TypeName);
        Assert.True(parsed.Fields[1].Nullable);
        Assert.Equal("decimal(10,2)", ((PrimitiveType)parsed.Fields[2].Type).TypeName);
    }

    // VARIANT. The load-bearing property throughout is that a variant column must never degrade into an
    // undifferentiated struct-of-binary: that would strip the parquet VARIANT annotation and mislead
    // every spec reader, silently. These pin both directions of the mapping and the degradation guard.

    [Fact]
    public void VariantDeltaType_MapsToTheArrowVariantExtension()
    {
        // A Spark 4.x / Delta 4.x table with a VARIANT column serializes its schema this way.
        const string json = """
            {"type":"struct","fields":[
              {"name":"v","type":"variant","nullable":true,"metadata":{}}
            ]}
            """;

        var arrow = SchemaConverter.ToArrowSchema(DeltaSchemaSerializer.Parse(json));

        var field = arrow.GetFieldByName("v");
        Assert.IsType<VariantType>(field.DataType);
        Assert.True(field.IsNullable);

        // Storage must be the spec's struct<metadata: binary, value: binary> — that is what the parquet
        // writer emits as the annotated group.
        var storage = Assert.IsType<Apache.Arrow.Types.StructType>(
            ((VariantType)field.DataType).StorageType);
        Assert.Equal(["metadata", "value"], storage.Fields.Select(f => f.Name));
    }

    [Fact]
    public void VariantArrowType_MapsBackToTheDeltaVariantTypeName()
    {
        // VariantType derives from ExtensionType, NOT StructType. If it ever became struct-derived
        // upstream, FromArrowType's ArrowStructType arm would silently convert it to a Delta
        // struct<metadata,value> — the exact degradation this mapping exists to prevent.
        Assert.False(
            typeof(Apache.Arrow.Types.StructType).IsAssignableFrom(typeof(VariantType)),
            "VariantType must not be StructType-derived; FromArrowType would silently map it to a struct.");

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("v", VariantType.Default, true))
            .Build();

        var delta = SchemaConverter.FromArrowSchema(arrowSchema);
        Assert.Equal("variant", ((PrimitiveType)delta.Fields.Single(f => f.Name == "v").Type).TypeName);
    }

    [Fact]
    public void VariantRoundTrips_ThroughBothConvertersAndTheSerializer()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();

        var json = DeltaSchemaSerializer.Serialize(SchemaConverter.FromArrowSchema(arrowSchema));
        var back = SchemaConverter.ToArrowSchema(DeltaSchemaSerializer.Parse(json));

        Assert.IsType<VariantType>(back.GetFieldByName("v").DataType);
        Assert.IsType<Int64Type>(back.GetFieldByName("id").DataType);
    }

    [Fact]
    public void UnknownArrowExtensionType_IsRejected_RatherThanDegradedToStorage()
    {
        // Only arrow.parquet.variant has a Delta equivalent. Any other extension must throw rather than
        // be written as its bare storage type, which would silently drop the extension's meaning.
        var unknown = new UnknownExtensionType(Apache.Arrow.Types.StringType.Default);
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", unknown, true))
            .Build();

        var ex = Assert.Throws<DeltaFormatException>(() => SchemaConverter.FromArrowSchema(arrowSchema));
        Assert.Contains("extension", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class UnknownExtensionType(IArrowType storageType)
        : Apache.Arrow.ExtensionType(storageType)
    {
        public override string Name => "test.unknown";

        public override string ExtensionMetadata => string.Empty;

        public override Apache.Arrow.ExtensionArray CreateArray(Apache.Arrow.IArrowArray storage) =>
            throw new NotSupportedException("test stub");
    }
}
