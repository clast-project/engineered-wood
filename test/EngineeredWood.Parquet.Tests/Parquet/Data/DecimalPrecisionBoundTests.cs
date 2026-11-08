// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// #763: SchemaAnnotations drops a DECIMAL whose scale is negative or above its precision, but
/// BYTE_ARRAY and a wide FIXED_LEN_BYTE_ARRAY put no bound on the precision itself, so a footer could
/// still declare decimal(50000000, 50000000) and hand that scale to Arrow. Decimal256 holds 76 digits;
/// anything wider is refused, as arrow C++ refuses it.
/// </summary>
public class DecimalPrecisionBoundTests
{
    private static ColumnDescriptor Describe(PhysicalType physicalType, int precision, int scale, int? typeLength = null)
    {
        var element = new SchemaElement
        {
            Name = "d",
            Type = physicalType,
            TypeLength = typeLength,
            RepetitionType = FieldRepetitionType.Optional,
            LogicalType = new LogicalType.DecimalType(scale, precision),
        };

        return new ColumnDescriptor
        {
            Path = ["d"],
            PhysicalType = physicalType,
            TypeLength = typeLength,
            MaxDefinitionLevel = 1,
            MaxRepetitionLevel = 0,
            SchemaElement = element,
            SchemaNode = new SchemaNode { Element = element, Children = [] },
        };
    }

    [Theory]
    [InlineData(PhysicalType.ByteArray, null)]
    [InlineData(PhysicalType.FixedLenByteArray, 32)]
    public void SeventySixDigits_IsADecimal256(PhysicalType physicalType, int? typeLength)
    {
        var type = Assert.IsType<Decimal256Type>(
            ArrowSchemaConverter.ToArrowType(Describe(physicalType, 76, 76, typeLength)));
        Assert.Equal((76, 76), (type.Precision, type.Scale));
    }

    [Theory]
    [InlineData(PhysicalType.ByteArray, 77, 2, null)]
    [InlineData(PhysicalType.ByteArray, 50_000_000, 50_000_000, null)]
    [InlineData(PhysicalType.FixedLenByteArray, 77, 2, 33)]
    public void APrecisionWiderThanDecimal256_IsRefused(PhysicalType physicalType, int precision, int scale, int? typeLength)
    {
        var column = Describe(physicalType, precision, scale, typeLength);
        Assert.True(SchemaAnnotations.IsValid(column.SchemaElement.LogicalType!, column.SchemaElement));

        Assert.Throws<NotSupportedException>(() => ArrowSchemaConverter.ToArrowType(column));
        Assert.Throws<NotSupportedException>(() => ArrowSchemaConverter.ToArrowType(
            column, new ParquetReadOptions { DecimalOutput = DecimalOutputKind.Decimal128 }));
    }
}
