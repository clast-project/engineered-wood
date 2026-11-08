// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Tests.Schema;

/// <summary>
/// #763: a decimal annotation's precision and scale went into the Arrow type unchecked, so a schema
/// declaring decimal(10, 50000000) handed the reader a scale it sizes a power of ten by. The Avro spec
/// says an invalid logical type is ignored and the column read as its underlying type, which is what
/// the parser now does.
/// </summary>
public class DecimalAnnotationTests
{
    private static IArrowType ArrowTypeOf(string schemaJson) =>
        ArrowSchemaConverter.ToArrowType(AvroSchemaParser.Parse(schemaJson)).type;

    private static string Bytes(string props) => $$"""{"type": "bytes", "logicalType": "decimal"{{props}}}""";

    private static string Fixed(int size, string props) =>
        $$"""{"type": "fixed", "name": "F", "size": {{size}}, "logicalType": "decimal"{{props}}}""";

    [Theory]
    [InlineData(", \"precision\": 10, \"scale\": 2", 10, 2)]
    [InlineData(", \"precision\": 38, \"scale\": 38", 38, 38)]
    [InlineData(", \"precision\": 5", 5, 0)]
    public void AValidBytesDecimal_IsADecimal128(string props, int precision, int scale)
    {
        var type = Assert.IsType<Decimal128Type>(ArrowTypeOf(Bytes(props)));
        Assert.Equal((precision, scale), (type.Precision, type.Scale));
    }

    [Theory]
    [InlineData(", \"precision\": 10, \"scale\": 50000000")]
    [InlineData(", \"precision\": 10, \"scale\": 11")]
    [InlineData(", \"precision\": 10, \"scale\": -1")]
    [InlineData(", \"precision\": 0")]
    [InlineData(", \"precision\": -3, \"scale\": -4")]
    [InlineData(", \"scale\": 2")]
    [InlineData(", \"precision\": 10, \"scale\": \"2\"")]
    [InlineData(", \"precision\": 10, \"scale\": 1.5")]
    [InlineData(", \"precision\": 1e10")]
    public void AnInvalidBytesDecimal_IsReadAsBytes(string props)
    {
        var node = AvroSchemaParser.Parse(Bytes(props));
        Assert.Null(node.LogicalType);
        Assert.IsType<BinaryType>(ArrowTypeOf(Bytes(props)));
    }

    [Fact]
    public void ADecimalOnAnInt_IsReadAsAnInt()
    {
        Assert.IsType<Int32Type>(ArrowTypeOf("""{"type": "int", "logicalType": "decimal", "precision": 5, "scale": 2}"""));
    }

    [Theory]
    [InlineData(4, 9)]
    [InlineData(8, 18)]
    [InlineData(16, 38)]
    public void AFixed_HoldsTheDigitsOfItsWidth(int size, int maxDigits)
    {
        var type = Assert.IsType<Decimal128Type>(ArrowTypeOf(Fixed(size, $", \"precision\": {maxDigits}, \"scale\": 1")));
        Assert.Equal(maxDigits, type.Precision);

        Assert.IsType<FixedSizeBinaryType>(ArrowTypeOf(Fixed(size, $", \"precision\": {maxDigits + 1}, \"scale\": 1")));
    }

    [Fact]
    public void AnInvalidFixedDecimal_IsReadAsFixed()
    {
        var type = Assert.IsType<FixedSizeBinaryType>(ArrowTypeOf(Fixed(16, ", \"precision\": 10, \"scale\": 50000000")));
        Assert.Equal(16, type.ByteWidth);
    }

    // Valid Avro, but the reader decodes into Decimal128 and would truncate every wider value.
    [Fact]
    public void APrecisionAbove38_IsRefused()
    {
        Assert.Throws<NotSupportedException>(() => ArrowTypeOf(Bytes(", \"precision\": 39, \"scale\": 2")));
        Assert.Throws<NotSupportedException>(() => ArrowTypeOf(Fixed(17, ", \"precision\": 40, \"scale\": 2")));
    }

    [Fact]
    public void AFileWithAnInvalidDecimal_ReadsItsBytes()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("amount", BinaryType.Default, false))
            .Build();
        var column = new BinaryArray.Builder().Append([0x01, 0x02]).Append([0xFF]).Build();
        var batch = new RecordBatch(arrowSchema, [column], 2);

        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 50000000}}
            ]
        }
        """;

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(arrowSchema)
            .WithAvroSchema(new AvroSchema(avroSchemaJson))
            .Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        var result = reader.ReadNextBatch()!;

        var read = Assert.IsType<BinaryArray>(result.Column(0));
        Assert.Equal([0x01, 0x02], read.GetBytes(0).ToArray());
        Assert.Equal([0xFF], read.GetBytes(1).ToArray());
    }
}
