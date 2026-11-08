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

    // The writer re-serializes the parsed schema, so the round trip above reads a header that already
    // says plain bytes. These build the file by hand to keep the annotation another writer would leave.

    [Fact]
    public void AnExternallyWrittenInvalidDecimal_ReadsItsBytes()
    {
        const string schemaJson = """
            {"type": "record", "name": "T", "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 50000000}}]}
            """;
        using var ms = RawOcf(schemaJson, [[0x01, 0x02], [0xFF]]);

        Assert.Contains("\"scale\": 50000000", System.Text.Encoding.UTF8.GetString(ms.ToArray()));

        using var reader = new AvroReaderBuilder().Build(ms);
        var result = reader.ReadNextBatch()!;

        var read = Assert.IsType<BinaryArray>(result.Column(0));
        Assert.Equal([0x01, 0x02], read.GetBytes(0).ToArray());
        Assert.Equal([0xFF], read.GetBytes(1).ToArray());
    }

    private const string HugeDecimalWriterSchema = """
        {"type": "record", "name": "T", "fields": [
            {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 50000000, "scale": 50000000}}]}
        """;

    // Valid Avro (bytes bound the precision by nothing), so the parser keeps it and the converter refuses it.
    [Fact]
    public void AnExternallyWrittenHugeDecimal_IsRefused()
    {
        using var ms = RawOcf(HugeDecimalWriterSchema, [[0x01]]);
        Assert.Throws<NotSupportedException>(() => new AvroReaderBuilder().Build(ms).ReadNextBatch());
    }

    // Schema resolution matches bytes to bytes without comparing the decimal annotations, and the
    // builders decode with the writer's: the reader's decimal(10,2) must not let decimal(50000000,
    // 50000000) through.
    [Fact]
    public void AHugeWriterDecimal_IsRefused_UnderAReaderSchema()
    {
        using var ms = RawOcf(HugeDecimalWriterSchema, [[0x01]]);
        var readerSchema = new AvroSchema("""
            {"type": "record", "name": "T", "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}]}
            """);

        Assert.Throws<NotSupportedException>(
            () => new AvroReaderBuilder().WithReaderSchema(readerSchema).Build(ms).ReadNextBatch());
    }

    /// <summary>A null-codec object container file holding one block of single-field <c>bytes</c> records.</summary>
    private static MemoryStream RawOcf(string schemaJson, byte[][] values)
    {
        var ms = new MemoryStream();
        ms.Write([(byte)'O', (byte)'b', (byte)'j', 1], 0, 4);
        WriteLong(ms, 1);
        WriteBytes(ms, "avro.schema"u8.ToArray());
        WriteBytes(ms, System.Text.Encoding.UTF8.GetBytes(schemaJson));
        WriteLong(ms, 0);
        byte[] sync = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
        ms.Write(sync, 0, sync.Length);

        using var block = new MemoryStream();
        foreach (var v in values)
            WriteBytes(block, v);
        WriteLong(ms, values.Length);
        WriteLong(ms, block.Length);
        block.WriteTo(ms);
        ms.Write(sync, 0, sync.Length);

        ms.Position = 0;
        return ms;

        static void WriteBytes(Stream s, byte[] b)
        {
            WriteLong(s, b.Length);
            s.Write(b, 0, b.Length);
        }

        static void WriteLong(Stream s, long v)
        {
            ulong z = (ulong)((v << 1) ^ (v >> 63));
            while (z >= 0x80)
            {
                s.WriteByte((byte)(z | 0x80));
                z >>= 7;
            }
            s.WriteByte((byte)z);
        }
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
