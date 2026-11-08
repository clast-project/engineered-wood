// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
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
        using var ms = RawOcfFile.Create(schemaJson, [[0x01, 0x02], [0xFF]]);

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
        using var ms = RawOcfFile.Create(HugeDecimalWriterSchema, [[0x01]]);
        Assert.Throws<NotSupportedException>(() => new AvroReaderBuilder().Build(ms).ReadNextBatch());
    }

    // The writer's annotation is bounded before it is compared with the reader's, so a reader
    // schema cannot let decimal(50000000, 50000000) through.
    [Fact]
    public void AHugeWriterDecimal_IsRefused_UnderAReaderSchema()
    {
        using var ms = RawOcfFile.Create(HugeDecimalWriterSchema, [[0x01]]);
        var readerSchema = new AvroSchema("""
            {"type": "record", "name": "T", "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}]}
            """);

        Assert.Throws<NotSupportedException>(
            () => new AvroReaderBuilder().WithReaderSchema(readerSchema).Build(ms).ReadNextBatch());
    }

    private static AvroSchema OneField(string type) =>
        new($$"""{"type": "record", "name": "T", "fields": [{"name": "amount", "type": {{type}}}]}""");

    private static RecordBatch ReadUnder(MemoryStream ms, AvroSchema readerSchema)
    {
        using var reader = new AvroReaderBuilder().WithReaderSchema(readerSchema).Build(ms);
        var batch = reader.ReadNextBatch()!;
        // Whatever else, the column must be what the batch's schema says it is.
        Assert.Equal(batch.Schema.FieldsList[0].DataType.TypeId, batch.Column(0).Data.DataType.TypeId);
        return batch;
    }

    private static MemoryStream WriteOneColumn(IArrowArray column, string avroType)
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder().Field(new Field("amount", column.Data.DataType, false)).Build();
        var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(arrowSchema).WithAvroSchema(OneField(avroType)).Build(ms))
        {
            writer.Write(new RecordBatch(arrowSchema, [column], column.Length));
            writer.Finish();
        }
        ms.Position = 0;
        return ms;
    }

    // A writer that declares no decimal says nothing of a scale, so the reader's is not applied to its
    // bytes (Avro Java applies it: AVRO-2078). Before, these built binary buffers under the reader's
    // Decimal128 and Arrow refused them ("Buffer count <3> must be at exactly <2>").
    private const string ReaderBytesDecimal = """{"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}""";

    private static void AssertUndeclaredWriterRefused(MemoryStream ms, AvroSchema readerSchema)
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => new AvroReaderBuilder().WithReaderSchema(readerSchema).Build(ms).ReadNextBatch());
        Assert.Contains("no decimal annotation", ex.Message);
    }

    [Fact]
    public void PlainWriterBytes_AreRefused_UnderAReaderDecimal()
    {
        using var ms = WriteOneColumn(new BinaryArray.Builder().Append([0x30, 0x39]).Build(), "\"bytes\"");
        AssertUndeclaredWriterRefused(ms, OneField(ReaderBytesDecimal));
    }

    [Fact]
    public void PlainWriterFixed_IsRefused_UnderAReaderDecimal()
    {
        using var ms = RawOcfFile.Create(OneField("""{"type": "fixed", "name": "F", "size": 4}""").Json, [[0, 0, 0x30, 0x39]], lengthPrefixed: false);
        AssertUndeclaredWriterRefused(
            ms, OneField("""{"type": "fixed", "name": "F", "size": 4, "logicalType": "decimal", "precision": 9, "scale": 2}"""));
    }

    // The parser drops the writer's invalid annotation, which leaves plain bytes.
    [Fact]
    public void AnInvalidWriterDecimal_IsRefused_UnderAReaderDecimal()
    {
        using var ms = RawOcfFile.Create(OneField("""{"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 50000000}""").Json, [[0x30, 0x39]]);
        AssertUndeclaredWriterRefused(ms, OneField(ReaderBytesDecimal));
    }

    // Avro promotes a writer string to the reader's bytes; a decimal on those bytes has no scale to match either.
    [Fact]
    public void AWriterString_IsRefused_UnderAReaderBytesDecimal()
    {
        using var ms = WriteOneColumn(new StringArray.Builder().Append("09").Build(), "\"string\"");
        AssertUndeclaredWriterRefused(ms, OneField(ReaderBytesDecimal));
    }

    [Fact]
    public void PlainWriterBytes_AreRefused_UnderANestedReaderDecimal()
    {
        using var ms = RawOcfFile.Create(OneField("""{"type": "array", "items": "bytes"}""").Json, [], lengthPrefixed: false);
        AssertUndeclaredWriterRefused(ms, OneField($$"""{"type": "array", "items": {{ReaderBytesDecimal}}}"""));
    }

    // The reverse: the reader asks for plain bytes and gets the bytes, not a Decimal128 under a Binary field.
    [Fact]
    public void AWriterDecimal_UnderPlainReaderBytes_ReadsTheBytes()
    {
        const string schemaJson = """
            {"type": "record", "name": "T", "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 4}}]}
            """;
        using var ms = RawOcfFile.Create(schemaJson, [[0x30, 0x39]]);

        var batch = ReadUnder(ms, OneField("\"bytes\""));

        Assert.Equal([0x30, 0x39], Assert.IsType<BinaryArray>(batch.Column(0)).GetBytes(0).ToArray());
    }

    // ── Writer decimal under reader decimal (#767) ──────────────────────────────────────────────────
    // The spec: two decimals match only if their scales and precisions match. A larger reader
    // precision at the same scale is let through as well; nothing else is.

    private static string BytesDecimal(int precision, int scale) =>
        $$"""{"type": "bytes", "logicalType": "decimal", "precision": {{precision}}, "scale": {{scale}}}""";

    // 12345 as a big-endian unscaled value.
    private static MemoryStream WriterDecimal(int precision, int scale) =>
        RawOcfFile.Create(OneField(BytesDecimal(precision, scale)).Json, [[0x30, 0x39]]);

    [Theory]
    [InlineData(20, 4, 20, 4, "1.2345")]
    [InlineData(10, 2, 20, 2, "123.45")]
    [InlineData(5, 0, 38, 0, "12345")]
    public void AWriterDecimal_ReadsUnderAReaderDecimal_OfTheSameScale_AndAtLeastItsPrecision(
        int writerPrecision, int writerScale, int readerPrecision, int readerScale, string expected)
    {
        using var ms = WriterDecimal(writerPrecision, writerScale);

        var batch = ReadUnder(ms, OneField(BytesDecimal(readerPrecision, readerScale)));

        var column = Assert.IsType<Decimal128Array>(batch.Column(0));
        var type = (Decimal128Type)column.Data.DataType;
        Assert.Equal((readerPrecision, readerScale), (type.Precision, type.Scale));
        Assert.Equal(expected, column.GetSqlDecimal(0)!.Value.ToString());
    }

    [Theory]
    [InlineData(20, 4, 20, 2)] // a smaller scale: Avro Java reads 1.2345 as 123.45
    [InlineData(10, 2, 12, 4)] // a larger scale, even with room for it: 123.45 would read as 1.2345
    [InlineData(20, 2, 10, 2)] // the same scale with a smaller precision: a value may not fit
    public void AWriterDecimal_IsRefused_UnderAReaderDecimalItWouldMisread(
        int writerPrecision, int writerScale, int readerPrecision, int readerScale)
    {
        using var ms = WriterDecimal(writerPrecision, writerScale);

        var ex = Assert.Throws<InvalidOperationException>(() => new AvroReaderBuilder()
            .WithReaderSchema(OneField(BytesDecimal(readerPrecision, readerScale))).Build(ms).ReadNextBatch());
        Assert.Contains($"decimal({writerPrecision},{writerScale})", ex.Message);
        Assert.Contains($"decimal({readerPrecision},{readerScale})", ex.Message);
    }

    // A fixed(8) decimal(9,2) holding 12345, i.e. 123.45.
    private static MemoryStream WriterFixedDecimal() => RawOcfFile.Create(
        OneField("""{"type": "fixed", "name": "F", "size": 8, "logicalType": "decimal", "precision": 9, "scale": 2}""").Json,
        [[0, 0, 0, 0, 0, 0, 0x30, 0x39]],
        lengthPrefixed: false);

    [Fact]
    public void AFixedWriterDecimal_TakesALargerReaderPrecision()
    {
        using var ms = WriterFixedDecimal();

        var batch = ReadUnder(ms, OneField("""{"type": "fixed", "name": "F", "size": 8, "logicalType": "decimal", "precision": 18, "scale": 2}"""));

        var type = (Decimal128Type)batch.Column(0).Data.DataType;
        Assert.Equal((18, 2), (type.Precision, type.Scale));
        Assert.Equal("123.45", ((Decimal128Array)batch.Column(0)).GetSqlDecimal(0)!.Value.ToString());
    }

    [Fact]
    public void AFixedWriterDecimal_IsRefused_UnderAnotherScale()
    {
        using var ms = WriterFixedDecimal();

        Assert.Throws<InvalidOperationException>(() => new AvroReaderBuilder()
            .WithReaderSchema(OneField("""{"type": "fixed", "name": "F", "size": 8, "logicalType": "decimal", "precision": 9, "scale": 3}"""))
            .Build(ms).ReadNextBatch());
    }

    // Records inside an array or map: the container used to be built from the reader's schema
    // alone, so the reader's decimal was compared with itself and 1.2345 read as 123.45.
    private static string RecordOf(string decimalType) =>
        $$"""{"type": "record", "name": "R", "fields": [{"name": "d", "type": {{decimalType}}}]}""";

    // An array block of one record holding 12345, then the end-of-array 0.
    private static readonly byte[] ArrayOfOneRecord = [2, 4, 0x30, 0x39, 0];

    // A map block of one entry "k" -> record holding 12345, then the end-of-map 0.
    private static readonly byte[] MapOfOneRecord = [2, 2, (byte)'k', 4, 0x30, 0x39, 0];

    [Theory]
    [InlineData("array", "items")]
    [InlineData("map", "values")]
    public void ADecimalInARecordInAContainer_IsRefused_UnderAnotherScale(string container, string child)
    {
        string Shape(int scale) => $$"""{"type": "{{container}}", "{{child}}": {{RecordOf(BytesDecimal(20, scale))}}}""";
        using var ms = RawOcfFile.Create(OneField(Shape(4)).Json,
            [container == "array" ? ArrayOfOneRecord : MapOfOneRecord], lengthPrefixed: false);

        Assert.Throws<InvalidOperationException>(
            () => new AvroReaderBuilder().WithReaderSchema(OneField(Shape(2))).Build(ms).ReadNextBatch());
    }

    [Fact]
    public void ADecimalInARecordInAnArray_TakesALargerReaderPrecision()
    {
        using var ms = RawOcfFile.Create(
            OneField($$"""{"type": "array", "items": {{RecordOf(BytesDecimal(10, 2))}}}""").Json,
            [ArrayOfOneRecord], lengthPrefixed: false);

        var batch = ReadUnder(ms, OneField($$"""{"type": "array", "items": {{RecordOf(BytesDecimal(20, 2))}}}"""));

        var records = Assert.IsType<StructArray>(((ListArray)batch.Column(0)).Values);
        var d = Assert.IsType<Decimal128Array>(records.Fields[0]);
        Assert.Equal(20, ((Decimal128Type)d.Data.DataType).Precision);
        Assert.Equal("123.45", d.GetSqlDecimal(0)!.Value.ToString());
    }

    // The item builder used to take the writer's type even under a reader schema.
    [Fact]
    public void AnArrayOfWriterDecimals_TakesTheReadersItemType()
    {
        // One record: an array block of one item (12345, length 2), then the end-of-array 0.
        using var ms = RawOcfFile.Create(
            OneField($$"""{"type": "array", "items": {{BytesDecimal(10, 2)}}}""").Json,
            [[2, 4, 0x30, 0x39, 0]],
            lengthPrefixed: false);

        var batch = ReadUnder(ms, OneField($$"""{"type": "array", "items": {{BytesDecimal(20, 2)}}}"""));

        var list = Assert.IsType<ListArray>(batch.Column(0));
        var items = Assert.IsType<Decimal128Array>(list.Values);
        Assert.Equal((20, 2), (((Decimal128Type)items.Data.DataType).Precision, ((Decimal128Type)items.Data.DataType).Scale));
        Assert.Equal("123.45", items.GetSqlDecimal(0)!.Value.ToString());
    }

    // The top-level compatibility check does not descend into containers; the builder check does.
    [Theory]
    [InlineData("""{"type": "array", "items": DEC}""")]
    [InlineData("""{"type": "map", "values": DEC}""")]
    [InlineData("""["null", DEC]""")]
    public void ANestedWriterDecimal_IsRefused_UnderAnotherScale(string shape)
    {
        var writerSchema = OneField(shape.Replace("DEC", BytesDecimal(10, 2)));
        var readerSchema = OneField(shape.Replace("DEC", BytesDecimal(10, 4)));
        // An empty file is enough: the builders are made from the schemas.
        using var ms = RawOcfFile.Create(writerSchema.Json, []);

        Assert.Throws<InvalidOperationException>(
            () => new AvroReaderBuilder().WithReaderSchema(readerSchema).Build(ms).ReadNextBatch());
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
