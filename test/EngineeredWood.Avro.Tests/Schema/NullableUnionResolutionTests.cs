// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Avro.Tests.Schema;

/// <summary>
/// A promoted field (int to long, float to double, ...) took its nullable wrapper from the reader's
/// union, not the writer's. The writer's union says whether a branch index is on the wire and which
/// index is null, so when the two disagreed every value was misread, mostly without an error.
/// The unpromoted path already followed the writer, and is pinned here alongside.
/// </summary>
public class NullableUnionResolutionTests
{
    private static AvroSchema OneField(string type) =>
        new($$"""{"type": "record", "name": "T", "fields": [{"name": "a", "type": {{type}}}]}""");

    private static string?[] Read(string writerType, string readerType, byte[][] records)
    {
        using var ms = RawOcfFile.Create(OneField(writerType).Json, records, lengthPrefixed: false);
        using var reader = new AvroReaderBuilder().WithReaderSchema(OneField(readerType)).Build(ms);
        return Values(reader.ReadNextBatch()!.Column(0));
    }

    private static string?[] Values(IArrowArray column) =>
        Enumerable.Range(0, column.Length).Select(i => column.IsNull(i) ? null : column switch
        {
            Int64Array l => l.GetValue(i).ToString(),
            Int32Array n => n.GetValue(i).ToString(),
            DoubleArray d => d.GetValue(i).ToString(),
            _ => throw new InvalidOperationException(column.GetType().Name),
        }).ToArray();

    // 5, null, 7 under each union order. A branch index and an int are both zigzag varints: 0 -> 0,
    // 1 -> 2, 5 -> 10, 7 -> 14.
    private static readonly byte[][] IntFirst = [[0, 10], [2], [0, 14]];
    private static readonly byte[][] NullFirst = [[2, 10], [0], [2, 14]];
    private static readonly string?[] FiveNullSeven = ["5", null, "7"];

    [Theory]
    [InlineData("""["int", "null"]""", """["null", "long"]""")]
    [InlineData("""["int", "null"]""", """["long", "null"]""")]
    [InlineData("""["int", "null"]""", """["null", "int"]""")]
    public void AnIntFirstWriter_ReadsUnderEitherReaderOrder(string writer, string reader) =>
        Assert.Equal(FiveNullSeven, Read(writer, reader, IntFirst));

    [Theory]
    [InlineData("""["null", "int"]""", """["long", "null"]""")]
    [InlineData("""["null", "int"]""", """["null", "long"]""")]
    [InlineData("""["null", "int"]""", """["int", "null"]""")]
    public void ANullFirstWriter_ReadsUnderEitherReaderOrder(string writer, string reader) =>
        Assert.Equal(FiveNullSeven, Read(writer, reader, NullFirst));

    [Fact]
    public void AFloatFirstWriter_PromotesUnderANullFirstReader()
    {
        byte[][] records = [[0, .. BitConverter.GetBytes(5f)], [2], [0, .. BitConverter.GetBytes(7f)]];
        Assert.Equal(FiveNullSeven, Read("""["float", "null"]""", """["null", "double"]""", records));
    }

    // A plain writer has no branch index on the wire; one used to be read anyway.
    [Theory]
    [InlineData("""["null", "long"]""")]
    [InlineData("""["null", "int"]""")]
    public void APlainWriter_ReadsUnderANullableReader(string reader) =>
        Assert.Equal(new string?[] { "5", "6", "7" }, Read("\"int\"", reader, [[10], [12], [14]]));

    // A union writer does have one; it used to be read as the value (1, 5, 1).
    [Theory]
    [InlineData("\"long\"")]
    [InlineData("\"int\"")]
    public void AUnionWriter_ReadsUnderAPlainReader(string reader) =>
        Assert.Equal(new string?[] { "5", "6", "7" }, Read("""["null", "int"]""", reader, [[2, 10], [2, 12], [2, 14]]));

    [Fact]
    public void ArrayItems_FollowTheWritersUnion()
    {
        // One record: an array block of three items (5, null, 7 as ["int","null"]), then the end 0.
        using var ms = RawOcfFile.Create(
            OneField("""{"type": "array", "items": ["int", "null"]}""").Json, [[6, 0, 10, 2, 0, 14, 0]], lengthPrefixed: false);
        using var reader = new AvroReaderBuilder()
            .WithReaderSchema(OneField("""{"type": "array", "items": ["null", "long"]}""")).Build(ms);

        Assert.Equal(FiveNullSeven, Values(((ListArray)reader.ReadNextBatch()!.Column(0)).Values));
    }

    [Fact]
    public void RecordFields_FollowTheWritersUnion()
    {
        // One record holding a nested record {b: 5 as ["int","null"]}.
        const string writer = """{"type": "record", "name": "R", "fields": [{"name": "b", "type": ["int", "null"]}]}""";
        const string readerType = """{"type": "record", "name": "R", "fields": [{"name": "b", "type": ["null", "long"]}]}""";
        using var ms = RawOcfFile.Create(OneField(writer).Json, [[0, 10], [2]], lengthPrefixed: false);
        using var reader = new AvroReaderBuilder().WithReaderSchema(OneField(readerType)).Build(ms);

        var nested = Assert.IsType<StructArray>(reader.ReadNextBatch()!.Column(0));
        Assert.Equal(new string?[] { "5", null }, Values(nested.Fields[0]));
    }
}
