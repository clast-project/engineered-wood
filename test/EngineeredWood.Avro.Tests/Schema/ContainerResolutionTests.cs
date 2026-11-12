// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Avro.Tests.Schema;

/// <summary>
/// An array or map whose items needed resolving was built from the reader's schema alone, so its
/// items were decoded in the reader's encoding rather than the writer's: a float as a double, and a
/// record without a field the writer still wrote. Found through #767, where the same path compared a
/// reader's decimal with itself.
/// </summary>
public class ContainerResolutionTests
{
    private static AvroSchema OneField(string type) =>
        new($$"""{"type": "record", "name": "T", "fields": [{"name": "a", "type": {{type}}}]}""");

    private static RecordBatch Read(string writerType, string readerType, byte[] record)
    {
        using var ms = RawOcfFile.Create(OneField(writerType).Json, [record], lengthPrefixed: false);
        using var reader = new AvroReaderBuilder().WithReaderSchema(OneField(readerType)).Build(ms);
        return reader.ReadNextBatch()!;
    }

    private static byte[] Float(float value) => BitConverter.GetBytes(value);

    [Fact]
    public void FloatItems_PromoteToDouble()
    {
        // One block of two items, then the end-of-array 0.
        byte[] record = [4, .. Float(1.5f), .. Float(-2.25f), 0];

        var batch = Read("""{"type": "array", "items": "float"}""", """{"type": "array", "items": "double"}""", record);

        var items = Assert.IsType<DoubleArray>(((ListArray)batch.Column(0)).Values);
        Assert.Equal([1.5, -2.25], items.Values.ToArray());
        GC.KeepAlive(items);
    }

    [Fact]
    public void FloatMapValues_PromoteToDouble()
    {
        byte[] record = [2, 2, (byte)'k', .. Float(1.5f), 0];

        var batch = Read("""{"type": "map", "values": "float"}""", """{"type": "map", "values": "double"}""", record);

        var values = Assert.IsType<DoubleArray>(((MapArray)batch.Column(0)).Values);
        Assert.Equal(1.5, values.GetValue(0));
    }

    [Fact]
    public void RecordItems_DropAFieldTheReaderDoesNotHave()
    {
        // One record {x: 1, y: 2}: zigzag 2 and 4.
        var batch = Read(
            """{"type": "array", "items": {"type": "record", "name": "R", "fields": [{"name": "x", "type": "int"}, {"name": "y", "type": "int"}]}}""",
            """{"type": "array", "items": {"type": "record", "name": "R", "fields": [{"name": "y", "type": "int"}]}}""",
            [2, 2, 4, 0]);

        var records = Assert.IsType<StructArray>(((ListArray)batch.Column(0)).Values);
        Assert.Single(records.Fields);
        Assert.Equal(2, ((Int32Array)records.Fields[0]).GetValue(0));
    }

    [Fact]
    public void NestedArrays_PromoteTheirInnerItems()
    {
        // One outer item: an inner array of one float. Then both end-of-array 0s.
        byte[] record = [2, 2, .. Float(1.5f), 0, 0];

        var batch = Read(
            """{"type": "array", "items": {"type": "array", "items": "float"}}""",
            """{"type": "array", "items": {"type": "array", "items": "double"}}""",
            record);

        var inner = Assert.IsType<ListArray>(((ListArray)batch.Column(0)).Values);
        Assert.Equal(1.5, Assert.IsType<DoubleArray>(inner.Values).GetValue(0));
    }

    // The compatibility check stopped at the outer array or map, so an incompatible item passed and
    // the read failed inside Arrow ("Buffer count <2> must be at exactly <3>"). It is now refused up
    // front, naming the path, as a top-level int under a string already was.
    [Theory]
    [InlineData("""{"type": "array", "items": "int"}""", """{"type": "array", "items": "string"}""", "a[]")]
    [InlineData("""{"type": "map", "values": "int"}""", """{"type": "map", "values": "string"}""", "a{}")]
    [InlineData("""{"type": "array", "items": ["null", "int"]}""", """{"type": "array", "items": ["null", "string"]}""", "a[]")]
    [InlineData("""{"type": "array", "items": {"type": "array", "items": "int"}}""",
                """{"type": "array", "items": {"type": "array", "items": "string"}}""", "a[][]")]
    [InlineData("""{"type": "map", "values": {"type": "array", "items": "int"}}""",
                """{"type": "map", "values": {"type": "array", "items": "string"}}""", "a{}[]")]
    public void AnIncompatibleNestedItem_IsRefused(string writerType, string readerType, string path)
    {
        using var ms = RawOcfFile.Create(OneField(writerType).Json, [], lengthPrefixed: false);

        var ex = Assert.Throws<InvalidOperationException>(
            () => new AvroReaderBuilder().WithReaderSchema(OneField(readerType)).Build(ms).ReadNextBatch());
        Assert.Contains($"field '{path}'", ex.Message);
        Assert.Contains("writer is Int, reader is String", ex.Message);
    }
}
