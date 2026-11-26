// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Avro.Tests.Schema;

/// <summary>
/// A record with no fields (Iceberg's <c>partition</c> of an unpartitioned table) has no Arrow type, because
/// Apache.Arrow refuses a struct of no fields; reading one threw an ArgumentNullException for 'fields'. Such a
/// record takes no bytes, so a field of that type is now left out of the Arrow schema. Where leaving it out
/// would lose something (array items, map values, union branches) it is refused by name instead, but only
/// when it is read.
/// </summary>
public class EmptyRecordTests
{
    private const string Empty = """{"type": "record", "name": "r102", "fields": []}""";

    /// <summary>A schema with <c>__EMPTY__</c> standing for the empty record.</summary>
    private static string S(string json) => json.Replace("__EMPTY__", Empty);

    private static MemoryStream Ocf(string schema, params byte[][] records) =>
        RawOcfFile.Create(schema, records, lengthPrefixed: false);

    private static RecordBatch ReadOne(Stream file, AvroSchema? readerSchema = null)
    {
        var builder = new AvroReaderBuilder();
        if (readerSchema != null)
            builder = builder.WithReaderSchema(readerSchema);
        using var reader = builder.Build(file);
        var batch = reader.ReadNextBatch()!;
        Assert.Null(reader.ReadNextBatch());
        return batch;
    }

    private static int?[] Ints(IArrowArray array) =>
        Enumerable.Range(0, array.Length).Select(i => ((Int32Array)array).GetValue(i)).ToArray();

    // a = 1, 3 and b = 2, 4: zigzag 1 -> 02, 2 -> 04, 3 -> 06, 4 -> 08. The empty record adds nothing.
    private static readonly string Between = S("""
        {"type": "record", "name": "T", "fields": [
          {"name": "a", "type": "int"}, {"name": "partition", "type": __EMPTY__}, {"name": "b", "type": "int"}
        ]}
        """);
    private static readonly byte[][] BetweenRecords = [[0x02, 0x04], [0x06, 0x08]];

    [Fact]
    public void AnEmptyRecordField_IsLeftOut_AndTheFieldsAroundItRead()
    {
        using var file = Ocf(Between, BetweenRecords);
        var batch = ReadOne(file);
        Assert.Equal(["a", "b"], batch.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal([1, 3], Ints(batch.Column("a")));
        Assert.Equal([2, 4], Ints(batch.Column("b")));
    }

    [Fact]
    public void TheWriterSchema_StillHasTheEmptyRecord()
    {
        using var file = Ocf(Between, BetweenRecords);
        using var reader = new AvroReaderBuilder().Build(file);
        Assert.Contains("\"partition\"", reader.WriterSchema.Json);
    }

    [Fact]
    public async Task AsyncReading_LeavesItOutToo()
    {
        using var file = Ocf(Between, BetweenRecords);
        await using var reader = await new AvroReaderBuilder().BuildAsync(file);
        var batch = (await reader.ReadNextBatchAsync())!;
        Assert.Equal(["a", "b"], batch.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal([2, 4], Ints(batch.Column("b")));
    }

    [Fact]
    public void ToArrowSchema_LeavesItOut() =>
        Assert.Equal(["a", "b"], new AvroSchema(Between).ToArrowSchema().FieldsList.Select(f => f.Name));

    /// <summary>The Iceberg shape: the empty record inside another record, next to its siblings.</summary>
    [Fact]
    public void ANestedEmptyRecordField_IsLeftOutOfItsStruct()
    {
        string schema = S("""
            {"type": "record", "name": "manifest_entry", "fields": [
              {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
                {"name": "x", "type": "int"}, {"name": "partition", "type": __EMPTY__}, {"name": "y", "type": "string"}
              ]}}
            ]}
            """);
        using var file = Ocf(schema, [0x02, 0x02, (byte)'q']);
        var dataFile = (StructArray)ReadOne(file).Column("data_file");
        var type = (Apache.Arrow.Types.StructType)dataFile.Data.DataType;
        Assert.Equal(["x", "y"], type.Fields.Select(f => f.Name));
        Assert.Equal([1], Ints(dataFile.Fields[0]));
        Assert.Equal("q", ((StringArray)dataFile.Fields[1]).GetString(0));
    }

    [Fact]
    public void ARecordOfOnlyEmptyRecords_IsEmptyToo()
    {
        string schema = S("""
            {"type": "record", "name": "T", "fields": [
              {"name": "a", "type": "int"},
              {"name": "q", "type": {"type": "record", "name": "Q", "fields": [{"name": "p", "type": __EMPTY__}]}}
            ]}
            """);
        using var file = Ocf(schema, [0x02]);
        var batch = ReadOne(file);
        Assert.Equal(["a"], batch.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal([1], Ints(batch.Column("a")));
    }

    /// <summary>
    /// Every record is zero bytes. Decoding a batch stopped when the block's bytes ran out, not after the
    /// block's record count, so this read as 0 rows.
    /// </summary>
    [Fact]
    public void RecordsOfOnlyAnEmptyRecord_KeepTheirCount()
    {
        string schema = S("""{"type": "record", "name": "T", "fields": [{"name": "partition", "type": __EMPTY__}]}""");
        using var file = Ocf(schema, [], [], []);
        var batch = ReadOne(file);
        Assert.Empty(batch.Schema.FieldsList);
        Assert.Equal(3, batch.Length);
    }

    /// <summary>The same row-count loss, with no empty record: a record of a single null takes zero bytes.</summary>
    [Fact]
    public void ZeroByteRecords_KeepTheirCount()
    {
        const string schema = """{"type": "record", "name": "T", "fields": [{"name": "n", "type": "null"}]}""";
        using var file = Ocf(schema, [], [], []);
        var batch = ReadOne(file);
        Assert.Equal(3, batch.Length);
        Assert.Equal(3, batch.Column("n").Length);
    }

    [Fact]
    public void ZeroByteRecords_KeepTheirCount_AcrossBatches()
    {
        const string schema = """{"type": "record", "name": "T", "fields": [{"name": "n", "type": "null"}]}""";
        using var file = Ocf(schema, Enumerable.Repeat(System.Array.Empty<byte>(), 5).ToArray());
        using var reader = new AvroReaderBuilder().WithBatchSize(2).Build(file);
        Assert.Equal([2, 2, 1], reader.Select(b => b.Length));
    }

    [Fact]
    public void AReaderSchemaWithTheEmptyRecord_ReadsAsWithout()
    {
        using var file = Ocf(Between, BetweenRecords);
        var batch = ReadOne(file, new AvroSchema(Between));
        Assert.Equal(["a", "b"], batch.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal([2, 4], Ints(batch.Column("b")));
    }

    [Fact]
    public void AReaderSchemaWithoutTheEmptyRecord_Reads()
    {
        const string reader = """{"type": "record", "name": "T", "fields": [{"name": "b", "type": "int"}]}""";
        using var file = Ocf(Between, BetweenRecords);
        Assert.Equal([2, 4], Ints(ReadOne(file, new AvroSchema(reader)).Column("b")));
    }

    /// <summary>A reader's empty record the writer lacks, filled from its default, is left out as well.</summary>
    [Fact]
    public void AReaderOnlyEmptyRecordWithADefault_IsLeftOut()
    {
        const string writer = """{"type": "record", "name": "T", "fields": [{"name": "a", "type": "int"}]}""";
        string reader = S("""
            {"type": "record", "name": "T", "fields": [
              {"name": "a", "type": "int"}, {"name": "partition", "type": __EMPTY__, "default": {}}
            ]}
            """);
        using var file = Ocf(writer, [0x02]);
        var batch = ReadOne(file, new AvroSchema(reader));
        Assert.Equal(["a"], batch.Schema.FieldsList.Select(f => f.Name));
    }

    /// <summary>Leaving these out would lose whether the value is null, or how many elements there are.</summary>
    [Theory]
    [InlineData("""["null", __EMPTY__]""")]
    [InlineData("""{"type": "array", "items": __EMPTY__}""")]
    [InlineData("""{"type": "map", "values": __EMPTY__}""")]
    public void AnEmptyRecordWhereLeavingItOutLosesData_IsRefusedByName(string type)
    {
        using var file = Ocf(Holding(type), [0x00, 0x02]);
        var e = Assert.Throws<NotSupportedException>(() => new AvroReaderBuilder().Build(file));
        Assert.Contains("'r102' has no fields", e.Message);
    }

    /// <summary>The refusal is for reading it: a projection that leaves it out reads the rest.</summary>
    [Theory]
    [InlineData("""["null", __EMPTY__]""", new byte[] { 0x02, 0x02 })]
    [InlineData("""{"type": "array", "items": __EMPTY__}""", new byte[] { 0x06, 0x00, 0x02 })]
    [InlineData("""{"type": "map", "values": __EMPTY__}""", new byte[] { 0x02, 0x02, (byte)'k', 0x00, 0x02 })]
    public void AnEmptyRecordWhereLeavingItOutLosesData_CanBeProjectedAway(string type, byte[] record)
    {
        using var file = Ocf(Holding(type), record);
        using var reader = new AvroReaderBuilder().WithProjection("b").Build(file);
        Assert.Equal([1], Ints(reader.ReadNextBatch()!.Column("b")));
    }

    // p holds the empty record in some container; b = 1 follows it.
    private static string Holding(string type) => S("""
        {"type": "record", "name": "T", "fields": [
          {"name": "p", "type": __TYPE__}, {"name": "b", "type": "int"}
        ]}
        """.Replace("__TYPE__", type));

    [SkippableFact]
    public void Fastavro_ReadsTheSameValues()
    {
        Skip.IfNot(FastavroOracle.IsAvailable, "fastavro is not installed.");

        using (var file = Ocf(Between, BetweenRecords))
        {
            var rows = FastavroOracle.Read(file).Records!.Value;
            Assert.Equal(2, rows.GetArrayLength());
            Assert.Equal(4, rows[1].GetProperty("b").GetInt32());
            Assert.Empty(rows[1].GetProperty("partition").EnumerateObject());
        }

        string onlyEmpty = S("""{"type": "record", "name": "T", "fields": [{"name": "partition", "type": __EMPTY__}]}""");
        using (var file = Ocf(onlyEmpty, [], [], []))
            Assert.Equal(3, FastavroOracle.Read(file).Records!.Value.GetArrayLength());
    }
}
