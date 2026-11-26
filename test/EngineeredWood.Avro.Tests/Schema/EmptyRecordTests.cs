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

    // ─── Review round 1 ───

    /// <summary>
    /// A reference to a record from inside its own definition is parsed as a stand-in with no fields. Stripping
    /// took it for an empty record and dropped <c>B.a</c>, whose values are whole records on the wire.
    /// </summary>
    private const string Recursive = """
        {"type": "record", "name": "A", "fields": [
          {"name": "value", "type": "int"},
          {"name": "next", "type": ["null", {"type": "record", "name": "B", "fields": [
            {"name": "x", "type": "int"}, {"name": "a", "type": "A"}
          ]}]}
        ]}
        """;

    [Fact]
    public void ARecursiveReference_IsRefusedAsRecursion_NotStripped()
    {
        var e = Assert.Throws<NotSupportedException>(() => new AvroSchema(Recursive).ToArrowSchema());
        Assert.Contains("'A' refers to itself", e.Message);
    }

    // A(value:int, next:[null, A]): value=1 then next = A(value=2, next=null); then value=3, next=null.
    private const string LinkedList = """
        {"type": "record", "name": "A", "fields": [{"name": "value", "type": "int"}, {"name": "next", "type": ["null", "A"]}]}
        """;
    private static readonly byte[][] LinkedListRecords = [[0x02, 0x02, 0x04, 0x00], [0x06, 0x00]];

    /// <summary>
    /// Pre-existing: skipping a recursive field skipped the stand-in's (zero) fields, not the record it stands
    /// for, so the second row read the first row's nested value and came back as 1, 2.
    /// </summary>
    [Fact]
    public void ProjectingAwayARecursiveField_SkipsWholeRecords()
    {
        using var file = Ocf(LinkedList, LinkedListRecords);
        using var reader = new AvroReaderBuilder().WithProjection("value").Build(file);
        Assert.Equal([1, 3], Ints(reader.ReadNextBatch()!.Column("value")));
    }

    [SkippableFact]
    public void Fastavro_ReadsTheLinkedListTheSameWay()
    {
        Skip.IfNot(FastavroOracle.IsAvailable, "fastavro is not installed.");
        using var file = Ocf(LinkedList, LinkedListRecords);
        var rows = FastavroOracle.Read(file).Records!.Value;
        Assert.Equal([1, 3], rows.EnumerateArray().Select(r => (int?)r.GetProperty("value").GetInt32()));
        Assert.Equal(2, rows[0].GetProperty("next").GetProperty("value").GetInt32());
    }

    /// <summary>
    /// The writer's empty record evolves into a reader record whose field has a default. Stripping the writer
    /// as well as the reader left the reader's record unmatched, and resolution asked for a default for it.
    /// </summary>
    [Fact]
    public void AWritersEmptyRecord_ResolvesToAReaderRecordsDefaults()
    {
        string writer = S("""
            {"type": "record", "name": "T", "fields": [{"name": "a", "type": "int"}, {"name": "p", "type": __EMPTY__}]}
            """);
        const string reader = """
            {"type": "record", "name": "T", "fields": [
              {"name": "a", "type": "int"},
              {"name": "p", "type": {"type": "record", "name": "r102", "fields": [{"name": "x", "type": "int", "default": 7}]}}
            ]}
            """;
        using var file = Ocf(writer, [0x02]);
        var p = (StructArray)ReadOne(file, new AvroSchema(reader)).Column("p");
        Assert.Equal([7], Ints(p.Fields[0]));
    }

    /// <summary>A projection that keeps the struct holding the empty record (the reader keeps it, the writer skips it).</summary>
    [Fact]
    public void ProjectingTheStructThatHoldsIt_LeavesItOut()
    {
        string schema = S("""
            {"type": "record", "name": "manifest_entry", "fields": [
              {"name": "status", "type": "int"},
              {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
                {"name": "x", "type": "int"}, {"name": "partition", "type": __EMPTY__}, {"name": "y", "type": "int"}
              ]}}
            ]}
            """);
        using var file = Ocf(schema, [0x02, 0x04, 0x06]);
        using var reader = new AvroReaderBuilder().WithProjection("data_file").Build(file);
        var dataFile = (StructArray)reader.ReadNextBatch()!.Column("data_file");
        Assert.Equal(["x", "y"], ((Apache.Arrow.Types.StructType)dataFile.Data.DataType).Fields.Select(f => f.Name));
        Assert.Equal([3], Ints(dataFile.Fields[1]));
    }

    /// <summary>
    /// Each block's records are decoded by count now, so a block whose bytes ran out inside a skipped
    /// fixed-width value (which advanced without a bounds check) read on as if they were there.
    /// </summary>
    [Fact]
    public void ABlockThatEndsInsideASkippedValue_IsRefused()
    {
        const string writer = """{"type": "record", "name": "T", "fields": [{"name": "n", "type": "null"}, {"name": "f", "type": "float"}]}""";
        const string reader = """{"type": "record", "name": "T", "fields": [{"name": "n", "type": "null"}]}""";
        // Two records declared, one float's worth of bytes.
        using var file = RawOcfFile.Create(writer, [[0x00, 0x00, 0x80, 0x3F], []], lengthPrefixed: false);
        using var r = new AvroReaderBuilder().WithReaderSchema(new AvroSchema(reader)).Build(file);
        Assert.Throws<InvalidDataException>(() => r.ReadNextBatch());
    }

    /// <summary>
    /// R1 has two fields of R0 (empty), R2 two of R1, and so on: walking every reference afresh is 2^n calls,
    /// which for 40 levels does not finish. Each record is checked once now. All of them are empty, so only
    /// <c>a</c> is left.
    /// </summary>
    [Fact]
    public void ANamedEmptyRecordReferredToExponentially_IsCheckedOnce()
    {
        // Field d_i defines R_i with two fields of R_(i-1), each referred to by name; R0 has no fields.
        var fields = new List<string> { """{"name": "a", "type": "int"}""", """{"name": "d0", "type": {"type": "record", "name": "R0", "fields": []}}""" };
        for (int i = 1; i <= 40; i++)
        {
            fields.Add("{\"name\": \"d" + i + "\", \"type\": {\"type\": \"record\", \"name\": \"R" + i + "\", \"fields\": [" +
                "{\"name\": \"f\", \"type\": \"R" + (i - 1) + "\"}, {\"name\": \"g\", \"type\": \"R" + (i - 1) + "\"}]}}");
        }
        string schema = """{"type": "record", "name": "T", "fields": [""" + string.Join(", ", fields) + "]}";

        using var file = Ocf(schema, [0x02]);
        var batch = ReadOne(file);
        Assert.Equal(["a"], batch.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal([1], Ints(batch.Column("a")));
    }
}
