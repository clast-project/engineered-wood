// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Avro.Tests.Encoding;

/// <summary>
/// A union branch index was never checked against the union. A nullable union took every index but the
/// null one as its value, so a stray index read the bytes after it as a value and went on, misaligned and
/// often without an error; a general union and a skipped union indexed past their branches. Each now
/// refuses the record, as fastavro does.
/// </summary>
public class UnionIndexTests
{
    private const string NullableThenInt = """
        {"type": "record", "name": "T", "fields": [
          {"name": "a", "type": ["null", "string"]},
          {"name": "b", "type": "int"}
        ]}
        """;

    // A branch index and an int are both zigzag varints: 1 -> 02, 2 -> 04, -1 -> 01, 5 -> 0A.
    private static readonly byte[] Valid = [0x02, 0x02, (byte)'x', 0x0A];
    private static readonly byte[] IndexTwo = [0x04, 0x02, (byte)'x', 0x0A];
    private static readonly byte[] IndexMinusOne = [0x01, 0x02, (byte)'x', 0x0A];

    private static MemoryStream Ocf(string schema, params byte[][] records) =>
        RawOcfFile.Create(schema, records, lengthPrefixed: false);

    private static InvalidDataException AssertRefused(Stream file, AvroSchema? readerSchema = null)
    {
        var builder = new AvroReaderBuilder();
        if (readerSchema != null)
            builder = builder.WithReaderSchema(readerSchema);
        using var reader = builder.Build(file);
        return Assert.Throws<InvalidDataException>(() => reader.ReadNextBatch());
    }

    [Fact]
    public void AValidNullableUnion_Reads()
    {
        using var file = Ocf(NullableThenInt, Valid);
        using var reader = new AvroReaderBuilder().Build(file);
        var batch = reader.ReadNextBatch()!;
        Assert.Equal("x", ((StringArray)batch.Column("a")).GetString(0));
        Assert.Equal(5, ((Int32Array)batch.Column("b")).GetValue(0));
    }

    /// <summary>
    /// Index 2 of a two-branch union used to read as the string, so this record came back as a="x", b=5
    /// with no error at all.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(-1)]
    public void ANullableUnionIndexOutOfRange_IsRefused(int index)
    {
        using var file = Ocf(NullableThenInt, index == 2 ? IndexTwo : IndexMinusOne);
        var e = AssertRefused(file);
        Assert.Contains($"index {index} ", e.Message);
    }

    [Fact]
    public void APromotedNullableUnionIndexOutOfRange_IsRefused()
    {
        const string writer = """{"type": "record", "name": "T", "fields": [{"name": "a", "type": ["null", "int"]}]}""";
        const string reader = """{"type": "record", "name": "T", "fields": [{"name": "a", "type": ["null", "long"]}]}""";
        using var file = Ocf(writer, [0x04, 0x0A]);
        AssertRefused(file, new AvroSchema(reader));
    }

    [Fact]
    public void AGeneralUnionIndexOutOfRange_IsRefused()
    {
        const string schema = """{"type": "record", "name": "T", "fields": [{"name": "u", "type": ["int", "string", "boolean"]}]}""";
        using var file = Ocf(schema, [0x06, 0x01]);
        var e = AssertRefused(file);
        Assert.Contains("union of 3 branches", e.Message);
    }

    /// <summary>A projection skips <c>a</c>; skipping indexed the union's branches with the bad index.</summary>
    [Fact]
    public void ASkippedUnionIndexOutOfRange_IsRefused()
    {
        const string reader = """{"type": "record", "name": "T", "fields": [{"name": "b", "type": "int"}]}""";
        using var file = Ocf(NullableThenInt, IndexTwo);
        AssertRefused(file, new AvroSchema(reader));
    }

    [Fact]
    public void ASkippedValidUnion_Reads()
    {
        const string reader = """{"type": "record", "name": "T", "fields": [{"name": "b", "type": "int"}]}""";
        using var file = Ocf(NullableThenInt, Valid);
        using var r = new AvroReaderBuilder().WithReaderSchema(new AvroSchema(reader)).Build(file);
        Assert.Equal(5, ((Int32Array)r.ReadNextBatch()!.Column("b")).GetValue(0));
    }

    [SkippableFact]
    public void Fastavro_RefusesTheSameRecords_AndReadsTheValidOne()
    {
        Skip.IfNot(FastavroOracle.IsAvailable, "fastavro is not installed.");

        using (var valid = Ocf(NullableThenInt, Valid))
        {
            var outcome = FastavroOracle.Read(valid);
            Assert.Null(outcome.Error);
            var row = outcome.Records!.Value[0];
            Assert.Equal("x", row.GetProperty("a").GetString());
            Assert.Equal(5, row.GetProperty("b").GetInt32());
        }

        const string general = """{"type": "record", "name": "T", "fields": [{"name": "u", "type": ["int", "string", "boolean"]}]}""";
        foreach (var file in new[] { Ocf(NullableThenInt, IndexTwo), Ocf(general, [0x06, 0x01]) })
        {
            using (file)
                Assert.Equal("IndexError", FastavroOracle.Read(file).Error);
        }
    }

    /// <summary>
    /// fastavro (1.12.2) indexes the branch list with the index as read, so Python's negative indexing takes
    /// -1 as the last branch. The specification gives the index as a position in the union and Java's Avro
    /// refuses a negative one; EngineeredWood refuses it too (<see cref="ANullableUnionIndexOutOfRange_IsRefused"/>).
    /// Pinned so that a fastavro change shows up here rather than being mistaken for ours.
    /// </summary>
    [SkippableFact]
    public void Fastavro_TakesIndexMinusOneAsTheLastBranch()
    {
        Skip.IfNot(FastavroOracle.IsAvailable, "fastavro is not installed.");

        using var file = Ocf(NullableThenInt, IndexMinusOne);
        var outcome = FastavroOracle.Read(file);
        Assert.Null(outcome.Error);
        Assert.Equal("x", outcome.Records!.Value[0].GetProperty("a").GetString());
    }
}
