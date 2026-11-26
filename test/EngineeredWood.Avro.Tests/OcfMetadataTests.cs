// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Utf8 = System.Text.Encoding;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Avro.Tests.Schema;

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// User metadata in the OCF header (Iceberg keeps a manifest's schema, partition spec and format version
/// there), and schemas written to the header keeping their extra attributes.
/// </summary>
public class OcfMetadataTests
{
    private static readonly Apache.Arrow.Schema ArrowSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Batch() =>
        new(ArrowSchema, [new Int64Array.Builder().Append(1).Append(2).Build()], 2);

    private const string SchemaWithIds = """
        {"type": "record", "name": "r", "fields": [{"name": "id", "type": "long", "field-id": 7}]}
        """;

    [Fact]
    public void Metadata_RoundTrips()
    {
        var ms = new MemoryStream();
        var binary = new byte[] { 0, 1, 2, 255 };
        using (var writer = new AvroWriterBuilder(ArrowSchema)
                   .WithMetadata("format-version", "2")
                   .WithMetadata("partition-spec", "[]")
                   .WithMetadata("binary", binary)
                   .WithCompression(AvroCodec.Deflate)
                   .Build(ms))
        {
            writer.Write(Batch());
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        Assert.Equal("2", Utf8.UTF8.GetString(reader.Metadata["format-version"]));
        Assert.Equal("[]", Utf8.UTF8.GetString(reader.Metadata["partition-spec"]));
        Assert.Equal(binary, reader.Metadata["binary"]);
        Assert.Equal("deflate", Utf8.UTF8.GetString(reader.Metadata["avro.codec"]));
        Assert.Equal(2, reader.ReadNextBatch()!.Length);
    }

    [Fact]
    public async Task Metadata_RoundTripsAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await new AvroWriterBuilder(ArrowSchema)
                         .WithMetadata("content", "data")
                         .BuildAsync(ms))
        {
            await writer.WriteAsync(Batch());
        }

        ms.Position = 0;
        await using var reader = await new AvroReaderBuilder().BuildAsync(ms);
        Assert.Equal("data", Utf8.UTF8.GetString(reader.Metadata["content"]));
    }

    [Fact]
    public void LaterValue_ReplacesEarlier()
    {
        var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(ArrowSchema)
                   .WithMetadata("k", "first")
                   .WithMetadata("k", "second")
                   .Build(ms))
        {
            writer.Write(Batch());
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        Assert.Equal("second", Utf8.UTF8.GetString(reader.Metadata["k"]));
    }

    [Theory]
    [InlineData("avro.schema")]
    [InlineData("avro.codec")]
    [InlineData("avro.anything")]
    public void ReservedKeys_AreRefused(string key)
    {
        var builder = new AvroWriterBuilder(ArrowSchema);
        Assert.Throws<ArgumentException>(() => builder.WithMetadata(key, "x"));
    }

    [Fact]
    public void ValueIsCopied()
    {
        var value = Utf8.UTF8.GetBytes("abc");
        var builder = new AvroWriterBuilder(ArrowSchema).WithMetadata("k", value);
        value[0] = (byte)'z';

        var ms = new MemoryStream();
        using (var writer = builder.Build(ms))
            writer.Write(Batch());

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        Assert.Equal("abc", Utf8.UTF8.GetString(reader.Metadata["k"]));
    }

    [Fact]
    public void ExplicitSchema_KeepsItsAttributesInTheHeader()
    {
        var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(ArrowSchema)
                   .WithAvroSchema(new AvroSchema(SchemaWithIds))
                   .Build(ms))
        {
            writer.Write(Batch());
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        var headerSchema = Utf8.UTF8.GetString(reader.Metadata["avro.schema"]);
        Assert.Equal(SchemaAttributeTests.Canonical(SchemaWithIds), SchemaAttributeTests.Canonical(headerSchema));
        var root = (AvroRecordSchema)AvroSchemaParser.Parse(headerSchema);
        Assert.Equal(7, root.Fields[0].FieldId);
    }
}
