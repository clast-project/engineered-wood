// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace EngineeredWood.Avro.Tests;

public class AvroEncoderTests
{
    private static Apache.Arrow.Schema SimpleSchema => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int32Type.Default, false))
        .Field(new Field("name", StringType.Default, false))
        .Build();

    private static RecordBatch MakeSimpleBatch(int rowCount)
    {
        var schema = SimpleSchema;
        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            idBuilder.Append(i);
            nameBuilder.Append($"row_{i}");
        }
        return new RecordBatch(schema, [idBuilder.Build(), nameBuilder.Build()], rowCount);
    }

    [Fact]
    public void Encode_SoeFraming_HeaderBytes()
    {
        var schema = SimpleSchema;
        var batch = MakeSimpleBatch(3);

        using var encoder = new AvroWriterBuilder(schema)
            .WithFingerprintStrategy(new FingerprintStrategy.Soe())
            .BuildEncoder();

        encoder.Encode(batch);
        var rows = encoder.Flush();

        Assert.Equal(3, rows.Count);
        Assert.False(rows.IsEmpty);

        for (int i = 0; i < rows.Count; i++)
        {
            var row = rows[i].Span;
            // SOE header: 0xC3, 0x01, then 8-byte LE fingerprint
            Assert.True(row.Length >= 10, "SOE row must be at least 10 bytes (header).");
            Assert.Equal(0xC3, row[0]);
            Assert.Equal(0x01, row[1]);
        }

        // All rows should have the same fingerprint (same schema)
        ulong fp0 = BinaryPrimitives.ReadUInt64LittleEndian(rows[0].Span.Slice(2));
        for (int i = 1; i < rows.Count; i++)
        {
            ulong fpi = BinaryPrimitives.ReadUInt64LittleEndian(rows[i].Span.Slice(2));
            Assert.Equal(fp0, fpi);
        }
    }

    [Fact]
    public void Encode_ConfluentFraming_HeaderBytes()
    {
        var schema = SimpleSchema;
        var batch = MakeSimpleBatch(2);
        uint schemaId = 42;

        using var encoder = new AvroWriterBuilder(schema)
            .WithFingerprintStrategy(new FingerprintStrategy.Confluent(schemaId))
            .BuildEncoder();

        encoder.Encode(batch);
        var rows = encoder.Flush();

        Assert.Equal(2, rows.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            var row = rows[i].Span;
            Assert.True(row.Length >= 5);
            Assert.Equal(0x00, row[0]);
            uint id = BinaryPrimitives.ReadUInt32BigEndian(row.Slice(1));
            Assert.Equal(schemaId, id);
        }
    }

    [Fact]
    public void Encode_ApicurioFraming_HeaderBytes()
    {
        var schema = SimpleSchema;
        var batch = MakeSimpleBatch(2);
        ulong globalId = 12345UL;

        using var encoder = new AvroWriterBuilder(schema)
            .WithFingerprintStrategy(new FingerprintStrategy.Apicurio(globalId))
            .BuildEncoder();

        encoder.Encode(batch);
        var rows = encoder.Flush();

        Assert.Equal(2, rows.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            var row = rows[i].Span;
            Assert.True(row.Length >= 9);
            Assert.Equal(0x00, row[0]);
            ulong id = BinaryPrimitives.ReadUInt64BigEndian(row.Slice(1));
            Assert.Equal(globalId, id);
        }
    }

    [Fact]
    public void EncodedRows_Indexer_AsEnumerable_GetBuffer()
    {
        var schema = SimpleSchema;
        var batch = MakeSimpleBatch(3);

        using var encoder = new AvroWriterBuilder(schema)
            .WithFingerprintStrategy(new FingerprintStrategy.Soe())
            .BuildEncoder();

        encoder.Encode(batch);
        var rows = encoder.Flush();

        // Count
        Assert.Equal(3, rows.Count);

        // Indexer range
        var row0 = rows[0];
        Assert.True(row0.Length > 0);

        // AsEnumerable
        var enumerated = rows.AsEnumerable().ToList();
        Assert.Equal(3, enumerated.Count);
        Assert.True(row0.Span.SequenceEqual(enumerated[0].Span));

        // GetBuffer contains all rows contiguously
        var buffer = rows.GetBuffer();
        int totalLen = 0;
        for (int i = 0; i < rows.Count; i++)
            totalLen += rows[i].Length;
        Assert.Equal(totalLen, buffer.Length);
    }

    [Fact]
    public void EncodedRows_Empty()
    {
        var schema = SimpleSchema;

        using var encoder = new AvroWriterBuilder(schema)
            .WithFingerprintStrategy(new FingerprintStrategy.Soe())
            .BuildEncoder();

        var rows = encoder.Flush();
        Assert.Equal(0, rows.Count);
        Assert.True(rows.IsEmpty);
    }

    [Fact]
    public void Encode_Flush_Encode_Flush_IndependentBatches()
    {
        var schema = SimpleSchema;

        using var encoder = new AvroWriterBuilder(schema)
            .WithFingerprintStrategy(new FingerprintStrategy.Confluent(1))
            .BuildEncoder();

        // First batch
        encoder.Encode(MakeSimpleBatch(2));
        var rows1 = encoder.Flush();
        Assert.Equal(2, rows1.Count);

        // Second batch
        encoder.Encode(MakeSimpleBatch(3));
        var rows2 = encoder.Flush();
        Assert.Equal(3, rows2.Count);
    }

    [Fact]
    public void Encode_SoeRoundTrip_DecodeManually()
    {
        // Encode with SOE framing, then strip header and decode the Avro payload
        var schema = SimpleSchema;
        var batch = MakeSimpleBatch(5);

        using var encoder = new AvroWriterBuilder(schema)
            .WithFingerprintStrategy(new FingerprintStrategy.Soe())
            .BuildEncoder();

        encoder.Encode(batch);
        var rows = encoder.Flush();

        // Decode each row manually: skip 10-byte SOE header, read Avro binary
        var avroSchema = AvroSchema.FromArrowSchema(schema);
        var store = new SchemaStore();
        store.Register(avroSchema);

        var decoder = new AvroReaderBuilder()
            .WithWriterSchemaStore(store)
            .WithWireFormat(AvroWireFormat.SingleObject)
            .WithBatchSize(10)
            .BuildDecoder();

        for (int i = 0; i < rows.Count; i++)
            decoder.Decode(rows[i].Span);

        var result = decoder.Flush();
        Assert.NotNull(result);
        Assert.Equal(5, result!.Length);

        var ids = (Int32Array)result.Column(0);
        var names = (StringArray)result.Column(1);
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(i, ids.GetValue(i));
            Assert.Equal($"row_{i}", names.GetString(i));
        }
    }

    [Fact]
    public void BuildEncoder_WithoutStrategy_Throws()
    {
        var schema = SimpleSchema;
        Assert.Throws<InvalidOperationException>(() =>
            new AvroWriterBuilder(schema).BuildEncoder());
    }
}
