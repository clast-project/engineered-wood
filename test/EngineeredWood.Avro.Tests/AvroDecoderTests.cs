// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace EngineeredWood.Avro.Tests;

public class AvroDecoderTests
{
    private static readonly string SchemaAJson = """
        {
            "type": "record",
            "name": "TestA",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
        }
        """;

    private static readonly string SchemaBJson = """
        {
            "type": "record",
            "name": "TestB",
            "fields": [
                {"name": "x", "type": "double"},
                {"name": "y", "type": "double"}
            ]
        }
        """;

    private static Apache.Arrow.Schema SimpleArrowSchema => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int32Type.Default, false))
        .Field(new Field("name", StringType.Default, false))
        .Build();

    private static RecordBatch MakeSimpleBatch(int rowCount)
    {
        var schema = SimpleArrowSchema;
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
    public void Decode_SoeFramedMessages_SingleSchema()
    {
        var avroSchema = new AvroSchema(SchemaAJson);
        var arrowSchema = avroSchema.ToArrowSchema();

        var store = new SchemaStore();
        store.Register(avroSchema);

        // Encode
        using var encoder = new AvroWriterBuilder(arrowSchema)
            .WithAvroSchema(avroSchema)
            .WithFingerprintStrategy(new FingerprintStrategy.Soe())
            .BuildEncoder();

        var batch = MakeBatchForSchemaA(3);
        encoder.Encode(batch);
        var rows = encoder.Flush();

        // Decode
        using var decoder = new AvroReaderBuilder()
            .WithWriterSchemaStore(store)
            .WithWireFormat(AvroWireFormat.SingleObject)
            .WithBatchSize(100)
            .BuildDecoder();

        for (int i = 0; i < rows.Count; i++)
            decoder.Decode(rows[i].Span);

        var result = decoder.Flush();
        Assert.NotNull(result);
        Assert.Equal(3, result!.Length);

        var ids = (Int32Array)result.Column(0);
        for (int i = 0; i < 3; i++)
            Assert.Equal(i, ids.GetValue(i));
    }

    [Fact]
    public void Decode_ConfluentFramedMessages()
    {
        var avroSchema = new AvroSchema(SchemaAJson);
        var arrowSchema = avroSchema.ToArrowSchema();
        uint schemaId = 77;

        var store = new SchemaStore();
        store.Set(new SchemaFingerprint.ConfluentId(schemaId), avroSchema);

        // Encode
        using var encoder = new AvroWriterBuilder(arrowSchema)
            .WithAvroSchema(avroSchema)
            .WithFingerprintStrategy(new FingerprintStrategy.Confluent(schemaId))
            .BuildEncoder();

        var batch = MakeBatchForSchemaA(2);
        encoder.Encode(batch);
        var rows = encoder.Flush();

        // Decode
        using var decoder = new AvroReaderBuilder()
            .WithWriterSchemaStore(store)
            .WithWireFormat(AvroWireFormat.Confluent)
            .WithBatchSize(100)
            .BuildDecoder();

        for (int i = 0; i < rows.Count; i++)
            decoder.Decode(rows[i].Span);

        var result = decoder.Flush();
        Assert.NotNull(result);
        Assert.Equal(2, result!.Length);
    }

    [Fact]
    public void Decode_ApicurioFramedMessages()
    {
        var avroSchema = new AvroSchema(SchemaAJson);
        var arrowSchema = avroSchema.ToArrowSchema();
        ulong globalId = 999UL;

        var store = new SchemaStore();
        store.Set(new SchemaFingerprint.ApicurioId(globalId), avroSchema);

        using var encoder = new AvroWriterBuilder(arrowSchema)
            .WithAvroSchema(avroSchema)
            .WithFingerprintStrategy(new FingerprintStrategy.Apicurio(globalId))
            .BuildEncoder();

        var batch = MakeBatchForSchemaA(2);
        encoder.Encode(batch);
        var rows = encoder.Flush();

        using var decoder = new AvroReaderBuilder()
            .WithWriterSchemaStore(store)
            .WithWireFormat(AvroWireFormat.Apicurio)
            .WithBatchSize(100)
            .BuildDecoder();

        for (int i = 0; i < rows.Count; i++)
            decoder.Decode(rows[i].Span);

        var result = decoder.Flush();
        Assert.NotNull(result);
        Assert.Equal(2, result!.Length);
    }

    [Fact]
    public void Flush_EmptyDecoder_ReturnsNull()
    {
        var store = new SchemaStore();
        using var decoder = new AvroReaderBuilder()
            .WithWriterSchemaStore(store)
            .WithWireFormat(AvroWireFormat.SingleObject)
            .BuildDecoder();

        var result = decoder.Flush();
        Assert.Null(result);
    }

    [Fact]
    public void Decode_BatchSizeAutoFlush()
    {
        var avroSchema = new AvroSchema(SchemaAJson);
        var arrowSchema = avroSchema.ToArrowSchema();

        var store = new SchemaStore();
        store.Register(avroSchema);

        using var encoder = new AvroWriterBuilder(arrowSchema)
            .WithAvroSchema(avroSchema)
            .WithFingerprintStrategy(new FingerprintStrategy.Soe())
            .BuildEncoder();

        // Encode 5 rows
        encoder.Encode(MakeBatchForSchemaA(5));
        var rows = encoder.Flush();

        // Decode with batch size 3 — should auto-flush after 3rd message
        using var decoder = new AvroReaderBuilder()
            .WithWriterSchemaStore(store)
            .WithWireFormat(AvroWireFormat.SingleObject)
            .WithBatchSize(3)
            .BuildDecoder();

        RecordBatch? autoFlushed = null;
        for (int i = 0; i < rows.Count; i++)
        {
            var result = decoder.Decode(rows[i].Span);
            if (result != null)
                autoFlushed = result;
        }

        Assert.NotNull(autoFlushed);
        Assert.Equal(3, autoFlushed!.Length);

        // Remaining 2 rows
        var remaining = decoder.Flush();
        Assert.NotNull(remaining);
        Assert.Equal(2, remaining!.Length);
    }

    [Fact]
    public void Decode_SchemaSwitch_ImplicitFlush()
    {
        var schemaA = new AvroSchema(SchemaAJson);
        var schemaB = new AvroSchema(SchemaBJson);
        var arrowA = schemaA.ToArrowSchema();
        var arrowB = schemaB.ToArrowSchema();

        var store = new SchemaStore();
        store.Register(schemaA);
        store.Register(schemaB);

        // Encode messages from schema A
        using var encoderA = new AvroWriterBuilder(arrowA)
            .WithAvroSchema(schemaA)
            .WithFingerprintStrategy(new FingerprintStrategy.Soe())
            .BuildEncoder();
        encoderA.Encode(MakeBatchForSchemaA(2));
        var rowsA = encoderA.Flush();

        // Encode messages from schema B
        using var encoderB = new AvroWriterBuilder(arrowB)
            .WithAvroSchema(schemaB)
            .WithFingerprintStrategy(new FingerprintStrategy.Soe())
            .BuildEncoder();
        encoderB.Encode(MakeBatchForSchemaB(3));
        var rowsB = encoderB.Flush();

        // Decode interleaved: A, A, B, B, B
        using var decoder = new AvroReaderBuilder()
            .WithWriterSchemaStore(store)
            .WithWireFormat(AvroWireFormat.SingleObject)
            .WithBatchSize(100)
            .BuildDecoder();

        // Feed A messages — no flush expected
        Assert.Null(decoder.Decode(rowsA[0].Span));
        Assert.Null(decoder.Decode(rowsA[1].Span));

        // Feed first B message — should trigger implicit flush of A rows
        var flushedA = decoder.Decode(rowsB[0].Span);
        Assert.NotNull(flushedA);
        Assert.Equal(2, flushedA!.Length);

        // Feed remaining B messages
        Assert.Null(decoder.Decode(rowsB[1].Span));
        Assert.Null(decoder.Decode(rowsB[2].Span));

        var flushedB = decoder.Flush();
        Assert.NotNull(flushedB);
        Assert.Equal(3, flushedB!.Length);
    }

    [Fact]
    public void Encode_Decode_FullRoundTrip()
    {
        var schema = SimpleArrowSchema;
        var batch = MakeSimpleBatch(10);

        // Encode
        using var encoder = new AvroWriterBuilder(schema)
            .WithFingerprintStrategy(new FingerprintStrategy.Soe())
            .BuildEncoder();
        encoder.Encode(batch);
        var rows = encoder.Flush();

        // Decode
        var avroSchema = AvroSchema.FromArrowSchema(schema);
        var store = new SchemaStore();
        store.Register(avroSchema);

        using var decoder = new AvroReaderBuilder()
            .WithWriterSchemaStore(store)
            .WithWireFormat(AvroWireFormat.SingleObject)
            .WithBatchSize(100)
            .BuildDecoder();

        for (int i = 0; i < rows.Count; i++)
            decoder.Decode(rows[i].Span);

        var result = decoder.Flush()!;
        Assert.Equal(10, result.Length);

        var ids = (Int32Array)result.Column(0);
        var names = (StringArray)result.Column(1);
        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(i, ids.GetValue(i));
            Assert.Equal($"row_{i}", names.GetString(i));
        }
    }

    [Fact]
    public void Decode_SchemaEvolution_WriterA_ReaderB()
    {
        // Writer schema: {id: int, name: string}
        // Reader schema: {id: long, name: string, tag: string default ""}
        var writerSchemaJson = """
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"}
                ]
            }
            """;
        var readerSchemaJson = """
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "tag", "type": "string", "default": ""}
                ]
            }
            """;

        var writerSchema = new AvroSchema(writerSchemaJson);
        var readerSchema = new AvroSchema(readerSchemaJson);
        var writerArrow = writerSchema.ToArrowSchema();

        var store = new SchemaStore();
        store.Register(writerSchema);

        // Encode using writer schema
        using var encoder = new AvroWriterBuilder(writerArrow)
            .WithAvroSchema(writerSchema)
            .WithFingerprintStrategy(new FingerprintStrategy.Soe())
            .BuildEncoder();

        var batch = MakeBatchForSchemaA(3);
        encoder.Encode(batch);
        var rows = encoder.Flush();

        // Decode with reader schema (evolution)
        using var decoder = new AvroReaderBuilder()
            .WithWriterSchemaStore(store)
            .WithReaderSchema(readerSchema)
            .WithWireFormat(AvroWireFormat.SingleObject)
            .WithBatchSize(100)
            .BuildDecoder();

        for (int i = 0; i < rows.Count; i++)
            decoder.Decode(rows[i].Span);

        var result = decoder.Flush()!;
        Assert.Equal(3, result.Length);
        Assert.Equal(3, result.ColumnCount); // id, name, tag

        // id should be promoted to long
        var ids = (Int64Array)result.Column(0);
        for (int i = 0; i < 3; i++)
            Assert.Equal(i, ids.GetValue(i));

        // name preserved
        var names = (StringArray)result.Column(1);
        for (int i = 0; i < 3; i++)
            Assert.Equal($"name_{i}", names.GetString(i));

        // tag should have default ""
        var tags = (StringArray)result.Column(2);
        for (int i = 0; i < 3; i++)
            Assert.Equal("", tags.GetString(i));
    }

    [Fact]
    public void BuildDecoder_WithoutStore_Throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
            new AvroReaderBuilder()
                .WithWireFormat(AvroWireFormat.SingleObject)
                .BuildDecoder());
    }

    // ─── Helpers ───

    private static RecordBatch MakeBatchForSchemaA(int rowCount)
    {
        var avroSchema = new AvroSchema(SchemaAJson);
        var arrowSchema = avroSchema.ToArrowSchema();

        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            idBuilder.Append(i);
            nameBuilder.Append($"name_{i}");
        }
        return new RecordBatch(arrowSchema, [idBuilder.Build(), nameBuilder.Build()], rowCount);
    }

    private static RecordBatch MakeBatchForSchemaB(int rowCount)
    {
        var avroSchema = new AvroSchema(SchemaBJson);
        var arrowSchema = avroSchema.ToArrowSchema();

        var xBuilder = new DoubleArray.Builder();
        var yBuilder = new DoubleArray.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            xBuilder.Append(i * 1.5);
            yBuilder.Append(i * 2.5);
        }
        return new RecordBatch(arrowSchema, [xBuilder.Build(), yBuilder.Build()], rowCount);
    }
}
