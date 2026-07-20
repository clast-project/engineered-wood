// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// Tests for gap fixes: batch size splitting, nested record resolution, enum symbol evolution.
/// </summary>
public class AvroGapFixTests
{
    // ─── Helpers ───

    private static byte[] WriteTestData(Apache.Arrow.Schema arrowSchema, RecordBatch batch,
        AvroCodec codec = AvroCodec.Null, string? avroSchemaJson = null)
    {
        using var ms = new MemoryStream();
        var builder = new AvroWriterBuilder(arrowSchema)
            .WithCompression(codec);
        if (avroSchemaJson != null)
            builder = builder.WithAvroSchema(new AvroSchema(avroSchemaJson));
        using (var writer = builder.Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }
        return ms.ToArray();
    }

    private static RecordBatch ReadWithSchema(byte[] data, AvroSchema readerSchema, int batchSize = 1024)
    {
        using var ms = new MemoryStream(data);
        using var reader = new AvroReaderBuilder()
            .WithBatchSize(batchSize)
            .WithReaderSchema(readerSchema)
            .Build(ms);
        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        return result;
    }

    private static List<RecordBatch> ReadAllBatches(byte[] data, int batchSize)
    {
        using var ms = new MemoryStream(data);
        using var reader = new AvroReaderBuilder()
            .WithBatchSize(batchSize)
            .Build(ms);

        var batches = new List<RecordBatch>();
        foreach (var batch in reader)
            batches.Add(batch);
        return batches;
    }

    private static async Task<List<RecordBatch>> ReadAllBatchesAsync(byte[] data, int batchSize)
    {
        using var ms = new MemoryStream(data);
        await using var reader = await new AvroReaderBuilder()
            .WithBatchSize(batchSize)
            .BuildAsync(ms);

        var batches = new List<RecordBatch>();
        await foreach (var batch in reader)
            batches.Add(batch);
        return batches;
    }

    // ═══════════════════════════════════════════════════════════════
    // 1. Batch size honored in OCF reader
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public void BatchSize_SplitsLargeBlockIntoMultipleBatches()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Build();

        // Write 100 rows in a single block
        var idBuilder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++)
            idBuilder.Append(i);
        var batch = new RecordBatch(schema, [idBuilder.Build()], 100);
        var data = WriteTestData(schema, batch);

        // Read with batchSize=30 — should get 4 batches: 30, 30, 30, 10
        var batches = ReadAllBatches(data, batchSize: 30);

        Assert.Equal(4, batches.Count);
        Assert.Equal(30, batches[0].Length);
        Assert.Equal(30, batches[1].Length);
        Assert.Equal(30, batches[2].Length);
        Assert.Equal(10, batches[3].Length);

        // Verify values are sequential across batches
        int expected = 0;
        foreach (var b in batches)
        {
            var arr = (Int32Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                Assert.Equal(expected++, arr.GetValue(i));
        }
        Assert.Equal(100, expected);
    }

    [Fact]
    public void BatchSize_ExactMultiple_ProducesEqualBatches()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 50; i++)
            builder.Append(i);
        var batch = new RecordBatch(schema, [builder.Build()], 50);
        var data = WriteTestData(schema, batch);

        // 50 rows / 25 = exactly 2 batches
        var batches = ReadAllBatches(data, batchSize: 25);

        Assert.Equal(2, batches.Count);
        Assert.Equal(25, batches[0].Length);
        Assert.Equal(25, batches[1].Length);
    }

    [Fact]
    public void BatchSize_LargerThanBlock_ReturnsEntireBlock()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 10; i++)
            builder.Append(i);
        var batch = new RecordBatch(schema, [builder.Build()], 10);
        var data = WriteTestData(schema, batch);

        // batchSize=1000 > 10 rows → single batch
        var batches = ReadAllBatches(data, batchSize: 1000);

        Assert.Single(batches);
        Assert.Equal(10, batches[0].Length);
    }

    [Fact]
    public void BatchSize_WithStringColumns_SplitsCorrectly()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("name", StringType.Default, false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        for (int i = 0; i < 20; i++)
        {
            idBuilder.Append(i);
            nameBuilder.Append($"row_{i}");
        }
        var batch = new RecordBatch(schema,
            [idBuilder.Build(), nameBuilder.Build()], 20);
        var data = WriteTestData(schema, batch);

        var batches = ReadAllBatches(data, batchSize: 7);

        // 20 / 7 → 3 batches of 7, 7, 6
        Assert.Equal(3, batches.Count);
        Assert.Equal(7, batches[0].Length);
        Assert.Equal(7, batches[1].Length);
        Assert.Equal(6, batches[2].Length);

        // Verify string values
        Assert.Equal("row_0", ((StringArray)batches[0].Column(1)).GetString(0));
        Assert.Equal("row_7", ((StringArray)batches[1].Column(1)).GetString(0));
        Assert.Equal("row_14", ((StringArray)batches[2].Column(1)).GetString(0));
    }

    [Fact]
    public async Task BatchSize_AsyncReader_SplitsLargeBlock()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 45; i++)
            builder.Append(i);
        var batch = new RecordBatch(schema, [builder.Build()], 45);
        var data = WriteTestData(schema, batch);

        // Async reader with batchSize=20 → 3 batches: 20, 20, 5
        var batches = await ReadAllBatchesAsync(data, batchSize: 20);

        Assert.Equal(3, batches.Count);
        Assert.Equal(20, batches[0].Length);
        Assert.Equal(20, batches[1].Length);
        Assert.Equal(5, batches[2].Length);
    }

    [Fact]
    public void BatchSize_NullableColumns_SplitsCorrectly()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", Int32Type.Default, true))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 15; i++)
        {
            if (i % 3 == 0)
                builder.AppendNull();
            else
                builder.Append(i * 10);
        }
        var batch = new RecordBatch(schema, [builder.Build()], 15);
        var data = WriteTestData(schema, batch);

        var batches = ReadAllBatches(data, batchSize: 4);

        // 15 / 4 → 4 batches: 4, 4, 4, 3
        Assert.Equal(4, batches.Count);

        // Verify null pattern preserved in first batch (indices 0,1,2,3)
        var firstArr = (Int32Array)batches[0].Column(0);
        Assert.False(firstArr.IsValid(0)); // i=0, null
        Assert.True(firstArr.IsValid(1));  // i=1, 10
        Assert.True(firstArr.IsValid(2));  // i=2, 20
        Assert.False(firstArr.IsValid(3)); // i=3, null
    }

    // ═══════════════════════════════════════════════════════════════
    // 2. Nested record schema evolution
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public void NestedRecord_AddField_UsesDefault()
    {
        // Writer: record with inner struct {x: int}
        var writerSchemaJson = """
        {
            "type": "record", "name": "Outer",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "inner", "type": {
                    "type": "record", "name": "Inner",
                    "fields": [{"name": "x", "type": "int"}]
                }}
            ]
        }
        """;

        var innerType = new StructType([new Field("x", Int32Type.Default, false)]);
        var writerArrow = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("inner", innerType, false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        idBuilder.Append(1);
        idBuilder.Append(2);
        var xBuilder = new Int32Array.Builder();
        xBuilder.Append(10);
        xBuilder.Append(20);
        var structArr = new StructArray(innerType, 2,
            [xBuilder.Build()], ArrowBuffer.Empty);
        var batch = new RecordBatch(writerArrow,
            [idBuilder.Build(), structArr], 2);

        var data = WriteTestData(writerArrow, batch, avroSchemaJson: writerSchemaJson);

        // Reader: inner struct has extra field {x: int, y: string (default="N/A")}
        var readerSchemaJson = """
        {
            "type": "record", "name": "Outer",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "inner", "type": {
                    "type": "record", "name": "Inner",
                    "fields": [
                        {"name": "x", "type": "int"},
                        {"name": "y", "type": "string", "default": "N/A"}
                    ]
                }}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));

        Assert.Equal(2, result.Length);
        Assert.Equal(1, ((Int32Array)result.Column(0)).GetValue(0));
        Assert.Equal(2, ((Int32Array)result.Column(0)).GetValue(1));

        var inner = (StructArray)result.Column(1);
        var xArr = (Int32Array)inner.Fields[0];
        var yArr = (StringArray)inner.Fields[1];
        Assert.Equal(10, xArr.GetValue(0));
        Assert.Equal(20, xArr.GetValue(1));
        Assert.Equal("N/A", yArr.GetString(0));
        Assert.Equal("N/A", yArr.GetString(1));
    }

    [Fact]
    public void NestedRecord_RemoveField_SkipsIt()
    {
        // Writer: inner has {x: int, y: string}
        var writerSchemaJson = """
        {
            "type": "record", "name": "Outer",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "inner", "type": {
                    "type": "record", "name": "Inner",
                    "fields": [
                        {"name": "x", "type": "int"},
                        {"name": "y", "type": "string"}
                    ]
                }}
            ]
        }
        """;

        var innerType = new StructType([
            new Field("x", Int32Type.Default, false),
            new Field("y", StringType.Default, false),
        ]);
        var writerArrow = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("inner", innerType, false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        idBuilder.Append(1);
        var xBuilder = new Int32Array.Builder();
        xBuilder.Append(42);
        var yBuilder = new StringArray.Builder();
        yBuilder.Append("drop_me");
        var structArr = new StructArray(innerType, 1,
            [xBuilder.Build(), yBuilder.Build()], ArrowBuffer.Empty);
        var batch = new RecordBatch(writerArrow,
            [idBuilder.Build(), structArr], 1);

        var data = WriteTestData(writerArrow, batch, avroSchemaJson: writerSchemaJson);

        // Reader: inner only has {x: int} — y is dropped
        var readerSchemaJson = """
        {
            "type": "record", "name": "Outer",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "inner", "type": {
                    "type": "record", "name": "Inner",
                    "fields": [{"name": "x", "type": "int"}]
                }}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));

        Assert.Equal(1, result.Length);
        Assert.Equal(1, ((Int32Array)result.Column(0)).GetValue(0));
        var inner = (StructArray)result.Column(1);
        Assert.Single(inner.Fields);
        Assert.Equal(42, ((Int32Array)inner.Fields[0]).GetValue(0));
    }

    [Fact]
    public void NestedRecord_ReorderFields()
    {
        // Writer: inner has {a: int, b: string}
        var writerSchemaJson = """
        {
            "type": "record", "name": "Outer",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "inner", "type": {
                    "type": "record", "name": "Inner",
                    "fields": [
                        {"name": "a", "type": "int"},
                        {"name": "b", "type": "string"}
                    ]
                }}
            ]
        }
        """;

        var innerType = new StructType([
            new Field("a", Int32Type.Default, false),
            new Field("b", StringType.Default, false),
        ]);
        var writerArrow = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("inner", innerType, false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        idBuilder.Append(1);
        var aBuilder = new Int32Array.Builder();
        aBuilder.Append(99);
        var bBuilder = new StringArray.Builder();
        bBuilder.Append("hello");
        var structArr = new StructArray(innerType, 1,
            [aBuilder.Build(), bBuilder.Build()], ArrowBuffer.Empty);
        var batch = new RecordBatch(writerArrow,
            [idBuilder.Build(), structArr], 1);

        var data = WriteTestData(writerArrow, batch, avroSchemaJson: writerSchemaJson);

        // Reader: inner has {b: string, a: int} — reversed
        var readerSchemaJson = """
        {
            "type": "record", "name": "Outer",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "inner", "type": {
                    "type": "record", "name": "Inner",
                    "fields": [
                        {"name": "b", "type": "string"},
                        {"name": "a", "type": "int"}
                    ]
                }}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));

        Assert.Equal(1, result.Length);
        var inner = (StructArray)result.Column(1);
        // Reader order: b first, a second
        Assert.Equal("hello", ((StringArray)inner.Fields[0]).GetString(0));
        Assert.Equal(99, ((Int32Array)inner.Fields[1]).GetValue(0));
    }

    // ═══════════════════════════════════════════════════════════════
    // 3. Enum symbol evolution
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public void EnumEvolution_SubsetSymbols_RemapsCorrectly()
    {
        // Writer: enum with [RED, GREEN, BLUE]
        var writerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "color", "type": {
                    "type": "enum", "name": "Color",
                    "symbols": ["RED", "GREEN", "BLUE"]
                }}
            ]
        }
        """;

        // Build with DictionaryArray matching writer enum
        var writerArrow = new Apache.Arrow.Schema.Builder()
            .Field(new Field("color", new DictionaryType(Int32Type.Default, StringType.Default, false), false))
            .Build();

        var valBuilder = new StringArray.Builder();
        valBuilder.Append("RED");
        valBuilder.Append("GREEN");
        valBuilder.Append("BLUE");
        var indexBuilder = new Int32Array.Builder();
        indexBuilder.Append(0); // RED
        indexBuilder.Append(1); // GREEN
        indexBuilder.Append(2); // BLUE
        indexBuilder.Append(0); // RED
        var dictArr = new DictionaryArray(
            new DictionaryType(Int32Type.Default, StringType.Default, false),
            indexBuilder.Build(), valBuilder.Build());
        var batch = new RecordBatch(writerArrow, [dictArr], 4);
        var data = WriteTestData(writerArrow, batch, avroSchemaJson: writerSchemaJson);

        // Reader: enum with [BLUE, GREEN, RED] — reordered
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "color", "type": {
                    "type": "enum", "name": "Color",
                    "symbols": ["BLUE", "GREEN", "RED"]
                }}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(4, result.Length);

        var resultDict = (DictionaryArray)result.Column(0);
        var resultValues = (StringArray)resultDict.Dictionary;

        // The dictionary should be the reader's symbols
        Assert.Equal("BLUE", resultValues.GetString(0));
        Assert.Equal("GREEN", resultValues.GetString(1));
        Assert.Equal("RED", resultValues.GetString(2));

        // Indices should be remapped: RED→2, GREEN→1, BLUE→0
        var indices = (Int32Array)resultDict.Indices;
        Assert.Equal(2, indices.GetValue(0)); // RED → index 2 in reader
        Assert.Equal(1, indices.GetValue(1)); // GREEN → index 1
        Assert.Equal(0, indices.GetValue(2)); // BLUE → index 0
        Assert.Equal(2, indices.GetValue(3)); // RED → index 2
    }

    [Fact]
    public void EnumEvolution_NewSymbolInReader_NoIssue()
    {
        // Writer: [A, B], Reader: [A, B, C]
        var writerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "status", "type": {
                    "type": "enum", "name": "Status",
                    "symbols": ["A", "B"]
                }}
            ]
        }
        """;

        var writerArrow = new Apache.Arrow.Schema.Builder()
            .Field(new Field("status", new DictionaryType(Int32Type.Default, StringType.Default, false), false))
            .Build();

        var valBuilder = new StringArray.Builder();
        valBuilder.Append("A");
        valBuilder.Append("B");
        var indexBuilder = new Int32Array.Builder();
        indexBuilder.Append(0);
        indexBuilder.Append(1);
        var dictArr = new DictionaryArray(
            new DictionaryType(Int32Type.Default, StringType.Default, false),
            indexBuilder.Build(), valBuilder.Build());
        var batch = new RecordBatch(writerArrow, [dictArr], 2);
        var data = WriteTestData(writerArrow, batch, avroSchemaJson: writerSchemaJson);

        // Reader has extra symbol C
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "status", "type": {
                    "type": "enum", "name": "Status",
                    "symbols": ["A", "B", "C"]
                }}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(2, result.Length);

        var dict = (DictionaryArray)result.Column(0);
        var values = (StringArray)dict.Dictionary;
        Assert.Equal(3, values.Length); // A, B, C
        var indices = (Int32Array)dict.Indices;
        Assert.Equal(0, indices.GetValue(0)); // A
        Assert.Equal(1, indices.GetValue(1)); // B
    }

    [Fact]
    public void EnumEvolution_WriterSymbolNotInReader_FallsBackToDefault()
    {
        // Writer: [X, Y, Z], Reader: [X, Y] with default "X"
        var writerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "val", "type": {
                    "type": "enum", "name": "E",
                    "symbols": ["X", "Y", "Z"]
                }}
            ]
        }
        """;

        var writerArrow = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", new DictionaryType(Int32Type.Default, StringType.Default, false), false))
            .Build();

        var valBuilder = new StringArray.Builder();
        valBuilder.Append("X");
        valBuilder.Append("Y");
        valBuilder.Append("Z");
        var indexBuilder = new Int32Array.Builder();
        indexBuilder.Append(2); // Z — not in reader
        indexBuilder.Append(0); // X
        indexBuilder.Append(1); // Y
        var dictArr = new DictionaryArray(
            new DictionaryType(Int32Type.Default, StringType.Default, false),
            indexBuilder.Build(), valBuilder.Build());
        var batch = new RecordBatch(writerArrow, [dictArr], 3);
        var data = WriteTestData(writerArrow, batch, avroSchemaJson: writerSchemaJson);

        // Reader: [X, Y] with default "X" — Z maps to X
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "val", "type": {
                    "type": "enum", "name": "E",
                    "symbols": ["X", "Y"],
                    "default": "X"
                }}
            ]
        }
        """;

        var result = ReadWithSchema(data, new AvroSchema(readerSchemaJson));
        Assert.Equal(3, result.Length);

        var dict = (DictionaryArray)result.Column(0);
        var indices = (Int32Array)dict.Indices;
        Assert.Equal(0, indices.GetValue(0)); // Z → default X (index 0)
        Assert.Equal(0, indices.GetValue(1)); // X (index 0)
        Assert.Equal(1, indices.GetValue(2)); // Y (index 1)
    }

    [Fact]
    public void EnumEvolution_WriterSymbolNotInReader_NoDefault_Throws()
    {
        var writerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "val", "type": {
                    "type": "enum", "name": "E",
                    "symbols": ["X", "Y", "Z"]
                }}
            ]
        }
        """;
        var readerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "val", "type": {
                    "type": "enum", "name": "E",
                    "symbols": ["X", "Y"]
                }}
            ]
        }
        """;

        var writerRecord = (AvroRecordSchema)AvroSchemaParser.Parse(writerSchemaJson);
        var readerRecord = (AvroRecordSchema)AvroSchemaParser.Parse(readerSchemaJson);

        var ex = Assert.Throws<InvalidOperationException>(
            () => SchemaResolver.Resolve(writerRecord, readerRecord));
        Assert.Contains("Z", ex.Message);
    }

    [Fact]
    public void EnumEvolution_SameSymbols_NoRemap()
    {
        // When writer and reader have identical symbols, no remap is needed
        var writerSchemaJson = """
        {
            "type": "record", "name": "Record",
            "fields": [
                {"name": "val", "type": {
                    "type": "enum", "name": "E",
                    "symbols": ["A", "B", "C"]
                }}
            ]
        }
        """;

        var writerArrow = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", new DictionaryType(Int32Type.Default, StringType.Default, false), false))
            .Build();

        var valBuilder = new StringArray.Builder();
        valBuilder.Append("A");
        valBuilder.Append("B");
        valBuilder.Append("C");
        var indexBuilder = new Int32Array.Builder();
        indexBuilder.Append(0);
        indexBuilder.Append(1);
        indexBuilder.Append(2);
        var dictArr = new DictionaryArray(
            new DictionaryType(Int32Type.Default, StringType.Default, false),
            indexBuilder.Build(), valBuilder.Build());
        var batch = new RecordBatch(writerArrow, [dictArr], 3);
        var data = WriteTestData(writerArrow, batch, avroSchemaJson: writerSchemaJson);

        // Same schema for reader
        var result = ReadWithSchema(data, new AvroSchema(writerSchemaJson));
        Assert.Equal(3, result.Length);

        var dict = (DictionaryArray)result.Column(0);
        var indices = (Int32Array)dict.Indices;
        Assert.Equal(0, indices.GetValue(0));
        Assert.Equal(1, indices.GetValue(1));
        Assert.Equal(2, indices.GetValue(2));
    }
}
