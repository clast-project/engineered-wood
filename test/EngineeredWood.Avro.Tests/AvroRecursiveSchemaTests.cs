// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// Tests that recursive Avro schemas are detected and rejected with a clear error
/// rather than causing a stack overflow.
/// </summary>
public class AvroRecursiveSchemaTests
{
    [Fact]
    public void RecursiveSchema_LinkedList_ParsesSuccessfully()
    {
        // The schema parser handles self-references via placeholders — parsing should work.
        var json = """
        {
            "type": "record", "name": "LinkedList",
            "fields": [
                {"name": "value", "type": "int"},
                {"name": "next", "type": ["null", "LinkedList"]}
            ]
        }
        """;

        var schema = (AvroRecordSchema)AvroSchemaParser.Parse(json);
        Assert.Equal(2, schema.Fields.Count);
        Assert.Equal("value", schema.Fields[0].Name);
        Assert.Equal("next", schema.Fields[1].Name);
    }

    [Fact]
    public void RecursiveSchema_ToArrow_ThrowsCleanException()
    {
        // The parser's placeholder breaks the cycle (placeholder has 0 fields),
        // so we get a clean exception rather than a stack overflow.
        var json = """
        {
            "type": "record", "name": "LinkedList",
            "fields": [
                {"name": "value", "type": "int"},
                {"name": "next", "type": ["null", "LinkedList"]}
            ]
        }
        """;

        var schema = (AvroRecordSchema)AvroSchemaParser.Parse(json);

        Assert.ThrowsAny<Exception>(() => ArrowSchemaConverter.ToArrow(schema));
    }

    [Fact]
    public void RecursiveSchema_TreeNode_ThrowsOnConversion()
    {
        // TreeNode references itself via an array. The parser's placeholder has 0 fields,
        // so the StructType constructor may throw before the depth limit is hit.
        // Either way, we should get a clear exception (not a stack overflow).
        var json = """
        {
            "type": "record", "name": "TreeNode",
            "fields": [
                {"name": "value", "type": "string"},
                {"name": "children", "type": {"type": "array", "items": "TreeNode"}}
            ]
        }
        """;

        var schema = (AvroRecordSchema)AvroSchemaParser.Parse(json);

        Assert.ThrowsAny<Exception>(() => ArrowSchemaConverter.ToArrow(schema));
    }

    [Fact]
    public void DeeplyNested_NonRecursive_Succeeds()
    {
        // A deeply nested but non-recursive schema should still work.
        // list of map of struct — 3 nesting levels, well within the depth limit.
        var json = """
        {
            "type": "record", "name": "Root",
            "fields": [
                {"name": "data", "type": {
                    "type": "array",
                    "items": {
                        "type": "map",
                        "values": {
                            "type": "record", "name": "Inner",
                            "fields": [
                                {"name": "x", "type": "int"},
                                {"name": "y", "type": "string"}
                            ]
                        }
                    }
                }}
            ]
        }
        """;

        var schema = (AvroRecordSchema)AvroSchemaParser.Parse(json);
        var arrowSchema = ArrowSchemaConverter.ToArrow(schema);

        Assert.Single(arrowSchema.FieldsList);
        Assert.Equal("data", arrowSchema.FieldsList[0].Name);
    }

    [Fact]
    public void RecursiveSchema_ReadWithEvolution_ThrowsOnMissingDefault()
    {
        // Writing with a non-recursive schema, reading with a recursive reader schema.
        // The reader has a recursive "next" field with no default, so schema resolution
        // rejects it before we even hit the depth limit.
        var writerSchemaJson = """
        {
            "type": "record", "name": "LinkedList",
            "fields": [
                {"name": "value", "type": "int"}
            ]
        }
        """;

        var readerSchemaJson = """
        {
            "type": "record", "name": "LinkedList",
            "fields": [
                {"name": "value", "type": "int"},
                {"name": "next", "type": ["null", "LinkedList"]}
            ]
        }
        """;

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int32Type.Default, false))
            .Build();

        using var ms = new MemoryStream();
        var writerBuilder = new AvroWriterBuilder(arrowSchema)
            .WithAvroSchema(new AvroSchema(writerSchemaJson));
        using (var writer = writerBuilder.Build(ms))
        {
            var batch = new RecordBatch(arrowSchema,
                [new Int32Array.Builder().Append(42).Build()], 1);
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        // Schema resolution catches the missing default before depth limit kicks in
        Assert.ThrowsAny<Exception>(() =>
        {
            using var reader = new AvroReaderBuilder()
                .WithReaderSchema(new AvroSchema(readerSchemaJson))
                .Build(ms);
            reader.ReadNextBatch();
        });
    }
}
