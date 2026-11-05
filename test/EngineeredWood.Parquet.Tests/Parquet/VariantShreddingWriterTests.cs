// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Pins <see cref="ParquetWriteOptions.ShredVariants"/> and
/// <see cref="ParquetWriteOptions.VariantShredSchemas"/> — the writer's side of variant shredding.
/// </summary>
/// <remarks>
/// The property these exist for is that a FILE has one layout: shredding is inferred once and reused,
/// so a second batch of a different shape still encodes against the first batch's schema instead of
/// producing a file whose row groups disagree. That is the case
/// <see cref="MultipleRowGroups_KeepOneLayout_AndLaterShapesFallToTheResidual"/> covers, and it is
/// the reason the writer owns the decision rather than each caller shredding batch by batch.
/// </remarks>
public class VariantShreddingWriterTests : IDisposable
{
    private readonly string _tempDir;

    public VariantShreddingWriterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-shredwrite-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    private static ExtensionTypeRegistry VariantRegistry()
    {
        var registry = new ExtensionTypeRegistry();
        registry.Register(VariantExtensionDefinition.Instance);
        return registry;
    }

    private static VariantValue Obj(int a, string b) => VariantValue.FromObject(
        new Dictionary<string, VariantValue>
        {
            ["a"] = VariantValue.FromInt32(a),
            ["b"] = VariantValue.FromString(b),
        });

    private static string Json(VariantValue v) => VariantJsonWriter.ToJson(v, indented: false);

    private static VariantValue FromJson(string json)
    {
        var (metadata, value) = VariantJsonReader.Parse(json);
        return new VariantReader(metadata, value).ToVariantValue();
    }

    private static RecordBatch Batch(params VariantValue[] values)
    {
        var builder = new VariantArray.Builder();
        foreach (var v in values)
            builder.Append(v);
        var array = builder.Build(allocator: null);
        var schema = new Apache.Arrow.Schema(
            new[] { new Field("v", array.Data.DataType, nullable: true) }, metadata: null);
        return new RecordBatch(schema, new IArrowArray[] { array }, values.Length);
    }

    private async Task<string> WriteAsync(ParquetWriteOptions options, params RecordBatch[] batches)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        foreach (var batch in batches)
            await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static async Task<(FileMetaData Meta, List<VariantValue> Values, List<bool> Nulls)> ReadAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = VariantRegistry() });

        var meta = await reader.ReadMetadataAsync();
        var values = new List<VariantValue>();
        var nulls = new List<bool>();
        for (int g = 0; g < meta.RowGroups.Count; g++)
        {
            var batch = await reader.ReadRowGroupAsync(g);
            var variant = Assert.IsType<VariantArray>(batch.Column(0));
            for (int i = 0; i < variant.Length; i++)
            {
                nulls.Add(variant.IsNull(i));
                values.Add(variant.IsNull(i) ? VariantValue.Null : variant.GetLogicalVariantValue(i));
            }
        }
        return (meta, values, nulls);
    }

    [Fact]
    public async Task Default_WritesUnshredded()
    {
        string path = await WriteAsync(ParquetWriteOptions.Default, Batch(Obj(1, "x"), Obj(2, "y")));

        var (meta, values, _) = await ReadAsync(path);
        Assert.DoesNotContain(meta.Schema, s => s.Name == "typed_value");
        Assert.Equal(Json(Obj(1, "x")), Json(values[0]));
    }

    [Fact]
    public async Task ShredVariants_ShredsTheSameBatch()
    {
        var options = ParquetWriteOptions.Default with { ShredVariants = ShredOptions.Default };
        string path = await WriteAsync(options, Batch(Obj(1, "x"), Obj(2, "y")));

        var (meta, values, _) = await ReadAsync(path);
        var group = meta.Schema.First(s => s.Name == "v");
        Assert.IsType<LogicalType.VariantType>(group.LogicalType);
        Assert.Contains(meta.Schema, s => s.Name == "typed_value");
        Assert.Equal(Json(Obj(1, "x")), Json(values[0]));
        Assert.Equal(Json(Obj(2, "y")), Json(values[1]));
    }

    /// <summary>
    /// The case the API is shaped around: batch 2 has a shape the first batch's layout does not
    /// describe. The file keeps ONE schema and the mismatched rows ride the residual <c>value</c>,
    /// which is what it is for — values survive, layout does not fragment.
    /// </summary>
    [Fact]
    public async Task MultipleRowGroups_KeepOneLayout_AndLaterShapesFallToTheResidual()
    {
        var options = ParquetWriteOptions.Default with { ShredVariants = ShredOptions.Default };
        // Keys in sorted order: a variant object stores its fields sorted, so writing them that way
        // here keeps the JSON comparison below about VALUES rather than about field order.
        var second = VariantValue.FromObject(new Dictionary<string, VariantValue>
        {
            ["shape"] = VariantValue.FromInt32(42),
            ["totally"] = VariantValue.FromString("different"),
        });

        string path = await WriteAsync(options, Batch(Obj(1, "x"), Obj(2, "y")), Batch(second, Obj(3, "z")));

        var (meta, values, _) = await ReadAsync(path);
        Assert.Equal(2, meta.RowGroups.Count);
        Assert.Contains(meta.Schema, s => s.Name == "typed_value");

        // One layout for the file: the two fields hoisted from batch 1, and nothing from batch 2.
        var typed = meta.Schema.First(s => s.Name == "typed_value");
        Assert.Equal(2, typed.NumChildren);
        Assert.Contains(meta.Schema, s => s.Name == "a");
        Assert.Contains(meta.Schema, s => s.Name == "b");
        Assert.DoesNotContain(meta.Schema, s => s.Name == "totally" || s.Name == "shape");

        // Every value still reads back, including the one that fit nothing.
        Assert.Equal(Json(Obj(1, "x")), Json(values[0]));
        Assert.Equal(Json(Obj(2, "y")), Json(values[1]));
        Assert.Equal(Json(second), Json(values[2]));
        Assert.Equal(Json(Obj(3, "z")), Json(values[3]));
    }

    [Fact]
    public async Task VariantShredSchemas_OverridesInference_AndAppliesWithoutShredVariants()
    {
        // Inference would decline this column: three unrelated shapes, no common one.
        var mixed = Batch(Obj(1, "x"), VariantValue.FromInt32(7), VariantValue.FromString("plain"));
        Assert.Null(VariantShredding.InferSchema((VariantArray)mixed.Column(0)));

        var declared = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
        {
            ["a"] = ShredSchema.Primitive(ShredType.Int32),
            ["b"] = ShredSchema.Primitive(ShredType.String),
        });
        var options = ParquetWriteOptions.Default with
        {
            VariantShredSchemas = new Dictionary<string, ShredSchema> { ["v"] = declared },
        };

        string path = await WriteAsync(options, mixed);

        var (meta, values, _) = await ReadAsync(path);
        Assert.Contains(meta.Schema, s => s.Name == "typed_value");
        Assert.Equal(Json(Obj(1, "x")), Json(values[0]));
        Assert.Equal(Json(VariantValue.FromInt32(7)), Json(values[1]));
        Assert.Equal(Json(VariantValue.FromString("plain")), Json(values[2]));
    }

    /// <summary>
    /// A column with no shape to shred is written unshredded rather than failing, and the decision is
    /// remembered — so a later batch that WOULD have inferred a shape does not change the file's
    /// layout half way through.
    /// </summary>
    [Fact]
    public async Task InferenceDeclines_ColumnStaysUnshredded_ForTheWholeFile()
    {
        var options = ParquetWriteOptions.Default with { ShredVariants = ShredOptions.Default };
        var mixed = Batch(Obj(1, "x"), VariantValue.FromInt32(7), VariantValue.FromString("plain"));
        var uniform = Batch(Obj(2, "y"), Obj(3, "z"));

        string path = await WriteAsync(options, mixed, uniform);

        var (meta, values, _) = await ReadAsync(path);
        Assert.DoesNotContain(meta.Schema, s => s.Name == "typed_value");
        Assert.Equal(5, values.Count);
        Assert.Equal(Json(Obj(3, "z")), Json(values[4]));
    }

    [Fact]
    public async Task SqlNullRows_SurviveTheWriter_AsNullRows()
    {
        var builder = new VariantArray.Builder();
        builder.Append(Obj(1, "x"));
        builder.AppendNull();
        builder.Append(Obj(3, "z"));
        var array = builder.Build(allocator: null);
        var schema = new Apache.Arrow.Schema(
            new[] { new Field("v", array.Data.DataType, nullable: true) }, metadata: null);
        var batch = new RecordBatch(schema, new IArrowArray[] { array }, 3);

        var options = ParquetWriteOptions.Default with { ShredVariants = ShredOptions.Default };
        string path = await WriteAsync(options, batch);

        var (meta, values, nulls) = await ReadAsync(path);
        Assert.Contains(meta.Schema, s => s.Name == "typed_value");
        Assert.Equal(new[] { false, true, false }, nulls);
        Assert.Equal(Json(Obj(1, "x")), Json(values[0]));
        Assert.Equal(Json(Obj(3, "z")), Json(values[2]));
    }

    /// <summary>
    /// A batch larger than <see cref="ParquetWriteOptions.RowGroupMaxRows"/> is auto-split, and each
    /// resulting row group is shredded to the file's one layout.
    /// </summary>
    [Fact]
    public async Task AutoSplitRowGroups_AreEachShredded_ToTheSameLayout()
    {
        var values = Enumerable.Range(0, 10).Select(i => Obj(i, "row" + i)).ToArray();
        var options = ParquetWriteOptions.Default with
        {
            ShredVariants = ShredOptions.Default,
            RowGroupMaxRows = 4,
        };

        string path = await WriteAsync(options, Batch(values));

        var (meta, read, _) = await ReadAsync(path);
        Assert.Equal(3, meta.RowGroups.Count); // 4 + 4 + 2
        Assert.Contains(meta.Schema, s => s.Name == "typed_value");
        Assert.Equal(values.Length, read.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(Json(values[i]), Json(read[i]));
    }
    /// <summary>
    /// The shredding spec makes every field group under an object's <c>typed_value</c> (and every array
    /// element group) a <c>required group</c>: an object that lacks the field writes the group with
    /// both <c>value</c> and <c>typed_value</c> null. Writing the group OPTIONAL and null instead is
    /// what Spark 4.1 reads as MALFORMED_VARIANT (#679).
    /// </summary>
    [Fact]
    public async Task ShreddedFieldGroups_AreRequired_AndAMissingFieldRoundTrips()
    {
        var declared = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
        {
            ["a"] = ShredSchema.Primitive(ShredType.Int8),
            ["c"] = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                ["d"] = ShredSchema.Primitive(ShredType.Int32),
            }),
            ["l"] = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32)),
        });
        var options = ParquetWriteOptions.Default with
        {
            VariantShredSchemas = new Dictionary<string, ShredSchema> { ["v"] = declared },
        };
        string[] json =
        [
            "{\"a\":1,\"c\":{\"d\":2},\"l\":[1,2]}",
            "{\"b\":1}",
            "{\"a\":2}",
            "{\"c\":{\"e\":3}}",
            "{\"a\":\"str\",\"l\":[3,\"x\"]}",
            "7",
        ];
        var builder = new VariantArray.Builder();
        foreach (var j in json)
            builder.Append(FromJson(j));
        builder.AppendNull();
        var array = builder.Build(allocator: null);
        var batch = new RecordBatch(
            new Apache.Arrow.Schema(new[] { new Field("v", array.Data.DataType, nullable: true) }, metadata: null),
            new IArrowArray[] { array }, array.Length);

        string path = await WriteAsync(options, batch);

        var (meta, values, nulls) = await ReadAsync(path);
        foreach (var name in new[] { "a", "c", "d", "l" })
        {
            var group = meta.Schema.Single(s => s.Name == name);
            Assert.Equal(FieldRepetitionType.Required, group.RepetitionType);
        }
        // The array's element group: the one child of the repeated "list" group.
        int list = meta.Schema.ToList().FindIndex(s => s.RepetitionType == FieldRepetitionType.Repeated);
        Assert.True(list >= 0);
        Assert.Equal(FieldRepetitionType.Required, meta.Schema[list + 1].RepetitionType);

        for (int i = 0; i < json.Length; i++)
        {
            Assert.False(nulls[i]);
            Assert.Equal(Json(FromJson(json[i])), Json(values[i]));
        }
        Assert.True(nulls[json.Length]);
    }

    /// <summary>
    /// A shredded column reads back reassembled, as <c>(metadata, value)</c>. The batch's field must describe that
    /// array: a field still declaring the file's <c>typed_value</c> made a writer handed the batch index a child the
    /// array does not have, so a shredded file could not be rewritten (a copy-on-write DML, a compaction).
    /// </summary>
    [Fact]
    public async Task ShreddedColumn_ReadsBackWithTheArraysType_AndWritesAgain()
    {
        var shred = ParquetWriteOptions.Default with { ShredVariants = ShredOptions.Default };
        string path = await WriteAsync(shred, Batch(Obj(1, "x"), Obj(2, "y")));

        RecordBatch batch;
        await using (var file = new LocalRandomAccessFile(path))
        await using (var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = VariantRegistry() }))
        {
            batch = await reader.ReadRowGroupAsync(0);
            // The schema-only API reports what a batch carries, storage included (it is the only way to see the
            // schema of a file with no row groups).
            Assert.Equal(Describe(batch.Schema), Describe(await reader.GetArrowSchemaAsync()));
        }

        var fieldType = Assert.IsAssignableFrom<ExtensionType>(batch.Schema.FieldsList[0].DataType);
        Assert.Equal(["metadata", "value"], ((StructType)fieldType.StorageType).Fields.Select(f => f.Name));
        Assert.Equal(
            ((StructType)fieldType.StorageType).Fields.Count,
            ((StructArray)((ExtensionArray)batch.Column(0)).Storage).Fields.Count);

        foreach (var options in new[] { ParquetWriteOptions.Default, shred })
        {
            var (_, values, _) = await ReadAsync(await WriteAsync(options, batch));
            Assert.Equal([Json(Obj(1, "x")), Json(Obj(2, "y"))], values.Select(Json));
        }
    }

    /// <summary>
    /// The schema-only API and a batch agree for a shredded and an unshredded file, a variant nested in a struct and a
    /// list included. An unshredded variant keeps the type its file declares; a shredded one reads back reassembled.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheArrowSchema_IsWhatABatchCarries(bool shredded)
    {
        var options = shredded ? ParquetWriteOptions.Default with { ShredVariants = ShredOptions.Default } : ParquetWriteOptions.Default;
        var top = Batch(Obj(1, "x"), Obj(2, "y"));
        var variant = top.Column(0);
        var inStruct = new StructArray(new StructType([new Field("v", variant.Data.DataType, true)]), 2, [variant], ArrowBuffer.Empty, 0);
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(2).Build();
        var inList = new ListArray(new ListType(new Field("item", variant.Data.DataType, true)), 2, offsets, variant, ArrowBuffer.Empty, 0);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(top.Schema.FieldsList[0])
            .Field(new Field("s", inStruct.Data.DataType, true))
            .Field(new Field("l", inList.Data.DataType, true))
            .Build();
        string path = await WriteAsync(options, new RecordBatch(schema, [variant, inStruct, inList], 2));

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = VariantRegistry() });
        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(Describe(batch.Schema), Describe(await reader.GetArrowSchemaAsync()));
        // and the batch's fields describe its arrays
        for (int i = 0; i < batch.ColumnCount; i++)
            Assert.Equal(Describe(batch.Schema.FieldsList[i].DataType), Describe(batch.Column(i).Data.DataType));
    }

    /// <summary>A schema with every type spelled out, an extension's storage included: <c>ToString</c> shows neither
    /// the storage nor nullability, so it cannot tell a shredded variant from a reassembled one.</summary>
    private static string Describe(Apache.Arrow.Schema schema) => string.Join(", ", schema.FieldsList.Select(Describe));

    private static string Describe(Field field) => $"{field.Name}:{Describe(field.DataType)}{(field.IsNullable ? "" : " not null")}";

    private static string Describe(IArrowType type) => type switch
    {
        ExtensionType e => $"{e.Name}<{Describe(e.StorageType)}>",
        StructType s => $"struct<{string.Join(", ", s.Fields.Select(Describe))}>",
        MapType m => $"map<{Describe(m.KeyField)}, {Describe(m.ValueField)}>",
        ListType l => $"list<{Describe(l.ValueField)}>",
        _ => type.TypeId.ToString(),
    };
}
