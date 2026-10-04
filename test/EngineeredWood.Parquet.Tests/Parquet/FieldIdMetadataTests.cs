// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// A Parquet <c>field_id</c> surfaces on read as <c>PARQUET:field_id</c> in the Arrow field's metadata — the key
/// PyArrow and arrow-rs use, and the one the writer reads — at every level that becomes an Arrow field, so
/// Parquet -> Arrow -> Parquet keeps every id (issue #446). The reader used to build fields from a name, a type
/// and nullability alone, so an id was gone the moment a file became Arrow.
/// </summary>
public class FieldIdMetadataTests : IDisposable
{
    private const string Key = "PARQUET:field_id";

    private readonly string _tempDir;

    public FieldIdMetadataTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-fieldid-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    private static Dictionary<string, string> Id(int id) => new() { [Key] = id.ToString() };

    private static string? IdOf(Field field) =>
        field.Metadata is { } md && md.TryGetValue(Key, out var id) ? id : null;

    // id 1: int32; no id: string; 10: struct<11: int32, 12: timestamp(s)>; 20: list<21: int32>;
    // 30: map<31: string, 32: int32>. The second-precision timestamp is rescaled on write, which rebuilds its
    // field, and the map key is rebuilt as non-nullable — both used to drop the id.
    private static RecordBatch TaggedBatch()
    {
        var structType = new StructType(
        [
            new Field("x", Int32Type.Default, nullable: true, Id(11)),
            new Field("at", new TimestampType(TimeUnit.Second, "UTC"), nullable: true, Id(12)),
        ]);
        var listType = new ListType(new Field("element", Int32Type.Default, nullable: true, Id(21)));
        var mapType = new MapType(
            new Field("key", StringType.Default, nullable: false, Id(31)),
            new Field("value", Int32Type.Default, nullable: true, Id(32)));

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false, Id(1)))
            .Field(new Field("plain", StringType.Default, nullable: true))
            .Field(new Field("s", structType, nullable: true, Id(10)))
            .Field(new Field("l", listType, nullable: true, Id(20)))
            .Field(new Field("m", mapType, nullable: true, Id(30)))
            .Build();

        var seconds = new ArrowBuffer.Builder<long>().Append(1_700_000_000L).Append(1_700_000_001L).Build();
        var timestamps = new TimestampArray(new ArrayData(
            structType.Fields[1].DataType, 2, 0, 0, [ArrowBuffer.Empty, seconds]));
        var structArray = new StructArray(
            structType, 2,
            [new Int32Array.Builder().Append(1).Append(2).Build(), timestamps],
            ArrowBuffer.Empty);

        var listArray = new ListArray(
            listType, 2,
            new ArrowBuffer.Builder<int>().Append(0).Append(2).Append(3).Build(),
            new Int32Array.Builder().Append(7).Append(8).Append(9).Build(),
            ArrowBuffer.Empty);

        var entries = new StructArray(
            new StructType([mapType.KeyField, mapType.ValueField]), 2,
            [
                new StringArray.Builder().Append("k1").Append("k2").Build(),
                new Int32Array.Builder().Append(5).Append(6).Build(),
            ],
            ArrowBuffer.Empty);
        var mapArray = new MapArray(
            mapType, 2, new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(2).Build(), entries,
            ArrowBuffer.Empty);

        return new RecordBatch(
            schema,
            [
                new Int32Array.Builder().Append(1).Append(2).Build(),
                new StringArray.Builder().Append("a").Append("b").Build(),
                structArray,
                listArray,
                mapArray,
            ],
            2);
    }

    private static async Task WriteAsync(string path, RecordBatch batch)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    // Every id in the schema tree, by dotted path, for comparing what a schema carries at every level.
    private static Dictionary<string, string?> Ids(IEnumerable<Field> fields)
    {
        var result = new Dictionary<string, string?>();
        void Walk(Field field, string path)
        {
            result[path] = IdOf(field);
            switch (field.DataType)
            {
                case StructType st:
                    foreach (var child in st.Fields)
                        Walk(child, path + "." + child.Name);
                    break;
                case MapType mt:
                    Walk(mt.KeyField, path + ".key");
                    Walk(mt.ValueField, path + ".value");
                    break;
                case ListType lt:
                    Walk(lt.ValueField, path + ".element");
                    break;
            }
        }

        foreach (var field in fields)
            Walk(field, field.Name);
        return result;
    }

    private static readonly Dictionary<string, string?> ExpectedIds = new()
    {
        ["id"] = "1",
        ["plain"] = null,
        ["s"] = "10",
        ["s.x"] = "11",
        ["s.at"] = "12",
        ["l"] = "20",
        ["l.element"] = "21",
        ["m"] = "30",
        ["m.key"] = "31",
        ["m.value"] = "32",
    };

    [Fact]
    public async Task ArrowSchema_CarriesFieldIdsAtEveryLevel()
    {
        string path = TempPath("tagged.parquet");
        await WriteAsync(path, TaggedBatch());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var schema = await reader.GetArrowSchemaAsync();

        Assert.Equal(ExpectedIds, Ids(schema.FieldsList));
        // A field with no id gets no metadata at all, rather than an empty dictionary.
        Assert.Null(schema.GetFieldByName("plain").Metadata);
    }

    [Fact]
    public async Task Batches_CarryFieldIdsInTheSchemaAndTheArrayTypes()
    {
        string path = TempPath("tagged_batches.parquet");
        await WriteAsync(path, TaggedBatch());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var batches = new List<RecordBatch>();
        await foreach (var batch in reader.ReadAllAsync())
            batches.Add(batch);

        var read = Assert.Single(batches);
        Assert.Equal(ExpectedIds, Ids(read.Schema.FieldsList));

        // The assembled arrays describe their children with the same ids as the schema does, so code that
        // walks an array's own type sees what the schema says.
        var arrayFields = read.Schema.FieldsList
            .Select((f, i) => new Field(f.Name, read.Column(i).Data.DataType, f.IsNullable, f.Metadata));
        Assert.Equal(ExpectedIds, Ids(arrayFields));
    }

    [Fact]
    public async Task ZeroRowFile_KeepsTheIdsOfRescaledNestedFields()
    {
        // With no rows, the footer comes from rescaling the declared SCHEMA rather than a batch — a separate
        // rewrite of the type tree that rebuilt every field it touched without its metadata.
        string path = TempPath("tagged_empty.parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            writer.DeclareSchema(TaggedBatch().Schema);
            await writer.CloseAsync();
        }

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        Assert.Equal(ExpectedIds, Ids((await reader.GetArrowSchemaAsync()).FieldsList));
    }

    [Fact]
    public async Task ProjectedFlatRead_CarriesTheFieldId()
    {
        // Selecting only flat columns takes the reader's leaf-only path, which builds its fields separately.
        string path = TempPath("tagged_flat.parquet");
        await WriteAsync(path, TaggedBatch());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0, ["id", "plain"]);

        Assert.Equal("1", IdOf(batch.Schema.GetFieldByName("id")));
        Assert.Null(batch.Schema.GetFieldByName("plain").Metadata);
    }

    [Fact]
    public async Task ReadThenWrite_KeepsEveryFieldId()
    {
        string first = TempPath("first.parquet");
        await WriteAsync(first, TaggedBatch());

        RecordBatch read;
        await using (var rf = new LocalRandomAccessFile(first))
        await using (var reader = new ParquetFileReader(rf, ownsFile: false))
            read = await reader.ReadRowGroupAsync(0);

        string second = TempPath("second.parquet");
        await WriteAsync(second, read);

        await using var rf2 = new LocalRandomAccessFile(second);
        await using var reader2 = new ParquetFileReader(rf2, ownsFile: false);
        var elements = (await reader2.ReadMetadataAsync()).Schema;
        var ids = elements.Where(e => e.FieldId.HasValue).ToDictionary(e => e.Name, e => e.FieldId!.Value);

        Assert.Equal(
            new Dictionary<string, int>
            {
                ["id"] = 1, ["s"] = 10, ["x"] = 11, ["at"] = 12, ["l"] = 20, ["element"] = 21,
                ["m"] = 30, ["key"] = 31, ["value"] = 32,
            },
            ids);
        Assert.Null(elements.Single(e => e.Name == "plain").FieldId);
    }
}
