// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Text;
using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;
using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.Iceberg.Serialization;

namespace EngineeredWood.Iceberg.Tests.Manifest;

/// <summary>
/// The manifest codec (#586): decoding by field id against any writer's schema, every format version,
/// the partition tuple typed by its spec, and writing the spec's schema with header metadata.
/// </summary>
public class ManifestCodecTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"iceberg-codec-{Guid.NewGuid():N}");
    private readonly LocalTableFileSystem _fs;

    public ManifestCodecTests()
    {
        Directory.CreateDirectory(_dir);
        _fs = new LocalTableFileSystem(_dir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    private string PathOf(string name) => Path.Combine(_dir, name);

    private static readonly Schema TableSchema = new(0,
    [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "ts", IcebergType.Timestamptz, false),
        new NestedField(3, "name", IcebergType.String, false),
        new NestedField(4, "amount", IcebergType.Decimal(9, 2), false),
        new NestedField(5, "key", IcebergType.Uuid, false),
    ]);

    private static readonly PartitionSpec Spec = new(1,
    [
        new PartitionField(1, 1000, "id_bucket", Transform.Bucket(4)),
        new PartitionField(2, 1001, "ts_day", Transform.Day),
        new PartitionField(3, 1002, "name", Transform.Identity),
        new PartitionField(4, 1003, "amount", Transform.Identity),
        new PartitionField(5, 1004, "key", Transform.Identity),
    ]);

    private static readonly Guid Key = Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7");

    private static PartitionData Tuple(int bucket, int? day, string? name) => new(
    [
        new PartitionValue(1000, "id_bucket", IcebergType.Int, bucket),
        new PartitionValue(1001, "ts_day", IcebergType.Date, day),
        new PartitionValue(1002, "name", IcebergType.String, name),
        new PartitionValue(1003, "amount", IcebergType.Decimal(9, 2), new BigInteger(-12345)),
        new PartitionValue(1004, "key", IcebergType.Uuid, Key),
    ]);

    private static TableMetadata Table(int formatVersion) =>
        TableMetadata.Create(TableSchema, Spec, formatVersion: formatVersion, location: "s3://b/t");

    /// <summary>A data file with every field a v3 manifest can hold set.</summary>
    private static DataFile FullFile(string path, int formatVersion) => new()
    {
        Content = FileContent.Data,
        FilePath = path,
        FileFormat = FileFormat.Parquet,
        Partition = Tuple(3, 19_675, "x"),
        RecordCount = 10,
        FileSizeInBytes = 1000,
        ColumnSizes = new Dictionary<int, long> { [1] = 80, [2] = 90 },
        ValueCounts = new Dictionary<int, long> { [1] = 10 },
        NullValueCounts = new Dictionary<int, long> { [2] = 1 },
        NanValueCounts = new Dictionary<int, long> { [4] = 0 },
        LowerBounds = new Dictionary<int, byte[]> { [1] = [1, 0, 0, 0, 0, 0, 0, 0] },
        UpperBounds = new Dictionary<int, byte[]> { [1] = [9, 0, 0, 0, 0, 0, 0, 0] },
        SplitOffsets = [4, 500],
        SortOrderId = 0,
        FirstRowId = formatVersion >= 3 ? 100 : null,
    };

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task EveryField_RoundTrips(int formatVersion)
    {
        var table = Table(formatVersion);
        var file = FullFile("s3://b/t/data/a.parquet", formatVersion) with
        {
            BlockSizeInBytes = formatVersion == 1 ? 4096 : null,
        };
        var entries = new List<ManifestEntry>
        {
            new() { Status = ManifestEntryStatus.Existing, SnapshotId = 7, SequenceNumber = formatVersion >= 2 ? 3 : null,
                    FileSequenceNumber = formatVersion >= 2 ? 3 : null, DataFile = file },
        };
        if (formatVersion >= 2)
        {
            entries.Add(new ManifestEntry
            {
                Status = ManifestEntryStatus.Added, SnapshotId = 8,
                DataFile = new DataFile
                {
                    Content = FileContent.EqualityDeletes, FilePath = "s3://b/t/data/eq.parquet", RecordCount = 1,
                    FileSizeInBytes = 50, Partition = Tuple(0, null, null), EqualityIds = [1, 3],
                    ReferencedDataFile = "s3://b/t/data/a.parquet",
                },
            });
        }
        if (formatVersion >= 3)
        {
            entries.Add(new ManifestEntry
            {
                Status = ManifestEntryStatus.Added, SnapshotId = 8,
                DataFile = new DataFile
                {
                    Content = FileContent.PositionDeletes, FilePath = "s3://b/t/data/dv.puffin", FileFormat = FileFormat.Puffin,
                    RecordCount = 2, FileSizeInBytes = 70, Partition = Tuple(3, 19_675, "x"),
                    ReferencedDataFile = "s3://b/t/data/a.parquet", ContentOffset = 4, ContentSizeInBytes = 40,
                },
            });
        }

        // Data files and delete files go to separate manifests.
        foreach (var content in new[] { ManifestContent.Data, ManifestContent.Deletes })
        {
            var expected = entries.Where(e => (e.DataFile.Content == FileContent.Data) == (content == ManifestContent.Data)).ToList();
            var path = PathOf($"m-{content}.avro");
            await ManifestIO.WriteManifestAsync(TestResolvers.Io(_fs), path, expected, ManifestWriteOptions.For(table, 1, content));
            var read = await ManifestIO.ReadManifestAsync(TestResolvers.Io(_fs), path);

            Assert.Equal(expected.Count, read.Count);
            for (int i = 0; i < expected.Count; i++)
                AssertEntryEqual(expected[i], read[i]);
        }
    }

    [Fact]
    public async Task Header_HoldsWhatReadersLookFor()
    {
        var table = Table(2);
        var path = PathOf("m.avro");
        await ManifestIO.WriteManifestAsync(TestResolvers.Io(_fs), path,
            [new ManifestEntry { Status = ManifestEntryStatus.Added, SnapshotId = 1, DataFile = FullFile("a", 2) }],
            ManifestWriteOptions.For(table, 1, ManifestContent.Data));

        using var stream = File.OpenRead(path);
        using var reader = new AvroReaderBuilder().Build(stream);
        string Get(string key) => Encoding.UTF8.GetString(reader.Metadata[key]);
        Assert.Equal("2", Get("format-version"));
        Assert.Equal("1", Get("partition-spec-id"));
        Assert.Equal("data", Get("content"));
        Assert.Equal("0", Get("schema-id"));
        Assert.Equal(IcebergJsonSerializer.Serialize(TableSchema), Get("schema"));
        Assert.Equal(Spec.Fields, IcebergJsonSerializer.Deserialize<IReadOnlyList<PartitionField>>(Get("partition-spec")));

        // Every field of the written schema carries its id, and the partition record is typed by the spec.
        var schema = (AvroRecordSchema)AvroSchemaParser.Parse(Get("avro.schema"));
        var dataFile = (AvroRecordSchema)schema.Fields.Single(f => f.Name == "data_file").Schema;
        Assert.All(dataFile.Fields, f => Assert.NotNull(f.FieldId));
        var partition = (AvroRecordSchema)dataFile.Fields.Single(f => f.Name == "partition").Schema;
        Assert.Equal([1000, 1001, 1002, 1003, 1004], partition.Fields.Select(f => f.FieldId!.Value));
        Assert.Equal(IcebergType.Date, IcebergAvroTypes.InferType(partition.Fields[1].Schema));
        Assert.Equal(IcebergType.Decimal(9, 2), IcebergAvroTypes.InferType(partition.Fields[3].Schema));
        Assert.Equal(IcebergType.Uuid, IcebergAvroTypes.InferType(partition.Fields[4].Schema));
    }

    [Fact]
    public async Task V2Manifest_HasNoV3Fields()
    {
        // cf. iceberg-rust #3399: a v2 manifest must not declare v3-only fields.
        var path = PathOf("m.avro");
        await ManifestIO.WriteManifestAsync(TestResolvers.Io(_fs), path,
            [new ManifestEntry { Status = ManifestEntryStatus.Added, SnapshotId = 1, DataFile = FullFile("a", 2) }],
            ManifestWriteOptions.For(Table(2), 1, ManifestContent.Data));
        using var stream = File.OpenRead(path);
        using var reader = new AvroReaderBuilder().Build(stream);
        var json = Encoding.UTF8.GetString(reader.Metadata["avro.schema"]);
        foreach (var v3 in new[] { "first_row_id", "content_offset", "content_size_in_bytes", "block_size_in_bytes" })
            Assert.DoesNotContain($"\"{v3}\"", json, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PartitionTuple_IsTypedBySpecOrByFile()
    {
        var table = Table(2);
        var path = PathOf("m.avro");
        await ManifestIO.WriteManifestAsync(TestResolvers.Io(_fs), path,
            [new ManifestEntry { Status = ManifestEntryStatus.Added, SnapshotId = 1, DataFile = FullFile("a", 2) }],
            ManifestWriteOptions.For(table, 1, ManifestContent.Data));

        // Without the spec, the tuple is typed from the Avro schema; the types agree.
        var raw = Assert.Single(await ManifestIO.ReadManifestAsync(TestResolvers.Io(_fs), path));
        Assert.Equal(Tuple(3, 19_675, "x"), raw.DataFile.Partition);
        Assert.True(raw.DataFile.Partition!.TryGetValue(1004, out var key));
        Assert.Equal(Key, key);
    }

    [Fact]
    public void MissingOrExtraPartitionValues_AreRefused()
    {
        var types = PartitionTypes.Resolve(Spec, Table(2));
        var noTuple = new ManifestEntry
        {
            Status = ManifestEntryStatus.Added, SnapshotId = 1,
            DataFile = FullFile("a", 2) with { Partition = null },
        };
        Assert.Throws<ArgumentException>(() =>
            ManifestCodec.EncodeManifest([noTuple], 2, ManifestContent.Data, types, new Dictionary<string, byte[]>()));

        var tupleInUnpartitioned = noTuple with { DataFile = FullFile("a", 2) };
        Assert.Throws<ArgumentException>(() =>
            ManifestCodec.EncodeManifest([tupleInUnpartitioned], 2, ManifestContent.Data, [], new Dictionary<string, byte[]>()));
    }

    [Fact]
    public void FieldsAVersionCannotHold_AreRefusedNotDropped()
    {
        var types = PartitionTypes.Resolve(Spec, Table(2));
        var v3Only = new ManifestEntry { Status = ManifestEntryStatus.Added, SnapshotId = 1, DataFile = FullFile("a", 3) };
        Assert.Throws<ArgumentException>(() =>
            ManifestCodec.EncodeManifest([v3Only], 2, ManifestContent.Data, types, new Dictionary<string, byte[]>()));

        var delete = new ManifestEntry
        {
            Status = ManifestEntryStatus.Added, SnapshotId = 1,
            DataFile = FullFile("a", 1) with { Content = FileContent.PositionDeletes },
        };
        Assert.Throws<ArgumentException>(() =>
            ManifestCodec.EncodeManifest([delete], 1, ManifestContent.Deletes, types, new Dictionary<string, byte[]>()));
    }

    // ---------------------------------------------------------------- other writers' layouts

    // The data_file layout Iceberg Java writes for a partitioned v2 table, with spec field ids, in Java's order.
    private const string JavaManifestSchema = """
        {"type": "record", "name": "manifest_entry", "fields": [
          {"name": "status", "type": "int", "field-id": 0},
          {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
          {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
          {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
          {"name": "data_file", "field-id": 2, "type": {"type": "record", "name": "r2", "fields": [
            {"name": "content", "type": "int", "doc": "Contents of the file", "field-id": 134},
            {"name": "file_path", "type": "string", "field-id": 100},
            {"name": "file_format", "type": "string", "field-id": 101},
            {"name": "partition", "field-id": 102, "type": {"type": "record", "name": "r102", "fields": [
              {"name": "id_bucket", "type": ["null", "int"], "default": null, "field-id": 1000},
              {"name": "ts_day", "type": ["null", {"type": "int", "logicalType": "date"}], "default": null, "field-id": 1001}
            ]}},
            {"name": "record_count", "type": "long", "field-id": 103},
            {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
            {"name": "column_sizes", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k117_v118", "fields": [
              {"name": "key", "type": "int", "field-id": 117}, {"name": "value", "type": "long", "field-id": 118}]}}], "default": null, "field-id": 108},
            {"name": "value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k119_v120", "fields": [
              {"name": "key", "type": "int", "field-id": 119}, {"name": "value", "type": "long", "field-id": 120}]}}], "default": null, "field-id": 109},
            {"name": "null_value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k121_v122", "fields": [
              {"name": "key", "type": "int", "field-id": 121}, {"name": "value", "type": "long", "field-id": 122}]}}], "default": null, "field-id": 110},
            {"name": "nan_value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k138_v139", "fields": [
              {"name": "key", "type": "int", "field-id": 138}, {"name": "value", "type": "long", "field-id": 139}]}}], "default": null, "field-id": 137},
            {"name": "lower_bounds", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k126_v127", "fields": [
              {"name": "key", "type": "int", "field-id": 126}, {"name": "value", "type": "bytes", "field-id": 127}]}}], "default": null, "field-id": 125},
            {"name": "upper_bounds", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k129_v130", "fields": [
              {"name": "key", "type": "int", "field-id": 129}, {"name": "value", "type": "bytes", "field-id": 130}]}}], "default": null, "field-id": 128},
            {"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 131},
            {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "default": null, "field-id": 132},
            {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int", "element-id": 136}], "default": null, "field-id": 135},
            {"name": "sort_order_id", "type": ["null", "int"], "default": null, "field-id": 140},
            {"name": "referenced_data_file", "type": ["null", "string"], "default": null, "field-id": 143}
          ]}}
        ]}
        """;

    [Fact]
    public void JavaLayout_DecodesEveryField()
    {
        var bytes = WriteOcf(JavaManifestSchema, schema => Entry(schema, new()
        {
            ["status"] = 1, ["snapshot_id"] = 42L, ["sequence_number"] = null, ["file_sequence_number"] = null,
            ["data_file"] = (Func<AvroRecordSchema, AvroRecordValue>)(df => Rec(df, new()
            {
                ["content"] = 2, ["file_path"] = "s3://b/t/data/eq.parquet", ["file_format"] = "parquet",
                ["partition"] = (Func<AvroRecordSchema, AvroRecordValue>)(p => Rec(p, new() { ["id_bucket"] = 2, ["ts_day"] = 19_000 })),
                ["record_count"] = 5L, ["file_size_in_bytes"] = 500L,
                ["column_sizes"] = Kv(df, "column_sizes", (1, 40L)),
                ["value_counts"] = Kv(df, "value_counts", (1, 5L)),
                ["null_value_counts"] = Kv(df, "null_value_counts", (1, 0L)),
                ["nan_value_counts"] = Kv(df, "nan_value_counts", (4, 1L)),
                ["lower_bounds"] = Kv(df, "lower_bounds", (1, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 })),
                ["upper_bounds"] = Kv(df, "upper_bounds", (1, new byte[] { 5, 0, 0, 0, 0, 0, 0, 0 })),
                ["key_metadata"] = null,
                ["split_offsets"] = new List<object?> { 4L },
                ["equality_ids"] = new List<object?> { 1 },
                ["sort_order_id"] = null,
                ["referenced_data_file"] = null,
            })),
        }));

        var decoded = ManifestCodec.DecodeManifest(bytes, "java.avro", partitionType: null);
        Assert.Empty(decoded.UnmodelledFields);
        var e = Assert.Single(decoded.Entries);
        Assert.Equal(ManifestEntryStatus.Added, e.Status);
        Assert.Equal(42, e.SnapshotId);
        Assert.Null(e.SequenceNumber);
        var df = e.DataFile;
        Assert.Equal(FileContent.EqualityDeletes, df.Content);
        Assert.Equal(FileFormat.Parquet, df.FileFormat);
        Assert.Equal(new PartitionData(
        [
            new PartitionValue(1000, "id_bucket", IcebergType.Int, 2),
            new PartitionValue(1001, "ts_day", IcebergType.Date, 19_000),
        ]), df.Partition);
        Assert.Equal(40, df.ColumnSizes![1]);
        Assert.Equal(1, df.NanValueCounts![4]);
        Assert.Equal([1, 0, 0, 0, 0, 0, 0, 0], df.LowerBounds![1]);
        Assert.Equal([5, 0, 0, 0, 0, 0, 0, 0], df.UpperBounds![1]);
        Assert.Equal([4L], df.SplitOffsets!);
        Assert.Equal([1], df.EqualityIds!);
    }

    [Fact]
    public void LegacyLayoutWithoutFieldIds_DecodesByName()
    {
        // EngineeredWood's manifests before #586 carried no field-id attributes.
        string schema = StripIds(JavaManifestSchema);
        Assert.DoesNotContain("field-id", schema, StringComparison.Ordinal);
        var bytes = WriteOcf(schema, s => Entry(s, new()
        {
            ["status"] = 0, ["snapshot_id"] = 1L, ["sequence_number"] = 2L, ["file_sequence_number"] = 2L,
            ["data_file"] = (Func<AvroRecordSchema, AvroRecordValue>)(df => Rec(df, new()
            {
                ["content"] = 0, ["file_path"] = "a.parquet", ["file_format"] = "PARQUET",
                ["partition"] = (Func<AvroRecordSchema, AvroRecordValue>)(p => Rec(p, new() { ["id_bucket"] = 1, ["ts_day"] = null })),
                ["record_count"] = 3L, ["file_size_in_bytes"] = 30L,
            })),
        }));

        // Partition fields without ids are placed by the spec, in order.
        var spec = new List<PartitionValueType>
        {
            new(1000, "id_bucket", IcebergType.Int),
            new(1001, "ts_day", IcebergType.Date),
        };
        var e = Assert.Single(ManifestCodec.DecodeManifest(bytes, "legacy.avro", spec).Entries);
        Assert.Equal("a.parquet", e.DataFile.FilePath);
        Assert.Equal(2, e.SequenceNumber);
        Assert.True(e.DataFile.Partition!.TryGetValue(1000, out var bucket));
        Assert.Equal(1, bucket);
    }

    private static readonly List<PartitionValueType> TwoFieldSpec =
    [
        new(1000, "id_bucket", IcebergType.Int),
        new(1001, "ts-day", IcebergType.Date),
    ];

    private static byte[] PartitionOnly(string partitionFields, Dictionary<string, object?> values) =>
        WriteOcf(V1ManifestSchema.Replace("\"name\": \"r102\", \"fields\": []", "\"name\": \"r102\", \"fields\": [" + partitionFields + "]"),
            s => Entry(s, new()
            {
                ["status"] = 1, ["snapshot_id"] = 1L,
                ["data_file"] = (Func<AvroRecordSchema, AvroRecordValue>)(df => Rec(df, new()
                {
                    ["file_path"] = "a.parquet", ["file_format"] = "PARQUET",
                    ["partition"] = (Func<AvroRecordSchema, AvroRecordValue>)(p => Rec(p, values)),
                    ["record_count"] = 1L, ["file_size_in_bytes"] = 1L, ["block_size_in_bytes"] = 1L,
                })),
            }));

    [Theory]
    // With ids, in an order other than the spec's.
    [InlineData("""{"name": "ts_x2Dday", "type": ["null", "int"], "field-id": 1001}, {"name": "id_bucket", "type": ["null", "int"], "field-id": 1000}""", "ts_x2Dday")]
    // Without ids, reordered: matched by name, including the escaped Avro form of "ts-day".
    [InlineData("""{"name": "ts_x2Dday", "type": ["null", "int"]}, {"name": "id_bucket", "type": ["null", "int"]}""", "ts_x2Dday")]
    public void PartitionValues_ComeOutInSpecOrder(string fields, string dayName)
    {
        var bytes = PartitionOnly(fields, new() { [dayName] = 19_000, ["id_bucket"] = 3 });
        var tuple = Assert.Single(ManifestCodec.DecodeManifest(bytes, "m.avro", TwoFieldSpec).Entries).DataFile.Partition!;
        Assert.Equal(
            [(1000, "id_bucket", (object?)3), (1001, "ts-day", (object?)19_000)],
            tuple.Values.Select(v => (v.FieldId, v.Name, v.Value)));
    }

    [Fact]
    public void IdLessPartitionFields_MatchingSomeNamesButNotAll_AreRefused()
    {
        var bytes = PartitionOnly(
            """{"name": "id_bucket", "type": ["null", "int"]}, {"name": "mystery", "type": ["null", "int"]}""",
            new() { ["id_bucket"] = 3, ["mystery"] = 1 });
        Assert.Throws<InvalidDataException>(() => ManifestCodec.DecodeManifest(bytes, "m.avro", TwoFieldSpec));

        // Names matching none of the spec's: the oldest v1 writers' positional layout.
        var positional = PartitionOnly(
            """{"name": "p0", "type": ["null", "int"]}, {"name": "p1", "type": ["null", "int"]}""",
            new() { ["p0"] = 3, ["p1"] = 19_000 });
        var tuple = Assert.Single(ManifestCodec.DecodeManifest(positional, "m.avro", TwoFieldSpec).Entries).DataFile.Partition!;
        Assert.Equal([3, 19_000], tuple.Values.Select(v => (int)v.Value!));
    }

    // A v2 manifest whose partition holds a bare fixed(4) and a string after it, so a misread fixed misaligns.
    private static string FixedPartitionSchema(string fixedType)
    {
        // Splice the partition record's fields, whatever the whitespace around them.
        const string open = "\"name\": \"r102\", \"fields\": [";
        const string lastField = "\"field-id\": 1001}";
        int from = JavaManifestSchema.IndexOf(open, StringComparison.Ordinal) + open.Length;
        int to = JavaManifestSchema.IndexOf(lastField, from, StringComparison.Ordinal) + lastField.Length;
        return JavaManifestSchema.Substring(0, from)
            + "{\"name\": \"fx\", \"type\": [\"null\", " + fixedType + "], \"default\": null, \"field-id\": 1000}, "
            + "{\"name\": \"s\", \"type\": [\"null\", \"string\"], \"default\": null, \"field-id\": 1001}"
            + JavaManifestSchema.Substring(to);
    }

    private static AvroRecordValue FixedPartitionEntry(AvroRecordSchema schema, object? fx) => Entry(schema, new()
    {
        ["status"] = 1, ["snapshot_id"] = 1L,
        ["data_file"] = (Func<AvroRecordSchema, AvroRecordValue>)(df => Rec(df, new()
        {
            ["content"] = 0, ["file_path"] = "a.parquet", ["file_format"] = "PARQUET",
            ["partition"] = (Func<AvroRecordSchema, AvroRecordValue>)(p => Rec(p, new() { ["fx"] = fx, ["s"] = "after" })),
            ["record_count"] = 1L, ["file_size_in_bytes"] = 1L,
        })),
    });

    [Fact]
    public void BareFixedPartitionValue_DecodesInBothEncodings()
    {
        const string fixedNode = "{\"type\": \"fixed\", \"name\": \"fixed_4\", \"size\": 4}";
        string schemaJson = FixedPartitionSchema(fixedNode);
        Assert.Contains("fixed_4", schemaJson, StringComparison.Ordinal);
        var value = new byte[] { 0, 1, 2, 3 };
        var spec = new List<PartitionValueType> { new(1000, "fx", new FixedType(4)), new(1001, "s", IcebergType.String) };

        // As the spec and the schema say: four bytes, no prefix (iceberg-go, pyiceberg, EW).
        var strict = WriteOcf(schemaJson, s => FixedPartitionEntry(s, value));
        AssertTuple(ManifestCodec.DecodeManifest(strict, "strict.avro", spec));

        // As Iceberg Java writes it (apache/iceberg#17501): the schema says fixed, the bytes have a length prefix.
        // Encode the records with a bytes field in its place, and write the fixed schema in the header.
        var asBytes = (AvroRecordSchema)AvroSchemaParser.Parse(FixedPartitionSchema("\"bytes\""));
        var header = (AvroRecordSchema)AvroSchemaParser.Parse(schemaJson);
        var w = new AvroBinaryWriter(new GrowableBuffer());
        AvroDatum.Write(w, asBytes, FixedPartitionEntry(asBytes, value));
        var ms = new MemoryStream();
        using (var ocf = new OcfWriter(ms, AvroCodec.Null))
        {
            ocf.WriteHeader(header);
            ocf.WriteBlock(w.WrittenSpan, 1);
        }
        AssertTuple(ManifestCodec.DecodeManifest(ms.ToArray(), "java.avro", spec));

        void AssertTuple(DecodedManifest decoded)
        {
            var tuple = Assert.Single(decoded.Entries).DataFile.Partition!;
            Assert.True(tuple.TryGetValue(1000, out var fx));
            Assert.Equal(value, (byte[])fx!);
            Assert.True(tuple.TryGetValue(1001, out var after));
            Assert.Equal("after", after);
        }
    }

    [Fact]
    public void ReadingsThatBothDecode_ToDifferentFiles_AreRefused()
    {
        // Java's 02 | 08 00 01 02 80 | record_count 02 also reads, as the schema says, as fixed 08 00 01 02 and
        // record_count 80 02 (= 128), with everything after it aligned.
        const string fixedNode = "{\"type\": \"fixed\", \"name\": \"fixed_4\", \"size\": 4}";
        string onlyFixed(string type)
        {
            string schema = FixedPartitionSchema(type);
            int s = schema.IndexOf(", {\"name\": \"s\"", StringComparison.Ordinal);
            int e = schema.IndexOf("\"field-id\": 1001}", s, StringComparison.Ordinal) + "\"field-id\": 1001}".Length;
            return schema.Substring(0, s) + schema.Substring(e);
        }
        var asBytes = (AvroRecordSchema)AvroSchemaParser.Parse(onlyFixed("\"bytes\""));
        var header = (AvroRecordSchema)AvroSchemaParser.Parse(onlyFixed(fixedNode));
        var w = new AvroBinaryWriter(new GrowableBuffer());
        AvroDatum.Write(w, asBytes, Entry(asBytes, new()
        {
            ["status"] = 1, ["snapshot_id"] = 1L,
            ["data_file"] = (Func<AvroRecordSchema, AvroRecordValue>)(df => Rec(df, new()
            {
                ["content"] = 0, ["file_path"] = "a.parquet", ["file_format"] = "PARQUET",
                ["partition"] = (Func<AvroRecordSchema, AvroRecordValue>)(p => Rec(p, new() { ["fx"] = new byte[] { 0, 1, 2, 0x80 } })),
                ["record_count"] = 1L, ["file_size_in_bytes"] = 1L,
            })),
        }));
        var ms = new MemoryStream();
        using (var ocf = new OcfWriter(ms, AvroCodec.Null))
        {
            ocf.WriteHeader(header);
            ocf.WriteBlock(w.WrittenSpan, 1);
        }

        var ex = Assert.Throws<InvalidDataException>(() => ManifestCodec.DecodeManifest(ms.ToArray(), "ambiguous.avro",
            [new PartitionValueType(1000, "fx", new FixedType(4))]));
        Assert.Contains("apache/iceberg#17501", ex.Message);
    }

    [Fact]
    public void PartitionValueTypedOtherwiseThanItsSpec_IsRefused()
    {
        var spec = new List<PartitionValueType> { new(1000, "amount", IcebergType.Decimal(9, 3)) };
        var entry = new ManifestEntry
        {
            Status = ManifestEntryStatus.Added, SnapshotId = 1,
            DataFile = new DataFile
            {
                FilePath = "a.parquet", RecordCount = 1, FileSizeInBytes = 1,
                // 1.23 as decimal(9, 2); written as decimal(9, 3) it would read back as 0.123.
                Partition = new PartitionData([new PartitionValue(1000, "amount", IcebergType.Decimal(9, 2), new BigInteger(123))]),
            },
        };
        Assert.Throws<ArgumentException>(() =>
            ManifestCodec.EncodeManifest([entry], 2, ManifestContent.Data, spec, new Dictionary<string, byte[]>()));
    }

    [Fact]
    public void DecimalWiderThanItsPrecision_IsRefused()
    {
        Assert.Equal(new byte[] { 99 }, IcebergAvroTypes.ToAvro(new BigInteger(99), IcebergType.Decimal(2, 0)));
        // 100 fits decimal(2, 0)'s one byte, but not its two digits.
        Assert.Throws<ArgumentException>(() => IcebergAvroTypes.ToAvro(new BigInteger(100), IcebergType.Decimal(2, 0)));
    }

    [Fact]
    public void DateUnderAPromotedTimestamp_IsMidnightOfThatDay()
    {
        // An identity partition on a date column, read after the column became a timestamp (a v3 promotion).
        Assert.Equal(86_400_000_000L, IcebergAvroTypes.ToPhysical(1, IcebergType.Timestamp));
        Assert.Equal(-86_400_000_000L, IcebergAvroTypes.ToPhysical(-1, IcebergType.Timestamptz));
        Assert.Equal(86_400_000_000_000L, IcebergAvroTypes.ToPhysical(1, IcebergType.TimestampNs));
        Assert.Equal(1L, IcebergAvroTypes.ToPhysical(1, IcebergType.Long));
    }

    [Fact]
    public void NonNullFixedPartitionValue_IsNotWritten()
    {
        var type = new List<PartitionValueType> { new(1000, "fx", new FixedType(4)) };
        ManifestEntry With(byte[]? fx) => new()
        {
            Status = ManifestEntryStatus.Added, SnapshotId = 1,
            DataFile = new DataFile
            {
                FilePath = "a.parquet", RecordCount = 1, FileSizeInBytes = 1,
                Partition = new PartitionData([new PartitionValue(1000, "fx", new FixedType(4), fx)]),
            },
        };

        var ex = Assert.Throws<NotSupportedException>(() =>
            ManifestCodec.EncodeManifest([With([0, 1, 2, 3])], 2, ManifestContent.Data, type, new Dictionary<string, byte[]>()));
        Assert.Contains("apache/iceberg#17501", ex.Message);
        // A null value encodes the same in both readings.
        ManifestCodec.EncodeManifest([With(null)], 2, ManifestContent.Data, type, new Dictionary<string, byte[]>());
    }

    [Fact]
    public void UnmodelledFieldWithAValue_IsReported()
    {
        string schema = JavaManifestSchema.Replace(
            """{"name": "referenced_data_file",""",
            """{"name": "future_field", "type": ["null", "long"], "default": null, "field-id": 199}, {"name": "referenced_data_file",""");
        AvroRecordValue Make(AvroRecordSchema s, long? future) => Entry(s, new()
        {
            ["status"] = 1, ["snapshot_id"] = 1L,
            ["data_file"] = (Func<AvroRecordSchema, AvroRecordValue>)(df => Rec(df, new()
            {
                ["content"] = 0, ["file_path"] = "a.parquet", ["file_format"] = "PARQUET",
                ["partition"] = (Func<AvroRecordSchema, AvroRecordValue>)(p => Rec(p, new())),
                ["record_count"] = 1L, ["file_size_in_bytes"] = 1L, ["future_field"] = future,
            })),
        });

        // A null value loses nothing; a value would be dropped by a rewrite.
        Assert.Empty(ManifestCodec.DecodeManifest(WriteOcf(schema, s => Make(s, null)), "m.avro", null).UnmodelledFields);
        var lost = ManifestCodec.DecodeManifest(WriteOcf(schema, s => Make(s, 7)), "m.avro", null).UnmodelledFields;
        Assert.Equal(["data_file.future_field (field-id 199)"], lost);
    }

    // The v1 layout: snapshot_id required, no content or sequence numbers, block_size_in_bytes required.
    private const string V1ManifestSchema = """
        {"type": "record", "name": "manifest_entry", "fields": [
          {"name": "status", "type": "int", "field-id": 0},
          {"name": "snapshot_id", "type": "long", "field-id": 1},
          {"name": "data_file", "field-id": 2, "type": {"type": "record", "name": "r2", "fields": [
            {"name": "file_path", "type": "string", "field-id": 100},
            {"name": "file_format", "type": "string", "field-id": 101},
            {"name": "partition", "field-id": 102, "type": {"type": "record", "name": "r102", "fields": []}},
            {"name": "record_count", "type": "long", "field-id": 103},
            {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
            {"name": "block_size_in_bytes", "type": "long", "field-id": 105}
          ]}}
        ]}
        """;

    private const string V1ManifestListSchema = """
        {"type": "record", "name": "manifest_file", "fields": [
          {"name": "manifest_path", "type": "string", "field-id": 500},
          {"name": "manifest_length", "type": "long", "field-id": 501},
          {"name": "partition_spec_id", "type": "int", "field-id": 502},
          {"name": "added_snapshot_id", "type": "long", "field-id": 503},
          {"name": "added_data_files_count", "type": ["null", "int"], "default": null, "field-id": 504}
        ]}
        """;

    [Fact]
    public void V1_DecodesWithSequenceNumberZero()
    {
        var manifest = ManifestCodec.DecodeManifest(WriteOcf(V1ManifestSchema, s => Entry(s, new()
        {
            ["status"] = 1, ["snapshot_id"] = 9L,
            ["data_file"] = (Func<AvroRecordSchema, AvroRecordValue>)(df => Rec(df, new()
            {
                ["file_path"] = "a.parquet", ["file_format"] = "avro",
                ["partition"] = (Func<AvroRecordSchema, AvroRecordValue>)(p => Rec(p, new())),
                ["record_count"] = 4L, ["file_size_in_bytes"] = 40L, ["block_size_in_bytes"] = 1024L,
            })),
        })), "v1.avro", null);

        var list = ManifestCodec.DecodeManifestList(WriteOcf(V1ManifestListSchema, s => Rec(s, new()
        {
            ["manifest_path"] = "v1.avro", ["manifest_length"] = 10L, ["partition_spec_id"] = 0,
            ["added_snapshot_id"] = 9L, ["added_data_files_count"] = null,
        })), "snap.avro");

        var mle = Assert.Single(list.Entries);
        Assert.Equal(ManifestContent.Data, mle.Content);
        Assert.Equal(0, mle.SequenceNumber);
        Assert.Null(mle.AddedDataFilesCount); // unknown, not zero
        Assert.Null(mle.ExistingDataFilesCount);

        var e = Assert.Single(ManifestCodec.Inherit(manifest, mle));
        Assert.Equal(0, e.SequenceNumber);
        Assert.Equal(0, e.FileSequenceNumber);
        Assert.Equal(FileFormat.Avro, e.DataFile.FileFormat);
        Assert.Equal(1024, e.DataFile.BlockSizeInBytes);

        // Unknown counts cannot be carried into a v2 list, which requires them.
        Assert.Throws<NotSupportedException>(() =>
            ManifestCodec.EncodeManifestList(list.Entries, 2, new Dictionary<string, byte[]>()));
        // A v1 list keeps them unknown.
        var again = ManifestCodec.DecodeManifestList(
            ManifestCodec.EncodeManifestList(list.Entries, 1, new Dictionary<string, byte[]>()), "again.avro");
        Assert.Null(again.Entries[0].AddedDataFilesCount);
    }

    // ---------------------------------------------------------------- inheritance

    [Fact]
    public void Inheritance_FillsSnapshotSequenceAndFirstRowIds()
    {
        DataFile File(string p, long rows, long? firstRowId = null) =>
            new() { FilePath = p, RecordCount = rows, FileSizeInBytes = 1, FirstRowId = firstRowId };
        var manifest = new DecodedManifest(
        [
            new() { Status = ManifestEntryStatus.Added, DataFile = File("a", 10) },
            new() { Status = ManifestEntryStatus.Existing, SnapshotId = 3, SequenceNumber = 2, FileSequenceNumber = 2, DataFile = File("b", 5, 500) },
            new() { Status = ManifestEntryStatus.Added, DataFile = File("c", 7) },
        ], new Dictionary<string, byte[]>(), HasSequenceNumbers: true, []);
        var mle = new ManifestListEntry
        {
            ManifestPath = "m", ManifestLength = 1, AddedSnapshotId = 8, SequenceNumber = 6, FirstRowId = 1000,
        };

        var inherited = ManifestCodec.Inherit(manifest, mle);

        // A DELETED entry neither takes a row id nor uses one up, as Iceberg Java reads it.
        var withDeleted = manifest with
        {
            Entries = [new() { Status = ManifestEntryStatus.Deleted, SnapshotId = 3, SequenceNumber = 1, FileSequenceNumber = 1, DataFile = File("gone", 99) },
                       .. manifest.Entries],
        };
        var afterDeleted = ManifestCodec.Inherit(withDeleted, mle);
        Assert.Null(afterDeleted[0].DataFile.FirstRowId);
        Assert.Equal([1000L, 500, 1010], afterDeleted.Skip(1).Select(e => e.DataFile.FirstRowId!.Value));
        Assert.Equal([8L, 3, 8], inherited.Select(e => e.SnapshotId!.Value));
        Assert.Equal([6L, 2, 6], inherited.Select(e => e.SequenceNumber!.Value));
        Assert.Equal([6L, 2, 6], inherited.Select(e => e.FileSequenceNumber!.Value));
        // a gets 1000; b keeps its own; c follows a (only files that inherit consume ids).
        Assert.Equal([1000L, 500, 1010], inherited.Select(e => e.DataFile.FirstRowId!.Value));
    }

    // ---------------------------------------------------------------- manifest list

    [Fact]
    public void ManifestList_RoundTripsEveryField()
    {
        var entries = new List<ManifestListEntry>
        {
            new()
            {
                ManifestPath = "m1.avro", ManifestLength = 100, PartitionSpecId = 1, Content = ManifestContent.Deletes,
                SequenceNumber = 4, MinSequenceNumber = 2, AddedSnapshotId = 9, AddedDataFilesCount = 1,
                ExistingDataFilesCount = 2, DeletedDataFilesCount = 3, AddedRowsCount = 10, ExistingRowsCount = 20,
                DeletedRowsCount = 30, KeyMetadata = [7, 7],
                Partitions = [new PartitionFieldSummary(true, false, [1, 0, 0, 0], [9, 0, 0, 0]), new PartitionFieldSummary(false, null, null, null)],
                FirstRowId = 77,
            },
        };
        var md = new ManifestListWriteOptions { FormatVersion = 3, SnapshotId = 9, ParentSnapshotId = null, SequenceNumber = 4, FirstRowId = 70 };
        var decoded = ManifestCodec.DecodeManifestList(ManifestCodec.EncodeManifestList(entries, 3, md.HeaderMetadata()), "l.avro");

        var e = Assert.Single(decoded.Entries);
        var x = entries[0];
        Assert.Equal((x.ManifestPath, x.ManifestLength, x.PartitionSpecId, x.Content, x.SequenceNumber, x.MinSequenceNumber,
                      x.AddedSnapshotId, x.AddedDataFilesCount, x.ExistingDataFilesCount, x.DeletedDataFilesCount,
                      x.AddedRowsCount, x.ExistingRowsCount, x.DeletedRowsCount, x.FirstRowId),
                     (e.ManifestPath, e.ManifestLength, e.PartitionSpecId, e.Content, e.SequenceNumber, e.MinSequenceNumber,
                      e.AddedSnapshotId, e.AddedDataFilesCount, e.ExistingDataFilesCount, e.DeletedDataFilesCount,
                      e.AddedRowsCount, e.ExistingRowsCount, e.DeletedRowsCount, e.FirstRowId));
        Assert.Equal(x.KeyMetadata, e.KeyMetadata);
        Assert.Equal(2, e.Partitions!.Count);
        Assert.True(e.Partitions[0].ContainsNull);
        Assert.Equal([9, 0, 0, 0], e.Partitions[0].UpperBound);
        Assert.Null(e.Partitions[1].ContainsNan);

        Assert.Equal("9", Encoding.UTF8.GetString(decoded.Metadata["snapshot-id"]));
        Assert.Equal("null", Encoding.UTF8.GetString(decoded.Metadata["parent-snapshot-id"]));
        Assert.Equal("4", Encoding.UTF8.GetString(decoded.Metadata["sequence-number"]));
        Assert.Equal("70", Encoding.UTF8.GetString(decoded.Metadata["first-row-id"]));
        Assert.Equal("3", Encoding.UTF8.GetString(decoded.Metadata["format-version"]));

        Assert.Throws<ArgumentException>(() =>
            ManifestCodec.EncodeManifestList(entries, 2, new Dictionary<string, byte[]>()));
    }

    // ---------------------------------------------------------------- partition summaries

    [Fact]
    public void PartitionSummaries_BoundTheValues()
    {
        var type = new List<PartitionValueType>
        {
            new(1000, "i", IcebergType.Int),
            new(1001, "d", IcebergType.Double),
            new(1002, "s", IcebergType.String),
        };
        DataFile F(int? i, double? d, string? s) => new()
        {
            FilePath = "f", RecordCount = 1, FileSizeInBytes = 1,
            Partition = new PartitionData(
            [
                new PartitionValue(1000, "i", IcebergType.Int, i),
                new PartitionValue(1001, "d", IcebergType.Double, d),
                new PartitionValue(1002, "s", IcebergType.String, s),
            ]),
        };
        // A stand-in for the Appendix D encoder: the bound's text, so the test can read it back.
        // (.NET Framework formats -0.0 as "0", so the sign is spelled out.)
        byte[]? Encode(IcebergType t, object v) => Encoding.UTF8.GetBytes(
            v is double d && d == 0 && BitConverter.DoubleToInt64Bits(d) < 0
                ? "-0"
                : Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture)!);
        string Text(byte[]? b) => b is null ? "null" : Encoding.UTF8.GetString(b);

        var summaries = PartitionSummaries.Build(type,
            [F(5, 0.0, "b"), F(-3, -0.0, "�"), F(null, double.NaN, "\U0001F600"), F(9, 2.5, null)], Encode)!;

        Assert.Equal((true, false, "-3", "9"), (summaries[0].ContainsNull, summaries[0].ContainsNan, Text(summaries[0].LowerBound), Text(summaries[0].UpperBound)));
        // -0 sorts below +0, and NaN is flagged, not a bound.
        Assert.Equal((false, true, "-0", "2.5"), (summaries[1].ContainsNull, summaries[1].ContainsNan, Text(summaries[1].LowerBound), Text(summaries[1].UpperBound)));
        // Code-point order: U+1F600 sorts after U+FFFD, although its UTF-16 surrogates sort before.
        Assert.Equal("\U0001F600", Text(summaries[2].UpperBound));

        // Without an encoder there is no summary at all, never one with missing bounds.
        Assert.Null(PartitionSummaries.Build(type, [F(1, 1, "a")], encodeBound: null));
        Assert.Empty(PartitionSummaries.Build([], [F(1, 1, "a")], encodeBound: null)!);
    }

    // ---------------------------------------------------------------- helpers

    private static void AssertEntryEqual(ManifestEntry expected, ManifestEntry actual)
    {
        Assert.Equal((expected.Status, expected.SnapshotId, expected.SequenceNumber, expected.FileSequenceNumber),
                     (actual.Status, actual.SnapshotId, actual.SequenceNumber, actual.FileSequenceNumber));
        var e = expected.DataFile;
        var a = actual.DataFile;
        Assert.Equal((e.Content, e.FilePath, e.FileFormat, e.RecordCount, e.FileSizeInBytes, e.BlockSizeInBytes, e.SortOrderId,
                      e.FirstRowId, e.ReferencedDataFile, e.ContentOffset, e.ContentSizeInBytes),
                     (a.Content, a.FilePath, a.FileFormat, a.RecordCount, a.FileSizeInBytes, a.BlockSizeInBytes, a.SortOrderId,
                      a.FirstRowId, a.ReferencedDataFile, a.ContentOffset, a.ContentSizeInBytes));
        Assert.Equal(e.Partition, a.Partition);
        Assert.Equal(e.ColumnSizes, a.ColumnSizes);
        Assert.Equal(e.ValueCounts, a.ValueCounts);
        Assert.Equal(e.NullValueCounts, a.NullValueCounts);
        Assert.Equal(e.NanValueCounts, a.NanValueCounts);
        Assert.Equal(e.LowerBounds?.ToDictionary(p => p.Key, p => Convert.ToBase64String(p.Value)),
                     a.LowerBounds?.ToDictionary(p => p.Key, p => Convert.ToBase64String(p.Value)));
        Assert.Equal(e.UpperBounds?.ToDictionary(p => p.Key, p => Convert.ToBase64String(p.Value)),
                     a.UpperBounds?.ToDictionary(p => p.Key, p => Convert.ToBase64String(p.Value)));
        Assert.Equal(e.SplitOffsets, a.SplitOffsets);
        Assert.Equal(e.EqualityIds, a.EqualityIds);
        Assert.Equal(e.KeyMetadata, a.KeyMetadata);
    }

    /// <summary>An OCF file of one record, in exactly <paramref name="schemaJson"/>.</summary>
    private static byte[] WriteOcf(string schemaJson, Func<AvroRecordSchema, AvroRecordValue> record)
    {
        var schema = (AvroRecordSchema)AvroSchemaParser.Parse(schemaJson);
        var buffer = new GrowableBuffer();
        var w = new AvroBinaryWriter(buffer);
        AvroDatum.Write(w, schema, record(schema));
        var ms = new MemoryStream();
        using (var ocf = new OcfWriter(ms, AvroCodec.Deflate))
        {
            ocf.WriteHeader(schema);
            ocf.WriteBlock(w.WrittenSpan, 1);
        }
        return ms.ToArray();
    }

    private static AvroRecordValue Entry(AvroRecordSchema schema, Dictionary<string, object?> values) => Rec(schema, values);

    /// <summary>A record by field name; a missing field is null, and a nested record is built by a function of its schema.</summary>
    private static AvroRecordValue Rec(AvroRecordSchema schema, Dictionary<string, object?> values)
    {
        var result = new object?[schema.Fields.Count];
        for (int i = 0; i < result.Length; i++)
        {
            var field = schema.Fields[i];
            values.TryGetValue(field.Name, out var v);
            if (v is Func<AvroRecordSchema, AvroRecordValue> nested)
            {
                var inner = field.Schema is AvroUnionSchema u && u.IsNullable(out var t, out _) ? t : field.Schema;
                v = nested((AvroRecordSchema)inner);
            }
            result[i] = v;
        }
        return new AvroRecordValue(schema, result);
    }

    private static List<object?> Kv<T>(AvroRecordSchema dataFile, string field, params (int Key, T Value)[] pairs)
    {
        var union = (AvroUnionSchema)dataFile.Fields.Single(f => f.Name == field).Schema;
        var item = (AvroRecordSchema)((AvroArraySchema)union.Branches[1]).Items;
        return pairs.Select(p => (object?)new AvroRecordValue(item, [p.Key, p.Value])).ToList();
    }

    private static string StripIds(string json) =>
        System.Text.RegularExpressions.Regex.Replace(json, @",\s*""(field|element)-id"":\s*[0-9]+", "");
}
