// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;
using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests.Manifest;

/// <summary>
/// Cases where a manifest write could misstate a table or lose another engine's data: names Avro cannot
/// hold, mixed content, deletion vectors sharing a Puffin file, a header schema the spec cannot bind,
/// encrypted or unmodelled manifest-list entries, NaN partition values and void geo fields.
/// </summary>
public class ManifestSafetyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"iceberg-safety-{Guid.NewGuid():N}");
    private readonly LocalTableFileSystem _fs;
    private readonly TableOperations _ops;

    public ManifestSafetyTests()
    {
        Directory.CreateDirectory(_dir);
        _fs = new LocalTableFileSystem(_dir);
        _ops = new TableOperations(_fs);
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    private static DataFile File(string path, PartitionData? partition = null) =>
        new() { FilePath = path, RecordCount = 10, FileSizeInBytes = 100, Partition = partition };

    private List<string> FilesUnder(string dir) =>
        Directory.Exists(dir) ? Directory.GetFiles(dir, "*", SearchOption.AllDirectories).OrderBy(p => p).ToList() : [];

    [Theory]
    [InlineData("event_day", "event_day")]
    [InlineData("event-day", "event_x2Dday")]
    [InlineData("1st", "_1st")]
    [InlineData("a.b c", "a_x2Eb_x20c")]
    public void PartitionNames_AreEscapedAsJavaEscapesThem(string name, string avro) =>
        Assert.Equal(avro, ManifestAvroSchemas.AvroCompatibleName(name));

    [Fact]
    public async Task PartitionNameAvroCannotHold_IsEscaped_AndReadBackFromTheSpec()
    {
        var schema = new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]);
        var spec = new PartitionSpec(0, [new PartitionField(1, 1000, "id-bucket", Transform.Bucket(4))]);
        var md = TableMetadata.Create(schema, spec, location: Path.Combine(_dir, "t"));
        var tuple = new PartitionData([new PartitionValue(1000, "id-bucket", IcebergType.Int, 3)]);
        md = await _ops.AppendFilesAsync(md, [File("data/a.parquet", tuple)]);

        var snapshot = md.Snapshots.Single();
        var mle = Assert.Single(await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList));
        using (var stream = System.IO.File.OpenRead(mle.ManifestPath))
        using (var reader = new AvroReaderBuilder().Build(stream))
        {
            var avro = (AvroRecordSchema)AvroSchemaParser.Parse(Encoding.UTF8.GetString(reader.Metadata["avro.schema"]));
            var dataFile = (AvroRecordSchema)avro.Fields.Single(f => f.Name == "data_file").Schema;
            var partition = (AvroRecordSchema)dataFile.Fields.Single(f => f.Name == "partition").Schema;
            Assert.Equal("id_x2Dbucket", Assert.Single(partition.Fields).Name);
        }

        Assert.Equal(tuple, Assert.Single(await _ops.ListDataFilesAsync(md)).Partition);
    }

    [Fact]
    public async Task ContentTheManifestDoesNotDeclare_IsRefused()
    {
        var delete = new ManifestEntry
        {
            Status = ManifestEntryStatus.Added, SnapshotId = 1,
            DataFile = File("d.parquet") with { Content = FileContent.PositionDeletes },
        };
        var data = new ManifestEntry { Status = ManifestEntryStatus.Added, SnapshotId = 1, DataFile = File("a.parquet") };

        // The default overload writes a data manifest.
        await Assert.ThrowsAsync<ArgumentException>(
            async () => await ManifestIO.WriteManifestAsync(_fs, Path.Combine(_dir, "m.avro"), [delete]));
        await Assert.ThrowsAsync<ArgumentException>(
            async () => await ManifestIO.WriteManifestAsync(_fs, Path.Combine(_dir, "m.avro"), [data, delete],
                new ManifestWriteOptions { Content = ManifestContent.Deletes }));
        await Assert.ThrowsAsync<ArgumentException>(
            async () => await ManifestIO.WriteManifestAsync(_fs, Path.Combine(_dir, "m.json"), [delete]));
    }

    [Fact]
    public void SequenceNumbersV1CannotHold_AreRefused()
    {
        var entry = new ManifestEntry
        {
            Status = ManifestEntryStatus.Existing, SnapshotId = 1, SequenceNumber = 5, FileSequenceNumber = 5,
            DataFile = File("a.parquet"),
        };
        Assert.Throws<ArgumentException>(() =>
            ManifestCodec.EncodeManifest([entry], 1, ManifestContent.Data, [], new Dictionary<string, byte[]>()));
        // Sequence number 0 is what every v1 file has.
        ManifestCodec.EncodeManifest([entry with { SequenceNumber = 0, FileSequenceNumber = 0 }], 1,
            ManifestContent.Data, [], new Dictionary<string, byte[]>());
    }

    [Fact]
    public async Task RemovingOneDeletionVector_KeepsTheOthersInItsPuffinFile()
    {
        var md = TableMetadata.Create(new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]),
            location: Path.Combine(_dir, "dv"), formatVersion: 3);
        md = await _ops.AppendFilesAsync(md, [File("data/a.parquet"), File("data/b.parquet")]);
        DataFile Dv(string target, long offset) => new()
        {
            Content = FileContent.PositionDeletes, FilePath = "data/dvs.puffin", FileFormat = FileFormat.Puffin,
            RecordCount = 1, FileSizeInBytes = 200, ReferencedDataFile = target,
            ContentOffset = offset, ContentSizeInBytes = 30,
        };
        md = await _ops.AppendDeleteFilesAsync(md, [Dv("data/a.parquet", 4), Dv("data/b.parquet", 40)]);

        md = await _ops.OverwriteFilesAsync(md, [File("data/a.parquet"), Dv("data/a.parquet", 4)], []);

        var dv = Assert.Single(await _ops.ListDeleteFilesAsync(md));
        Assert.Equal(("data/b.parquet", 40L), (dv.ReferencedDataFile, dv.ContentOffset!.Value));

        // A DV named without its offset is not one the table holds.
        await Assert.ThrowsAsync<CommitValidationException>(
            async () => await _ops.OverwriteFilesAsync(md, [Dv("data/b.parquet", 40) with { ContentOffset = null }], []));
    }

    [Fact]
    public async Task OverwriteRemovingDeleteFiles_CountsOnlyDataFilesAsRows()
    {
        var md = TableMetadata.Create(new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]),
            location: Path.Combine(_dir, "sum"), formatVersion: 3);
        md = await _ops.AppendFilesAsync(md, [File("data/a.parquet"), File("data/b.parquet")]);   // 20 rows
        var dv = new DataFile
        {
            Content = FileContent.PositionDeletes, FilePath = "data/dvs.puffin", FileFormat = FileFormat.Puffin,
            RecordCount = 3, FileSizeInBytes = 200, ReferencedDataFile = "data/a.parquet", ContentOffset = 4, ContentSizeInBytes = 30,
        };
        var posDelete = new DataFile { Content = FileContent.PositionDeletes, FilePath = "data/pos.parquet", RecordCount = 2, FileSizeInBytes = 50 };
        var eqDelete = new DataFile { Content = FileContent.EqualityDeletes, FilePath = "data/eq.parquet", RecordCount = 1, FileSizeInBytes = 50, EqualityIds = [1] };
        md = await _ops.AppendDeleteFilesAsync(md, [dv, posDelete, eqDelete]);
        // Delete files leave the row count where it was.
        Assert.Equal("20", md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId).Summary["total-records"]);

        // The caller's record count is not what is counted: the table's own entry is.
        md = await _ops.OverwriteFilesAsync(md, [File("data/a.parquet") with { RecordCount = 999 }, dv, posDelete, eqDelete], []);

        var summary = md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId).Summary;
        Assert.Equal("1", summary["deleted-data-files"]);
        Assert.Equal("10", summary["deleted-records"]);
        Assert.Equal("10", summary["total-records"]);
        Assert.Equal("3", summary["removed-delete-files"]);
        Assert.Equal("1", summary["removed-dvs"]);
        Assert.Equal("1", summary["removed-position-delete-files"]);
        Assert.Equal("1", summary["removed-equality-delete-files"]);
        Assert.Equal("5", summary["removed-position-deletes"]);
        Assert.Equal("1", summary["removed-equality-deletes"]);

        // A data-only overwrite writes none of the delete-file keys.
        md = await _ops.OverwriteFilesAsync(md, [File("data/b.parquet")], []);
        var plain = md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId).Summary;
        Assert.Equal("0", plain["total-records"]);
        Assert.DoesNotContain(plain.Keys, k => k.StartsWith("removed-", StringComparison.Ordinal));
    }

    [Fact]
    public async Task UnknownPreviousTotal_IsNotTreatedAsZero()
    {
        var md = await _ops.AppendFilesAsync(
            TableMetadata.Create(new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]), location: Path.Combine(_dir, "tot")),
            [File("data/a.parquet")]);
        // Another writer's snapshot without a total-records.
        var snap = md.Snapshots.Single();
        md = md with { Snapshots = [snap with { Summary = new Dictionary<string, string> { ["operation"] = "append" } }] };

        md = await _ops.AppendFilesAsync(md, [File("data/b.parquet")]);
        Assert.False(md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId).Summary.ContainsKey("total-records"));
        md = await _ops.OverwriteFilesAsync(md, [File("data/a.parquet")], []);
        Assert.False(md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId).Summary.ContainsKey("total-records"));
    }

    [Fact]
    public void RewriteOfAnOldSpec_GetsAHeaderSchemaThatBindsIt()
    {
        // The spec's source column (2, cat) was dropped in schema 1.
        var s0 = new Schema(0, [new NestedField(1, "id", IcebergType.Long, true), new NestedField(2, "cat", IcebergType.String, false)]);
        var s1 = new Schema(1, [new NestedField(1, "id", IcebergType.Long, true)]);
        var oldSpec = new PartitionSpec(1, [new PartitionField(2, 1000, "cat", Transform.Identity)]);
        var md = TableMetadata.Create(s0, location: "s3://b/t") with
        {
            Schemas = [s0, s1],
            CurrentSchemaId = 1,
            PartitionSpecs = [PartitionSpec.Unpartitioned, oldSpec],
        };

        Assert.Equal(0, ManifestWriteOptions.For(md, 1, ManifestContent.Data).Schema!.SchemaId);
        Assert.Equal(1, ManifestWriteOptions.For(md, 0, ManifestContent.Data).Schema!.SchemaId);
    }

    [Fact]
    public void RewriteOfAnUnknownTransformSpec_GetsAHeaderSchemaWithItsSources()
    {
        // TryResolve cannot type the spec, but the header only needs its source fields.
        var s0 = new Schema(0, [new NestedField(1, "id", IcebergType.Long, true), new NestedField(2, "cat", IcebergType.String, false)]);
        var s1 = new Schema(1, [new NestedField(1, "id", IcebergType.Long, true)]);
        var spec = new PartitionSpec(1, [new PartitionField(1, 1000, "z", new UnknownTransform("zorder"), [1, 2])]);
        var md = TableMetadata.Create(s0, location: "s3://b/t") with
        {
            Schemas = [s0, s1],
            CurrentSchemaId = 1,
            PartitionSpecs = [PartitionSpec.Unpartitioned, spec],
        };
        Assert.Equal(0, ManifestWriteOptions.For(md, 1, ManifestContent.Data).Schema!.SchemaId);
    }

    [Fact]
    public async Task AddingAnEncryptedFile_IsRefused_AndWritesNothing()
    {
        var md = TableMetadata.Create(new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]), location: Path.Combine(_dir, "k"));
        var encrypted = File("data/a.parquet") with { KeyMetadata = [1, 2] };
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(async () => await _ops.AppendFilesAsync(md, [encrypted]));
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(async () => await _ops.AppendDeleteFilesAsync(md,
            [encrypted with { Content = FileContent.PositionDeletes }]));
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(async () => await _ops.OverwriteFilesAsync(md, [], [encrypted]));
        Assert.Empty(FilesUnder(md.Location));
    }

    [Fact]
    public async Task EncryptedEntryInAJsonManifestList_IsRefused()
    {
        var md = await _ops.AppendFilesAsync(
            TableMetadata.Create(new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]), location: Path.Combine(_dir, "j")),
            [File("data/a.parquet")]);
        var snapshot = md.Snapshots.Single();
        var mle = Assert.Single(await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList));
        var jsonList = Path.Combine(md.Location, "metadata", "snap-json.json");
        await ManifestIO.WriteManifestListAsync(_fs, jsonList, [mle with { KeyMetadata = [1] }]);
        md = md with { Snapshots = [snapshot with { ManifestList = jsonList }] };

        var before = FilesUnder(md.Location);
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(
            async () => await _ops.AppendFilesAsync(md, [File("data/b.parquet")]));
        Assert.Equal(before, FilesUnder(md.Location));
    }

    [Fact]
    public async Task ExpiringATableWithAnEncryptedManifest_IsRefused()
    {
        var md = TableMetadata.Create(new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]), location: Path.Combine(_dir, "x"));
        md = await _ops.AppendFilesAsync(md, [File("data/a.parquet")]);
        md = await _ops.AppendFilesAsync(md, [File("data/b.parquet")]);
        var current = md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId);
        var list = await ManifestIO.ReadManifestListAsync(_fs, current.ManifestList);
        await ManifestIO.WriteManifestListAsync(_fs, current.ManifestList, [list[0] with { KeyMetadata = [1] }, .. list.Skip(1)]);

        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(async () =>
            await new ExpireSnapshots().ExpireSnapshotId(md.Snapshots[0].SnapshotId).ApplyAsync(md, _fs));
    }

    [Fact]
    public async Task AppendCarryingAnEncryptedManifest_IsRefused_AndWritesNothing()
    {
        var md = await _ops.AppendFilesAsync(
            TableMetadata.Create(new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]), location: Path.Combine(_dir, "e")),
            [File("data/a.parquet")]);
        var snapshot = md.Snapshots.Single();
        var mle = Assert.Single(await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList));
        await ManifestIO.WriteManifestListAsync(_fs, snapshot.ManifestList, [mle with { KeyMetadata = [1] }]);

        var before = FilesUnder(md.Location);
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(
            async () => await _ops.AppendFilesAsync(md, [File("data/b.parquet")]));
        Assert.Equal(before, FilesUnder(md.Location));
    }

    [Fact]
    public async Task ManifestListWithAnUnmodelledField_IsNotRewritten()
    {
        var md = await _ops.AppendFilesAsync(
            TableMetadata.Create(new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]), location: Path.Combine(_dir, "u")),
            [File("data/a.parquet")]);
        var snapshot = md.Snapshots.Single();
        var mle = Assert.Single(await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList));

        // Rewrite the list with a populated field this library does not know.
        var schemaJson = AvroSchemaWriter.ToJson(ManifestAvroSchemas.ManifestFile(2)).Replace(
            "{\"name\":\"key_metadata\"",
            "{\"name\":\"future_field\",\"type\":[\"null\",\"long\"],\"default\":null,\"field-id\":599},{\"name\":\"key_metadata\"");
        var schema = (AvroRecordSchema)AvroSchemaParser.Parse(schemaJson);
        var original = (AvroRecordValue)DecodeFirst(await _fs.ReadAllBytesAsync(snapshot.ManifestList));
        var values = schema.Fields.Select(f => f.Name == "future_field"
            ? 1L
            : original.Values[original.Schema.Fields.ToList().FindIndex(o => o.Name == f.Name)]).ToArray();
        await _fs.WriteAllBytesAsync(snapshot.ManifestList, WriteOne(schema, new AvroRecordValue(schema, values)));

        // Reading still works; a commit, which writes the next list from this one, refuses.
        Assert.Single(await _ops.ListDataFilesAsync(md));
        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await _ops.AppendFilesAsync(md, [File("data/b.parquet")]));
        Assert.Contains("future_field", ex.Message);
    }

    [Fact]
    public void NaNPartitionValues_AreEqual_ButSignedZerosAreNot()
    {
        double otherNaN = BitConverter.Int64BitsToDouble(unchecked((long)0xFFF8_0000_0000_0001));
        Assert.True(double.IsNaN(otherNaN));
        var a = new PartitionValue(1000, "d", IcebergType.Double, double.NaN);
        var b = a with { Value = otherNaN };
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.Equal(a with { Value = float.NaN, Type = IcebergType.Float },
                     a with { Value = BitConverter.ToSingle(BitConverter.GetBytes(unchecked((int)0xFFC00001)), 0), Type = IcebergType.Float });
        Assert.NotEqual(a with { Value = 0.0 }, a with { Value = -0.0 });
    }

    [Fact]
    public async Task VoidFieldOverAGeoColumn_IsWrittenAsInt()
    {
        Assert.Equal(IcebergType.Int, PartitionTypes.ResultType(Transform.Void, IcebergType.Geometry("srid:4326")));
        Assert.Equal(IcebergType.Long, PartitionTypes.ResultType(Transform.Void, IcebergType.Long));

        var schema = new Schema(0, [new NestedField(1, "id", IcebergType.Long, true), new NestedField(2, "g", IcebergType.Geometry("srid:4326"), false)]);
        var spec = new PartitionSpec(0, [new PartitionField(2, 1000, "g_void", Transform.Void)]);
        var md = TableMetadata.Create(schema, spec, location: Path.Combine(_dir, "geo"), formatVersion: 3);
        var tuple = new PartitionData([new PartitionValue(1000, "g_void", IcebergType.Int, null)]);
        md = await _ops.AppendFilesAsync(md, [File("data/a.parquet", tuple)]);
        Assert.Equal(tuple, Assert.Single(await _ops.ListDataFilesAsync(md)).Partition);
    }

    [Fact]
    public async Task NewFilesUnderAnUnknownTransform_AreRefusedByTheManifestWriter()
    {
        var spec = new PartitionSpec(0, [new PartitionField(1, 1000, "z", new UnknownTransform("zorder"))]);
        var tuple = new PartitionData([new PartitionValue(1000, "z", IcebergType.Int, 1)]);
        var entry = new ManifestEntry { Status = ManifestEntryStatus.Added, SnapshotId = 1, DataFile = File("a.parquet", tuple) };
        var options = new ManifestWriteOptions { PartitionSpec = spec };

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await ManifestIO.WriteManifestAsync(_fs, Path.Combine(_dir, "n.avro"), [entry], options));

        // Rewriting a manifest that already holds such files types the tuple from the entries.
        await ManifestIO.WriteManifestAsync(_fs, Path.Combine(_dir, "r.avro"), [entry], options with { RewritesExistingManifest = true });
        Assert.Equal(tuple, Assert.Single(await ManifestIO.ReadManifestAsync(_fs, Path.Combine(_dir, "r.avro"))).DataFile.Partition);
    }

    private static object? DecodeFirst(byte[] ocf)
    {
        using var ms = new MemoryStream(ocf);
        using var reader = OcfReader.Open(ms);
        var block = reader.ReadBlock()!.Value;
        var br = new AvroBinaryReader(block.data.Span);
        return AvroDatum.Read(ref br, reader.WriterSchema);
    }

    private static byte[] WriteOne(AvroRecordSchema schema, AvroRecordValue record)
    {
        var w = new AvroBinaryWriter(new GrowableBuffer());
        AvroDatum.Write(w, schema, record);
        var ms = new MemoryStream();
        using (var ocf = new OcfWriter(ms, AvroCodec.Null))
        {
            ocf.WriteHeader(schema);
            ocf.WriteBlock(w.WrittenSpan, 1);
        }
        return ms.ToArray();
    }
}
