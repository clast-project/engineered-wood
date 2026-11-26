// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Expressions;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests.Manifest;

/// <summary>
/// What a commit writes into manifests: new entries left to inherit, and a rewritten manifest whose
/// entries carry every inherited value, since they now sit in a manifest of a later snapshot.
/// </summary>
public class ManifestRewriteTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"iceberg-rewrite-{Guid.NewGuid():N}");
    private readonly LocalTableFileSystem _fs;
    private readonly TableOperations _ops;

    public ManifestRewriteTests()
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

    private TableMetadata NewTable(int formatVersion = 2) => TableMetadata.Create(
        new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]),
        location: Path.Combine(_dir, "t"), formatVersion: formatVersion);

    private static DataFile File(string name, long rows = 10) =>
        new() { FilePath = $"data/{name}.parquet", RecordCount = rows, FileSizeInBytes = 100 };

    private async Task<List<ManifestEntry>> RawEntriesAsync(TableMetadata md)
    {
        var snapshot = md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId);
        var result = new List<ManifestEntry>();
        foreach (var mle in await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList))
            result.AddRange(await ManifestIO.ReadManifestAsync(_fs, mle.ManifestPath));
        return result;
    }

    [Fact]
    public async Task NewEntries_InheritTheirSequenceNumber()
    {
        var md = await _ops.AppendFilesAsync(NewTable(), [File("a")]);

        var raw = Assert.Single(await RawEntriesAsync(md));
        Assert.Equal(ManifestEntryStatus.Added, raw.Status);
        Assert.Null(raw.SequenceNumber);
        Assert.Null(raw.FileSequenceNumber);

        // Read through the snapshot, they take the manifest's sequence number.
        var live = Assert.Single(await _ops.ListDataFilesAsync(md));
        Assert.Equal("data/a.parquet", live.FilePath);
        var snapshot = md.Snapshots.Single();
        var mle = Assert.Single(await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList));
        var entry = Assert.Single(await ManifestIO.ReadSnapshotManifestAsync(_fs, md, snapshot, mle, default));
        Assert.Equal(1, entry.SequenceNumber);
        Assert.Equal(1, entry.FileSequenceNumber);
    }

    [Fact]
    public async Task Rewrite_MaterializesInheritedValues()
    {
        var md = await _ops.AppendFilesAsync(NewTable(), [File("a"), File("b")]);   // sequence 1
        md = await _ops.AppendFilesAsync(md, [File("c")]);                           // sequence 2
        var firstSnapshot = md.Snapshots[0].SnapshotId;
        md = await _ops.OverwriteFilesAsync(md, [File("a")], []);                    // sequence 3

        var raw = await RawEntriesAsync(md);
        var b = raw.Single(e => e.DataFile.FilePath == "data/b.parquet");
        // b moved to a manifest of snapshot 3, so it can no longer inherit: it is EXISTING, with the
        // snapshot and sequence numbers it was added with written out.
        Assert.Equal(ManifestEntryStatus.Existing, b.Status);
        Assert.Equal(firstSnapshot, b.SnapshotId);
        Assert.Equal(1, b.SequenceNumber);
        Assert.Equal(1, b.FileSequenceNumber);

        var a = raw.Single(e => e.DataFile.FilePath == "data/a.parquet");
        Assert.Equal(ManifestEntryStatus.Deleted, a.Status);
        Assert.Equal(md.CurrentSnapshotId, a.SnapshotId);
        Assert.Equal(1, a.SequenceNumber);

        // c's manifest was not touched, so its entry still inherits sequence 2.
        var c = raw.Single(e => e.DataFile.FilePath == "data/c.parquet");
        Assert.Equal(ManifestEntryStatus.Added, c.Status);
        Assert.Null(c.SequenceNumber);

        var live = await _ops.ListDataFilesAsync(md);
        Assert.Equal(["data/b.parquet", "data/c.parquet"], live.Select(f => f.FilePath).OrderBy(p => p));

        // The rewritten manifest's counts describe what it now holds.
        var snapshot = md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId);
        var rewritten = (await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList))
            .Single(m => m.AddedSnapshotId == md.CurrentSnapshotId);
        Assert.Equal((0, 1, 1, 0L, 10L, 10L, 1L),
            (rewritten.AddedDataFilesCount, rewritten.ExistingDataFilesCount, rewritten.DeletedDataFilesCount,
             rewritten.AddedRowsCount, rewritten.ExistingRowsCount, rewritten.DeletedRowsCount, rewritten.MinSequenceNumber));

        // A later rewrite does not carry the DELETED entry forward.
        md = await _ops.OverwriteFilesAsync(md, [File("b")], []);
        Assert.DoesNotContain(await RawEntriesAsync(md),
            e => e.DataFile.FilePath == "data/a.parquet");
    }

    [Fact]
    public async Task Rewrite_OfAManifestWithFieldsItCannotHold_IsRefused()
    {
        var md = await _ops.AppendFilesAsync(NewTable(), [File("a")]);
        var snapshot = md.Snapshots.Single();
        var mle = Assert.Single(await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList));

        // Replace the manifest with one holding a field this library does not model.
        var bytes = await _fs.ReadAllBytesAsync(mle.ManifestPath);
        var withExtra = WithExtraDataFileField(bytes);
        await _fs.WriteAllBytesAsync(mle.ManifestPath, withExtra);

        // Reading is fine; rewriting would drop the field.
        Assert.Single(await _ops.ListDataFilesAsync(md));
        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await _ops.OverwriteFilesAsync(md, [File("a")], []));
        Assert.Contains("future_field", ex.Message);
    }

    [Fact]
    public async Task EncryptedManifestOrFile_IsRefused()
    {
        var md = await _ops.AppendFilesAsync(NewTable(), [File("a")]);
        var snapshot = md.Snapshots.Single();
        var mle = Assert.Single(await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList));

        // A manifest-list entry with key metadata: the manifest is encrypted.
        var encryptedList = Path.Combine(_dir, "t", "metadata", "snap-encrypted.avro");
        await ManifestIO.WriteManifestListAsync(_fs, encryptedList, [mle with { KeyMetadata = [1, 2, 3] }]);
        var mdEncryptedManifest = md with
        {
            Snapshots = [snapshot with { ManifestList = encryptedList }],
        };
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(
            async () => await new TableScan(mdEncryptedManifest, _fs).PlanFilesAsync());

        // A data file with key metadata: the file is encrypted.
        var entries = await ManifestIO.ReadManifestAsync(_fs, mle.ManifestPath);
        await ManifestIO.WriteManifestAsync(_fs, mle.ManifestPath,
            entries.Select(e => e with { DataFile = e.DataFile with { KeyMetadata = [9] } }).ToList());
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(
            async () => await _ops.ListDataFilesAsync(md));
    }

    /// <summary>The manifest with a populated <c>future_field</c> (field id 199) added to its data_file record.</summary>
    private static byte[] WithExtraDataFileField(byte[] manifest)
    {
        var decoded = ManifestCodec.DecodeManifest(manifest, "m.avro", partitionType: null);
        var json = System.Text.Encoding.UTF8.GetString(decoded.Metadata["avro.schema"]).Replace(
            "{\"name\":\"sort_order_id\"",
            "{\"name\":\"future_field\",\"type\":[\"null\",\"long\"],\"default\":null,\"field-id\":199},{\"name\":\"sort_order_id\"");
        var schema = (EngineeredWood.Avro.Schema.AvroRecordSchema)EngineeredWood.Avro.Schema.AvroSchemaParser.Parse(json);

        // Re-read the original records generically, and write them with a value in the new field.
        using var ms = new MemoryStream(manifest);
        using var reader = EngineeredWood.Avro.Container.OcfReader.Open(ms);
        var buffer = new EngineeredWood.Buffers.GrowableBuffer();
        var w = new EngineeredWood.Avro.Encoding.AvroBinaryWriter(buffer);
        int count = 0;
        while (reader.ReadBlock() is { } block)
        {
            var br = new EngineeredWood.Avro.Encoding.AvroBinaryReader(block.data.Span);
            for (long i = 0; i < block.objectCount; i++, count++)
            {
                var entry = (AvroRecordValue)AvroDatum.Read(ref br, reader.WriterSchema)!;
                var dfIndex = entry.Schema.Fields.ToList().FindIndex(f => f.Name == "data_file");
                var df = (AvroRecordValue)entry.Values[dfIndex]!;
                var dfSchema = (EngineeredWood.Avro.Schema.AvroRecordSchema)schema.Fields[dfIndex].Schema;
                var values = new List<object?>();
                for (int f = 0; f < dfSchema.Fields.Count; f++)
                {
                    var name = dfSchema.Fields[f].Name;
                    values.Add(name == "future_field"
                        ? 5L
                        : df.Values[df.Schema.Fields.ToList().FindIndex(x => x.Name == name)]);
                }
                var newEntry = entry.Values.ToArray();
                newEntry[dfIndex] = new AvroRecordValue(dfSchema, values.ToArray());
                AvroDatum.Write(w, schema, new AvroRecordValue(schema, newEntry));
            }
        }

        var output = new MemoryStream();
        using (var writer = new EngineeredWood.Avro.Container.OcfWriter(output, EngineeredWood.Avro.AvroCodec.Null))
        {
            writer.WriteHeader(schema);
            writer.WriteBlock(w.WrittenSpan, count);
        }
        return output.ToArray();
    }
}
