// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Schema;
using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests.Manifest;

/// <summary>
/// The manifest codec decodes by position in EW's own layout (#586), so a file with any
/// other writer schema must be refused rather than misread.
/// </summary>
public class ManifestSchemaGuardTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;

    public ManifestSchemaGuardTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"iceberg-guard-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // The data_file layout Iceberg Java writes for an unpartitioned V2 table: nan_value_counts,
    // lower_bounds, upper_bounds and key_metadata sit between null_value_counts and split_offsets.
    private const string JavaLayoutManifestEntry = """
        {
          "type": "record", "name": "manifest_entry", "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": ["null", "long"], "field-id": 1},
            {"name": "sequence_number", "type": ["null", "long"], "field-id": 3},
            {"name": "file_sequence_number", "type": ["null", "long"], "field-id": 4},
            {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
              {"name": "content", "type": "int", "field-id": 134},
              {"name": "file_path", "type": "string", "field-id": 100},
              {"name": "file_format", "type": "string", "field-id": 101},
              {"name": "partition", "type": {"type": "record", "name": "r102", "fields": []}, "field-id": 102},
              {"name": "record_count", "type": "long", "field-id": 103},
              {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
              {"name": "column_sizes", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v118", "fields": [
                {"name": "key", "type": "int", "field-id": 117}, {"name": "value", "type": "long", "field-id": 118}]}, "logicalType": "map"}], "field-id": 116},
              {"name": "value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k119_v120", "fields": [
                {"name": "key", "type": "int", "field-id": 119}, {"name": "value", "type": "long", "field-id": 120}]}, "logicalType": "map"}], "field-id": 121},
              {"name": "null_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k121_v122", "fields": [
                {"name": "key", "type": "int", "field-id": 121}, {"name": "value", "type": "long", "field-id": 122}]}, "logicalType": "map"}], "field-id": 110},
              {"name": "nan_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k138_v139", "fields": [
                {"name": "key", "type": "int", "field-id": 138}, {"name": "value", "type": "long", "field-id": 139}]}, "logicalType": "map"}], "field-id": 137},
              {"name": "lower_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k126_v127", "fields": [
                {"name": "key", "type": "int", "field-id": 126}, {"name": "value", "type": "bytes", "field-id": 127}]}, "logicalType": "map"}], "field-id": 125},
              {"name": "upper_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k129_v130", "fields": [
                {"name": "key", "type": "int", "field-id": 129}, {"name": "value", "type": "bytes", "field-id": 130}]}, "logicalType": "map"}], "field-id": 128},
              {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 131},
              {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "field-id": 132},
              {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int", "element-id": 136}], "field-id": 135},
              {"name": "sort_order_id", "type": ["null", "int"], "field-id": 140}
            ]}, "field-id": 2}
          ]
        }
        """;

    // A manifest list without the V2 sequence-number fields (the V1 layout).
    private const string V1LayoutManifestList = """
        {
          "type": "record", "name": "manifest_file", "fields": [
            {"name": "manifest_path", "type": "string", "field-id": 500},
            {"name": "manifest_length", "type": "long", "field-id": 501},
            {"name": "partition_spec_id", "type": "int", "field-id": 502},
            {"name": "added_snapshot_id", "type": ["null", "long"], "field-id": 503}
          ]
        }
        """;

    private string WriteEmptyOcf(string name, string schemaJson)
    {
        var path = Path.Combine(_tempDir, name);
        using var fs = File.Create(path);
        using var writer = new OcfWriter(fs, AvroCodec.Null);
        writer.WriteHeader((AvroRecordSchema)AvroSchemaParser.Parse(schemaJson));
        return path;
    }

    [Fact]
    public async Task ForeignManifestLayout_IsRefused()
    {
        var path = WriteEmptyOcf("foreign.avro", JavaLayoutManifestEntry);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await ManifestIO.ReadManifestAsync(_fs, path));
        Assert.Contains("issues/586", ex.Message);
    }

    [Fact]
    public async Task PartitionedManifestLayout_IsRefused()
    {
        // EW's own layout except for a non-empty partition tuple.
        var partitioned = ManifestAvroSchemas.ManifestEntryV2.Replace(
            "\"name\": \"r102\", \"fields\": []",
            "\"name\": \"r102\", \"fields\": [{\"name\": \"day\", \"type\": [\"null\", \"int\"], \"field-id\": 1000}]");
        Assert.NotEqual(ManifestAvroSchemas.ManifestEntryV2, partitioned);
        var path = WriteEmptyOcf("partitioned.avro", partitioned);

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await ManifestIO.ReadManifestAsync(_fs, path));
    }

    [Fact]
    public async Task ForeignManifestListLayout_IsRefused()
    {
        var path = WriteEmptyOcf("snap-foreign.avro", V1LayoutManifestList);

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await ManifestIO.ReadManifestListAsync(_fs, path));
    }

    [Fact]
    public async Task OwnLayout_DifferingOnlyInFieldIds_IsAccepted()
    {
        // Attributes outside the binary layout (field-id, doc) don't matter to the decoder.
        var renumbered = ManifestAvroSchemas.ManifestEntryV2
            .Replace("\"field-id\": 140", "\"field-id\": 140, \"doc\": \"sort order\"")
            .Replace("\"field-id\": 0}", "\"field-id\": 9999}");
        Assert.NotEqual(ManifestAvroSchemas.ManifestEntryV2, renumbered);
        var path = WriteEmptyOcf("renumbered.avro", renumbered);

        var entries = await ManifestIO.ReadManifestAsync(_fs, path);
        Assert.Empty(entries);
    }

    [Fact]
    public async Task OwnWrittenFiles_StillRead()
    {
        var manifestPath = Path.Combine(_tempDir, "own.avro");
        await ManifestIO.WriteManifestAsync(_fs, manifestPath,
        [
            new ManifestEntry
            {
                Status = ManifestEntryStatus.Added,
                SnapshotId = 1,
                SequenceNumber = 1,
                FileSequenceNumber = 1,
                DataFile = new DataFile { FilePath = "data/a.parquet", RecordCount = 10, FileSizeInBytes = 100 },
            },
        ]);
        Assert.Single(await ManifestIO.ReadManifestAsync(_fs, manifestPath));

        var listPath = Path.Combine(_tempDir, "snap-own.avro");
        await ManifestIO.WriteManifestListAsync(_fs, listPath,
        [
            new ManifestListEntry { ManifestPath = manifestPath, ManifestLength = 1, AddedSnapshotId = 1 },
        ]);
        Assert.Single(await ManifestIO.ReadManifestListAsync(_fs, listPath));
    }
}
