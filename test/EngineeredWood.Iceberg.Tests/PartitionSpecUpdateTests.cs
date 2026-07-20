// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests;

public class PartitionSpecUpdateTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TableOperations _ops;

    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "timestamp", IcebergType.Timestamp, true),
        new NestedField(3, "region", IcebergType.String, true),
        new NestedField(4, "amount", IcebergType.Double, false),
    ]);

    public PartitionSpecUpdateTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"iceberg-psu-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
        _ops = new TableOperations(_fs);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private TableMetadata CreateTable(PartitionSpec? spec = null) =>
        TableMetadata.Create(_schema, partitionSpec: spec, location: "table");

    // --- Basic add ---

    [Fact]
    public void AddField_CreatesNewSpec()
    {
        var metadata = CreateTable();

        var updated = new PartitionSpecUpdate(metadata)
            .AddField("timestamp", Transform.Day)
            .Apply();

        Assert.Equal(2, updated.PartitionSpecs.Count);
        Assert.Equal(1, updated.DefaultSpecId);

        var spec = updated.PartitionSpecs.First(s => s.SpecId == updated.DefaultSpecId);
        Assert.Single(spec.Fields);
        Assert.Equal(2, spec.Fields[0].SourceId); // timestamp field ID
        Assert.IsType<DayTransform>(spec.Fields[0].Transform);
        Assert.Equal("timestamp_day", spec.Fields[0].Name);
    }

    [Fact]
    public void AddField_WithExplicitName()
    {
        var metadata = CreateTable();

        var updated = new PartitionSpecUpdate(metadata)
            .AddField("id", Transform.Bucket(16), "id_bucket")
            .Apply();

        var spec = updated.PartitionSpecs.First(s => s.SpecId == updated.DefaultSpecId);
        Assert.Equal("id_bucket", spec.Fields[0].Name);
    }

    [Fact]
    public void AddField_AssignsNewFieldId()
    {
        var metadata = CreateTable();
        Assert.Equal(0, metadata.LastPartitionId);

        var updated = new PartitionSpecUpdate(metadata)
            .AddField("id", Transform.Bucket(16))
            .Apply();

        var spec = updated.PartitionSpecs.First(s => s.SpecId == updated.DefaultSpecId);
        Assert.Equal(1, spec.Fields[0].FieldId);
        Assert.Equal(1, updated.LastPartitionId);
    }

    [Fact]
    public void AddMultipleFields_AssignsSequentialIds()
    {
        var metadata = CreateTable();

        var updated = new PartitionSpecUpdate(metadata)
            .AddField("timestamp", Transform.Day)
            .AddField("region", Transform.Identity)
            .Apply();

        var spec = updated.PartitionSpecs.First(s => s.SpecId == updated.DefaultSpecId);
        Assert.Equal(2, spec.Fields.Count);
        Assert.Equal(1, spec.Fields[0].FieldId);
        Assert.Equal(2, spec.Fields[1].FieldId);
        Assert.Equal(2, updated.LastPartitionId);
    }

    [Fact]
    public void AddField_UnknownColumn_Throws()
    {
        var metadata = CreateTable();

        Assert.Throws<ArgumentException>(() =>
            new PartitionSpecUpdate(metadata)
                .AddField("nonexistent", Transform.Identity)
                .Apply());
    }

    // --- Remove ---

    [Fact]
    public void RemoveField_ReplacesTransformWithVoid()
    {
        var spec = new PartitionSpec(0, [
            new PartitionField(2, 1000, "ts_day", Transform.Day),
        ]);
        var metadata = CreateTable(spec);

        var updated = new PartitionSpecUpdate(metadata)
            .RemoveField("ts_day")
            .Apply();

        var newSpec = updated.PartitionSpecs.First(s => s.SpecId == updated.DefaultSpecId);
        Assert.Single(newSpec.Fields);
        Assert.IsType<VoidTransform>(newSpec.Fields[0].Transform);
        Assert.Equal("ts_day", newSpec.Fields[0].Name);
        Assert.Equal(1000, newSpec.Fields[0].FieldId); // field ID preserved
    }

    [Fact]
    public void RemoveField_AlreadyVoid_Throws()
    {
        var spec = new PartitionSpec(0, [
            new PartitionField(2, 1000, "ts_day", Transform.Void),
        ]);
        var metadata = CreateTable(spec);

        Assert.Throws<ArgumentException>(() =>
            new PartitionSpecUpdate(metadata)
                .RemoveField("ts_day")
                .Apply());
    }

    // --- Combined add + remove ---

    [Fact]
    public void AddAndRemove_InOneEvolution()
    {
        var spec = new PartitionSpec(0, [
            new PartitionField(2, 1000, "ts_day", Transform.Day),
        ]);
        var metadata = CreateTable(spec);

        // Switch from daily to hourly partitioning
        var updated = new PartitionSpecUpdate(metadata)
            .RemoveField("ts_day")
            .AddField("timestamp", Transform.Hour)
            .Apply();

        var newSpec = updated.PartitionSpecs.First(s => s.SpecId == updated.DefaultSpecId);
        Assert.Equal(2, newSpec.Fields.Count);

        // Old field is voided
        Assert.Equal("ts_day", newSpec.Fields[0].Name);
        Assert.IsType<VoidTransform>(newSpec.Fields[0].Transform);

        // New field added
        Assert.Equal("timestamp_hour", newSpec.Fields[1].Name);
        Assert.IsType<HourTransform>(newSpec.Fields[1].Transform);
    }

    // --- Preserves history ---

    [Fact]
    public void Apply_PreservesOriginalSpec()
    {
        var metadata = CreateTable();

        var updated = new PartitionSpecUpdate(metadata)
            .AddField("id", Transform.Bucket(16))
            .Apply();

        Assert.Equal(2, updated.PartitionSpecs.Count);
        var original = updated.PartitionSpecs.First(s => s.SpecId == 0);
        Assert.Empty(original.Fields); // unpartitioned
    }

    [Fact]
    public void NoOp_ReturnsSameMetadata()
    {
        var spec = new PartitionSpec(0, [
            new PartitionField(1, 1000, "id_bucket", Transform.Bucket(16)),
        ]);
        var metadata = CreateTable(spec);

        var result = new PartitionSpecUpdate(metadata).Apply();

        Assert.Same(metadata, result);
    }

    // --- Multi-step evolution ---

    [Fact]
    public void MultipleEvolutions_ChainCorrectly()
    {
        var metadata = CreateTable();

        // Step 1: partition by day
        var v1 = new PartitionSpecUpdate(metadata)
            .AddField("timestamp", Transform.Day)
            .Apply();

        // Step 2: also partition by region
        var v2 = new PartitionSpecUpdate(v1)
            .AddField("region", Transform.Identity)
            .Apply();

        // Step 3: switch day to hour, drop region
        var v3 = new PartitionSpecUpdate(v2)
            .RemoveField("timestamp_day")
            .RemoveField("region_identity")
            .AddField("timestamp", Transform.Hour)
            .Apply();

        Assert.Equal(4, v3.PartitionSpecs.Count); // original + 3 evolutions
        Assert.Equal(3, v3.DefaultSpecId);

        var finalSpec = v3.PartitionSpecs.First(s => s.SpecId == v3.DefaultSpecId);
        // voided day, carried + voided region, new hour
        Assert.Equal(3, finalSpec.Fields.Count);
        Assert.IsType<VoidTransform>(finalSpec.Fields[0].Transform);
        Assert.IsType<VoidTransform>(finalSpec.Fields[1].Transform);
        Assert.IsType<HourTransform>(finalSpec.Fields[2].Transform);

        // Each evolution allocates new field IDs — they're never reused for different fields
        Assert.Equal(3, v3.LastPartitionId);
        // Verify the 3 distinct field IDs map to 3 different source columns/transforms
        var finalFields = finalSpec.Fields;
        Assert.Equal(1, finalFields[0].FieldId); // carried from step 1
        Assert.Equal(2, finalFields[1].FieldId); // carried from step 2
        Assert.Equal(3, finalFields[2].FieldId); // new in step 3
    }

    // --- JSON round-trip ---

    [Fact]
    public void EvolvedMetadata_JsonRoundTrips()
    {
        var metadata = CreateTable();

        var updated = new PartitionSpecUpdate(metadata)
            .AddField("timestamp", Transform.Day)
            .AddField("region", Transform.Identity)
            .Apply();

        var json = EngineeredWood.Iceberg.Serialization.IcebergJsonSerializer.Serialize(updated);
        var deserialized = EngineeredWood.Iceberg.Serialization.IcebergJsonSerializer.Deserialize<TableMetadata>(json);

        Assert.Equal(updated.DefaultSpecId, deserialized.DefaultSpecId);
        Assert.Equal(updated.PartitionSpecs.Count, deserialized.PartitionSpecs.Count);

        var spec = deserialized.PartitionSpecs.First(s => s.SpecId == deserialized.DefaultSpecId);
        Assert.Equal(2, spec.Fields.Count);
        Assert.IsType<DayTransform>(spec.Fields[0].Transform);
        Assert.IsType<IdentityTransform>(spec.Fields[1].Transform);
    }

    // --- End-to-end with table operations ---

    [Fact]
    public async Task EndToEnd_FilesTrackPartitionSpecId()
    {
        var metadata = CreateTable();

        // Append files with unpartitioned spec (spec 0)
        var v1 = await _ops.AppendFilesAsync(metadata, [
            new DataFile { FilePath = "data/old.parquet", RecordCount = 100, FileSizeInBytes = 5000 },
        ]);

        // Evolve partitioning
        var v2 = new PartitionSpecUpdate(v1)
            .AddField("timestamp", Transform.Day)
            .Apply();

        // Append files with new spec (spec 1)
        var v3 = await _ops.AppendFilesAsync(v2, [
            new DataFile { FilePath = "data/new.parquet", RecordCount = 200, FileSizeInBytes = 10000 },
        ]);

        // Both files should be visible
        var allFiles = await _ops.ListDataFilesAsync(v3);
        Assert.Equal(2, allFiles.Count);

        // Check manifest list entries have correct spec IDs
        var snapshot = v3.Snapshots.Last();
        var manifestList = await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList);

        // The newest manifest should use the new spec
        var newestManifest = manifestList.Last();
        Assert.Equal(v2.DefaultSpecId, newestManifest.PartitionSpecId);

        // The carried-forward manifest should have the old spec
        var oldManifest = manifestList.First();
        Assert.Equal(0, oldManifest.PartitionSpecId);
    }

    [Fact]
    public async Task EndToEnd_WithFileSystemCatalog()
    {
        var catalog = new FileSystemCatalog(_fs);
        var ns = Namespace.Of("db");
        await catalog.CreateNamespaceAsync(ns);

        var id = new TableIdentifier(ns, "events");
        var table = await catalog.CreateTableAsync(id, _schema);

        // Evolve: add day partitioning
        var evolved = new PartitionSpecUpdate(table.Metadata)
            .AddField("timestamp", Transform.Day)
            .Apply();
        await catalog.UpdateTableAsync(id, evolved);

        // Reload and verify
        var reloaded = await catalog.LoadTableAsync(id);
        Assert.Equal(2, reloaded.Metadata.PartitionSpecs.Count);
        Assert.Equal(1, reloaded.Metadata.DefaultSpecId);

        var spec = reloaded.CurrentSpec;
        Assert.Single(spec.Fields);
        Assert.IsType<DayTransform>(spec.Fields[0].Transform);
    }
}
