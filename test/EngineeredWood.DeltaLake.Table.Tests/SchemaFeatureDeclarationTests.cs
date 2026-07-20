// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Schema-driven table features must be DECLARED in the protocol — at creation, and via a protocol upgrade in
/// the same commit when an ALTER introduces one. A strict reader (Spark, delta-kernel) rejects a table whose
/// metadata uses a feature the protocol doesn't advertise.
/// </summary>
public class SchemaFeatureDeclarationTests : IDisposable
{
    private readonly string _tempDir;

    public SchemaFeatureDeclarationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_feat_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // A naive timestamp (no timezone) is timestamp_ntz in Delta.
    private static Field NtzField(string name) =>
        new(name, new TimestampType(TimeUnit.Microsecond, (string?)null), true);

    private static Field TzField(string name) =>
        new(name, new TimestampType(TimeUnit.Microsecond, "UTC"), true);

    [Fact]
    public async Task Create_PlainSchema_StaysOnLegacyProtocol()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(TzField("ts"))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        var protocol = table.CurrentSnapshot.Protocol;

        Assert.Equal(1, protocol.MinReaderVersion);
        Assert.Equal(2, protocol.MinWriterVersion);
        Assert.Null(protocol.ReaderFeatures);
        Assert.Null(protocol.WriterFeatures);
    }

    [Fact]
    public async Task Create_TimestampNtz_DeclaresReaderAndWriterFeature()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(NtzField("ts"))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        var protocol = table.CurrentSnapshot.Protocol;

        Assert.Equal(3, protocol.MinReaderVersion);
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("timestampNtz", protocol.ReaderFeatures!);
        Assert.Contains("timestampNtz", protocol.WriterFeatures!);
    }

    // The type is detected at any nesting depth, not just top level.
    [Fact]
    public async Task Create_NestedTimestampNtz_DeclaresFeature()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("nested", new ArrowStructType([NtzField("ts")]), true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        Assert.Equal(3, table.CurrentSnapshot.Protocol.MinReaderVersion);
        Assert.Contains("timestampNtz", table.CurrentSnapshot.Protocol.ReaderFeatures!);
    }

    // Once another feature forces table-features mode, columnMapping must be listed explicitly too — a v7
    // protocol with no columnMapping entry reads as "column mapping not supported".
    [Fact]
    public async Task Create_MappingPlusTimestampNtz_ListsColumnMappingInBothLists()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(NtzField("ts"))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);
        var protocol = table.CurrentSnapshot.Protocol;

        Assert.Equal(3, protocol.MinReaderVersion);
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("columnMapping", protocol.ReaderFeatures!);
        Assert.Contains("columnMapping", protocol.WriterFeatures!);
        Assert.Contains("timestampNtz", protocol.ReaderFeatures!);
    }

    // Column mapping ALONE keeps the legacy reader-v2/writer-v5 versioning (what Spark itself writes).
    [Fact]
    public async Task Create_MappingAlone_KeepsLegacyVersioning()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);
        var protocol = table.CurrentSnapshot.Protocol;

        Assert.Equal(2, protocol.MinReaderVersion);
        Assert.Equal(5, protocol.MinWriterVersion);
        Assert.Null(protocol.ReaderFeatures);
        Assert.Null(protocol.WriterFeatures);
    }

    [Fact]
    public async Task AddColumn_TimestampNtz_UpgradesProtocolInTheSameCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        Assert.Equal(1, table.CurrentSnapshot.Protocol.MinReaderVersion);

        long version = await table.AddColumnAsync(NtzField("ts"));

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal(3, protocol.MinReaderVersion);
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("timestampNtz", protocol.ReaderFeatures!);
        Assert.Contains("timestampNtz", protocol.WriterFeatures!);

        // The upgrade rode in the SAME commit as the schema change — not a separate version.
        var atVersion = await table.GetSnapshotAtVersionAsync(version);
        Assert.Equal(3, atVersion.Protocol.MinReaderVersion);
        Assert.Equal(2, atVersion.ArrowSchema.FieldsList.Count);
    }

    // Upgrading a LEGACY-versioned protocol to table-features mode must enumerate everything the legacy
    // version implied, else those capabilities are silently dropped.
    [Fact]
    public async Task AddColumn_UpgradeFromLegacy_PreservesImpliedFeatures()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        // Column mapping alone → legacy reader v2 / writer v5.
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);
        Assert.Null(table.CurrentSnapshot.Protocol.WriterFeatures);

        await table.AddColumnAsync(NtzField("ts"));

        var protocol = table.CurrentSnapshot.Protocol;
        // writer v5 implied appendOnly/invariants/checkConstraints/changeDataFeed/generatedColumns/columnMapping
        Assert.Contains("columnMapping", protocol.WriterFeatures!);
        Assert.Contains("appendOnly", protocol.WriterFeatures!);
        Assert.Contains("invariants", protocol.WriterFeatures!);
        Assert.Contains("generatedColumns", protocol.WriterFeatures!);
        // reader v2 implied columnMapping
        Assert.Contains("columnMapping", protocol.ReaderFeatures!);
        Assert.Contains("timestampNtz", protocol.ReaderFeatures!);
    }

    [Fact]
    public async Task AddColumn_NoNewFeature_LeavesProtocolAlone()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.AddColumnAsync(new Field("name", StringType.Default, true));

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal(1, protocol.MinReaderVersion);
        Assert.Equal(2, protocol.MinWriterVersion);
        Assert.Null(protocol.ReaderFeatures);
    }

    [Fact]
    public async Task AddColumn_FeatureAlreadyDeclared_DoesNotReUpgrade()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(NtzField("ts"))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        int featureCount = table.CurrentSnapshot.Protocol.ReaderFeatures!.Count;

        await table.AddColumnAsync(NtzField("ts2"));

        // Already declared — no duplicate entry, no second upgrade.
        Assert.Equal(featureCount, table.CurrentSnapshot.Protocol.ReaderFeatures!.Count);
    }
}
