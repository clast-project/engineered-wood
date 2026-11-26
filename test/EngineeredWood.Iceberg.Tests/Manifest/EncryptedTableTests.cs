// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Expressions;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests.Manifest;

/// <summary>
/// Table encryption is not implemented, so an encrypted table's data must be neither read (its manifest
/// lists are not plaintext) nor written (plaintext manifests in an encrypted table leave data unprotected).
/// Its metadata still round-trips; see LosslessMetadataTests.
/// </summary>
public class EncryptedTableTests : IDisposable
{
    private readonly string _warehouse;
    private readonly LocalTableFileSystem _fs;
    private readonly TableOperations _ops;

    public EncryptedTableTests()
    {
        _warehouse = Path.Combine(Path.GetTempPath(), $"iceberg-enc-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_warehouse);
        _fs = new LocalTableFileSystem(_warehouse);
        _ops = new TableOperations(_fs);
    }

    public void Dispose()
    {
        if (Directory.Exists(_warehouse))
            Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly DataFile File1 = new()
    {
        FilePath = "data/file1.parquet",
        RecordCount = 10,
        FileSizeInBytes = 100,
    };

    private async Task<TableMetadata> TableWithOneSnapshotAsync()
    {
        var md = TableMetadata.Create(
            new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]),
            location: Path.Combine(_warehouse, "t"),
            formatVersion: 3);
        return await _ops.AppendFilesAsync(md, [File1]);
    }

    private static TableMetadata WithTableKey(TableMetadata md) =>
        md with { EncryptionKeys = [new EncryptedKey("k1", "AAEC")] };

    private static TableMetadata WithSnapshotKey(TableMetadata md) =>
        md with { Snapshots = md.Snapshots.Select(s => s with { KeyId = "k1" }).ToList() };

    public static TheoryData<string> Encryptions => new() { "table", "snapshot" };

    private static TableMetadata Encrypt(TableMetadata md, string how) =>
        how == "table" ? WithTableKey(md) : WithSnapshotKey(md);

    [Theory]
    [MemberData(nameof(Encryptions))]
    public async Task Scan_IsRefused(string how)
    {
        var md = Encrypt(await TableWithOneSnapshotAsync(), how);
        Assert.True(md.IsEncrypted);
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(
            async () => await new TableScan(md, _fs).PlanFilesAsync());
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(
            async () => await _ops.ListDataFilesAsync(md));
    }

    [Theory]
    [MemberData(nameof(Encryptions))]
    public async Task Writes_AreRefused(string how)
    {
        var md = Encrypt(await TableWithOneSnapshotAsync(), how);
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(
            async () => await _ops.AppendFilesAsync(md, [File1 with { FilePath = "data/file2.parquet" }]));
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(
            async () => await _ops.OverwriteFilesAsync(md, [File1], []));
    }

    [Fact]
    public async Task AppendToAnEncryptedTable_WritesNothing()
    {
        var md = WithTableKey(TableMetadata.Create(
            new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]),
            location: Path.Combine(_warehouse, "empty"),
            formatVersion: 3));

        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(
            async () => await _ops.AppendFilesAsync(md, [File1]));
        Assert.False(Directory.Exists(Path.Combine(_warehouse, "empty")));
    }

    [Fact]
    public async Task ExpireSnapshots_IsRefused()
    {
        var md = await TableWithOneSnapshotAsync();
        md = WithTableKey(await _ops.AppendFilesAsync(md, [File1 with { FilePath = "data/file2.parquet" }]));
        var oldest = md.Snapshots[0].SnapshotId;
        await Assert.ThrowsAsync<UnsupportedTableFeatureException>(
            async () => await new ExpireSnapshots().ExpireSnapshotId(oldest).ApplyAsync(md, _fs));
    }

    [Fact]
    public async Task UnencryptedTable_StillScans()
    {
        var md = await TableWithOneSnapshotAsync();
        Assert.False(md.IsEncrypted);
        var result = await new TableScan(md, _fs).PlanFilesAsync();
        Assert.Single(result.DataFiles);
    }
}
