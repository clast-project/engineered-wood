// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.Iceberg.Values;

namespace EngineeredWood.Iceberg.Tests.Manifest;

/// <summary>
/// The manifest-list partition summaries a commit writes: per partition field, whether any value is null or
/// NaN, and the least and greatest value as Appendix D binary.
/// </summary>
public class PartitionSummaryTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"iceberg-summary-{Guid.NewGuid():N}");
    private readonly LocalTableFileSystem _fs;
    private readonly TableOperations _ops;

    public PartitionSummaryTests()
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

    private static readonly Schema TableSchema = new(0,
    [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "amount", IcebergType.Decimal(9, 2), false),
        new NestedField(3, "day", IcebergType.Date, false),
        new NestedField(4, "name", IcebergType.String, false),
        new NestedField(5, "score", IcebergType.Double, false),
        new NestedField(6, "key", IcebergType.Uuid, false),
    ]);

    private static readonly PartitionSpec Spec = new(0,
    [
        new PartitionField(1, 1000, "id_bucket", Transform.Bucket(8)),
        new PartitionField(2, 1001, "amount", Transform.Identity),
        new PartitionField(3, 1002, "day", Transform.Identity),
        new PartitionField(4, 1003, "name", Transform.Identity),
        new PartitionField(5, 1004, "score", Transform.Identity),
    ]);

    private static DataFile File(string path, int bucket, long? cents, int? day, string? name, double? score) => new()
    {
        FilePath = path, RecordCount = 1, FileSizeInBytes = 1,
        Partition = new PartitionData(
        [
            new PartitionValue(1000, "id_bucket", IcebergType.Int, bucket),
            new PartitionValue(1001, "amount", IcebergType.Decimal(9, 2), cents is { } c ? new BigInteger(c) : null),
            new PartitionValue(1002, "day", IcebergType.Date, day),
            new PartitionValue(1003, "name", IcebergType.String, name),
            new PartitionValue(1004, "score", IcebergType.Double, score),
        ]),
    };

    [Fact]
    public async Task AppendWritesSummariesThatBoundTheValues()
    {
        var md = TableMetadata.Create(TableSchema, Spec, location: Path.Combine(_dir, "t"));
        md = await _ops.AppendFilesAsync(md,
        [
            File("data/a.parquet", 3, 1420, 19_000, "b", 0.0),
            File("data/b.parquet", 7, -500, 18_000, "�", -0.0),
            File("data/c.parquet", 0, null, 19_500, null, double.NaN),
        ]);

        var snapshot = md.Snapshots.Single();
        var summaries = Assert.Single(await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList)).Partitions!;
        Assert.Equal(5, summaries.Count);

        void Bounds(int i, IcebergType type, object lower, object upper)
        {
            Assert.Equal(SingleValueBinary.Encode(type, ManifestValues.ToLiteral(type, lower)), summaries[i].LowerBound);
            Assert.Equal(SingleValueBinary.Encode(type, ManifestValues.ToLiteral(type, upper)), summaries[i].UpperBound);
        }

        Assert.False(summaries[0].ContainsNull);
        Bounds(0, IcebergType.Int, 0, 7);
        Assert.True(summaries[1].ContainsNull);
        Bounds(1, IcebergType.Decimal(9, 2), new BigInteger(-500), new BigInteger(1420));
        Bounds(2, IcebergType.Date, 18_000, 19_500);
        Assert.True(summaries[3].ContainsNull);
        Bounds(3, IcebergType.String, "b", "�");
        // NaN is flagged, not a bound, and -0 sorts below +0.
        Assert.True(summaries[4].ContainsNan);
        Bounds(4, IcebergType.Double, -0.0, 0.0);
        Assert.NotEqual(summaries[4].LowerBound, summaries[4].UpperBound);
    }

    [Fact]
    public async Task AFieldWhoseValuesAreAllNull_HasNoBounds()
    {
        var spec = new PartitionSpec(0, [new PartitionField(4, 1000, "name", Transform.Identity)]);
        var md = TableMetadata.Create(TableSchema, spec, location: Path.Combine(_dir, "n"));
        md = await _ops.AppendFilesAsync(md,
        [
            new DataFile
            {
                FilePath = "data/a.parquet", RecordCount = 1, FileSizeInBytes = 1,
                Partition = new PartitionData([new PartitionValue(1000, "name", IcebergType.String, null)]),
            },
        ]);
        var summary = Assert.Single(Assert.Single(
            await ManifestIO.ReadManifestListAsync(_fs, md.Snapshots.Single().ManifestList)).Partitions!);
        Assert.True(summary.ContainsNull);
        Assert.Null(summary.LowerBound);
        Assert.Null(summary.UpperBound);
    }

    [Fact]
    public async Task AUuidField_GetsNoSummariesAtAll()
    {
        // Implementations order UUIDs differently, so a summary could be pruned against wrongly.
        var spec = new PartitionSpec(0, [new PartitionField(6, 1000, "key", Transform.Identity)]);
        var md = TableMetadata.Create(TableSchema, spec, location: Path.Combine(_dir, "u"));
        md = await _ops.AppendFilesAsync(md,
        [
            new DataFile
            {
                FilePath = "data/a.parquet", RecordCount = 1, FileSizeInBytes = 1,
                Partition = new PartitionData([new PartitionValue(1000, "key", IcebergType.Uuid, Guid.NewGuid())]),
            },
        ]);
        Assert.Null(Assert.Single(await ManifestIO.ReadManifestListAsync(_fs, md.Snapshots.Single().ManifestList)).Partitions);
    }

    [Fact]
    public async Task UnpartitionedManifests_HaveAnEmptySummaryList()
    {
        var md = TableMetadata.Create(TableSchema, location: Path.Combine(_dir, "p"));
        md = await _ops.AppendFilesAsync(md, [new DataFile { FilePath = "data/a.parquet", RecordCount = 1, FileSizeInBytes = 1 }]);
        Assert.Empty(Assert.Single(await ManifestIO.ReadManifestListAsync(_fs, md.Snapshots.Single().ManifestList)).Partitions!);
    }
}
