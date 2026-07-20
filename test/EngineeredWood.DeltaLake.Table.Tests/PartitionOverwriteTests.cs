// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The unified write path (WriteCoreAsync): full overwrite, static partition overwrite, dynamic partition
/// overwrite, and repartition-on-overwrite — plus the Delta-spec convention that a file's partitionValues are
/// keyed by the PHYSICAL column name under column mapping.
/// </summary>
public class PartitionOverwriteTests : IDisposable
{
    private readonly string _tempDir;

    public PartitionOverwriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_pow_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema Schema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, true))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params (string Region, long Id)[] rows)
    {
        var regions = new StringArray.Builder();
        var ids = new Int64Array.Builder();
        foreach (var (r, i) in rows)
        {
            regions.Append(r);
            ids.Append(i);
        }
        return new RecordBatch(schema, [regions.Build(), ids.Build()], rows.Length);
    }

    private static async Task<List<(string Region, long Id)>> ReadAsync(DeltaTable table)
    {
        var outRows = new List<(string, long)>();
        await foreach (var b in table.ReadAllAsync())
        {
            var regions = (StringArray)b.Column(b.Schema.GetFieldIndex("region"));
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            for (int i = 0; i < b.Length; i++)
                outRows.Add((regions.GetString(i), ids.GetValue(i)!.Value));
        }
        outRows.Sort();
        return outRows;
    }

    private static DeltaTableOptions Options => new() { CheckpointInterval = 0 };

    [Fact]
    public async Task OverwritePartitions_ReplacesOnlyTargetPartitions()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(fs, schema, Options, partitionColumns: ["region"]);
        await table.WriteAsync([Rows(schema, ("us", 1), ("eu", 2), ("ap", 3))]);

        await table.OverwritePartitionsAsync(
            [Rows(schema, ("us", 10), ("us", 11))],
            new Dictionary<string, string> { ["region"] = "us" });

        // us replaced; eu and ap untouched.
        Assert.Equal([("ap", 3L), ("eu", 2L), ("us", 10L), ("us", 11L)], await ReadAsync(table));
    }

    [Fact]
    public async Task OverwritePartitions_RejectsDataOutsideTheTarget()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(fs, schema, Options, partitionColumns: ["region"]);
        await table.WriteAsync([Rows(schema, ("us", 1))]);

        // Writing an "eu" row into an overwrite scoped to "us" would mix overwrite and append semantics.
        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.OverwritePartitionsAsync(
                [Rows(schema, ("eu", 9))],
                new Dictionary<string, string> { ["region"] = "us" }));
    }

    [Fact]
    public async Task OverwritePartitions_RejectsNonPartitionKey()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(fs, schema, Options, partitionColumns: ["region"]);
        await table.WriteAsync([Rows(schema, ("us", 1))]);

        // A data-column predicate could partially match a file — removing the whole file would drop rows.
        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.OverwritePartitionsAsync(
                [Rows(schema, ("us", 9))],
                new Dictionary<string, string> { ["id"] = "1" }));
    }

    [Fact]
    public async Task DynamicOverwrite_ReplacesOnlyPartitionsPresentInTheInput()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(fs, schema, Options, partitionColumns: ["region"]);
        await table.WriteAsync([Rows(schema, ("us", 1), ("eu", 2), ("ap", 3))]);

        // The target set is derived from the data: us and eu are replaced, ap is untouched.
        await table.DynamicOverwriteAsync([Rows(schema, ("us", 10), ("eu", 20))]);

        Assert.Equal([("ap", 3L), ("eu", 20L), ("us", 10L)], await ReadAsync(table));
    }

    [Fact]
    public async Task DynamicOverwrite_RequiresAPartitionedTable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(fs, schema, Options);
        await table.WriteAsync([Rows(schema, ("us", 1))]);

        // On an unpartitioned table this would be a full replace in disguise.
        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.DynamicOverwriteAsync([Rows(schema, ("us", 2))]));
    }

    [Fact]
    public async Task Repartition_ChangesPartitionColumnsInTheSameCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(fs, schema, Options, partitionColumns: ["region"]);
        await table.WriteAsync([Rows(schema, ("us", 1), ("eu", 2))]);
        Assert.Equal(["region"], table.CurrentSnapshot.Metadata.PartitionColumns);

        long version = await table.WriteAsync(
            [Rows(schema, ("us", 5), ("eu", 6))],
            DeltaWriteMode.Overwrite,
            repartitionTo: []);

        // Departitioned atomically: the new metaData rode with the full file swap.
        Assert.Empty(table.CurrentSnapshot.Metadata.PartitionColumns);
        Assert.Equal(version, table.CurrentSnapshot.Version);
        Assert.Equal([("eu", 6L), ("us", 5L)], await ReadAsync(table));
    }

    [Fact]
    public async Task Repartition_RequiresAFullOverwrite()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(fs, schema, Options, partitionColumns: ["region"]);
        await table.WriteAsync([Rows(schema, ("us", 1))]);

        // A new partition schema is only protocol-legal when every active file is replaced in the commit.
        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync([Rows(schema, ("us", 2))], DeltaWriteMode.Append, repartitionTo: []));
    }

    // The Delta-spec convention: partitionValues keys are PHYSICAL under column mapping (they survive a
    // partition-column rename, since a rename never rewrites add actions). metaData.partitionColumns stays
    // logical. Reads must still resolve, which is what makes this safe to change.
    [Theory]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task PartitionValues_AreKeyedByPhysicalName_UnderColumnMapping(ColumnMappingMode mode)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, Options, partitionColumns: ["region"], columnMappingMode: mode);
        await table.WriteAsync([Rows(schema, ("us", 1), ("eu", 2))]);

        var regionField = table.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "region");
        string physical = ColumnMapping.GetPhysicalName(regionField, mode);
        Assert.StartsWith("col-", physical);

        foreach (var addFile in table.CurrentSnapshot.ActiveFiles.Values)
        {
            Assert.True(addFile.PartitionValues.ContainsKey(physical),
                $"partitionValues should be keyed by the physical name, got: {string.Join(",", addFile.PartitionValues.Keys)}");
            Assert.False(addFile.PartitionValues.ContainsKey("region"));
        }

        // metaData.partitionColumns stays LOGICAL.
        Assert.Equal(["region"], table.CurrentSnapshot.Metadata.PartitionColumns);

        // ...and the read path still materializes the partition column under its logical name.
        Assert.Equal([("eu", 2L), ("us", 1L)], await ReadAsync(table));
    }

    [Fact]
    public async Task PartitionPruning_StillWorks_UnderColumnMapping()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, Options, partitionColumns: ["region"], columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Rows(schema, ("us", 1), ("eu", 2))]);

        // The pruner resolves the logical predicate name against physical-keyed partitionValues.
        var rows = new List<long>();
        await foreach (var b in table.ReadAllAsync(
            columns: null, EngineeredWood.Expressions.Expressions.Equal("region", "us")))
        {
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            for (int i = 0; i < b.Length; i++)
                rows.Add(ids.GetValue(i)!.Value);
        }

        Assert.Equal([1L], rows);
    }
}
