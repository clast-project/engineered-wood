// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Compaction on a PARTITIONED table. A data file belongs to exactly one partition, so candidates must be
/// grouped by partition and compacted independently. Merging across partitions produced one file at the table
/// root stamped with a single partition's values — every other row silently read the wrong partition value.
/// </summary>
public class PartitionedCompactionTests : IDisposable
{
    private readonly string _tempDir;

    public PartitionedCompactionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_pcompact_{Guid.NewGuid():N}");
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

    private static RecordBatch Row(Apache.Arrow.Schema schema, string region, long id) =>
        new(schema,
        [
            new StringArray.Builder().Append(region).Build(),
            new Int64Array.Builder().Append(id).Build(),
        ], 1);

    private static async Task<List<(string Region, long Id)>> ReadAsync(DeltaTable table)
    {
        var rows = new List<(string, long)>();
        await foreach (var b in table.ReadAllAsync())
        {
            var regions = (StringArray)b.Column(b.Schema.GetFieldIndex("region"));
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            for (int i = 0; i < b.Length; i++)
                rows.Add((regions.GetString(i), ids.GetValue(i)!.Value));
        }
        rows.Sort();
        return rows;
    }

    private static CompactionOptions CompactAll => new()
    {
        MinFileSize = long.MaxValue,
        TargetFileSize = long.MaxValue,
    };

    private static DeltaTableOptions Options => new() { CheckpointInterval = 0 };

    // The corruption pin: rows must survive compaction with their own partition values intact.
    [Fact]
    public async Task Compact_PartitionedTable_PreservesEveryPartitionValue()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, Options, partitionColumns: ["region"]);

        foreach (var region in new[] { "us", "eu", "ap" })
            for (int i = 0; i < 2; i++)
                await table.WriteAsync([Row(schema, region, i)]);

        var before = await ReadAsync(table);
        Assert.Equal(6, before.Count);

        await table.CompactAsync(CompactAll);

        // Before the fix this threw (the target schema backfilled the partition column into the data),
        // and where it did not throw it stamped every row with the first candidate's partition.
        Assert.Equal(before, await ReadAsync(table));
    }

    [Fact]
    public async Task Compact_PartitionedTable_ProducesOneFilePerPartition_InItsHiveDirectory()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, Options, partitionColumns: ["region"]);

        foreach (var region in new[] { "us", "eu" })
            for (int i = 0; i < 3; i++)
                await table.WriteAsync([Row(schema, region, i)]);

        Assert.Equal(6, table.CurrentSnapshot.FileCount);
        await table.CompactAsync(CompactAll);

        var active = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        Assert.Equal(2, active.Count);

        foreach (var addFile in active)
        {
            string region = addFile.PartitionValues["region"];
            // The compacted file lives in its partition's Hive directory, not the table root.
            Assert.StartsWith($"region={region}/", DeltaPath.Decode(addFile.Path));
        }

        // Each add carries exactly one partition's value, and the two differ.
        Assert.Equal(["eu", "us"], active.Select(a => a.PartitionValues["region"]).OrderBy(v => v));
    }

    // A partition holding a single small file has nothing to merge — leave it alone rather than
    // rewriting it or folding it into another partition.
    [Fact]
    public async Task Compact_PartitionWithOneFile_IsLeftAlone()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, Options, partitionColumns: ["region"]);

        await table.WriteAsync([Row(schema, "us", 1)]);
        await table.WriteAsync([Row(schema, "us", 2)]);
        await table.WriteAsync([Row(schema, "solo", 9)]); // single file in its partition

        string soloPath = table.CurrentSnapshot.ActiveFiles.Values
            .Single(f => f.PartitionValues["region"] == "solo").Path;

        await table.CompactAsync(CompactAll);

        // us compacted to one file; solo untouched (same path).
        Assert.Contains(table.CurrentSnapshot.ActiveFiles.Values, f => f.Path == soloPath);
        Assert.Equal([("solo", 9L), ("us", 1L), ("us", 2L)], await ReadAsync(table));
    }

    [Fact]
    public async Task Compact_UnpartitionedTable_IsUnchanged()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(fs, schema, Options);
        for (int i = 0; i < 4; i++)
            await table.WriteAsync([Row(schema, "any", i)]);

        var before = await ReadAsync(table);
        await table.CompactAsync(CompactAll);

        // One group covering the whole table — a single compacted file at the root.
        Assert.Equal(1, table.CurrentSnapshot.FileCount);
        Assert.DoesNotContain("/", DeltaPath.Decode(
            table.CurrentSnapshot.ActiveFiles.Values.Single().Path));
        Assert.Equal(before, await ReadAsync(table));
    }

    // Partition values reach the Hive path and the log through column mapping, so compaction has to group
    // correctly when partitionValues are keyed by the PHYSICAL name.
    [Fact]
    public async Task Compact_PartitionedTable_UnderColumnMapping()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, Options, partitionColumns: ["region"], columnMappingMode: ColumnMappingMode.Name);

        foreach (var region in new[] { "us", "eu" })
            for (int i = 0; i < 2; i++)
                await table.WriteAsync([Row(schema, region, i)]);

        var before = await ReadAsync(table);
        await table.CompactAsync(CompactAll);

        Assert.Equal(2, table.CurrentSnapshot.FileCount);
        Assert.Equal(before, await ReadAsync(table));
    }

    // Deleted rows must not come back, and the surviving rows keep their partitions.
    [Fact]
    public async Task Compact_PartitionedTable_AfterDelete_DoesNotResurrectRows()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, Options, partitionColumns: ["region"]);

        foreach (var region in new[] { "us", "eu" })
            for (int i = 0; i < 2; i++)
                await table.WriteAsync([Row(schema, region, i)]);

        await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                mask.Append(ids.GetValue(i) == 0);
            return mask.Build();
        });

        var before = await ReadAsync(table);
        Assert.Equal([("eu", 1L), ("us", 1L)], before);

        await table.CompactAsync(CompactAll);
        Assert.Equal(before, await ReadAsync(table));
    }
}
