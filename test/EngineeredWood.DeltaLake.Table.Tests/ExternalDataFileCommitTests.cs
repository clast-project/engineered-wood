// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The external-data-file write/commit seam — <see cref="DeltaTable.WriteDataFilesAsync"/> +
/// <see cref="DeltaTable.CommitDataFilesAsync"/>. The write half writes append-shaped parquet files WITHOUT
/// committing (invisible orphans); the commit half references them — optionally FUSED with a caller's
/// <c>extraActions</c> — as one atomic Delta version. This is the foundation the buffered (multi-statement)
/// transaction flow builds on: a host can write files at statement time, then commit them together with schema
/// / DML actions. Row-tracking baseRowId, overwrite / dynamic-partition removes, the rewrite commit shape
/// (dataChange=false + clusteringProvider), and first-committer-wins snapshot isolation (expectedVersion) all
/// live in the commit half.
/// </summary>
public class ExternalDataFileCommitTests : IDisposable
{
    private readonly string _tempDir;

    public ExternalDataFileCommitTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_extcommit_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdValueSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append("v" + (startId + i));
        }
        return new RecordBatch(IdValueSchema, [ids.Build(), values.Build()], count);
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    private async Task<List<long>> ReadIdsFresh()
    {
        await using var reader = await OpenAsync();
        var ids = new List<long>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    /// <summary>Reads the raw actions of a single commit file (<c>_delta_log/000…N.json</c>).</summary>
    private List<JsonElement> ReadCommitActions(long version)
    {
        string path = Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json");
        var actions = new List<JsonElement>();
        foreach (var line in File.ReadAllLines(path))
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;
            actions.Add(JsonDocument.Parse(line).RootElement.Clone());
        }
        return actions;
    }

    [Fact]
    public async Task WriteThenCommitAppend_RoundTrips()
    {
        await using (var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdValueSchema))
        {
            await table.WriteAsync([Batch(1, 3)]); // v1

            var files = await table.WriteDataFilesAsync([Batch(4, 2)]);
            Assert.Single(files);
            Assert.Equal(2, files[0].NumRecords);

            long committed = await table.CommitDataFilesAsync(files, DeltaWriteMode.Append);
            Assert.Equal(2, committed);
        }

        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task CommitDataFiles_Overwrite_ReplacesAllActiveFiles()
    {
        await using (var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdValueSchema))
        {
            await table.WriteAsync([Batch(1, 5)]); // v1 — five rows

            var files = await table.WriteDataFilesAsync([Batch(100, 2)]);
            await table.CommitDataFilesAsync(files, DeltaWriteMode.Overwrite);
        }

        Assert.Equal(new long[] { 100, 101 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task CommitDataFiles_DynamicPartitionOverwrite_ReplacesOnlyTouchedPartition()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, false))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        static RecordBatch Part(string region, params long[] ids) => new(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("region", StringType.Default, false))
                .Field(new Field("id", Int64Type.Default, false)).Build(),
            [
                new StringArray.Builder().AppendRange(Enumerable.Repeat(region, ids.Length)).Build(),
                new Int64Array.Builder().AppendRange(ids).Build(),
            ], ids.Length);

        await using (var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, partitionColumns: new[] { "region" }))
        {
            await table.WriteAsync([Part("US", 1, 2), Part("EU", 3, 4)]); // v1

            // dynamic overwrite touching only US
            var files = await table.WriteDataFilesAsync([Part("US", 9)]);
            await table.CommitDataFilesAsync(files, DeltaWriteMode.Append, dynamicPartitionOverwrite: true);
        }

        Assert.Equal(new long[] { 3, 4, 9 }, await ReadIdsFresh()); // EU kept, US replaced
    }

    [Fact]
    public async Task CommitDataFiles_RowTracking_AssignsContiguousBaseRowIds()
    {
        await using (var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdValueSchema, enableRowTracking: true))
        {
            await table.WriteAsync([Batch(1, 3)]); // v1 — baseRowId 0, three rows

            var files = await table.WriteDataFilesAsync([Batch(4, 2)]);
            await table.CommitDataFilesAsync(files, DeltaWriteMode.Append); // v2 — baseRowId 3
        }

        // The high-water mark advanced to 5 (3 + 2), and the new add's baseRowId is 3 (contiguous).
        await using var check = await OpenAsync();
        Assert.Equal(5, check.CurrentSnapshot.RowIdHighWaterMark);
        var baseIds = check.CurrentSnapshot.ActiveFiles.Values
            .Select(a => a.BaseRowId!.Value).OrderBy(x => x).ToArray();
        Assert.Equal(new long[] { 0, 3 }, baseIds);
    }

    [Fact]
    public async Task Properties_PlainTable_SupportExternalCommit()
    {
        // A plain (no identity, no IcebergCompat) table supports the external commit path.
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdValueSchema);
        Assert.False(table.IsIcebergCompat);
        Assert.False(table.HasIdentityColumns);
        Assert.True(table.SupportsExternalDataFileCommit);
    }

    /// <summary>A REWRITE commit (compaction / clustering OPTIMIZE): removes AND adds carry dataChange=false
    /// (CDF readers exclude it, appendOnly permits it — it removes files, not rows) and each add is stamped
    /// with add.clusteringProvider. Un-parked from PendingCoverageTests.</summary>
    [Fact]
    public async Task CommitDataFiles_RewriteShape_DataChangeFalseAndClusteringProvider()
    {
        long rewriteVersion;
        await using (var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdValueSchema))
        {
            await table.WriteAsync([Batch(1, 2)]); // v1
            await table.WriteAsync([Batch(3, 2)]); // v2 — two small files

            // Rewrite the two files into one, Overwrite-shaped, dataChange=false, tagged "liquid".
            var rewritten = await table.WriteDataFilesAsync([Batch(1, 4)]);
            rewriteVersion = await table.CommitDataFilesAsync(
                rewritten, DeltaWriteMode.Overwrite,
                operation: "OPTIMIZE", dataChange: false, clusteringProvider: "liquid");
        }

        // Every remove AND add in the rewrite commit carries dataChange=false.
        foreach (var action in ReadCommitActions(rewriteVersion))
        {
            if (action.TryGetProperty("add", out var add))
            {
                Assert.False(add.GetProperty("dataChange").GetBoolean());
                Assert.Equal("liquid", add.GetProperty("clusteringProvider").GetString());
            }
            else if (action.TryGetProperty("remove", out var remove))
            {
                Assert.False(remove.GetProperty("dataChange").GetBoolean());
            }
        }

        // The rows are unchanged (a rewrite reorganizes files, not rows).
        Assert.Equal(new long[] { 1, 2, 3, 4 }, await ReadIdsFresh());

        // clusteringProvider survives log replay onto the snapshot's active files.
        await using var check = await OpenAsync();
        Assert.All(check.CurrentSnapshot.ActiveFiles.Values,
            a => Assert.Equal("liquid", a.ClusteringProvider));
    }

    /// <summary>expectedVersion turns the append OCC retry into a conflict-ABORT — snapshot-coupled
    /// extraActions must not silently land on a table a concurrent writer moved (first-committer-wins).
    /// Un-parked from PendingCoverageTests.</summary>
    [Fact]
    public async Task ExpectedVersion_ConcurrentWriter_ConflictAborts()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdValueSchema);
        await table.WriteAsync([Batch(1, 3)]); // v1
        long pinned = table.CurrentSnapshot.Version;

        var files = await table.WriteDataFilesAsync([Batch(4, 2)]);

        // a concurrent writer advances the table off the pinned version
        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([Batch(100, 2)]);
        }

        await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await table.CommitDataFilesAsync(files, DeltaWriteMode.Append, expectedVersion: pinned));
    }

    /// <summary>Without expectedVersion, a plain append rebases past a non-conflicting concurrent commit
    /// (bounded retry) instead of aborting — the already-written files are reused as-is.</summary>
    [Fact]
    public async Task NoExpectedVersion_ConcurrentAppend_Rebases()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdValueSchema);
        await table.WriteAsync([Batch(1, 3)]); // v1

        var files = await table.WriteDataFilesAsync([Batch(4, 2)]);

        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([Batch(100, 2)]); // v2 — concurrent, non-conflicting
        }

        long committed = await table.CommitDataFilesAsync(files, DeltaWriteMode.Append); // rebases → v3
        Assert.Equal(3, committed);

        Assert.Equal(new long[] { 1, 2, 3, 4, 5, 100, 101 }, await ReadIdsFresh());
    }
}
