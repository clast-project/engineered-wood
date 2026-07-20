// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// commitInfo (operation + timestamp) is written on EVERY commit — those are standard, feature-free fields,
/// so a plain writer-v2 table gets a usable history and resolvable timestamps without opting into the
/// inCommitTimestamps feature. GetHistoryAsync is the reader for it.
/// </summary>
public class HistoryTests : IDisposable
{
    private readonly string _tempDir;

    public HistoryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_hist_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema Schema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params long[] ids)
    {
        var b = new Int64Array.Builder();
        foreach (var id in ids)
            b.Append(id);
        return new RecordBatch(schema, [b.Build()], ids.Length);
    }

    private static async Task<List<DeltaTable.DeltaHistoryEntry>> HistoryAsync(DeltaTable table)
    {
        var list = new List<DeltaTable.DeltaHistoryEntry>();
        await foreach (var e in table.GetHistoryAsync())
            list.Add(e);
        return list;
    }

    [Fact]
    public async Task History_IsOldestFirst_AndRecordsOperations()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(fs, Schema(), options);
        await table.WriteAsync([Rows(table.ArrowSchema, 1, 2)]);
        await table.WriteAsync([Rows(table.ArrowSchema, 3)]);

        var history = await HistoryAsync(table);

        Assert.Equal(3, history.Count);
        Assert.Equal([0L, 1L, 2L], history.Select(h => h.Version));
        Assert.Equal("CREATE TABLE", history[0].Operation);
        Assert.Equal("WRITE", history[1].Operation);
        Assert.Equal("WRITE", history[2].Operation);
    }

    // Every commit carries a timestamp even on a plain table, so the history is fully dated.
    [Fact]
    public async Task History_EveryCommitHasATimestamp_OnAPlainTable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(fs, Schema(), options);
        await table.WriteAsync([Rows(table.ArrowSchema, 1)]);

        // A plain table: no inCommitTimestamps feature declared.
        Assert.Null(table.CurrentSnapshot.Protocol.WriterFeatures);

        var history = await HistoryAsync(table);
        Assert.All(history, h => Assert.NotNull(h.TimestampMs));
        // operationParameters is present (at least as an empty object) for history tooling.
        Assert.All(history, h => Assert.NotNull(h.OperationParameters));
    }

    // OPTIMIZE was the only commit path that wrote no commitInfo at all.
    [Fact]
    public async Task History_RecordsOptimize()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(fs, Schema(), options);
        for (int i = 0; i < 4; i++)
            await table.WriteAsync([Rows(table.ArrowSchema, i)]);

        var compacted = await table.CompactAsync(new CompactionOptions
        {
            MinFileSize = long.MaxValue,
            TargetFileSize = long.MaxValue,
        });
        Assert.NotNull(compacted);

        var history = await HistoryAsync(table);
        var optimize = history.Single(h => h.Version == compacted!.Value);
        Assert.Equal("OPTIMIZE", optimize.Operation);
        Assert.NotNull(optimize.TimestampMs);
    }

    // VACUUM writes a Spark-parity START/END commitInfo-only pair around the physical deletes, so other
    // engines can see why older versions stopped being physically readable.
    [Fact]
    public async Task History_RecordsVacuumStartAndEnd()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(fs, Schema(), options);
        await table.WriteAsync([Rows(table.ArrowSchema, 1)]);

        long versionBefore = table.CurrentSnapshot.Version;
        await table.VacuumAsync(TimeSpan.FromDays(7), dryRun: false);

        var history = await HistoryAsync(table);
        var after = history.Where(h => h.Version > versionBefore).ToList();

        Assert.Equal(2, after.Count);
        Assert.Equal("VACUUM START", after[0].Operation);
        Assert.Equal("VACUUM END", after[1].Operation);
        // The START commit records what it intended to delete; END records what it did.
        Assert.Contains("retentionDurationMillis", after[0].OperationParameters!);
        Assert.Contains("COMPLETED", after[1].OperationParameters!);
    }

    [Fact]
    public async Task Vacuum_DryRun_WritesNoCommits()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(fs, Schema(), options);
        await table.WriteAsync([Rows(table.ArrowSchema, 1)]);

        long versionBefore = table.CurrentSnapshot.Version;
        await table.VacuumAsync(TimeSpan.FromDays(7), dryRun: true);

        var history = await HistoryAsync(table);
        Assert.Equal(versionBefore, history.Max(h => h.Version));
    }

    // The point of the always-on timestamp: time travel by timestamp works on a plain table.
    [Fact]
    public async Task TimestampTimeTravel_WorksOnAPlainTable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(fs, Schema(), options);
        await table.WriteAsync([Rows(table.ArrowSchema, 1)]);

        var history = await HistoryAsync(table);
        long firstWriteTs = history.Single(h => h.Version == 1).TimestampMs!.Value;

        await table.WriteAsync([Rows(table.ArrowSchema, 2)]);

        // Resolving at the first write's timestamp must land on v1, not the latest.
        var snapshot = await table.GetSnapshotAtTimestampAsync(
            DateTimeOffset.FromUnixTimeMilliseconds(firstWriteTs));
        Assert.Equal(1L, snapshot.Version);
    }
}
