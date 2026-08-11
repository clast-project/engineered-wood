// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A table written through DML must checkpoint on the interval, exactly as a batch-written one does.
///
/// <para>These pin issue #86. <c>CommitOccAsync</c> — the loop behind the transaction commit, both DELETE
/// paths, <c>UpdateAsync</c> and <c>UpdateRowsAsync</c> — used to pass
/// <c>WriteCheckpointOnInterval = false</c>, so only <c>CommitWriteAsync</c> and
/// <c>CommitDataFilesAsync</c> ever produced one. A table written exclusively through DML therefore never
/// got a checkpoint and never got a <c>_last_checkpoint</c>, which costs three compounding things: every
/// open replays the log from v0; foreign readers get no resume hint; and commits accumulate without bound,
/// because log cleanup is defined in terms of what a checkpoint subsumes.</para>
///
/// <para>OPTIMIZE and the metadata-only changes do not come through that loop — they commit through
/// <c>TransactionLog</c> directly, and are pinned separately.</para>
///
/// <para><b>⚠ The DELETE case is the load-bearing one.</b> A test that only checks a batch APPEND passes
/// with the defect fully present — that path always checkpointed. What distinguishes them is the commit
/// loop, so the assertion has to name a version that a DML statement produced.</para>
/// </summary>
public class DmlCheckpointTests : IDisposable
{
    private readonly string _tempDir;

    public DmlCheckpointTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dmlckpt_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema IdSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params long[] ids)
    {
        var b = new Int64Array.Builder();
        foreach (long id in ids)
            b.Append(id);
        return new RecordBatch(schema, [b.Build()], ids.Length);
    }

    /// <summary>
    /// The regression itself: drive the version to a multiple of the interval using DELETEs and assert the
    /// checkpoint for THAT version exists. A checkpoint interval of 4 keeps the test to a handful of
    /// commits; the version is asserted rather than assumed, so the test cannot pass by landing somewhere
    /// else.
    /// </summary>
    [Fact]
    public async Task DeleteCommits_Checkpoint_OnTheInterval()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 4 };

        // Deletion vectors ON: these DELETEs remove SOME rows of one file, which is the row-level path
        // that goes through CommitOccAsync. Without them the partial delete is refused outright and the
        // test would never reach the commit loop it exists to check.
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, options, enableDeletionVectors: true);                     // v0
        await table.WriteAsync([Rows(schema, 1, 2, 3, 4, 5, 6)]);                  // v1

        // v2, v3, v4 — all through DeleteAsync, i.e. CommitOccAsync.
        long version = 0;
        for (long target = 1; target <= 3; target++)
        {
            long capture = target;
            (_, version) = await table.DeleteAsync(batch =>
            {
                var ids = (Int64Array)batch.Column(0);
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < ids.Length; i++)
                    mask.Append(ids.GetValue(i) == capture);
                return mask.Build();
            });
        }

        // The assertion is only meaningful if a DELETE actually produced the interval version.
        Assert.Equal(4, version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)),
            "a DELETE landed on the checkpoint interval but wrote no checkpoint — issue #86");

        // ...and the table is still readable through it, so this is a checkpoint and not just a file.
        int rows = 0;
        await foreach (var b in table.ReadAllAsync())
            rows += b.Length;
        Assert.Equal(3, rows); // 6 written, 3 deleted
    }

    /// <summary>
    /// POSITIVE CONTROL. The batch write path always checkpointed, so if this fails the mechanism is broken
    /// generally (interval, writer wiring) rather than specifically on the DML loop — which is a different
    /// bug, and without this test the DML assertion above could not tell the two apart.
    /// </summary>
    [Fact]
    public async Task WriteCommits_Checkpoint_OnTheInterval()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 4 };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options); // v0
        for (long i = 1; i <= 4; i++)
            await table.WriteAsync([Rows(schema, i)]);                             // v1..v4

        Assert.Equal(4, table.CurrentSnapshot.Version);
        Assert.True(await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)));
    }

    /// <summary>
    /// The hint file is what a FOREIGN reader uses to skip the replay — a checkpoint nobody can find helps
    /// only us. Asserted on the DML path for the same reason as above.
    /// </summary>
    [Fact]
    public async Task DeleteCommits_Publish_LastCheckpointHint()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 2 };

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, options, enableDeletionVectors: true);                     // v0
        await table.WriteAsync([Rows(schema, 1, 2, 3)]);                           // v1

        (_, long version) = await table.DeleteAsync(batch =>                       // v2
        {
            var ids = (Int64Array)batch.Column(0);
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < ids.Length; i++)
                mask.Append(ids.GetValue(i) == 1);
            return mask.Build();
        });

        Assert.Equal(2, version);
        Assert.True(await fs.ExistsAsync(DeltaVersion.LastCheckpointPath));
    }

    /// <summary>
    /// The transaction commit, which is the path <c>doc/embedding-host-guide.md</c> recommends to an
    /// embedding host — so a host that took that advice was the one guaranteed never to checkpoint.
    /// Distinct from the autocommit DELETE above only in how it reaches <c>CommitOccAsync</c>, which is
    /// exactly why it is worth its own case: it is the entry point with the most callers downstream.
    /// </summary>
    [Fact]
    public async Task TransactionCommit_Checkpoints_OnTheInterval()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 2 };

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, options, enableDeletionVectors: true);                     // v0
        await table.WriteAsync([Rows(schema, 1, 2, 3)]);                           // v1

        var txn = table.StartTransaction();                                        // v2
        await txn.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < ids.Length; i++)
                mask.Append(ids.GetValue(i) == 1);
            return mask.Build();
        });
        long version = await txn.CommitAsync();

        Assert.Equal(2, version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(2)),
            "a transaction commit landed on the checkpoint interval but wrote no checkpoint — issue #86");
    }

    /// <summary>
    /// ONE checkpoint per interval version, not two.
    ///
    /// <para>A blind append reaches the log through <c>CommitOccAsync</c>, which now checkpoints on the
    /// interval — and <c>CommitWriteAsync</c> has always run an interval check of its own afterwards. Both
    /// firing writes the same checkpoint twice. Under the classic form that is only duplicated work, since
    /// every write lands on the one path <c>&lt;version&gt;.checkpoint.parquet</c>; a V2 checkpoint is
    /// UUID-named, so the second write does not replace the first, it ORPHANS it — and
    /// <c>VacuumExecutor</c> excludes <c>_delta_log</c>, so nothing ever collects it.</para>
    ///
    /// <para>Asserted under the V2 form for that reason: it is the form that can tell the two apart. A
    /// count is the assertion — <c>NotEmpty</c> passes with the duplicate present.</para>
    /// </summary>
    [Fact]
    public async Task IntervalCheckpoint_IsWrittenOnce_NotOncePerTrigger()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 2 };

        await using var table = await DeltaTable.CreateAsync(fs, schema,
            configuration: new Dictionary<string, string> { ["delta.checkpointPolicy"] = "v2" },
            options: options);                                                     // v0
        await table.WriteAsync([Rows(schema, 1)]);                                 // v1
        await table.WriteAsync([Rows(schema, 2)]);                                 // v2 → checkpoint

        Assert.Equal(2, table.CurrentSnapshot.Version);

        var checkpoints = Directory
            .GetFiles(Path.Combine(_tempDir, "_delta_log"), "*.checkpoint.*")
            .Select(Path.GetFileName)
            .ToList();

        Assert.True(checkpoints.Count == 1,
            $"expected exactly one checkpoint at v2, got {checkpoints.Count}: "
            + string.Join(", ", checkpoints));
    }

    /// <summary>
    /// The other half of the guard above: the overwrite family does NOT go through the commit loop (it
    /// makes a single atomic attempt at the read version + 1), so <c>CommitWriteAsync</c>'s own interval
    /// check is still the only thing that checkpoints it. Suppressing that check for the whole method
    /// rather than for the append branch would have taken this with it, silently.
    /// </summary>
    [Fact]
    public async Task OverwriteCommits_Checkpoint_OnTheInterval()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 2 };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options); // v0
        await table.WriteAsync([Rows(schema, 1)]);                                 // v1
        await table.WriteAsync([Rows(schema, 2)], DeltaWriteMode.Overwrite);       // v2

        Assert.Equal(2, table.CurrentSnapshot.Version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(2)),
            "an overwrite landed on the checkpoint interval but wrote no checkpoint");
    }
}
