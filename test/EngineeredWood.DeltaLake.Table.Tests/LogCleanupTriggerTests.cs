// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Log cleanup must run behind EVERY checkpoint, not only the ones the commit loop writes.
///
/// <para>Cleanup deletes what a checkpoint covers, so a checkpoint becoming durable is the moment the work
/// is legitimate — which makes "wrote a checkpoint" and "reclaimed what it covers" one operation with
/// three triggers: <c>LogCommitter</c>'s, <c>AfterCommitAsync</c> (the overwrite family, OPTIMIZE and
/// the metadata-only changes) and the explicit <c>CheckpointAsync</c>. Wiring one and not the others
/// reclaims on some commit paths and silently not on others.</para>
///
/// <para>That is not a hypothetical failure here. The checkpoint INTERVAL drifted between triggers twice
/// — once inside #94 and again when #86's fix moved where the second trigger lived — so these assert per
/// trigger rather than once. The tests that pin the deletion RULE itself live at the log layer, in
/// <c>LogCleanupTests</c>; these pin only that each trigger reaches it.</para>
/// </summary>
public class LogCleanupTriggerTests : IDisposable
{
    private readonly string _tempDir;

    public LogCleanupTriggerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cleantrig_{Guid.NewGuid():N}");
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

    private static RecordBatch Row(Apache.Arrow.Schema schema, long id) =>
        new(schema, [new Int64Array.Builder().Append(id).Build()], 1);

    private string LogDir => Path.Combine(_tempDir, "_delta_log");

    /// <summary>
    /// Ages every log file already written past the default 30-day retention. Cleanup compares the
    /// filesystem's modification time against <c>now</c>, so backdating is what makes the horizon
    /// reachable without sleeping through it.
    /// </summary>
    private void AgeExistingLogFiles()
    {
        var old = DateTime.UtcNow.AddDays(-60);
        foreach (string file in Directory.GetFiles(LogDir))
            File.SetLastWriteTimeUtc(file, old);
    }

    private bool CommitExists(long version) =>
        File.Exists(Path.Combine(LogDir, $"{DeltaVersion.Format(version)}.json"));

    /// <summary>The commit loop's own trigger — the one PR #93 wired.</summary>
    [Fact]
    public async Task CommitLoopCheckpoint_RunsCleanup()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, new DeltaTableOptions { CheckpointInterval = 2 });   // v0
        await table.WriteAsync([Row(schema, 1)]);                            // v1
        AgeExistingLogFiles();
        await table.WriteAsync([Row(schema, 2)]);                            // v2 → checkpoint + cleanup

        Assert.True(await fs.ExistsAsync(DeltaVersion.CheckpointPath(2)));
        Assert.False(CommitExists(0), "v0 is covered by the v2 checkpoint and long expired");
        Assert.False(CommitExists(1), "v1 is covered by the v2 checkpoint and long expired");
        Assert.True(CommitExists(2), "the checkpoint's own version must survive — it is what replay starts from");
    }

    /// <summary>
    /// A metadata-only commit, which reaches the log through <c>AfterCommitAsync</c> rather than the
    /// committer. This is the case that was silently not reclaiming.
    /// </summary>
    [Fact]
    public async Task MetadataCommitCheckpoint_RunsCleanup()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, new DeltaTableOptions { CheckpointInterval = 2 });   // v0
        await table.WriteAsync([Row(schema, 1)]);                            // v1
        AgeExistingLogFiles();
        await table.SetDomainMetadataAsync("test.domain", "{\"k\":1}");      // v2 → checkpoint + cleanup

        Assert.True(await fs.ExistsAsync(DeltaVersion.CheckpointPath(2)));
        Assert.False(CommitExists(0), "a metadata commit checkpointed but reclaimed nothing");
        Assert.False(CommitExists(1), "a metadata commit checkpointed but reclaimed nothing");
        Assert.True(CommitExists(2));
    }

    /// <summary>OPTIMIZE, the other <c>AfterCommitAsync</c> caller worth naming on its own.</summary>
    [Fact]
    public async Task OptimizeCheckpoint_RunsCleanup()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, new DeltaTableOptions { CheckpointInterval = 4 });   // v0
        await table.WriteAsync([Row(schema, 1)]);                            // v1
        await table.WriteAsync([Row(schema, 2)]);                            // v2
        await table.WriteAsync([Row(schema, 3)]);                            // v3
        AgeExistingLogFiles();
        long? version = await table.CompactAsync();                          // v4 → checkpoint + cleanup

        Assert.Equal(4, version);
        Assert.True(await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)));
        Assert.False(CommitExists(0), "OPTIMIZE checkpointed but reclaimed nothing");
        Assert.False(CommitExists(3), "OPTIMIZE checkpointed but reclaimed nothing");
        Assert.True(CommitExists(4));

        // The table still reads through the checkpoint the survivors are replayed from.
        int rows = 0;
        await foreach (var b in table.ReadAllAsync())
            rows += b.Length;
        Assert.Equal(3, rows);
    }

    /// <summary>
    /// The explicit <c>CheckpointAsync</c>. It reclaims too — the checkpoint is what makes the older
    /// commits redundant, not the reason it was written — and that is documented on the method, because a
    /// public API that deletes files has to say so.
    /// </summary>
    [Fact]
    public async Task ExplicitCheckpoint_RunsCleanup()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, new DeltaTableOptions { CheckpointInterval = 0 });   // v0, never auto-checkpoints
        await table.WriteAsync([Row(schema, 1)]);                            // v1
        await table.WriteAsync([Row(schema, 2)]);                            // v2
        AgeExistingLogFiles();

        Assert.True(CommitExists(0));

        long version = await table.CheckpointAsync();

        Assert.Equal(2, version);
        Assert.False(CommitExists(0), "an explicit checkpoint reclaimed nothing");
        Assert.False(CommitExists(1), "an explicit checkpoint reclaimed nothing");
        Assert.True(CommitExists(2));
    }

    /// <summary>
    /// THE CONTROL for all four. <c>delta.enableExpiredLogCleanup = false</c> keeps every file, so a test
    /// above cannot pass merely because something else removed the commits — and a host that has switched
    /// cleanup off because something else owns its log retention keeps what it asked to keep.
    /// </summary>
    [Fact]
    public async Task CleanupDisabled_KeepsEveryCommit_OnEveryTrigger()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema,
            configuration: new Dictionary<string, string>
            {
                ["delta.enableExpiredLogCleanup"] = "false",
            },
            options: new DeltaTableOptions { CheckpointInterval = 2 });      // v0
        await table.WriteAsync([Row(schema, 1)]);                            // v1
        AgeExistingLogFiles();
        await table.WriteAsync([Row(schema, 2)]);                            // v2 → checkpoint, no cleanup
        AgeExistingLogFiles();
        await table.SetDomainMetadataAsync("test.domain", "{}");             // v3
        await table.WriteAsync([Row(schema, 4)]);                            // v4 → checkpoint, no cleanup
        AgeExistingLogFiles();
        await table.CheckpointAsync();                                       // explicit, no cleanup

        for (long v = 0; v <= 4; v++)
            Assert.True(CommitExists(v), $"v{v} was deleted with cleanup switched off");
    }
}
