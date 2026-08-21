// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The commit paths that do NOT go through <c>LogCommitter</c> must honour the checkpoint interval too.
///
/// <para>OPTIMIZE and every metadata-only change (schema edits, clustering, domain metadata) build their
/// actions and call <c>TransactionLog.WriteCommitAsync</c> directly, so the committer's interval check
/// never saw them — they were the half of issue #86 that flipping <c>WriteCheckpointOnInterval</c> could
/// not reach. A table maintained purely through, say, ALTER TABLE would still never have checkpointed.</para>
///
/// <para>Each case drives the version onto the interval with the operation under test as the LAST commit,
/// so the checkpoint asserted is the one that operation was responsible for and not one an earlier
/// write already produced.</para>
/// </summary>
public class MetadataCommitCheckpointTests : IDisposable
{
    private readonly string _tempDir;

    public MetadataCommitCheckpointTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_metackpt_{Guid.NewGuid():N}");
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

    /// <summary>A schema change — <c>CommitMetadataOnlyAsync</c>, which seven public methods reach.</summary>
    [Fact]
    public async Task AddColumn_Checkpoints_OnTheInterval()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 2 };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options); // v0
        await table.WriteAsync([Rows(schema, 1)]);                                 // v1

        long version = await table.AddColumnAsync(                                 // v2
            new Field("note", StringType.Default, nullable: true));

        Assert.Equal(2, version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(2)),
            "a schema change landed on the checkpoint interval but wrote no checkpoint — issue #86");
    }

    /// <summary>Domain metadata — its own <c>WriteCommitAsync</c>, separate from the metadata-only helper.</summary>
    [Fact]
    public async Task DomainMetadata_Checkpoints_OnTheInterval()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 2 };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options); // v0
        await table.WriteAsync([Rows(schema, 1)]);                                 // v1

        long version = await table.SetDomainMetadataAsync("test.domain", "{\"k\":1}"); // v2

        Assert.Equal(2, version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(2)),
            "a domain-metadata commit landed on the checkpoint interval but wrote no checkpoint");
    }

    /// <summary>
    /// OPTIMIZE — <c>CompactionExecutor</c>, which bypasses the committer entirely. The operation with the
    /// most to gain: it removes every file it rewrote, so its commit is the largest the table produces.
    /// </summary>
    [Fact]
    public async Task Optimize_Checkpoints_OnTheInterval()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 4 };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options); // v0
        await table.WriteAsync([Rows(schema, 1)]);                                 // v1
        await table.WriteAsync([Rows(schema, 2)]);                                 // v2
        await table.WriteAsync([Rows(schema, 3)]);                                 // v3

        // Three one-row files, all far below MinFileSize, so they compact into one.
        long? version = await table.CompactAsync();                                // v4

        Assert.Equal(4, version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)),
            "OPTIMIZE landed on the checkpoint interval but wrote no checkpoint — issue #86");

        // The checkpoint has to carry the compacted table, not the pre-compaction one.
        int rows = 0;
        await foreach (var b in table.ReadAllAsync())
            rows += b.Length;
        Assert.Equal(3, rows);
    }

    /// <summary>
    /// <c>CheckpointInterval = 0</c> means "never checkpoint", and it is an absolute caller override — a
    /// host driving checkpoints on its own cadence must not find one appearing underneath it. Asserted on
    /// a metadata path because that is the one that just gained the trigger.
    /// </summary>
    [Fact]
    public async Task IntervalZero_Suppresses_TheMetadataCheckpoint()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options); // v0
        await table.WriteAsync([Rows(schema, 1)]);                                 // v1
        await table.SetDomainMetadataAsync("test.domain", "{\"k\":1}");            // v2

        string logDir = Path.Combine(_tempDir, "_delta_log");
        Assert.Empty(Directory.GetFiles(logDir, "*.checkpoint.*"));
        Assert.False(await fs.ExistsAsync(DeltaVersion.LastCheckpointPath));
    }
}
