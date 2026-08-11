// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <c>DeltaTable.CheckpointAsync</c> — a checkpoint on demand, for a host that owns the cadence.
///
/// <para>Before it existed a caller who wanted one had to construct a <c>CheckpointWriter</c> against the
/// table's file system and re-supply the parquet options and checkpoint format by hand, which works and
/// duplicates the table's configuration. Issue #86 asked for this regardless of the interval fixes.</para>
/// </summary>
public class ExplicitCheckpointTests : IDisposable
{
    private readonly string _tempDir;

    public ExplicitCheckpointTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_explckpt_{Guid.NewGuid():N}");
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
    /// Off the interval and with checkpointing disabled outright — the case the interval fixes cannot
    /// reach, and the reason this method earns its place rather than being a convenience.
    /// </summary>
    [Fact]
    public async Task CheckpointAsync_WritesOne_OffTheIntervalAndWithIntervalDisabled()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options); // v0
        await table.WriteAsync([Rows(schema, 1)]);                                 // v1
        await table.WriteAsync([Rows(schema, 2)]);                                 // v2
        await table.WriteAsync([Rows(schema, 3)]);                                 // v3

        Assert.Empty(Directory.GetFiles(Path.Combine(_tempDir, "_delta_log"), "*.checkpoint.*"));

        long version = await table.CheckpointAsync();

        Assert.Equal(3, version);
        Assert.True(await fs.ExistsAsync(DeltaVersion.CheckpointPath(3)));
        Assert.True(await fs.ExistsAsync(DeltaVersion.LastCheckpointPath));
    }

    /// <summary>
    /// It has to be a CHECKPOINT, not a file with the right name: delete every commit it subsumes and the
    /// table must still open at the same version with the same rows.
    /// </summary>
    [Fact]
    public async Task CheckpointAsync_Produces_AReadableCheckpoint()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using (var table = await DeltaTable.CreateAsync(fs, schema, options))
        {
            await table.WriteAsync([Rows(schema, 1, 2)]);                          // v1
            await table.WriteAsync([Rows(schema, 3)]);                             // v2
            await table.CheckpointAsync();
        }

        string logDir = Path.Combine(_tempDir, "_delta_log");
        foreach (string commit in Directory.GetFiles(logDir, "*.json"))
        {
            string name = Path.GetFileName(commit);
            if (name.Contains(".checkpoint.", StringComparison.Ordinal))
                continue;
            if (long.TryParse(Path.GetFileNameWithoutExtension(name), out long v) && v <= 2)
                File.Delete(commit);
        }

        await using var reopened = await DeltaTable.OpenAsync(fs, options);
        Assert.Equal(2, reopened.CurrentSnapshot.Version);

        int rows = 0;
        await foreach (var b in reopened.ReadAllAsync())
            rows += b.Length;
        Assert.Equal(3, rows);
    }

    /// <summary>
    /// It honours the table's checkpoint FORMAT, which is the whole reason for routing through the table
    /// rather than constructing a <c>CheckpointWriter</c> at the call site.
    /// </summary>
    [Fact]
    public async Task CheckpointAsync_Honours_TheTablesCheckpointFormat()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(fs, schema,
            configuration: new Dictionary<string, string> { ["delta.checkpointPolicy"] = "v2" },
            options: options);
        await table.WriteAsync([Rows(schema, 1)]);

        await table.CheckpointAsync();

        string logDir = Path.Combine(_tempDir, "_delta_log");
        Assert.NotEmpty(Directory.GetFiles(logDir, "*.checkpoint.*.json"));
        Assert.Empty(Directory.GetFiles(logDir, "*.checkpoint.parquet"));
    }
}
