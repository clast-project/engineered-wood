// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> Spark's deletion-vector DML on a Change Data Feed table writes NO change file: its
/// reader recovers what changed by diffing each <c>remove</c>/<c>add</c> pair's vectors
/// (<c>CDCReader.processDeletionVectorActions</c>). So engineered-wood's feed of a table Spark deleted from
/// with vectors is only right if our reader diffs the pair too (#529). Spark's own feed of the same versions
/// is the oracle. See <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class CdfDeletionVectorInteropTests : IDisposable
{
    private readonly string _tempDir;

    public CdfDeletionVectorInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdfdv_interop_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    /// <summary>
    /// Two Spark vector DELETEs (the second onto a file that already has a vector) and a RESTORE that un-hides
    /// the rows again: engineered-wood's feed of those versions is Spark's, row for row.
    /// </summary>
    [SkippableFact]
    public async Task SparkVectorDeletes_FeedMatchesSparksOwn()
    {
        Spark.Require();

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(
            fs, IdSchema, enableDeletionVectors: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" }))
        {
            await table.WriteAsync(
                [new RecordBatch(IdSchema, [new Int64Array.Builder().AppendRange([1, 2, 3, 4, 5]).Build()], 5)]);
        }

        Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = new[]
            {
                "DELETE FROM delta.`{path}` WHERE id = 3",                // v2
                "DELETE FROM delta.`{path}` WHERE id IN (4, 5)",          // v3
                "RESTORE TABLE delta.`{path}` TO VERSION AS OF 1",        // v4
            },
        });

        // The premise: each version is a vector edit of the one file with no change file, so both readers
        // must INFER the feed. Were Spark to write a change file here, this would test nothing.
        var log = new TransactionLog(fs);
        for (long v = 2; v <= 4; v++)
        {
            var actions = await log.ReadCommitAsync(v);
            Assert.DoesNotContain(actions, a => a is CdcFile);
            var remove = Assert.Single(actions.OfType<RemoveFile>());
            var add = Assert.Single(actions.OfType<AddFile>());
            Assert.Equal(remove.Path, add.Path);
            Assert.True(remove.DeletionVector is not null || add.DeletionVector is not null, $"v{v} has no vector");
        }

        var spark = Spark.Invoke("read_changes", new { path = _tempDir, start = 2, end = 4 })
            .GetProperty("rows").EnumerateArray()
            .Select(r => (
                Version: r.GetProperty("_commit_version").GetInt64(),
                Type: r.GetProperty("_change_type").GetString()!,
                Id: r.GetProperty("id").GetInt64()))
            .OrderBy(r => r.Version).ThenBy(r => r.Type, StringComparer.Ordinal).ThenBy(r => r.Id)
            .ToList();

        var ours = new List<(long Version, string Type, long Id)>();
        await using (var reader = await DeltaTable.OpenAsync(fs))
        {
            await foreach (var batch in reader.ReadChangesAsync(
                new DeltaChangeReadOptions { StartVersion = 2, EndVersion = 4 }))
            {
                var ids = (Int64Array)batch.Column("id");
                var types = (StringArray)batch.Column("_change_type");
                var versions = (Int64Array)batch.Column("_commit_version");
                for (int i = 0; i < batch.Length; i++)
                    ours.Add((versions.GetValue(i)!.Value, types.GetString(i), ids.GetValue(i)!.Value));
            }
        }
        ours = [.. ours.OrderBy(r => r.Version).ThenBy(r => r.Type, StringComparer.Ordinal).ThenBy(r => r.Id)];

        Assert.Equal(
            new (long, string, long)[]
            {
                (2, "delete", 3),
                (3, "delete", 4), (3, "delete", 5),
                (4, "insert", 3), (4, "insert", 4), (4, "insert", 5),
            },
            spark);
        Assert.Equal(spark, ours);
    }
}
