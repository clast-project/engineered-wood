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

    /// <summary>
    /// The other direction, for a commit no Spark operation writes: one engineered-wood transaction fusing a
    /// copy-on-write delete (which writes a change file), a vector delete and an append (which write none).
    /// The version is read from its change files alone, so the commit fills in the other two (#530), and
    /// Spark's feed of it must report all three operations, as ours does.
    /// </summary>
    [SkippableFact]
    public async Task EwMultiOperationTransaction_SparkFeedReportsEveryOperation()
    {
        Spark.Require();

        var fs = new LocalTableFileSystem(_tempDir);
        long version;
        await using (var table = await DeltaTable.CreateAsync(
            fs, IdSchema, enableDeletionVectors: true, enableRowTracking: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" }))
        {
            await table.WriteAsync([Ids(1, 2, 3)]);
            await table.WriteAsync([Ids(11, 12, 13)]);
            var ordered = table.CurrentSnapshot.ActiveFiles.Values
                .Select(a => a.Path).OrderBy(p => p, StringComparer.Ordinal).ToList();

            // Which file holds which rows: each write is one file, its rows in the order written.
            string fileOf1 = "", fileOf11 = "";
            await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }))
            {
                var ids = (Int64Array)batch.Column("id");
                var rids = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
                for (int i = 0; i < batch.Length; i++)
                {
                    string path = ordered[TransientRowAddress.FileOrdinal(rids.GetValue(i)!.Value)];
                    if (ids.GetValue(i) == 1) fileOf1 = path;
                    if (ids.GetValue(i) == 11) fileOf11 = path;
                }
            }

            var txn = table.StartTransaction();
            await txn.StageRowDeletesAsync(At(fileOf1, 1), RowDeleteMode.CopyOnWrite); // id 2
            await txn.StageRowDeletesAsync(At(fileOf11, 2));                           // id 13, by vector
            await txn.WriteAsync([Ids(20, 21)]);
            version = await txn.CommitAsync();
        }

        Assert.Contains(await new TransactionLog(fs).ReadCommitAsync(version), a => a is CdcFile);

        var spark = Spark.Invoke("read_changes", new { path = _tempDir, start = version, end = version })
            .GetProperty("rows").EnumerateArray()
            .Select(r => (Type: r.GetProperty("_change_type").GetString()!, Id: r.GetProperty("id").GetInt64()))
            .OrderBy(r => r.Type, StringComparer.Ordinal).ThenBy(r => r.Id)
            .ToList();

        var ours = new List<(string Type, long Id)>();
        await using (var reader = await DeltaTable.OpenAsync(fs))
        {
            await foreach (var batch in reader.ReadChangesAsync(
                new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
            {
                var ids = (Int64Array)batch.Column("id");
                var types = (StringArray)batch.Column("_change_type");
                for (int i = 0; i < batch.Length; i++)
                    ours.Add((types.GetString(i), ids.GetValue(i)!.Value));
            }
        }
        ours = [.. ours.OrderBy(r => r.Type, StringComparer.Ordinal).ThenBy(r => r.Id)];

        Assert.Equal(
            new (string, long)[] { ("delete", 2), ("delete", 13), ("insert", 20), ("insert", 21) },
            spark);
        Assert.Equal(spark, ours);
    }

    private static RecordBatch Ids(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    private static RowSelection At(string path, long position) =>
        RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal)
        {
            [path] = new HashSet<long> { position },
        });
}
