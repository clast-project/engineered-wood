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

    private static Apache.Arrow.Schema IdPartSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("part", StringType.Default, true))
        .Build();

    /// <summary>
    /// Unlike Spark, engineered-wood's vector DELETE writes a change file (#731: delta-rs cannot yet infer the
    /// feed of one). The commit then holds both the vector edits and a change file, and Spark must read it from
    /// the change file alone: the rows deleted, nothing re-inserted, and not the rows a vector hid before. Three
    /// deletes on a partitioned table: part of a file, the rest of that file (a remove carrying its vector), and
    /// part of one file with all of another.
    /// </summary>
    [SkippableTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EwPredicateVectorDeletes_SparkFeedIsTheDeletedRows(bool rowTracking)
    {
        Spark.Require();

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(
            fs, IdPartSchema, partitionColumns: ["part"], enableDeletionVectors: true,
            enableRowTracking: rowTracking,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" }))
        {
            await table.WriteAsync([IdParts((1, "a"), (2, "a"), (3, "a"), (4, "b"), (5, "b"), (6, "c"))]); // v1
            await table.DeleteAsync(IdIn(2), RowDeleteMode.DeletionVector);       // v2
            await table.DeleteAsync(IdIn(1, 3), RowDeleteMode.DeletionVector);    // v3
            await table.DeleteAsync(IdIn(4, 6), RowDeleteMode.DeletionVector);    // v4
        }

        // v3 removes a file that already has a vector, the case delta-rs reads wrongly without a change file
        // (delta-io/delta-rs#4946).
        var v3 = await new TransactionLog(fs).ReadCommitAsync(3);
        Assert.Contains(v3, a => a is RemoveFile { DeletionVector: not null });
        Assert.DoesNotContain(v3, a => a is AddFile);

        await AssertVectorDeleteFeedsAsync(fs);
    }

    /// <summary>
    /// <see cref="EwPredicateVectorDeletes_SparkFeedIsTheDeletedRows"/> through the row-level
    /// <see cref="DeltaTable.DeleteRowsAsync"/>, which writes its change files on a path of its own. Its v3 re-adds
    /// the file under a vector hiding every row rather than removing it (#737); the feed is the same.
    /// </summary>
    [SkippableFact]
    public async Task EwRowLevelVectorDeletes_SparkFeedIsTheDeletedRows()
    {
        Spark.Require();

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(
            fs, IdPartSchema, partitionColumns: ["part"], enableDeletionVectors: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" }))
        {
            await table.WriteAsync([IdParts((1, "a"), (2, "a"), (3, "a"), (4, "b"), (5, "b"), (6, "c"))]); // v1
            foreach (long[] ids in new[] { new long[] { 2 }, [1, 3], [4, 6] })                         // v2..v4
                await table.DeleteRowsAsync(await SelectionOfAsync(table, ids));
        }

        await AssertVectorDeleteFeedsAsync(fs);
    }

    private async Task AssertVectorDeleteFeedsAsync(LocalTableFileSystem fs)
    {
        // The premise: each version carries the vector edit AND a change file.
        var log = new TransactionLog(fs);
        for (long v = 2; v <= 4; v++)
            Assert.Contains(await log.ReadCommitAsync(v), a => a is CdcFile);

        var spark = Spark.Invoke("read_changes", new { path = _tempDir, start = 2, end = 4 })
            .GetProperty("rows").EnumerateArray()
            .Select(r => (
                Version: r.GetProperty("_commit_version").GetInt64(),
                Type: r.GetProperty("_change_type").GetString()!,
                Id: r.GetProperty("id").GetInt64(),
                Part: r.GetProperty("part").GetString()!))
            .OrderBy(r => r.Version).ThenBy(r => r.Id)
            .ToList();

        var ours = new List<(long Version, string Type, long Id, string Part)>();
        await using (var reader = await DeltaTable.OpenAsync(fs))
        {
            await foreach (var batch in reader.ReadChangesAsync(
                new DeltaChangeReadOptions { StartVersion = 2, EndVersion = 4 }))
            {
                var ids = (Int64Array)batch.Column("id");
                var parts = (StringArray)batch.Column("part");
                var types = (StringArray)batch.Column("_change_type");
                var versions = (Int64Array)batch.Column("_commit_version");
                for (int i = 0; i < batch.Length; i++)
                {
                    ours.Add((versions.GetValue(i)!.Value, types.GetString(i), ids.GetValue(i)!.Value,
                        parts.GetString(i)));
                }
            }
        }
        ours = [.. ours.OrderBy(r => r.Version).ThenBy(r => r.Id)];

        Assert.Equal(
            new (long, string, long, string)[]
            {
                (2, "delete", 2, "a"),
                (3, "delete", 1, "a"), (3, "delete", 3, "a"),
                (4, "delete", 4, "b"), (4, "delete", 6, "c"),
            },
            spark);
        Assert.Equal(spark, ours);
    }

    private static Func<RecordBatch, BooleanArray> IdIn(params long[] ids) => batch =>
    {
        var column = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            mask.Append(System.Array.IndexOf(ids, column.GetValue(i)!.Value) >= 0);
        return mask.Build();
    };

    /// <summary>The live rows holding <paramref name="ids"/>, by file and absolute position.</summary>
    private static async Task<RowSelection> SelectionOfAsync(DeltaTable table, long[] ids)
    {
        var selected = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
        {
            var column = (Int64Array)batch.Column("id");
            var paths = (StringArray)batch.Column(
                RowSelection.DefaultMetadataPrefix + RowSelection.FilePathColumnSuffix);
            var positions = (Int64Array)batch.Column(
                RowSelection.DefaultMetadataPrefix + RowSelection.RowIndexColumnSuffix);
            for (int i = 0; i < batch.Length; i++)
            {
                if (System.Array.IndexOf(ids, column.GetValue(i)!.Value) < 0)
                    continue;
                string path = paths.GetString(i);
                if (!selected.TryGetValue(path, out var set))
                    selected[path] = set = new HashSet<long>();
                ((HashSet<long>)set).Add(positions.GetValue(i)!.Value);
            }
        }
        return RowSelection.ByPath(selected);
    }

    private static RecordBatch IdParts(params (long Id, string Part)[] rows)
    {
        var ids = new Int64Array.Builder();
        var parts = new StringArray.Builder();
        foreach (var (id, part) in rows)
        {
            ids.Append(id);
            parts.Append(part);
        }
        return new RecordBatch(IdPartSchema, [ids.Build(), parts.Build()], rows.Length);
    }

    private static RecordBatch Ids(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    private static RowSelection At(string path, long position) =>
        RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal)
        {
            [path] = new HashSet<long> { position },
        });
}
