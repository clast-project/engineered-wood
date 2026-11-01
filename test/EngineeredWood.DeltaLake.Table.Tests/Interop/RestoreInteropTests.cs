// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> engineered-wood's RESTORE (#567) against Spark's own: each case builds two identical tables
/// with the same history, restores one with <see cref="DeltaTable.RestoreAsync(long, RestoreOptions?, CancellationToken, CommitProperties?)"/>
/// and the other with Spark's <c>RESTORE TABLE … TO VERSION AS OF</c>, and requires the same outcome: the same rows,
/// the same file counts in the commit's metrics, the same change feed for the restore commit and, on a row-tracking
/// table, the same row ids. Spark reads both, so this is also the proof that the commit EW writes is one Spark reads
/// and can write through. See <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class RestoreInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public RestoreInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_restore_interop_{Guid.NewGuid():N}");
        _ours = Path.Combine(root, "ours");
        _theirs = Path.Combine(root, "theirs");
        Directory.CreateDirectory(_ours);
        Directory.CreateDirectory(_theirs);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        string root = Path.GetDirectoryName(_ours)!;
        if (Directory.Exists(root))
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Field(new Field("v", StringType.Default, true))
        .Build();

    private static RecordBatch Rows(params long[] ids) => new(
        TableSchema,
        [new Int64Array.Builder().AppendRange(ids).Build(),
         new StringArray.Builder().AppendRange(ids.Select(i => $"v{i}")).Build()],
        ids.Length);

    private static Func<RecordBatch, BooleanArray> IdIs(long id) => batch =>
    {
        var column = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            mask.Append(column.GetValue(i) == id);
        return mask.Build();
    };

    private static RecordBatch Renamed(RecordBatch batch)
    {
        var v = (StringArray)batch.Column("v");
        var updated = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            updated.Append(v.GetString(i) + "!");
        return new RecordBatch(batch.Schema, [batch.Column("id"), updated.Build()], batch.Length);
    }

    /// <summary>
    /// The same history in both tables, all written by engineered-wood: v1 append, v2 append, v3 a DELETE, v4 an
    /// UPDATE, v5 an OPTIMIZE.
    /// </summary>
    private async Task CreateTwinsAsync(bool deletionVectors, bool rowTracking)
    {
        foreach (string dir in new[] { _ours, _theirs })
        {
            await using var table = await DeltaTable.CreateAsync(
                new LocalTableFileSystem(dir), TableSchema, enableDeletionVectors: deletionVectors,
                enableRowTracking: rowTracking,
                configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
            await table.WriteAsync([Rows(1, 2, 3)]);
            await table.WriteAsync([Rows(4, 5)]);
            await table.DeleteAsync(IdIs(2), deletionVectors ? RowDeleteMode.DeletionVector : RowDeleteMode.CopyOnWrite);
            await table.UpdateAsync(IdIs(4), Renamed);
            await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue });
        }
    }

    private static List<string> SparkRows(string dir) =>
        [.. Spark.Invoke("read", new { path = dir }).GetProperty("rows").EnumerateArray()
            .Select(r => $"{r.GetProperty("id").GetInt64()}:{r.GetProperty("v").GetString()}")
            .OrderBy(x => x, StringComparer.Ordinal)];

    private static List<string> SparkFeed(string dir, long version) =>
        [.. Spark.Invoke("read_changes", new { path = dir, start = version, end = version })
            .GetProperty("rows").EnumerateArray()
            .Select(r => $"{r.GetProperty("_change_type").GetString()}:{r.GetProperty("id").GetInt64()}:{r.GetProperty("v").GetString()}")
            .OrderBy(x => x, StringComparer.Ordinal)];

    private static List<string> SparkRowIds(string dir) =>
        [.. Spark.Invoke("read_row_ids", new { path = dir }).GetProperty("rows").EnumerateArray()
            .Select(r => $"{r.GetProperty("id").GetInt64()}:{r.GetProperty("row_id").GetInt64()}")
            .OrderBy(x => x, StringComparer.Ordinal)];

    private static Dictionary<string, string> Metrics(string dir, long version)
    {
        string text = File.ReadAllText(Path.Combine(dir, "_delta_log", $"{version:D20}.json"));
        var commitInfo = text.Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(line => JsonDocument.Parse(line).RootElement)
            .Single(a => a.TryGetProperty("commitInfo", out _))
            .GetProperty("commitInfo");
        Assert.Equal("RESTORE", commitInfo.GetProperty("operation").GetString());
        return commitInfo.GetProperty("operationMetrics").EnumerateObject()
            .ToDictionary(p => p.Name, p => p.Value.ToString());
    }

    private static void SparkRestore(string dir, long version)
    {
        var result = Spark.InvokeRaw("sql", new
        {
            path = dir,
            sql = new[] { $"RESTORE TABLE delta.`{{path}}` TO VERSION AS OF {version}" },
        });
        Assert.True(result.GetProperty("ok").GetBoolean(), result.ToString());
    }

    public static TheoryData<bool, bool, long> Cases => new()
    {
        // Back past the OPTIMIZE, the UPDATE and the DELETE, with and without deletion vectors and row tracking.
        { false, false, 2 },
        { true, false, 2 },
        { true, false, 3 },
        { true, true, 2 },
        { false, true, 4 },
        // Back to the empty table.
        { true, false, 0 },
    };

    [SkippableTheory]
    [MemberData(nameof(Cases))]
    public async Task Restore_LeavesWhatSparksRestoreLeaves(bool deletionVectors, bool rowTracking, long version)
    {
        Spark.Require();
        await CreateTwinsAsync(deletionVectors, rowTracking);

        SparkRestore(_theirs, version);
        long restoreVersion;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
        {
            var result = await table.RestoreAsync(version);
            Assert.True(result.Committed);
            restoreVersion = result.Version;
        }
        Assert.True(File.Exists(Path.Combine(_theirs, "_delta_log", $"{restoreVersion:D20}.json")));
        Assert.False(File.Exists(Path.Combine(_theirs, "_delta_log", $"{restoreVersion + 1:D20}.json")));
        Assert.Equal(SparkRows(_theirs), SparkRows(_ours));
        Assert.Equal(SparkFeed(_theirs, restoreVersion), SparkFeed(_ours, restoreVersion));
        var ours = Metrics(_ours, restoreVersion);
        var theirs = Metrics(_theirs, restoreVersion);
        foreach (string count in new[] { "numRestoredFiles", "numRemovedFiles", "numOfFilesAfterRestore" })
            Assert.Equal(theirs[count], ours[count]);
        Assert.Equal(theirs.Keys.OrderBy(k => k, StringComparer.Ordinal), ours.Keys.OrderBy(k => k, StringComparer.Ordinal));
        if (rowTracking)
            Assert.Equal(SparkRowIds(_theirs), SparkRowIds(_ours));

        // Spark writes through the restored table.
        var append = Spark.InvokeRaw("sql", new
        {
            path = _ours,
            sql = new[] { "INSERT INTO delta.`{path}` VALUES (100L, 'v100')" },
        });
        Assert.True(append.GetProperty("ok").GetBoolean(), append.ToString());
    }

    /// <summary>
    /// The schema comes back with the files: a column added after the target version is gone in both, and both
    /// tables read the same.
    /// </summary>
    [SkippableFact]
    public async Task Restore_AcrossASchemaChange_MatchesSpark()
    {
        Spark.Require();
        var wide = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("v", StringType.Default, true))
            .Field(new Field("extra", Int32Type.Default, true))
            .Build();
        foreach (string dir in new[] { _ours, _theirs })
        {
            await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(dir), TableSchema);
            await table.WriteAsync([Rows(1, 2)]);                                         // v1
            await table.AddColumnAsync(new Field("extra", Int32Type.Default, true));      // v2
            await table.WriteAsync([new RecordBatch(wide,
                [new Int64Array.Builder().Append(3).Build(), new StringArray.Builder().Append("v3").Build(),
                 new Int32Array.Builder().Append(30).Build()], 1)]);                     // v3
        }

        SparkRestore(_theirs, 1);
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
            await table.RestoreAsync(1);

        var ours = Spark.Invoke("read", new { path = _ours });
        var theirs = Spark.Invoke("read", new { path = _theirs });
        Assert.Equal(theirs.GetProperty("columns").ToString(), ours.GetProperty("columns").ToString());
        Assert.Equal(SparkRows(_theirs), SparkRows(_ours));
    }
}
