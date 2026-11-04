// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// #598 against Spark: EW turns in-commit timestamps on for an existing table, through each public path that can,
/// and Spark's <c>DESCRIBE HISTORY</c> and <c>TIMESTAMP AS OF</c> still work, before and after a Spark append.
/// Before the fix every path failed both with <c>DELTA_MISSING_COMMIT_TIMESTAMP</c>, because the enabling commit
/// recorded no enablement point and Spark then expected a timestamp on every earlier commit.
/// </summary>
[Collection("Interop")]
public class InCommitTimestampMidLifeInteropTests : IDisposable
{
    private readonly string _dir;

    public InCommitTimestampMidLifeInteropTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"delta_ict_midlife_interop_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_dir))
        {
            try { Directory.Delete(_dir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema IdSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, nullable: true))
        .Build();

    private static RecordBatch Ids(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    private static DeltaAction[] Enabling(DeltaTable table)
    {
        var current = table.CurrentSnapshot.Metadata;
        var configuration = current.Configuration?.ToDictionary(p => p.Key, p => p.Value, StringComparer.Ordinal)
            ?? new Dictionary<string, string>(StringComparer.Ordinal);
        configuration[InCommitTimestamp.EnableKey] = "true";
        var protocol = table.CurrentSnapshot.Protocol;
        return
        [
            current with { Configuration = configuration },
            new ProtocolAction
            {
                MinReaderVersion = protocol.MinReaderVersion,
                MinWriterVersion = 7,
                ReaderFeatures = protocol.ReaderFeatures,
                WriterFeatures = [.. protocol.WriterFeatures ?? ["appendOnly", "invariants"], "inCommitTimestamp"],
            },
        ];
    }

    private long InCommitTimestampOnDisk(long version) =>
        File.ReadAllLines(Path.Combine(_dir, "_delta_log", $"{version:D20}.json"))
            .Where(line => line.Length > 0)
            .Select(line => JsonDocument.Parse(line).RootElement)
            .Single(a => a.TryGetProperty("commitInfo", out _))
            .GetProperty("commitInfo").GetProperty("inCommitTimestamp").GetInt64();

    private JsonElement Sql(params string[] statements)
    {
        var result = Spark.InvokeRaw("sql", new { path = _dir, sql = statements });
        Assert.True(result.GetProperty("ok").GetBoolean(), result.ToString());
        return result;
    }

    private List<long> SparkHistoryVersions() =>
        [.. Sql("DESCRIBE HISTORY delta.`{path}`").GetProperty("statements")[0].GetProperty("rows").EnumerateArray()
            .Select(r => r.GetProperty("version").GetInt64())
            .OrderBy(v => v)];

    private long SparkRowCountAsOf(long timestampMs)
    {
        var result = Spark.InvokeRaw("read_as_of", new { path = _dir, timestamp_ms = timestampMs });
        Assert.True(result.GetProperty("ok").GetBoolean(), result.ToString());
        return result.GetProperty("row_count").GetInt64();
    }

    [SkippableTheory]
    [InlineData("commit data files")]
    [InlineData("clustering")]
    [InlineData("transaction")]
    [InlineData("create or replace")]
    public async Task EnabledMidLifeByEw_SparksHistoryAndTimeTravelWork(string seam)
    {
        Spark.Require();
        var fs = new LocalTableFileSystem(_dir);
        await using (var table = await DeltaTable.CreateAsync(fs, IdSchema))
        {
            await table.WriteAsync([Ids(1, 2)]);                                       // v1
            await table.WriteAsync([Ids(3)]);                                          // v2
            switch (seam)                                                              // v3 enables
            {
                case "commit data files":
                    await table.CommitDataFilesAsync([], extraActions: Enabling(table), operation: "SET TBLPROPERTIES");
                    break;
                case "clustering":
                    await table.SetClusteringColumnsAsync(["id"], extraActions: Enabling(table));
                    break;
                case "transaction":
                {
                    await using var txn = table.StartTransaction();
                    txn.StageActions(Enabling(table));
                    await txn.CommitAsync();
                    break;
                }
            }
        }
        if (seam == "create or replace")
        {
            await (await DeltaTable.CreateOrReplaceAsync(
                fs, IdSchema, [Ids(1, 2, 3)],
                configuration: new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "true" }))
                .DisposeAsync();
        }
        await using (var table = await DeltaTable.OpenAsync(fs))
            await table.WriteAsync([Ids(4)]);                                          // v4, timestamped by EW

        long enabledAt = InCommitTimestampOnDisk(3);
        long v4At = InCommitTimestampOnDisk(4);

        // History, and time travel by timestamp to the enabling commit (by its in-commit timestamp), are the two
        // reads that failed with DELTA_MISSING_COMMIT_TIMESTAMP before the fix.
        Assert.Equal([0L, 1L, 2L, 3L, 4L], SparkHistoryVersions());
        Assert.Equal(3L, SparkRowCountAsOf(enabledAt));

        Sql("INSERT INTO delta.`{path}` VALUES (5)");                                  // v5, Spark's
        Assert.Equal([0L, 1L, 2L, 3L, 4L, 5L], SparkHistoryVersions());
        Assert.Equal(4L, SparkRowCountAsOf(v4At));

        // Spark's own commit continues the sequence.
        Assert.True(InCommitTimestampOnDisk(5) > v4At);
    }
}
