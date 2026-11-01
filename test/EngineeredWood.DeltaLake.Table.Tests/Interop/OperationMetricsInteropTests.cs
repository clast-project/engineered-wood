// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <c>commitInfo.operationMetrics</c> of a DELETE, UPDATE and MERGE against Spark's (#577). Twin tables get the
/// same operation, one from this library and one from Spark SQL, and every metric this library records must be
/// one Spark records for that operation. Row counts, removed files and deletion vectors must have the same
/// value. Added files, change files and bytes are compared only as zero or not: they describe the files each
/// writer chose to write, and this library writes one per rewritten file where Spark writes one per task.
/// <see cref="OperationMetricsTests"/> pins those against this library's own commits.
/// </summary>
[Collection("Interop")]
public class OperationMetricsInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public OperationMetricsInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_opmetrics_interop_{Guid.NewGuid():N}");
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
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Rows(params (long Id, string Value)[] rows)
    {
        var id = new Int64Array.Builder();
        var value = new StringArray.Builder();
        foreach (var (i, v) in rows)
        {
            id.Append(i);
            value.Append(v);
        }
        return new RecordBatch(TableSchema, [id.Build(), value.Build()], rows.Length);
    }

    /// <summary>Two identical tables of two files each: (1, 2, 3) and (4, 5).</summary>
    private async Task CreateTwinsAsync(bool deletionVectors, bool changeDataFeed = false)
    {
        var configuration = new Dictionary<string, string>();
        if (changeDataFeed)
            configuration["delta.enableChangeDataFeed"] = "true";
        foreach (string dir in new[] { _ours, _theirs })
        {
            await using var table = await DeltaTable.CreateAsync(
                new LocalTableFileSystem(dir), TableSchema, enableDeletionVectors: deletionVectors,
                configuration: configuration);
            await table.WriteAsync([Rows((1, "a"), (2, "b"), (3, "c"))]);
            await table.WriteAsync([Rows((4, "d"), (5, "e"))]);
        }
    }

    private async Task<IReadOnlyDictionary<string, string>> OurMetricsAsync(long version)
    {
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours));
        await foreach (var entry in table.GetHistoryAsync())
        {
            if (entry.Version == version)
                return entry.OperationMetrics ?? throw new InvalidOperationException($"v{version} has no metrics.");
        }
        throw new InvalidOperationException($"No version {version}.");
    }

    /// <summary>Runs Spark's statement on its twin and returns that commit's operationMetrics.</summary>
    private Dictionary<string, string> TheirMetrics(string sql, long version)
    {
        var result = Spark.Invoke("sql", new
        {
            path = _theirs,
            sql = new[] { sql, "DESCRIBE HISTORY delta.`{path}`" },
        });
        foreach (var row in result.GetProperty("statements")[1].GetProperty("rows").EnumerateArray())
        {
            if (row.GetProperty("version").GetInt64() != version)
                continue;
            return row.GetProperty("operationMetrics").EnumerateObject()
                .ToDictionary(p => p.Name, p => p.Value.ValueKind == JsonValueKind.String
                    ? p.Value.GetString()!
                    : p.Value.GetRawText());
        }
        throw new InvalidOperationException($"Spark's history has no version {version}.");
    }

    private static void AssertMatchesSpark(
        IReadOnlyDictionary<string, string> ours, IReadOnlyDictionary<string, string> theirs)
    {
        Assert.NotEmpty(ours);
        foreach (var (key, value) in ours)
        {
            Assert.True(theirs.TryGetValue(key, out string? sparks),
                $"Spark records no '{key}'. Spark's metrics: {string.Join(", ", theirs.Keys)}");
            if (DependsOnFileLayout(key))
            {
                Assert.True(value == "0" == (sparks == "0"), $"{key}: ours {value}, Spark's {sparks}");
            }
            else
            {
                Assert.True(value == sparks, $"{key}: ours {value}, Spark's {sparks}");
            }
        }
    }

    private static bool DependsOnFileLayout(string key) =>
        key.Contains("Bytes")
        || key.Contains("ChangeFiles")
        || key is "numAddedFiles" or "numTargetFilesAdded";

    [SkippableTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ADelete_RecordsSparksMetrics(bool deletionVectors)
    {
        Spark.Require();
        await CreateTwinsAsync(deletionVectors, changeDataFeed: !deletionVectors);

        long version;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
            (_, version) = await table.DeleteAsync(Ex.In("id", LiteralValue.Of(2L), LiteralValue.Of(4L), LiteralValue.Of(5L)));

        AssertMatchesSpark(
            await OurMetricsAsync(version),
            TheirMetrics("DELETE FROM delta.`{path}` WHERE id IN (2, 4, 5)", version));
    }

    [SkippableFact]
    public async Task AnUpdate_RecordsSparksMetrics()
    {
        Spark.Require();
        await CreateTwinsAsync(deletionVectors: false, changeDataFeed: true);

        long version;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
        {
            (_, version) = await table.UpdateAsync(Ex.Equal("id", 3L), batch =>
            {
                var values = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    values.Append("x");
                return new RecordBatch(batch.Schema, [batch.Column(0), values.Build()], batch.Length);
            });
        }

        AssertMatchesSpark(
            await OurMetricsAsync(version),
            TheirMetrics("UPDATE delta.`{path}` SET value = 'x' WHERE id = 3", version));
    }

    [SkippableFact]
    public async Task AMerge_RecordsSparksMetrics()
    {
        Spark.Require();
        await CreateTwinsAsync(deletionVectors: false, changeDataFeed: true);

        long version;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
        {
            (_, version) = await table.MergeAsync([Rows((2, "B"), (6, "f"))], new MergeOptions
            {
                Keys = ["id"],
                WhenMatched = MergeAction.Update,
                WhenNotMatched = MergeAction.Insert,
                WhenNotMatchedBySource = MergeAction.Delete,
                WhenNotMatchedBySourceCondition = Ex.Equal(
                    UnboundReference.FromParts(["target", "id"]), Ex.Lit(LiteralValue.Of(5L))),
            });
        }

        AssertMatchesSpark(
            await OurMetricsAsync(version),
            TheirMetrics(
                "MERGE INTO delta.`{path}` t USING (SELECT * FROM VALUES (2L, 'B'), (6L, 'f') AS s(id, value)) s "
                + "ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * "
                + "WHEN NOT MATCHED BY SOURCE AND t.id = 5 THEN DELETE",
                version));
    }
}
