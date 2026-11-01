// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <c>commitInfo.userMetadata</c> against Spark (#564), in both directions: Spark's <c>DESCRIBE HISTORY</c>
/// shows what <see cref="CommitProperties.UserMetadata"/> wrote, and <see cref="DeltaTable.GetHistoryAsync"/>
/// reads what Spark's <c>spark.databricks.delta.commitInfo.userMetadata</c> wrote.
/// </summary>
[Collection("Interop")]
public class CommitPropertiesInteropTests : IDisposable
{
    private readonly string _dir;

    public CommitPropertiesInteropTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"delta_commitprops_interop_{Guid.NewGuid():N}");
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

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Rows(params long[] ids)
    {
        var b = new Int64Array.Builder();
        foreach (long id in ids)
            b.Append(id);
        return new RecordBatch(TableSchema, [b.Build()], ids.Length);
    }

    /// <summary>
    /// Spark's history rows by version, from <c>DESCRIBE HISTORY</c>, which reads the Delta log's commitInfo
    /// through Spark's own typed <c>CommitInfo</c>, so an extra key that broke it would fail here too.
    /// </summary>
    private Dictionary<long, JsonElement> SparkHistory()
    {
        var result = Spark.Invoke("sql", new { path = _dir, sql = new[] { "DESCRIBE HISTORY delta.`{path}`" } });
        var rows = result.GetProperty("statements")[0].GetProperty("rows");
        return rows.EnumerateArray().ToDictionary(r => r.GetProperty("version").GetInt64(), r => r.Clone());
    }

    [SkippableFact]
    public async Task SparksHistory_ShowsTheUserMetadataEwWrote()
    {
        Spark.Require();
        var fs = new LocalTableFileSystem(_dir);
        long written;
        long deleted;
        await using (var table = await DeltaTable.CreateAsync(fs, TableSchema))
        {
            written = await table.WriteAsync([Rows(1, 2, 3)], commitProperties: new CommitProperties
            {
                UserMetadata = "job-17 \"nightly\"",
                Metadata = new Dictionary<string, JsonElement>
                {
                    ["watermark"] = JsonDocument.Parse("""{"offset":42}""").RootElement.Clone(),
                },
            });
            (_, deleted) = await table.DeleteAsync(
                Expressions.Expressions.Equal("id", 2L), new CommitProperties { UserMetadata = "cleanup" });
        }

        var history = SparkHistory();
        Assert.Equal("job-17 \"nightly\"", history[written].GetProperty("userMetadata").GetString());
        Assert.Equal("cleanup", history[deleted].GetProperty("userMetadata").GetString());
        Assert.Equal(JsonValueKind.Null, history[0].GetProperty("userMetadata").ValueKind);
    }

    [SkippableFact]
    public async Task EwsHistory_ReadsTheUserMetadataSparkWrote()
    {
        Spark.Require();
        var fs = new LocalTableFileSystem(_dir);
        await using (var table = await DeltaTable.CreateAsync(fs, TableSchema))
            await table.WriteAsync([Rows(1)]);

        Spark.Invoke("sql", new
        {
            path = _dir,
            sql = new[]
            {
                "SET spark.databricks.delta.commitInfo.userMetadata=from-spark",
                "INSERT INTO delta.`{path}` VALUES (2)",
                "RESET spark.databricks.delta.commitInfo.userMetadata",
            },
        });

        await using var reopened = await DeltaTable.OpenAsync(fs);
        var entries = new List<DeltaTable.DeltaHistoryEntry>();
        await foreach (var entry in reopened.GetHistoryAsync())
            entries.Add(entry);

        Assert.Equal(3, entries.Count);
        Assert.Null(entries[1].UserMetadata);
        Assert.Equal("from-spark", entries[2].UserMetadata);
    }
}
