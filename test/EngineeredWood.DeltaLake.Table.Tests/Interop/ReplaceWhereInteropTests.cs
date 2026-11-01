// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> engineered-wood's replaceWhere (#566) against Spark's own: each case builds two identical
/// tables, replaces a window in one with <see cref="DeltaTable.OverwriteWhereAsync"/> and in the other with the
/// equivalent <c>INSERT INTO … REPLACE WHERE</c> run by Spark, and requires the same outcome: the same rows and the
/// same change feed, or <c>DELTA_REPLACE_WHERE_MISMATCH</c> from both. Spark reads both tables, so this is also the
/// proof that what our replaceWhere commits is a table Spark reads. See <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class ReplaceWhereInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public ReplaceWhereInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_replacewhere_interop_{Guid.NewGuid():N}");
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
        .Field(new Field("part", StringType.Default, true))
        .Field(new Field("v", Int32Type.Default, true))
        .Build();

    private static RecordBatch Rows(params (long Id, string? Part, int? V)[] rows)
    {
        var ids = new Int64Array.Builder();
        var parts = new StringArray.Builder();
        var vs = new Int32Array.Builder();
        foreach (var (id, part, v) in rows)
        {
            ids.Append(id);
            if (part is null) parts.AppendNull(); else parts.Append(part);
            if (v is int value) vs.Append(value); else vs.AppendNull();
        }
        return new RecordBatch(TableSchema, [ids.Build(), parts.Build(), vs.Build()], rows.Length);
    }

    private static string SqlRows(params (long Id, string? Part, int? V)[] rows) =>
        "SELECT * FROM VALUES "
        + string.Join(", ", rows.Select(r =>
            $"({r.Id}L, {(r.Part is null ? "CAST(NULL AS STRING)" : $"'{r.Part}'")}, "
            + $"{(r.V is int v ? v.ToString(System.Globalization.CultureInfo.InvariantCulture) : "CAST(NULL AS INT)")})"))
        + " AS s(id, part, v)";

    /// <summary>Two identical change-feed tables, written by engineered-wood in three commits (one file each, or
    /// one per partition): v 1, 2 / v 3, 4 / v 5, 15 and a NULL.</summary>
    private async Task CreateTwinsAsync(bool partitioned, bool deletionVectors)
    {
        foreach (string dir in new[] { _ours, _theirs })
        {
            await using var table = await DeltaTable.CreateAsync(
                new LocalTableFileSystem(dir), TableSchema, partitionColumns: partitioned ? ["part"] : null,
                enableDeletionVectors: deletionVectors,
                configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
            await table.WriteAsync([Rows((1, "a", 1), (2, "a", 2))]);
            await table.WriteAsync([Rows((3, "b", 3), (4, "b", 4))]);
            await table.WriteAsync([Rows((5, "c", 5), (6, "c", 15), (7, "c", null))]);
        }
    }

    private static List<(long? Id, string? Part, long? V)> SparkRows(string dir) =>
        [.. Spark.Invoke("read", new { path = dir }).GetProperty("rows").EnumerateArray()
            .Select(r => (Long(r, "id"), r.GetProperty("part").GetString(), Long(r, "v")))
            .OrderBy(r => r.Item1)];

    private static List<(string Type, long? Id)> SparkFeed(string dir, long version) =>
        [.. Spark.Invoke("read_changes", new { path = dir, start = version, end = version })
            .GetProperty("rows").EnumerateArray()
            .Select(r => (r.GetProperty("_change_type").GetString()!, Long(r, "id")))
            .OrderBy(r => r.Item1, StringComparer.Ordinal).ThenBy(r => r.Item2)];

    private static long? Long(JsonElement row, string name) =>
        row.GetProperty(name).ValueKind == JsonValueKind.Null ? null : row.GetProperty(name).GetInt64();

    private static JsonElement SparkReplaceWhere(string dir, string where, params (long, string?, int?)[] rows) =>
        Spark.InvokeRaw("sql", new
        {
            path = dir,
            sql = new[] { $"INSERT INTO delta.`{{path}}` REPLACE WHERE {where} {SqlRows(rows)}" },
        });

    private async Task<long> OursAsync(Expressions.Predicate predicate, params (long, string?, int?)[] rows)
    {
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours));
        return await table.OverwriteWhereAsync(rows.Length == 0 ? [] : [Rows(rows)], predicate);
    }

    public static TheoryData<string, bool, bool> Windows => new()
    {
        // A data window taking one file whole and cutting another, a NULL row in it kept.
        { "data", false, false },
        { "data", false, true },
        { "data", true, false },
        // A window over the partition column alone.
        { "partition", true, false },
        // Both: a window inside one partition.
        { "mixed", true, false },
        { "mixed", true, true },
    };

    private static (string Sql, Expressions.Predicate Predicate, (long, string?, int?)[] Rows) Window(string kind) =>
        kind switch
        {
            "data" => ("v >= 3 AND v < 10",
                Ex.And(Ex.GreaterThanOrEqual("v", 3), Ex.LessThan("v", 10)),
                [(20, "x", 7), (21, "c", 3)]),
            "partition" => ("part = 'b'", Ex.Equal("part", "b"), [(20, "b", 10)]),
            "mixed" => ("part = 'c' AND v > 10",
                Ex.And(Ex.Equal("part", "c"), Ex.GreaterThan("v", 10)),
                [(20, "c", 20)]),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };

    [SkippableTheory]
    [MemberData(nameof(Windows))]
    public async Task AWindow_LeavesWhatSparksReplaceWhereLeaves(string kind, bool partitioned, bool deletionVectors)
    {
        Spark.Require();
        await CreateTwinsAsync(partitioned, deletionVectors);
        var (sql, predicate, rows) = Window(kind);

        var spark = SparkReplaceWhere(_theirs, sql, rows);
        Assert.True(spark.GetProperty("ok").GetBoolean(), spark.ToString());
        long version = await OursAsync(predicate, rows);

        Assert.Equal(SparkRows(_theirs), SparkRows(_ours));
        Assert.Equal(SparkFeed(_theirs, version), SparkFeed(_ours, version));
    }

    public static TheoryData<string, bool> Mismatches => new()
    {
        { "data-outside", false },
        { "data-null", false },
        { "partition-outside", true },
        { "partition-null", true },
        { "mixed-data-half", true },
    };

    [SkippableTheory]
    [MemberData(nameof(Mismatches))]
    public async Task ARowOutsideTheWindow_IsRefusedByBoth(string kind, bool partitioned)
    {
        Spark.Require();
        await CreateTwinsAsync(partitioned, deletionVectors: false);
        var (sql, predicate, rows) = kind switch
        {
            "data-outside" => (Window("data").Sql, Window("data").Predicate, new (long, string?, int?)[] { (20, "x", 50) }),
            "data-null" => (Window("data").Sql, Window("data").Predicate, [(20, "x", null)]),
            "partition-outside" => (Window("partition").Sql, Window("partition").Predicate, [(20, "b", 1), (21, "c", 1)]),
            "partition-null" => (Window("partition").Sql, Window("partition").Predicate, [(20, null, 1)]),
            "mixed-data-half" => (Window("mixed").Sql, Window("mixed").Predicate, [(20, "c", 1)]),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };

        var spark = SparkReplaceWhere(_theirs, sql, rows);
        Assert.False(spark.GetProperty("ok").GetBoolean());
        Assert.Contains(DeltaTableErrorCodes.ReplaceWhereMismatch, spark.GetProperty("error").GetString());

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(() => OursAsync(predicate, rows));
        Assert.Equal(DeltaTableErrorCodes.ReplaceWhereMismatch, ex.ErrorCode);
    }
}
