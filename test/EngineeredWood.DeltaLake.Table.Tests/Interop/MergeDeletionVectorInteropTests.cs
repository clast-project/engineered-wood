// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using static EngineeredWood.DeltaLake.Table.Tests.Interop.MergeSchemaInteropTests;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> A MERGE on a table with deletion vectors (#574), against Spark's: Spark creates and seeds two
/// identical tables with deletion vectors, the change data feed and row tracking, and each case MERGEs one with
/// <see cref="DeltaTable.MergeAsync"/> and the other with the equivalent <c>MERGE INTO</c>. Spark must then read the
/// same rows from both, the same change feed for the MERGE's version, and the same stable row ids and commit
/// versions; and both must have masked the same number of rows in each file they kept. See <see cref="Spark"/> for
/// setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class MergeDeletionVectorInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public MergeDeletionVectorInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_merge_dv_interop_{Guid.NewGuid():N}");
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

    private const string Properties =
        "TBLPROPERTIES ('delta.enableDeletionVectors'='true', 'delta.enableChangeDataFeed'='true', "
        + "'delta.enableRowTracking'='true')";

    private const string UpsertSql =
        "WHEN MATCHED AND s.v = 'DEL' THEN DELETE WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *";

    private static readonly MergeOptions Upsert = new()
    {
        Keys = ["id"],
        WhenMatched = [MergeAction.Delete.When(SparkSqlParser.ParsePredicate("source.v = 'DEL'")), MergeAction.Update],
        WhenNotMatched = [MergeAction.Insert],
    };

    /// <summary>A case: the seeding statements, the source, and the clauses.</summary>
    private sealed record Case(
        string[] Seed, (int Id, string V, string? P)[] Source, string ClausesSql, MergeOptions Options,
        bool Partitioned = false);

    private const string Five = "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')";

    private static Case CaseNamed(string name) => name switch
    {
        // Update one row, delete another, insert a third: one file keeps three rows.
        "upsert" => new([$"INSERT INTO delta.`{{path}}` VALUES {Five}"],
            [(2, "B", null), (3, "DEL", null), (9, "z", null)], UpsertSql, Upsert),
        // Every row of the first file changes, so it is removed whole; the second is not touched.
        "whole file" => new(
            ["INSERT INTO delta.`{path}` VALUES (2, 'b'), (3, 'c')", "INSERT INTO delta.`{path}` VALUES (1, 'a'), (4, 'd')"],
            [(2, "B", null), (3, "DEL", null), (9, "z", null)], UpsertSql, Upsert),
        // The file already has a deletion vector, which the MERGE's rows join.
        "existing vector" => new(
            [$"INSERT INTO delta.`{{path}}` VALUES {Five}", "DELETE FROM delta.`{path}` WHERE id = 5"],
            [(2, "B", null), (3, "DEL", null), (9, "z", null)], UpsertSql, Upsert),
        "not matched by source" => new([$"INSERT INTO delta.`{{path}}` VALUES {Five}"],
            [(2, "B", null)],
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED BY SOURCE AND t.id > 4 THEN DELETE "
            + "WHEN NOT MATCHED BY SOURCE AND t.id = 1 THEN UPDATE SET v = 'nm'",
            new MergeOptions
            {
                Keys = ["id"],
                WhenMatched = [MergeAction.Update],
                WhenNotMatchedBySource =
                [
                    MergeAction.Delete.When(SparkSqlParser.ParsePredicate("target.id > 4")),
                    MergeAction.UpdateSet(("v", SparkSqlParser.ParseExpression("'nm'")))
                        .When(SparkSqlParser.ParsePredicate("target.id = 1")),
                ],
            }),
        // The updated row moves to another partition; the rest of its file stays.
        "partition move" => new(["INSERT INTO delta.`{path}` VALUES (1, 'a', 'x'), (2, 'b', 'x'), (3, 'c', 'x')"],
            [(2, "B", "y")], "WHEN MATCHED THEN UPDATE SET *",
            new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update] }, Partitioned: true),
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    public static TheoryData<string> Cases => new()
    {
        "upsert", "whole file", "existing vector", "not matched by source", "partition move",
    };

    [SkippableTheory]
    [MemberData(nameof(Cases))]
    public async Task AMergeOnADeletionVectorTable_LeavesWhatSparksLeaves(string name)
    {
        Spark.Require();
        var c = CaseNamed(name);
        string ddl = c.Partitioned
            ? $"CREATE TABLE delta.`{{path}}` (id INT, v STRING, p STRING) USING delta PARTITIONED BY (p) {Properties}"
            : $"CREATE TABLE delta.`{{path}}` (id INT, v STRING) USING delta {Properties}";
        Sql(_ours, [ddl, .. c.Seed]);
        Sql(_theirs, [ddl, .. c.Seed]);

        string sourceSql = "SELECT * FROM VALUES " + string.Join(", ", c.Source.Select(r => c.Partitioned
            ? $"({r.Id}, '{r.V}', '{r.P}')"
            : $"({r.Id}, '{r.V}')")) + (c.Partitioned ? " AS s(id, v, p)" : " AS s(id, v)");
        Sql(_theirs, $"MERGE INTO delta.`{{path}}` t USING ({sourceSql}) s ON t.id = s.id {c.ClausesSql}");

        long version;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
        {
            (_, version) = await table.MergeAsync([SourceBatch(c)], c.Options);
        }

        Assert.Equal(SparkRows(_theirs), SparkRows(_ours));
        Assert.Equal(Changes(_theirs, version), Changes(_ours, version));
        Assert.Equal(RowIds(_theirs), RowIds(_ours));
        Assert.Equal(await MaskedAsync(_theirs), await MaskedAsync(_ours));
    }

    private static RecordBatch SourceBatch(Case c)
    {
        var ids = new Int32Array.Builder().AppendRange(c.Source.Select(r => r.Id)).Build();
        var values = new StringArray.Builder().AppendRange(c.Source.Select(r => r.V)).Build();
        var fields = new List<Field> { new("id", Int32Type.Default, true), new("v", StringType.Default, true) };
        var columns = new List<IArrowArray> { ids, values };
        if (c.Partitioned)
        {
            fields.Add(new Field("p", StringType.Default, true));
            columns.Add(new StringArray.Builder().AppendRange(c.Source.Select(r => r.P!)).Build());
        }
        return new RecordBatch(new Apache.Arrow.Schema(fields, null), columns, c.Source.Length);
    }

    /// <summary>The change feed of <paramref name="version"/> as Spark reads it, without the commit timestamps.</summary>
    private static List<string> Changes(string dir, long version) =>
        [.. Spark.Invoke("read_changes", new { path = dir, start = version, end = version }).GetProperty("rows")
            .EnumerateArray()
            .Select(r => string.Join(",", r.EnumerateObject()
                .Where(p => p.Name != "_commit_timestamp")
                .Select(p => $"{p.Name}={p.Value.GetRawText()}")))
            .OrderBy(x => x, StringComparer.Ordinal)];

    /// <summary>Each row's id with its stable row id and commit version, as Spark resolves them.</summary>
    private static List<string> RowIds(string dir) =>
        [.. Spark.Invoke("read_row_ids", new { path = dir }).GetProperty("rows").EnumerateArray()
            .Select(r => r.GetRawText())
            .OrderBy(x => x, StringComparer.Ordinal)];

    /// <summary>The rows each live file's deletion vector masks, for the files that have one.</summary>
    private static async Task<List<long>> MaskedAsync(string dir)
    {
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(dir));
        return [.. table.CurrentSnapshot.ActiveFiles.Values
            .Where(f => f.DeletionVector is not null)
            .Select(f => f.DeletionVector!.Cardinality)
            .OrderBy(x => x)];
    }
}
