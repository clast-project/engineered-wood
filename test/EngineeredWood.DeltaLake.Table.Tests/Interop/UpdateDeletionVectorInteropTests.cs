// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using static EngineeredWood.DeltaLake.Table.Tests.Interop.MergeSchemaInteropTests;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> An UPDATE on a table with deletion vectors (#704), against Spark's: Spark creates and seeds two
/// identical tables with deletion vectors, the change data feed and row tracking, and each case UPDATEs one with
/// <see cref="DeltaTable.UpdateAsync(Expressions.Predicate, Func{RecordBatch, RecordBatch}, CancellationToken, CommitProperties?, RowDeleteMode?)"/>
/// and the other with the equivalent <c>UPDATE</c>. Spark must then read the same rows from both, the same change
/// feed for the UPDATE's version, and the same stable row ids and commit versions; and both must have masked the
/// same number of rows in each file they kept. See <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class UpdateDeletionVectorInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public UpdateDeletionVectorInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_update_dv_interop_{Guid.NewGuid():N}");
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

    private const string Five = "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')";

    /// <summary>A case: the seeding statements, the WHERE condition, the column set and its new value.</summary>
    private sealed record Case(string[] Seed, string Where, string Column, string Value, bool Partitioned = false);

    private static Case CaseNamed(string name) => name switch
    {
        // Two of the file's five rows change: it keeps three.
        "part of a file" => new([$"INSERT INTO delta.`{{path}}` VALUES {Five}"], "id IN (2, 3)", "v", "X"),
        // Every row of the first file changes, so it is removed whole; the second is not touched.
        "whole file" => new(
            ["INSERT INTO delta.`{path}` VALUES (2, 'b'), (3, 'c')", "INSERT INTO delta.`{path}` VALUES (1, 'a'), (4, 'd')"],
            "id IN (2, 3)", "v", "X"),
        // The file already has a deletion vector, which the updated rows join.
        "existing vector" => new(
            [$"INSERT INTO delta.`{{path}}` VALUES {Five}", "DELETE FROM delta.`{path}` WHERE id = 5"],
            "id IN (2, 3)", "v", "X"),
        // The updated row moves to another partition; the rest of its file stays.
        "partition move" => new(["INSERT INTO delta.`{path}` VALUES (1, 'a', 'x'), (2, 'b', 'x'), (3, 'c', 'x')"],
            "id = 2", "p", "y", Partitioned: true),
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    public static TheoryData<string> Cases => new() { "part of a file", "whole file", "existing vector", "partition move" };

    [SkippableTheory]
    [MemberData(nameof(Cases))]
    public async Task AnUpdateOnADeletionVectorTable_LeavesWhatSparksLeaves(string name)
    {
        Spark.Require();
        var c = CaseNamed(name);
        string ddl = c.Partitioned
            ? $"CREATE TABLE delta.`{{path}}` (id INT, v STRING, p STRING) USING delta PARTITIONED BY (p) {Properties}"
            : $"CREATE TABLE delta.`{{path}}` (id INT, v STRING) USING delta {Properties}";
        Sql(_ours, [ddl, .. c.Seed]);
        Sql(_theirs, [ddl, .. c.Seed]);

        Sql(_theirs, $"UPDATE delta.`{{path}}` SET {c.Column} = '{c.Value}' WHERE {c.Where}");

        long version;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
        {
            (_, version) = await table.UpdateAsync(SparkSqlParser.ParsePredicate(c.Where), batch =>
            {
                int index = batch.Schema.GetFieldIndex(c.Column);
                var values = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    values.Append(c.Value);
                var columns = Enumerable.Range(0, batch.ColumnCount)
                    .Select(i => i == index ? values.Build() : batch.Column(i)).ToList();
                return new RecordBatch(batch.Schema, columns, batch.Length);
            });
        }

        Assert.Equal(SparkRows(_theirs), SparkRows(_ours));
        Assert.Equal(Changes(_theirs, version), Changes(_ours, version));
        Assert.Equal(RowIds(_theirs), RowIds(_ours));
        Assert.Equal(await MaskedAsync(_theirs), await MaskedAsync(_ours));
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
