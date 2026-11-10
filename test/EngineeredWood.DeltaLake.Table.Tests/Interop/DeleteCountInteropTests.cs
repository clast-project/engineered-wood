// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <b>Tier 3</b>: the same DELETE run by Spark and by EW on identical tables deletes the same rows (#502 phase 3c's
/// gate, doc/row-level-filtering.md §8 and §18). The tables hold a value only a deletion-vector-deleted row has,
/// which the predicate cannot evaluate: Spark filters deletion-vector rows before evaluating, and since 3c so does
/// EW's DELETE (a selected read with the vector as its excluded rows). Before 3c, EW raised
/// <c>CAST_INVALID_INPUT</c> where Spark deleted.
/// </summary>
[Collection("Interop")]
public class DeleteCountInteropTests : IDisposable
{
    private readonly string _tempDir;

    public DeleteCountInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_spark_delete_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("s", StringType.Default, true))
        .Field(new Field("p", StringType.Default, false))
        .Build();

    /// <summary>
    /// <c>s</c> holds integers as text, NULL, and 'abc' / 'x9', which no integer cast reads. The rows holding those
    /// two are deleted by a deletion vector before the DELETE under test, so no live row raises.
    /// </summary>
    [SkippableTheory]
    [InlineData("s = 5")]
    [InlineData("CAST(s AS INT) > 4")]
    [InlineData("s = 5 OR p = 'b'")]
    [InlineData("s IS NULL OR CAST(s AS INT) < 3")]
    public async Task EwDeletesTheRowsSparkDeletes(string condition)
    {
        Spark.Require();

        string sparkPath = Path.Combine(_tempDir, "spark");
        string ewPath = Path.Combine(_tempDir, "ew");
        foreach (string path in new[] { sparkPath, ewPath })
            await CreateAsync(path);

        var spark = Spark.Invoke("sql", new
        {
            path = sparkPath,
            sql = new[] { "DELETE FROM delta.`{path}` WHERE " + condition },
        });
        long sparkDeleted = DeletedRows(spark);

        long ewDeleted;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(ewPath)))
            (ewDeleted, _) = await table.DeleteAsync(SparkSqlParser.ParsePredicate(condition));

        Assert.True(sparkDeleted > 0, $"Spark deleted nothing for {condition}");
        Assert.Equal(sparkDeleted, ewDeleted);

        // The same rows are left, as Spark reads both tables.
        Assert.Equal(Ids(spark), Ids(Spark.Invoke("read", new { path = ewPath })));
    }

    /// <summary>The premise: with the 'abc' row live, the cast raises in Spark too.</summary>
    [SkippableFact]
    public async Task SparkRaisesWhenALiveRowHoldsTheValue()
    {
        Spark.Require();
        string path = Path.Combine(_tempDir, "live");
        await CreateAsync(path, deleteUnreadable: false);
        Assert.ThrowsAny<Exception>(() => Spark.Invoke("sql", new
        {
            path,
            sql = new[] { "DELETE FROM delta.`{path}` WHERE CAST(s AS INT) > 4" },
        }));
    }

    private static async Task CreateAsync(string path, bool deleteUnreadable = true)
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(path), TableSchema, partitionColumns: ["p"], enableDeletionVectors: true);
        string?[] values = ["5", "abc", "7", null, "5", "x9", "2", "9", "5", "1"];
        for (int f = 0; f < 2; f++)
        {
            var id = new Int64Array.Builder();
            var s = new StringArray.Builder();
            var p = new StringArray.Builder();
            for (int i = 0; i < values.Length; i++)
            {
                id.Append(f * 100 + i);
                if (values[i] is { } v)
                    s.Append(v);
                else
                    s.AppendNull();
                p.Append(f == 0 ? "a" : "b");
            }

            await table.WriteAsync([new RecordBatch(TableSchema, [id.Build(), s.Build(), p.Build()], values.Length)]);
        }

        if (deleteUnreadable)
        {
            // A deletion vector on each file, over the rows no integer cast reads (ids 1, 5, 101, 105).
            var (deleted, _) = await table.DeleteAsync(SparkSqlParser.ParsePredicate("s IN ('abc', 'x9')"));
            Assert.Equal(4, deleted);
        }
    }

    // Spark's DELETE returns one row: num_affected_rows.
    private static long DeletedRows(JsonElement result)
    {
        var row = result.GetProperty("statements")[0].GetProperty("rows")[0];
        foreach (var property in row.EnumerateObject())
        {
            if (property.Name == "num_affected_rows")
                return property.Value.GetInt64();
        }

        throw new InvalidOperationException($"No num_affected_rows in {row}");
    }

    private static List<long> Ids(JsonElement result)
    {
        var ids = new List<long>();
        foreach (var row in result.GetProperty("rows").EnumerateArray())
            ids.Add(row.GetProperty("id").GetInt64());
        ids.Sort();
        return ids;
    }
}
