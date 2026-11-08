// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.Vortex.Writer;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Zone pruning resolves a predicate's column by its parts (#771). Vortex keeps zone stats for top-level
/// columns only, and found them by the reference's dotted text, so struct field <c>s.x</c> was judged by a
/// top-level column named <c>s.x</c>. Here that column holds 0..99 and the struct's field 100..199.
/// </summary>
public class DottedNamePruningTests
{
    [Theory]
    [InlineData("s.x > 150", 100)]   // struct field: every row matches
    [InlineData("`s.x` > 150", 0)]   // top-level column: none matches (pruning it is fine)
    public async Task EachSpellingIsJudgedByItsOwnColumn(string sql, int minRows)
    {
        var structType = new StructType([new Field("x", Int32Type.Default, false)]);
        var schema = new Apache.Arrow.Schema(
            [new Field("s.x", Int32Type.Default, false), new Field("s", structType, false)], null);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                var top = new Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build();
                var leaf = new Int32Array.Builder().AppendRange(Enumerable.Range(100, 100)).Build();
                w.WriteBatch(new RecordBatch(schema,
                    [top, new StructArray(structType, 100, [leaf], ArrowBuffer.Empty, 0)], 100));
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            int rows = 0;
            await foreach (var batch in reader.ReadAllAsync(SparkSqlParser.ParsePredicate(sql)))
                rows += batch.Length;
            Assert.True(rows >= minRows, $"`{sql}` read {rows} rows, expected at least {minRows}");
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }
}
