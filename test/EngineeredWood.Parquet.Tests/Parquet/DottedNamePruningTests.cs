// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Every pruning source resolves a predicate's column as the row evaluator does, by its parts (#771): SQL
/// <c>s.x</c> is field <c>x</c> of struct <c>s</c>, and <c>`s.x`</c> a top-level column whose name holds a dot.
/// Their dotted text is the same, and statistics, the page index, dictionaries and Bloom filters all looked the
/// text up preferring the top-level column, so a row group holding rows of the struct field that match was
/// skipped. A spelling naming a column the file lacks must not prune either.
/// </summary>
public class DottedNamePruningTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), $"ew_771_{Guid.NewGuid():N}");

    public DottedNamePruningTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // Top-level `s.x` holds 0..9 and struct s's field x 100..109, each value repeated (1000 rows) so the writer
    // builds dictionaries; interleaved, `s.x` holds evens 0..18 and s.x odds 1..19, which only a membership
    // source tells apart. Shape: "both", "topOnly", "structOnly".
    private async Task<string> WriteAsync(string shape, bool statistics, bool interleaved = false)
    {
        var fields = new List<Field>();
        var columns = new List<IArrowArray>();
        if (shape != "structOnly")
        {
            fields.Add(new Field("s.x", Int32Type.Default, false));
            columns.Add(new Int32Array.Builder().AppendRange(Enumerable.Range(0, 1000).Select(i => interleaved ? 2 * (i % 10) : i % 10)).Build());
        }
        if (shape != "topOnly")
        {
            var type = new StructType([new Field("x", Int32Type.Default, false)]);
            var leaf = new Int32Array.Builder().AppendRange(Enumerable.Range(0, 1000).Select(i => interleaved ? 2 * (i % 10) + 1 : 100 + i % 10)).Build();
            fields.Add(new Field("s", type, false));
            columns.Add(new StructArray(type, 1000, [leaf], ArrowBuffer.Empty, 0));
        }

        string path = Path.Combine(_tempDir, $"{shape}-{statistics}-{interleaved}.parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
        {
            WriteStatistics = statistics,
            WritePageIndex = statistics,
            BloomFilterColumns = new HashSet<string> { "s.x" },
        });
        await writer.WriteRowGroupAsync(new RecordBatch(new Apache.Arrow.Schema(fields, null), columns, 1000));
        await writer.CloseAsync();
        return path;
    }

    public static TheoryData<string, string, string, bool> Cases()
    {
        var data = new TheoryData<string, string, string, bool>();
        foreach (var mode in new[] { "stats", "pages", "dictionary", "bloom" })
        {
            // Both columns in the file: each spelling must be judged by its own column.
            data.Add(mode, "both", "s.x = 105", true);
            data.Add(mode, "both", "s.x = 5", false);
            data.Add(mode, "both", "`s.x` = 5", true);
            data.Add(mode, "both", "`s.x` = 105", false);
            // Only one of them: the other spelling names a column the file lacks, so it must not prune.
            data.Add(mode, "topOnly", "s.x = 105", true);
            data.Add(mode, "structOnly", "`s.x` = 5", true);
        }
        foreach (var mode in new[] { "dictionary", "bloom" })
        {
            // Interleaved: top-level `s.x` holds evens 0..18, s.x odds 1..19, so only membership can tell.
            data.Add(mode, "both-interleaved", "s.x = 5", true);
            data.Add(mode, "both-interleaved", "s.x = 4", false);
            data.Add(mode, "both-interleaved", "`s.x` = 4", true);
            data.Add(mode, "both-interleaved", "`s.x` = 5", false);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task EachSpellingIsJudgedByItsOwnColumn(string mode, string shape, string sql, bool expectKeep)
    {
        bool interleaved = shape.EndsWith("-interleaved", StringComparison.Ordinal);
        string path = await WriteAsync(interleaved ? "both" : shape, statistics: interleaved || mode is "stats" or "pages", interleaved);
        var options = new ParquetReadOptions
        {
            FilterUseDictionaries = mode == "dictionary",
            FilterUseBloomFilters = mode == "bloom",
            FilterUsePageIndex = mode == "pages",
        };
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, options);
        var predicate = SparkSqlParser.ParsePredicate(sql);

        bool kept = mode == "pages"
            ? (await reader.GetCandidateRowRangesAsync(0, predicate)).Count > 0
            : (await reader.GetCandidateRowGroupsAsync(predicate))[0];

        Assert.True(kept == expectKeep, $"{mode} {shape} `{sql}`: kept={kept}, expected {expectKeep}");
    }
}
