// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> A Spark-written table whose string column falls back from the dictionary partway through
/// each chunk (#804). parquet-mr writes it, at a dictionary page size small enough to overflow. Its V1 writer falls
/// back to PLAIN, and its V2 writer to DELTA_BYTE_ARRAY. A value-wise filter answers the dictionary-encoded pages
/// from the dictionary and asks the fallback pages' values. It returns the same rows as with
/// <see cref="DeltaTable.AnswersFromDictionaries"/> off. See <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class SparkFallbackPageInteropTests : IDisposable
{
    private const int Rows = 20_000;

    private readonly string _tempDir;

    public SparkFallbackPageInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_spark_fallback_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    [SkippableTheory]
    [InlineData("v1", Encoding.Plain)]
    [InlineData("v2", Encoding.DeltaByteArray)]
    public async Task AValueWiseFilterReturnsTheSameRowsOverChunksThatFallBack(string writerVersion, Encoding fallback)
    {
        Spark.Require();

        // An empty table, then the rows, written with the parquet-mr settings as session confs: Spark copies every SQL
        // conf into the Hadoop conf a write hands parquet-mr. cat is NULL every tenth row, a unique filler every fourth
        // row of each file's second half (which overflows the dictionary after pages of it are written: parquet-mr
        // rewrites a chunk PLAIN whole if it overflows before its first page), and otherwise c0..c7. Two partitions, so two files of 10,000 rows.
        Spark.Invoke("write", new { path = _tempDir, schema = "id long, cat string", rows = System.Array.Empty<object>() });
        Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = new[]
            {
                "INSERT INTO delta.`{path}` SELECT id, CASE WHEN id % 10 = 0 THEN NULL " +
                "WHEN id % 4 = 1 AND id % 10000 >= 5000 THEN concat('fill-', id) ELSE concat('c', id % 8) END " +
                $"FROM range(0, {Rows}, 1, 2)",
            },
            confs = new Dictionary<string, string>
            {
                ["parquet.dictionary.page.size"] = "4096",
                ["parquet.page.size"] = "1024",
                ["parquet.page.row.count.limit"] = "500",
                ["parquet.writer.version"] = writerVersion,
            },
        });

        // The premise: some chunk of cat holds dictionary-encoded pages and fallback pages in the expected encoding.
        bool mixed = false;
        var seen = new List<string>();
        foreach (var path in Directory.GetFiles(_tempDir, "*.parquet"))
        {
            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            var metadata = await reader.ReadMetadataAsync();
            foreach (var chunk in metadata.RowGroups.SelectMany(g => g.Columns).Where(c => c.MetaData!.PathInSchema[0] == "cat"))
            {
                var data = chunk.MetaData!.EncodingStats!
                    .Where(s => s.PageType is PageType.DataPage or PageType.DataPageV2 && s.Count > 0).ToList();
                seen.Add(string.Join(",", data.Select(s => $"{s.Encoding}x{s.Count}")));
                mixed |= data.Any(s => s.Encoding is Encoding.RleDictionary or Encoding.PlainDictionary)
                    && data.Any(s => s.Encoding == fallback);
            }
        }

        Assert.True(mixed, $"no chunk of cat falls back to {fallback}: {string.Join("; ", seen)}");

        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        foreach (var sql in new[]
        {
            "cat = 'c3'", "cat IN ('c1', 'c5')", "cat IS NULL", "lower(cat) <> 'c2'", "cat < 'fill-3'",
            "cat <> 'c2' AND cat <> 'c5'",
        })
        {
            foreach (var projection in new[] { new[] { "id" }, new[] { "id", "cat" } })
            {
                var options = new DeltaReadOptions
                {
                    Columns = projection, Filter = SparkSqlParser.ParsePredicate(sql), FilterRows = true,
                };
                table.AnswersFromDictionaries = false;
                var expected = await ReadAsync(table, options);
                table.AnswersFromDictionaries = true;
                var actual = await ReadAsync(table, options);

                Assert.NotEmpty(expected);
                Assert.Equal(expected, actual);
            }
        }
    }

    private static async Task<List<string>> ReadAsync(DeltaTable table, DeltaReadOptions options)
    {
        var rows = new List<string>();
        await foreach (var batch in table.ReadAsync(options))
        {
            using (batch)
            {
                var ids = (Int64Array)batch.Column(0);
                var cats = batch.ColumnCount > 1 ? (StringArray)batch.Column(1) : null;
                for (int r = 0; r < batch.Length; r++)
                    rows.Add($"{ids.GetValue(r)}|{(cats is null ? "" : cats.GetString(r) ?? "null")}");
            }
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }
}
