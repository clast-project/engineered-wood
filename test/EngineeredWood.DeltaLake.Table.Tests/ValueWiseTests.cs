// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #502 phase 4c: the scan's filter conjuncts, DELETE's predicate and value-wise host masks marked
/// <see cref="RowPredicate.ValueWise"/>, so the reader answers them from a dictionary-encoded column's dictionary.
/// The oracle is the same scan or DELETE with <see cref="DeltaTable.AnswersFromDictionaries"/> off.
/// </summary>
public class ValueWiseTests : IDisposable
{
    private const int Files = 3;
    private const int RowsPerFile = 2_000;

    private readonly string _tempDir;

    public ValueWiseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_valuewise_{Guid.NewGuid():N}");
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
        .Field(new Field("cat", StringType.Default, true))
        .Field(new Field("code", StringType.Default, false))
        .Field(new Field("p", StringType.Default, false))
        .Build();

    private static Predicate P(string sql) => SparkSqlParser.ParsePredicate(sql);

    // ---- Which conjuncts are value-wise -----------------------------------------------------------------------------

    [Theory]
    [InlineData("cat = 'c3'", true)]
    [InlineData("cat IN ('c1', 'c5') OR cat IS NULL", true)]
    [InlineData("NOT (lower(cat) <> 'c2')", true)]
    [InlineData("CAST(code AS INT) > 3", true)]
    [InlineData("length(cat) + id > 4", true)]
    [InlineData("no_such_function(cat) = 1", false)]
    public void AConjunctIsValueWiseWhenEveryFunctionItCallsIsFoldable(string sql, bool expected) =>
        Assert.Equal(expected, DeltaTable.IsValueWise(P(sql)));

    // ---- Scans ------------------------------------------------------------------------------------------------------

    public static IEnumerable<object[]> Filters() =>
    [
        ["cat = 'c3'"],
        ["cat IN ('c1', 'c5')"],
        ["cat IS NULL"],
        ["lower(cat) <> 'c2'"],
        ["cat = 'c3' AND id > 2500"],
        ["p = 'q1' AND cat = 'c4'"],
        // 'x' is only on deleted rows: asking the dictionary raises, and the rows are asked instead.
        ["CAST(code AS INT) > 3"],
    ];

    [Theory]
    [MemberData(nameof(Filters))]
    public async Task AScanReturnsTheSameRowsAnsweredFromTheDictionary(string sql)
    {
        foreach (int concurrency in new[] { 1, 4 })
        {
            await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency);
            foreach (var projection in new[] { new[] { "id" }, new[] { "id", "cat" } })
            {
                var options = new DeltaReadOptions { Columns = projection, Filter = P(sql), FilterRows = true };
                var marked = new ConcurrentBag<bool>();
                table.RowFilterObserver = filter => { foreach (var p in filter.Predicates) marked!.Add(p.ValueWise); };
                table.AnswersFromDictionaries = false;
                var expected = await ReadAsync(table, options);
                Assert.DoesNotContain(true, marked);

                marked = new ConcurrentBag<bool>();
                table.AnswersFromDictionaries = true;
                var actual = await ReadAsync(table, options);
                Assert.Equal(expected, actual);
                Assert.NotEmpty(actual);

                // Every conjunct asked of the rows is marked (a partition conjunct is answered per file instead).
                Assert.NotEmpty(marked);
                Assert.DoesNotContain(false, marked);
            }
        }
    }

    /// <summary>The premise of the CAST case: the cast raises on a live row's 'x'.</summary>
    [Fact]
    public async Task TheCastRaisesOnALiveRow()
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 1, deleteBadCodes: false);
        await Assert.ThrowsAnyAsync<Exception>(() => ReadAsync(
            table, new DeltaReadOptions { Columns = ["id"], Filter = P("CAST(code AS INT) > 3"), FilterRows = true }));
    }

    // ---- DELETE -----------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("cat = 'c3'")]
    [InlineData("cat IS NULL OR cat = 'c6'")]
    [InlineData("CAST(code AS INT) > 3")]
    public async Task ADeleteMatchesTheSameRowsAnsweredFromTheDictionary(string sql)
    {
        var results = new List<(long Deleted, List<string> Left)>();
        foreach (bool dictionary in new[] { false, true })
        {
            await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 4);
            table.AnswersFromDictionaries = dictionary;
            var marked = new ConcurrentBag<bool>();
            table.RowFilterObserver = filter => { foreach (var p in filter.Predicates) marked.Add(p.ValueWise); };
            var (deleted, _) = await table.DeleteAsync(P(sql));
            Assert.NotEmpty(marked);
            Assert.All(marked, m => Assert.Equal(dictionary, m));
            results.Add((deleted, await ReadAsync(table, new DeltaReadOptions { Columns = ["id", "cat"] })));
        }

        Assert.True(results[0].Deleted > 0);
        Assert.Equal(results[0].Deleted, results[1].Deleted);
        Assert.Equal(results[0].Left, results[1].Left);
    }

    // ---- Host masks -------------------------------------------------------------------------------------------------

    /// <summary>
    /// A value-wise mask over a column the scan does not return is asked once per row group, over the dictionary and
    /// one NULL, and keeps the same rows.
    /// </summary>
    [Fact]
    public async Task AValueWiseHostMaskIsAskedOverTheDictionary()
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 4);
        var rowLengths = new ConcurrentBag<int>();
        var expected = await ReadAsync(table, MaskOptions(valueWise: false, rowLengths));
        var dictionaryLengths = new ConcurrentBag<int>();
        var actual = await ReadAsync(table, MaskOptions(valueWise: true, dictionaryLengths));

        Assert.Equal(expected, actual);
        Assert.NotEmpty(actual);
        Assert.Equal(Files * RowsPerFile / 1_000, dictionaryLengths.Count);
        Assert.All(dictionaryLengths, n => Assert.Equal(8 + 1, n));
        Assert.True(rowLengths.Sum() > RowsPerFile);

        // The mask is not value-wise with the switch off, whatever the host says.
        table.AnswersFromDictionaries = false;
        var off = new ConcurrentBag<int>();
        Assert.Equal(expected, await ReadAsync(table, MaskOptions(valueWise: true, off)));
        Assert.Equal(rowLengths.Sum(), off.Sum());
    }

    /// <summary>A value-wise mask that raises over a value only deleted rows hold is asked over the rows instead.</summary>
    [Fact]
    public async Task AValueWiseHostMaskThatRaisesOverTheDictionaryIsAskedOverTheRows()
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 4);
        var lengths = new ConcurrentBag<int>();
        var mask = new DeltaRowMask(["code"], batch =>
        {
            lengths.Add(batch.Length);
            var codes = (StringArray)batch.Column(0);
            var answer = new BooleanArray.Builder().Reserve(codes.Length);
            for (int i = 0; i < codes.Length; i++)
            {
                string? code = codes.GetString(i);
                answer.Append(code == "x" ? throw new InvalidOperationException("x") : code == "3");
            }

            return answer.Build();
        }) { ValueWise = true };

        var rows = await ReadAsync(table, new DeltaReadOptions { Columns = ["id"], RowMasks = [mask] });

        Assert.Equal(Enumerable.Range(0, Files * RowsPerFile).Count(i => !IsDeleted(i) && Code(i) == "3"), rows.Count);
        Assert.Contains(lengths, n => n == 8); // the dictionary: '0'..'6' and 'x'
        Assert.True(lengths.Sum() > RowsPerFile);
    }

    private static DeltaReadOptions MaskOptions(bool valueWise, ConcurrentBag<int> lengths) => new()
    {
        Columns = ["id"],
        RowMasks =
        [
            new DeltaRowMask(["cat"], batch =>
            {
                lengths.Add(batch.Length);
                var cats = (StringArray)batch.Column(0);
                var answer = new BooleanArray.Builder().Reserve(cats.Length);
                for (int i = 0; i < cats.Length; i++)
                    answer.Append(cats.GetString(i) is "c2" or "c7");
                return answer.Build();
            }) { ValueWise = valueWise },
        ],
    };

    // ---- Whole-batch answers (#502 phase 5a) ------------------------------------------------------------------------

    public static IEnumerable<object[]> WholeBatchFilters() =>
    [
        ["id > 100 AND cat = 'c3'"],
        ["cat <> 'c2' AND id < 5000 AND cat IS NOT NULL"],
        ["id > 2500 OR cat IS NULL"],
        ["NOT (id < 300) AND cat <> 'c1'"],
        // An arithmetic conjunct declines and is asked about the live rows.
        ["p = 'q1' AND id % 2 = 0 AND cat = 'c4'"],
    ];

    /// <summary>
    /// With conjuncts that cannot raise asked about whole batches (the reader taking every one), a scan returns the
    /// same rows as with none offered; and they are offered. Dictionary answers are off, so every conjunct is asked
    /// about rows.
    /// </summary>
    [Theory]
    [MemberData(nameof(WholeBatchFilters))]
    public async Task AScanReturnsTheSameRowsWithWholeBatchAnswers(string sql)
    {
        foreach (int concurrency in new[] { 1, 4 })
        {
            await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency);
            table.AnswersFromDictionaries = false;
            table.WholeBatchLiveShare = 0;
            foreach (var projection in new[] { new[] { "id" }, new[] { "id", "cat", "code" } })
            {
                var options = new DeltaReadOptions { Columns = projection, Filter = P(sql), FilterRows = true };
                var offered = new ConcurrentBag<bool>();
                table.RowFilterObserver = filter =>
                {
                    foreach (var predicate in filter.Predicates)
                        offered.Add(DeltaTable.OffersWholeBatch(predicate));
                };

                table.EvaluatesWholeBatches = false;
                var expected = await ReadAsync(table, options);
                Assert.DoesNotContain(true, offered);

                offered = new ConcurrentBag<bool>();
                table.EvaluatesWholeBatches = true;
                var actual = await ReadAsync(table, options);
                Assert.Equal(expected, actual);
                Assert.NotEmpty(actual);
                Assert.Contains(true, offered);
            }
        }
    }

    /// <summary>
    /// A conjunct that can raise is never asked about a row an earlier conjunct rejected, even with whole batches
    /// taken whenever a row is live: <c>CAST(code AS INT)</c> raises on 'x', which <c>code &lt;&gt; 'x'</c> rejects
    /// first. The rows holding 'x' are live here (no deletion vector).
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AConjunctThatCanRaiseIsNotAskedAboutRowsAnEarlierOneRejected(int concurrency)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency, deleteBadCodes: false);
        table.AnswersFromDictionaries = false;
        table.WholeBatchLiveShare = 0;
        var options = new DeltaReadOptions
        {
            Columns = ["id", "code"], Filter = P("code <> 'x' AND CAST(code AS INT) > 3"), FilterRows = true,
        };

        table.EvaluatesWholeBatches = false;
        var expected = await ReadAsync(table, options);
        table.EvaluatesWholeBatches = true;
        var actual = await ReadAsync(table, options);
        Assert.Equal(expected, actual);
        Assert.NotEmpty(actual);
    }

    /// <summary>
    /// An expression DELETE offers whole-batch answers when its form cannot raise (its deletion vector leaves most rows
    /// live, so the reader takes them), and matches the same rows either way.
    /// </summary>
    [Theory]
    [InlineData("cat = 'c3'", true)]
    [InlineData("id > 1000 AND cat IS NOT NULL", true)]
    [InlineData("CAST(code AS INT) > 3", false)] // a form that could raise is not offered
    public async Task ADeleteMatchesTheSameRowsWithWholeBatchAnswers(string sql, bool offers)
    {
        var results = new List<(long Deleted, List<string> Left)>();
        foreach (bool whole in new[] { false, true })
        {
            await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 4);
            table.AnswersFromDictionaries = false;
            table.WholeBatchLiveShare = 0;
            table.EvaluatesWholeBatches = whole;
            var offered = new ConcurrentBag<bool>();
            table.RowFilterObserver = filter =>
            {
                foreach (var predicate in filter.Predicates)
                    offered.Add(DeltaTable.OffersWholeBatch(predicate));
            };
            var (deleted, _) = await table.DeleteAsync(P(sql));
            Assert.NotEmpty(offered);
            Assert.All(offered, o => Assert.Equal(whole && offers, o));
            results.Add((deleted, await ReadAsync(table, new DeltaReadOptions { Columns = ["id", "cat"] })));
        }

        Assert.True(results[0].Deleted > 0);
        Assert.Equal(results[0].Deleted, results[1].Deleted);
        Assert.Equal(results[0].Left, results[1].Left);
    }

    /// <summary>
    /// Review round 1: bringing a whole batch to the table's shape can raise on a value where a gather of the live rows
    /// does not. A DATE column widened to TIMESTAMP_NTZ overflows on a far-future day count (checked, in microseconds),
    /// so a row holding one, rejected by an earlier conjunct or deleted by the deletion vector, must not fail the read
    /// or the DELETE: the whole batch is declined and the live rows are asked instead.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AWholeBatchWhoseWideningOverflowsFallsBackToTheLiveRows(bool whole)
    {
        const int rows = 20;
        const int bad = 7;
        string root = Path.Combine(_tempDir, $"widened_{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        var fs = new LocalTableFileSystem(root);
        var fileSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("d", Date32Type.Default, true))
            .Build();
        var days = new ArrowBuffer.Builder<int>();
        for (int i = 0; i < rows; i++)
            days.Append(i == bad ? int.MaxValue : 11_000 + i); // 2000-02-13 onwards; the bad row overflows
        var ids = new Int64Array.Builder();
        for (int i = 0; i < rows; i++)
            ids.Append(i);
        await using (var file = await fs.CreateAsync("data.parquet"))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(fileSchema,
                [ids.Build(), new Date32Array(days.Build(), ArrowBuffer.Empty, rows, 0, 0)], rows));
            await writer.CloseAsync();
        }

        await new EngineeredWood.DeltaLake.Log.TransactionLog(fs).WriteCommitAsync(0, new List<EngineeredWood.DeltaLake.Actions.DeltaAction>
        {
            new EngineeredWood.DeltaLake.Actions.ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["typeWidening", "deletionVectors", "timestampNtz"],
                WriterFeatures = ["typeWidening", "deletionVectors", "timestampNtz"],
            },
            new EngineeredWood.DeltaLake.Actions.MetadataAction
            {
                Id = "widened",
                Format = EngineeredWood.DeltaLake.Actions.Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"d","type":"timestamp_ntz","nullable":true,"metadata":{"delta.typeChanges":"[{\"fromType\":\"date\",\"toType\":\"timestamp_ntz\"}]"}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { EngineeredWood.DeltaLake.Schema.TypeWidening.EnableKey, "true" },
                    { EngineeredWood.DeltaLake.DeletionVectors.DeletionVectorConfig.EnableKey, "true" },
                },
            },
            new EngineeredWood.DeltaLake.Actions.AddFile
            {
                Path = "data.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = new FileInfo(Path.Combine(root, "data.parquet")).Length,
                ModificationTime = 1000,
                DataChange = true,
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        table.EvaluatesWholeBatches = whole;
        table.WholeBatchLiveShare = 0;

        // An earlier conjunct rejects the bad row: it is never widened.
        var scanned = await ReadAsync(table, new DeltaReadOptions
        {
            Columns = ["id"],
            Filter = P($"id <> {bad} AND d > DATE'2000-02-20'"),
            FilterRows = true,
        });
        Assert.Equal(Enumerable.Range(0, rows).Where(i => i != bad && 11_000 + i > 11_007).Select(i => i.ToString(CultureInfo.InvariantCulture)).OrderBy(x => x, StringComparer.Ordinal), scanned);

        // The deletion vector deletes it: an expression DELETE over d never widens it.
        var (removed, _) = await table.DeleteAsync(P($"id = {bad}"));
        Assert.Equal(1, removed);
        var (deleted, _) = await table.DeleteAsync(P("d > DATE'2000-02-20'"));
        Assert.Equal(12, deleted); // rows 8-19 (2000-02-21 on)
    }

    // ---- The table --------------------------------------------------------------------------------------------------

    private static bool IsDeleted(int id) => id % 97 == 5;

    private static string Code(int id) => IsDeleted(id) ? "x" : (id % 7).ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Three files of 2,000 rows in row groups of 1,000: <c>cat</c> c0..c7 and NULL every 11th row, dictionary-encoded;
    /// <c>code</c> '0'..'6', and 'x' on the rows a deletion vector deletes (unless <paramref name="deleteBadCodes"/> is
    /// false); <c>p</c> a partition, q0 or q1.
    /// </summary>
    private async Task<DeltaTable> CreateAsync(string name, int concurrency, bool deleteBadCodes = true)
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, name)), TableSchema,
            new DeltaTableOptions
            {
                ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 1_000, DataPageRowCountLimit = 100 },
                MaxConcurrentReads = concurrency,
            },
            partitionColumns: ["p"],
            enableDeletionVectors: true);
        for (int f = 0; f < Files; f++)
        {
            var id = new Int64Array.Builder();
            var cat = new StringArray.Builder();
            var code = new StringArray.Builder();
            var p = new StringArray.Builder();
            for (int i = f * RowsPerFile; i < (f + 1) * RowsPerFile; i++)
            {
                id.Append(i);
                if (i % 11 == 0)
                    cat.AppendNull();
                else
                    cat.Append("c" + (i % 8).ToString(CultureInfo.InvariantCulture));
                code.Append(Code(i));
                p.Append(f == 1 ? "q1" : "q0");
            }

            await table.WriteAsync([new RecordBatch(TableSchema, [id.Build(), cat.Build(), code.Build(), p.Build()], RowsPerFile)]);
        }

        if (deleteBadCodes)
        {
            var (deleted, _) = await table.DeleteAsync(batch =>
            {
                var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
                var bad = new BooleanArray.Builder().Reserve(ids.Length);
                for (int i = 0; i < ids.Length; i++)
                    bad.Append(IsDeleted((int)ids.GetValue(i)!.Value));
                return bad.Build();
            });
            Assert.True(deleted > 0);
        }

        return table;
    }

    private static async Task<List<string>> ReadAsync(DeltaTable table, DeltaReadOptions options)
    {
        var rows = new List<string>();
        await foreach (var batch in table.ReadAsync(options))
        {
            using (batch)
            {
                for (int r = 0; r < batch.Length; r++)
                {
                    var cells = new List<string>();
                    for (int c = 0; c < batch.ColumnCount; c++)
                    {
                        var column = batch.Column(c);
                        cells.Add(column.IsNull(r) ? "null" : column switch
                        {
                            Int64Array a => a.GetValue(r)!.Value.ToString(CultureInfo.InvariantCulture),
                            StringArray a => a.GetString(r),
                            _ => column.Data.DataType.Name,
                        });
                    }

                    rows.Add(string.Join("|", cells));
                }
            }
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }
}
