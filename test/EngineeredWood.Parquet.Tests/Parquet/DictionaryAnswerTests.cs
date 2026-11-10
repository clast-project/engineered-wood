// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// A value-wise predicate over a dictionary-encoded column answered from the chunk's dictionary (#502 phase 4b,
/// doc/row-level-filtering.md §6 and §20): the same rows as the predicate asked row by row, the predicate asked
/// once per chunk over the dictionary instead, and the rows asked after all when the dictionary cannot answer.
/// </summary>
public class DictionaryAnswerTests : IDisposable
{
    private const int Rows = 30_000;
    private const int RowsPerGroup = 10_000;

    /// <summary>The categories of <c>s</c>: its dictionary holds this many entries per chunk.</summary>
    private const int Categories = 50;

    private readonly string _tempDir;

    public DictionaryAnswerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-dictanswer-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch (IOException) { }
    }

    // ---- Same rows as asked row by row -------------------------------------------------------------------------

    private static readonly Dictionary<string, Func<string?, bool?>> Answers = new()
    {
        ["s = cat-03"] = v => v is null ? null : v == "cat-03",
        ["s < cat-10"] = v => v is null ? null : string.CompareOrdinal(v, "cat-10") < 0,
        ["s IS NULL"] = v => v is null,
        ["NULL for odd categories"] = v => v is null ? false : (v[^1] - '0') % 2 == 0 ? true : null,
        ["nothing"] = _ => false,
    };

    public static IEnumerable<object[]> Cases()
    {
        foreach (var answer in Answers.Keys)
        {
            foreach (bool batched in new[] { false, true })
            {
                foreach (bool twoPass in new[] { false, true })
                {
                    // Exclusions and a second predicate after the answered one vary with the concurrency, and
                    // whether the read returns the answered column with the batching.
                    yield return [answer, batched, twoPass, 1];
                    yield return [answer, batched, twoPass, 4];
                    yield return [answer, !batched, twoPass, batched ? 4 : 1, "returned"];
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ReturnsTheRowsThePredicateSelectsRowByRow(
        string answerName, bool batched, bool twoPass, int concurrency, string column = "dropped")
    {
        // A returned column is built from the dictionary as well as answered from it (#502 phase 4d).
        string[] projection = column == "returned" ? ["id", "s"] : ["id"];
        string path = await WriteFileAsync();
        var answer = Answers[answerName];
        bool parallel = concurrency > 1;
        var excluded = parallel ? new Exclusions(Enumerable.Range(0, Rows).Where(r => r % 19 == 0).Select(r => (long)r)) : null;
        var options = new ParquetReadOptions { MaxConcurrentRowGroups = concurrency, BatchSize = batched ? 2_500 : null };

        RowFilter Filter(bool valueWise, ConcurrentBag<int> lengths, ConcurrentBag<long> later)
        {
            var predicates = new List<RowPredicate> { Over("s", answer, valueWise, lengths) };

            // A later predicate sees the rows the answered one left live, NULL rows included.
            if (parallel)
            {
                predicates.Add(new RowPredicate(["k", "id"], batch =>
                {
                    var ids = (Int64Array)batch.Column(1);
                    for (int i = 0; i < ids.Length; i++)
                        later.Add(ids.GetValue(i)!.Value);
                    return Map((Int32Array)batch.Column(0), v => v % 3 != 0);
                }));
            }

            return new RowFilter(predicates);
        }

        var rowLengths = new ConcurrentBag<int>();
        var rowLater = new ConcurrentBag<long>();
        var expected = await ReadAsync(path, options, new ParquetScan
        {
            Columns = projection, RowFilter = Filter(valueWise: false, rowLengths, rowLater), ExcludedRows = excluded,
        }, twoPass);

        var dictionaryLengths = new ConcurrentBag<int>();
        var dictionaryLater = new ConcurrentBag<long>();
        var actual = await ReadAsync(path, options, new ParquetScan
        {
            Columns = projection, RowFilter = Filter(valueWise: true, dictionaryLengths, dictionaryLater), ExcludedRows = excluded,
        }, twoPass);

        Assert.Equal(expected, actual);
        Assert.Equal(rowLater.OrderBy(r => r), dictionaryLater.OrderBy(r => r));
        if (answerName != "nothing")
            Assert.NotEmpty(expected);

        // Asked once per row group, over the dictionary and one NULL; never over the rows.
        Assert.Equal(Rows / RowsPerGroup, dictionaryLengths.Count);
        Assert.All(dictionaryLengths, n => Assert.Equal(Categories + 1, n));
        Assert.True(rowLengths.Sum() > Rows / 2);
    }

    // ---- When the dictionary does not answer ----------------------------------------------------------------

    public static IEnumerable<object[]> Unanswered() =>
    [
        ["not value-wise"], ["read by two predicates"], ["two columns"], ["too many distinct values"],
        ["PLAIN-encoded"],
    ];

    [Theory]
    [MemberData(nameof(Unanswered))]
    public async Task IsAskedRowByRowWhenTheDictionaryCannotAnswer(string why)
    {
        string path = await WriteFileAsync();
        var lengths = new ConcurrentBag<int>();
        Func<string?, bool?> answer = v => v is null ? null : v.EndsWith("3", StringComparison.Ordinal);
        string column = why switch
        {
            "too many distinct values" => "u",
            "PLAIN-encoded" => "p",
            _ => "s",
        };

        var predicates = new List<RowPredicate>();
        string[] projection = ["id"];
        if (why == "two columns")
        {
            predicates.Add(new RowPredicate(["s", "id"], batch =>
            {
                lengths.Add(batch.Length);
                return Map((StringArray)batch.Column(0), answer);
            }) { ValueWise = true });
        }
        else
        {
            predicates.Add(Over(column, answer, valueWise: why != "not value-wise", lengths));
            if (why == "read by two predicates")
                predicates.Add(Over("s", v => v is null || v != "cat-00", valueWise: true, new ConcurrentBag<int>()));
        }

        var rows = await ReadAsync(path, new ParquetReadOptions(), new ParquetScan
        {
            Columns = projection, RowFilter = new RowFilter(predicates),
        }, twoPass: false);

        Assert.NotEmpty(rows);
        Assert.Equal(Rows, lengths.Sum());
    }

    /// <summary>
    /// A returned column read as a view type is answered from its dictionary too: its values are decoded as ever,
    /// with the indices beside them (#502 phase 4d).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AReturnedViewTypeColumnIsAnsweredFromItsDictionary(bool batched)
    {
        string path = await WriteFileAsync();
        var options = new ParquetReadOptions { ByteArrayOutput = ByteArrayOutputKind.ViewType, BatchSize = batched ? 2_500 : null };
        Func<string?, bool?> answer = v => v is null ? null : v.EndsWith("3", StringComparison.Ordinal);
        var rowLengths = new ConcurrentBag<int>();
        var expected = await ReadAsync(path, options, new ParquetScan
        {
            Columns = ["id", "s"], RowFilter = new RowFilter([Over("s", answer, valueWise: false, rowLengths)]),
        }, twoPass: false);
        var dictionaryLengths = new ConcurrentBag<int>();
        var actual = await ReadAsync(path, options, new ParquetScan
        {
            Columns = ["id", "s"], RowFilter = new RowFilter([Over("s", answer, valueWise: true, dictionaryLengths)]),
        }, twoPass: false);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
        Assert.Equal(Rows, rowLengths.Sum());
        Assert.All(dictionaryLengths, n => Assert.Equal(Categories + 1, n));
        Assert.Equal(Rows / RowsPerGroup, dictionaryLengths.Count);
    }

    /// <summary>
    /// A returned column answered over a read the page index narrows to two ranges of a row group, with batches
    /// that do not fall on page boundaries: the answers are held, skipped and cut as the values are.
    /// </summary>
    [Theory]
    [InlineData(0L, 700)]
    [InlineData(2_000L, 300)] // the first range starts past the group's first row (review round 1)
    public async Task AReturnedColumnIsAnsweredOverANarrowedRead(long from, int batchSize)
    {
        string path = await WriteFileAsync();
        var options = new ParquetReadOptions { BatchSize = batchSize };
        var prune = EngineeredWood.Expressions.Expressions.Or(
            EngineeredWood.Expressions.Expressions.And(
                EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", EngineeredWood.Expressions.LiteralValue.Of(from)),
                EngineeredWood.Expressions.Expressions.LessThan("id", EngineeredWood.Expressions.LiteralValue.Of(from + 1_500L))),
            EngineeredWood.Expressions.Expressions.And(
                EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", EngineeredWood.Expressions.LiteralValue.Of(6_200L)),
                EngineeredWood.Expressions.Expressions.LessThan("id", EngineeredWood.Expressions.LiteralValue.Of(7_900L))));
        Func<string?, bool?> answer = v => v is null ? null : string.CompareOrdinal(v, "cat-20") < 0;
        var rowLengths = new ConcurrentBag<int>();
        var expected = await ReadAsync(path, options, new ParquetScan
        {
            Columns = ["id", "s"], Prune = prune,
            RowFilter = new RowFilter([Over("s", answer, valueWise: false, rowLengths)]),
        }, twoPass: false);
        var dictionaryLengths = new ConcurrentBag<int>();
        var actual = await ReadAsync(path, options, new ParquetScan
        {
            Columns = ["id", "s"], Prune = prune,
            RowFilter = new RowFilter([Over("s", answer, valueWise: true, dictionaryLengths)]),
        }, twoPass: false);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
        Assert.Contains(expected, r => r.StartsWith("7", StringComparison.Ordinal)); // the second range
        Assert.All(dictionaryLengths, n => Assert.Equal(Categories + 1, n));
        Assert.NotEmpty(dictionaryLengths);
    }

    /// <summary>
    /// A file whose own column has the name a returned column's answers would take: that column is returned, and the
    /// predicate is asked row by row (review round 1).
    /// </summary>
    [Fact]
    public async Task AColumnNamedAsAnAnswerColumnIsReturnedAndTheDictionaryIsNotUsed()
    {
        string answerName = EngineeredWood.Parquet.Data.DictionaryAnswer.AnswerColumnName("s");
        string path = Path.Combine(_tempDir, "collision.parquet");
        var id = new Int64Array.Builder();
        var s = new StringArray.Builder();
        var other = new StringArray.Builder();
        for (int r = 0; r < 2_000; r++)
        {
            id.Append(r);
            s.Append("cat-" + (r % 5).ToString(System.Globalization.CultureInfo.InvariantCulture));
            other.Append("other-" + r.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("s", StringType.Default, false))
            .Field(new Field(answerName, StringType.Default, false))
            .Build();
        using (var batch = new RecordBatch(schema, [id.Build(), s.Build(), other.Build()], 2_000))
        {
            await using var output = new LocalSequentialFile(path);
            await using var writer = new ParquetFileWriter(output, ownsFile: false, ParquetWriteOptions.Default);
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        var lengths = new ConcurrentBag<int>();
        var rows = await ReadAsync(path, new ParquetReadOptions(), new ParquetScan
        {
            Columns = ["id", "s", answerName],
            RowFilter = new RowFilter([Over("s", v => v == "cat-3", valueWise: true, lengths)]),
        }, twoPass: false);

        Assert.Equal(400, rows.Count);
        Assert.All(rows, r => Assert.Equal(3, r.Split('|').Length)); // id, s and the file's own column
        Assert.Equal(2_000, lengths.Sum());
    }

    /// <summary>
    /// A dictionary at least as large as the rows the read decodes is not evaluated: here 1500 entries, for the one
    /// 1000-row page the page index narrows the read to. (EW's writer never writes a dictionary as large as a
    /// whole chunk's rows: it falls back to PLAIN past 20% distinct.)
    /// </summary>
    [Fact]
    public async Task ADictionaryAsLargeAsTheRowsReadIsNotEvaluated()
    {
        string path = await WriteFileAsync();
        var lengths = new ConcurrentBag<int>();
        var rows = await ReadAsync(path, new ParquetReadOptions(), new ParquetScan
        {
            Columns = ["id"],
            Prune = EngineeredWood.Expressions.Expressions.LessThan(
                "id", EngineeredWood.Expressions.LiteralValue.Of(500L)),
            RowFilter = new RowFilter([Over("w", v => v!.EndsWith("7", StringComparison.Ordinal), valueWise: true, lengths)]),
        }, twoPass: false);

        Assert.Equal(Enumerable.Range(0, 1_000).Count(r => r % 10 == 7), rows.Count);
        Assert.Equal([1_000], lengths);
    }

    // ---- Errors ---------------------------------------------------------------------------------------------

    /// <summary>
    /// An entry no row asks about can raise over the dictionary (a value only an excluded row holds): the chunk is
    /// then read as values and the predicate asked row by row, over rows that never include it.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnEntryOnlyAnExcludedRowHoldsDoesNotFailTheRead(bool batched)
    {
        string path = await WriteFileAsync();
        var lengths = new ConcurrentBag<int>();
        var excluded = new Exclusions(Enumerable.Range(0, Rows).Where(IsBad).Select(r => (long)r));
        var rows = await ReadAsync(path, new ParquetReadOptions { BatchSize = batched ? 2_500 : null }, new ParquetScan
        {
            Columns = ["id"],
            RowFilter = new RowFilter([Over("e", RaisingOnBad, valueWise: true, lengths)]),
            ExcludedRows = excluded,
        }, twoPass: false);

        Assert.Equal(Enumerable.Range(0, Rows).Count(r => !IsBad(r) && r % 7 == 1), rows.Count);

        // Asked over each dictionary (ok-0..ok-6 and bad), which raised, then over the rows.
        Assert.Contains(lengths, n => n == 8);
        Assert.Equal(Rows - excluded.Count, lengths.Where(n => n != 8).Sum());
    }

    /// <summary>A value a live row holds raises as it would row by row.</summary>
    [Fact]
    public async Task AnEntryALiveRowHoldsStillRaises()
    {
        string path = await WriteFileAsync();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => ReadAsync(path, new ParquetReadOptions(), new ParquetScan
        {
            Columns = ["id"], RowFilter = new RowFilter([Over("e", RaisingOnBad, valueWise: true, new ConcurrentBag<int>())]),
        }, twoPass: false));
        Assert.Contains("bad value", ex.ToString());
    }

    /// <summary>
    /// An entry the column's Arrow type cannot hold (an INT96 past 2262 read as timestamp[ns]) makes building the
    /// dictionary fail, where building only the rows a read decodes may not: the dictionary then declines, and the
    /// rows decide (review round 1).
    /// </summary>
    [Fact]
    public void ADictionaryEntryTheArrowTypeCannotHoldDeclinesRatherThanRaises()
    {
        const int julianDayOfUnixEpoch = 2_440_588;
        var element = new EngineeredWood.Parquet.Metadata.SchemaElement
        {
            Name = "ts",
            Type = PhysicalType.Int96,
            RepetitionType = FieldRepetitionType.Required,
        };
        var column = new EngineeredWood.Parquet.Schema.ColumnDescriptor
        {
            Path = ["ts"],
            PhysicalType = PhysicalType.Int96,
            MaxDefinitionLevel = 0,
            MaxRepetitionLevel = 0,
            SchemaElement = element,
            SchemaNode = new EngineeredWood.Parquet.Schema.SchemaNode { Element = element, Children = [] },
        };

        // 1970-01-01, and 9999-12-31: out of timestamp[ns]'s range.
        var plain = new byte[24];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(plain.AsSpan(8), julianDayOfUnixEpoch);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(plain.AsSpan(20), julianDayOfUnixEpoch + 2_932_896);
        using var dictionary = new EngineeredWood.Parquet.Data.DictionaryDecoder(PhysicalType.Int96);
        dictionary.Load(plain, numValues: 2, typeLength: 12);

        int calls = 0;
        var predicate = new RowPredicate(["ts"], batch =>
        {
            calls++;
            return Map(new Int32Array.Builder().AppendRange(new int[batch.Length]).Build(), _ => true);
        }) { ValueWise = true };
        var field = new Field("ts", new TimestampType(TimeUnit.Nanosecond, (string?)null), nullable: false);
        var answer = new EngineeredWood.Parquet.Data.DictionaryAnswer(predicate, column, field, (f, a) => (f, a), rows: 100);

        Assert.False(answer.Prepare(dictionary));
        Assert.Equal(0, calls);
    }

    // ---- Helpers --------------------------------------------------------------------------------------------

    private static bool IsBad(int row) => row % 1_000 == 3;

    private static bool? RaisingOnBad(string? value) =>
        value == "bad" ? throw new InvalidOperationException("bad value") : value == "ok-1";

    private static RowPredicate Over(string column, Func<string?, bool?> answer, bool valueWise, ConcurrentBag<int> lengths) =>
        new([column], batch =>
        {
            lengths.Add(batch.Length);
            return batch.Column(0) is StringViewArray views
                ? Map(Enumerable.Range(0, views.Length).Select(i => views.GetString(i)).ToArray(), answer)
                : Map((StringArray)batch.Column(0), answer);
        }) { ValueWise = valueWise };

    private static BooleanArray Map(string?[] values, Func<string?, bool?> answer)
    {
        var builder = new BooleanArray.Builder().Reserve(values.Length);
        foreach (var value in values)
        {
            if (answer(value) is { } b)
                builder.Append(b);
            else
                builder.AppendNull();
        }

        return builder.Build();
    }

    private static BooleanArray Map(StringArray values, Func<string?, bool?> answer)
    {
        var builder = new BooleanArray.Builder().Reserve(values.Length);
        for (int i = 0; i < values.Length; i++)
        {
            if (answer(values.GetString(i)) is { } b)
                builder.Append(b);
            else
                builder.AppendNull();
        }

        return builder.Build();
    }

    private static BooleanArray Map(Int32Array values, Func<int, bool> answer)
    {
        var builder = new BooleanArray.Builder().Reserve(values.Length);
        for (int i = 0; i < values.Length; i++)
            builder.Append(answer(values.GetValue(i)!.Value));
        return builder.Build();
    }

    private sealed class Exclusions(IEnumerable<long> positions) : IExcludedRows
    {
        private readonly HashSet<long> _positions = positions.ToHashSet();

        public int Count => _positions.Count;

        public void ClearExcluded(long firstRow, int rows, Span<ulong> selection)
        {
            for (int i = 0; i < rows; i++)
            {
                if (_positions.Contains(firstRow + i))
                    selection[i >> 6] &= ~(1UL << (i & 63));
            }
        }

        public bool ExcludesAll(long firstRow, long rows)
        {
            for (long r = firstRow; r < firstRow + rows; r++)
            {
                if (!_positions.Contains(r))
                    return false;
            }

            return true;
        }
    }

    private static async Task<List<string>> ReadAsync(string path, ParquetReadOptions options, ParquetScan scan, bool twoPass)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, options) { TwoPassReads = twoPass };
        var rows = new List<string>();
        await foreach (var selected in reader.ReadSelectedAsync(scan))
        {
            using (selected)
            {
                for (int i = 0; i < selected.Batch.Length; i++)
                    rows.Add($"{selected.Positions[i]}:{ArrowValues.RenderRow(selected.Batch, i)}");
            }
        }

        return rows;
    }

    /// <summary>
    /// id; k (0-999); s, 50 categories and 10% NULL; e, "ok-0".."ok-6" and "bad" every 1000th row; u, a value per
    /// row (so PLAIN: past the writer's 20% distinct); p, as s but PLAIN-encoded; w, 1500 values. Three row groups
    /// of 1000-row pages.
    /// </summary>
    private async Task<string> WriteFileAsync()
    {
        string path = Path.Combine(_tempDir, "data.parquet");
        if (File.Exists(path))
            return path;

        var random = new Random(502);
        var id = new Int64Array.Builder();
        var k = new Int32Array.Builder();
        var s = new StringArray.Builder();
        var e = new StringArray.Builder();
        var u = new StringArray.Builder();
        var p = new StringArray.Builder();
        var w = new StringArray.Builder();
        for (int r = 0; r < Rows; r++)
        {
            id.Append(r);
            k.Append(random.Next(1_000));

            // Every category in every row group, so each dictionary holds all 50.
            string category = "cat-" + (r % Categories).ToString("D2", System.Globalization.CultureInfo.InvariantCulture);
            if (random.Next(10) == 0)
                s.AppendNull();
            else
                s.Append(category);
            e.Append(IsBad(r) ? "bad" : "ok-" + (r % 7).ToString(System.Globalization.CultureInfo.InvariantCulture));
            u.Append("u" + r.ToString(System.Globalization.CultureInfo.InvariantCulture));
            p.Append(category);
            w.Append("w" + (r % 1_500).ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("k", Int32Type.Default, false))
            .Field(new Field("s", StringType.Default, true))
            .Field(new Field("e", StringType.Default, false))
            .Field(new Field("u", StringType.Default, false))
            .Field(new Field("p", StringType.Default, false))
            .Field(new Field("w", StringType.Default, false))
            .Build();
        using var batch = new RecordBatch(
            schema, [id.Build(), k.Build(), s.Build(), e.Build(), u.Build(), p.Build(), w.Build()], Rows);

        await using var output = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(output, ownsFile: false,
            ParquetWriteOptions.Default with
            {
                RowGroupMaxRows = RowsPerGroup,
                DataPageRowCountLimit = 1_000,
                ColumnDictionaryEnabled = new Dictionary<string, bool> { ["p"] = false },
            });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }
}
