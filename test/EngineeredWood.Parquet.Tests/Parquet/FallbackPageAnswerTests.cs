// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using System.Globalization;
using Apache.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Value-wise predicates answered from the dictionary of a chunk that falls back to PLAIN partway through (#804).
/// arrow-cpp (through ParquetSharp) writes the fixtures: it dictionary-encodes each chunk until the dictionary passes
/// <c>dictionary_pagesize_limit</c>, then writes the rest of the chunk PLAIN. The oracle is the predicate asked row by
/// row (not value-wise).
/// </summary>
public class FallbackPageAnswerTests : IDisposable
{
    private const int Groups = 3;
    private const int RowsPerGroup = 4_000;
    private const int Rows = Groups * RowsPerGroup;

    private readonly string _tempDir;

    public FallbackPageAnswerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-fallback-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch (IOException) { }
    }

    // The fixture's kinds: data page V1 or V2, with or without a page index (the batched read's page maps come from
    // the offset index, and pages are fetched per window, rather than the chunk held whole), and with encoding_stats
    // stripped from the footer.
    public static readonly string[] Variants = ["v1", "v2", "v1 page index", "v1 no encoding_stats"];

    // ---- The fixture ------------------------------------------------------------------------------------------------

    /// <summary>
    /// Every chunk of <c>s</c>, <c>k</c> and <c>e</c> holds dictionary-encoded pages and then PLAIN ones; with the
    /// stats stripped, the reader has no <c>encoding_stats</c> to say so.
    /// </summary>
    [Theory]
    [InlineData("v1")]
    [InlineData("v2")]
    [InlineData("v1 page index")]
    public async Task TheFixtureMixesDictionaryAndPlainPages(string variant)
    {
        string path = WriteFile(variant);
        var metadata = await ReadMetadataAsync(path);
        Assert.Equal(Groups, metadata.RowGroups.Count);
        foreach (var group in metadata.RowGroups)
        {
            foreach (var chunk in group.Columns.Where(c => c.MetaData!.PathInSchema[0] is "s" or "k" or "e"))
            {
                var stats = chunk.MetaData!.EncodingStats!;
                Assert.Contains(stats, s => s.PageType is PageType.DataPage or PageType.DataPageV2
                    && s.Encoding is Encoding.RleDictionary or Encoding.PlainDictionary && s.Count > 0);
                Assert.Contains(stats, s => s.PageType is PageType.DataPage or PageType.DataPageV2
                    && s.Encoding == Encoding.Plain && s.Count > 0);
                Assert.False(MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(chunk.MetaData));
            }
        }

        var stripped = await ReadMetadataAsync(WriteFile("v1 no encoding_stats"));
        Assert.All(stripped.RowGroups.SelectMany(g => g.Columns), c => Assert.Null(c.MetaData!.EncodingStats));
    }

    // ---- Same rows as asked row by row ------------------------------------------------------------------------------

    private static readonly Dictionary<string, Func<string?, bool?>> Answers = new()
    {
        ["s = cat-03"] = v => v is null ? null : v == "cat-03",
        ["s < cat-10"] = v => v is null ? null : string.CompareOrdinal(v, "cat-10") < 0,
        ["s IS NULL"] = v => v is null,
        ["NULL for odd endings"] = v => v is null ? false : (v[^1] - '0') % 2 == 0 ? true : null,
        ["fillers"] = v => v is not null && v.StartsWith("fill", StringComparison.Ordinal),
        ["nothing"] = _ => false,

        // NULL only for values on the fallback pages (the late fillers), FALSE for the NULL row: the answers need a
        // validity bitmap that no dictionary entry calls for.
        ["NULL for late fillers only"] = v => v is null ? false
            : v.StartsWith("fill-", StringComparison.Ordinal)
              && int.Parse(v.Substring(5), CultureInfo.InvariantCulture) % RowsPerGroup >= 3_000 ? null
            : v == "cat-03",
    };

    public static IEnumerable<object[]> Cases()
    {
        foreach (var answer in Answers.Keys)
        {
            foreach (var variant in Variants)
            {
                // batch size, returned, concurrency (4: exclusions and a later predicate), two-pass
                yield return [answer, variant, 0, false, 1, false];
                yield return [answer, variant, 700, true, 4, false];
                yield return [answer, variant, 700, false, 4, true];
                yield return [answer, variant, 0, true, 1, true];
            }
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ReturnsTheRowsThePredicateSelectsRowByRow(
        string answerName, string variant, int batchSize, bool returned, int concurrency, bool twoPass)
    {
        string path = WriteFile(variant);
        string[] projection = returned ? ["id", "s"] : ["id"];
        var answer = Answers[answerName];
        bool parallel = concurrency > 1;
        var excluded = parallel ? new Exclusions(Enumerable.Range(0, Rows).Where(r => r % 19 == 0).Select(r => (long)r)) : null;
        var options = new ParquetReadOptions { MaxConcurrentRowGroups = concurrency, BatchSize = batchSize > 0 ? batchSize : null };

        RowFilter Filter(bool valueWise, ConcurrentBag<string?[]> asked, ConcurrentBag<long> later)
        {
            var predicates = new List<RowPredicate> { Over("s", answer, valueWise, asked) };
            if (parallel)
            {
                // A later predicate sees the rows the answered one left live, NULL rows included.
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

        var rowAsked = new ConcurrentBag<string?[]>();
        var rowLater = new ConcurrentBag<long>();
        var expected = await ReadAsync(path, options, new ParquetScan
        {
            Columns = projection, RowFilter = Filter(valueWise: false, rowAsked, rowLater), ExcludedRows = excluded,
        }, twoPass);

        var asked = new ConcurrentBag<string?[]>();
        var later = new ConcurrentBag<long>();
        var actual = await ReadAsync(path, options, new ParquetScan
        {
            Columns = projection, RowFilter = Filter(valueWise: true, asked, later), ExcludedRows = excluded,
        }, twoPass);

        Assert.Equal(expected, actual);
        Assert.Equal(rowLater.OrderBy(r => r), later.OrderBy(r => r));
        if (answerName != "nothing")
            Assert.NotEmpty(expected);

        // Asked once per row group over the dictionary (the only batch ending in the NULL row), then over the values
        // of the pages that fell back from it, which hold no NULL. Never over the dictionary-encoded pages' rows.
        Assert.Equal(Groups, asked.Count(values => values.Length > 0 && values[^1] is null));
        Assert.Contains(asked, values => values.Length > 0 && values[^1] is not null);
        Assert.All(asked.Where(values => values.Length > 0 && values[^1] is not null),
            values => Assert.All(values, Assert.NotNull));
        Assert.True(asked.Sum(values => values.Length) < Rows * 3 / 4);
    }

    /// <summary>
    /// A returned column read as a view or large-offset type: a fallback page's values, decoded apart for the answers,
    /// are not in the read's layout, so the page is decoded again for the values the read returns.
    /// </summary>
    [Theory]
    [InlineData(ByteArrayOutputKind.ViewType, 0)]
    [InlineData(ByteArrayOutputKind.ViewType, 700)]
    [InlineData(ByteArrayOutputKind.LargeOffsets, 0)]
    [InlineData(ByteArrayOutputKind.LargeOffsets, 700)]
    public async Task AReturnedViewOrLargeColumnIsAnswered(ByteArrayOutputKind output, int batchSize)
    {
        string path = WriteFile("v1");
        var options = new ParquetReadOptions { ByteArrayOutput = output, BatchSize = batchSize > 0 ? batchSize : null };
        Func<string?, bool?> answer = v => v is null ? null : v.EndsWith("3", StringComparison.Ordinal);
        var expected = await ReadAsync(path, options, new ParquetScan
        {
            Columns = ["id", "s"], RowFilter = new RowFilter([Over("s", answer, valueWise: false, new())]),
        }, twoPass: false);
        var asked = new ConcurrentBag<string?[]>();
        var actual = await ReadAsync(path, options, new ParquetScan
        {
            Columns = ["id", "s"], RowFilter = new RowFilter([Over("s", answer, valueWise: true, asked)]),
        }, twoPass: false);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
        Assert.Equal(Groups, asked.Count(values => values.Length > 0 && values[^1] is null));
        Assert.True(asked.Sum(values => values.Length) < Rows * 3 / 4);
    }

    /// <summary>
    /// A mask is free to set a NULL answer's value bit: that bit means nothing, so the row is still answered NULL. Here
    /// every NULL answer's value bit is set, over the dictionary and the fallback pages alike.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(700)]
    public async Task ANullAnswersValueBitIsIgnored(int batchSize)
    {
        string path = WriteFile("v1");
        var options = new ParquetReadOptions { BatchSize = batchSize > 0 ? batchSize : null };
        var answer = Answers["NULL for odd endings"];
        RowFilter Filter(bool valueWise)
        {
            var inner = Over("s", answer, valueWise, new());
            var predicate = new RowPredicate(["s"], batch => SetNullValueBits(inner.Evaluate(batch))) { ValueWise = valueWise };

            // A later predicate keeps the rows answered NULL live, so a NULL read as TRUE or FALSE changes what it sees.
            var later = new RowPredicate(["k"], batch => Map((Int32Array)batch.Column(0), v => v % 3 != 0));
            return new RowFilter([predicate, later]);
        }

        var expected = await ReadAsync(path, options, new ParquetScan { Columns = ["id"], RowFilter = Filter(false) }, twoPass: false);
        var actual = await ReadAsync(path, options, new ParquetScan { Columns = ["id"], RowFilter = Filter(true) }, twoPass: false);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);

        static BooleanArray SetNullValueBits(BooleanArray mask)
        {
            var values = new byte[(mask.Length + 7) / 8];
            for (int i = 0; i < mask.Length; i++)
            {
                if (mask.GetValue(i) != false)
                    values[i >> 3] |= (byte)(1 << (i & 7));
            }

            var validity = new byte[(mask.Length + 7) / 8];
            for (int i = 0; i < mask.Length; i++)
            {
                if (mask.IsValid(i))
                    validity[i >> 3] |= (byte)(1 << (i & 7));
            }

            return new BooleanArray(
                new ArrowBuffer(values), new ArrowBuffer(validity), mask.Length, mask.NullCount, 0);
        }
    }

    /// <summary>An INT32 column with no NULL row that falls back too.</summary>
    [Theory]
    [InlineData("v1", 0, false)]
    [InlineData("v1", 700, true)]
    [InlineData("v2", 700, false)]
    [InlineData("v1 page index", 300, true)]
    [InlineData("v1 no encoding_stats", 0, true)]
    public async Task AnIntegerColumnIsAnsweredAsItsRowsAre(string variant, int batchSize, bool returned)
    {
        string path = WriteFile(variant);
        var options = new ParquetReadOptions { BatchSize = batchSize > 0 ? batchSize : null };
        string[] projection = returned ? ["id", "k"] : ["id"];
        RowPredicate Predicate(bool valueWise, ConcurrentBag<int> lengths) => new(["k"], batch =>
        {
            lengths.Add(batch.Length);
            return Map((Int32Array)batch.Column(0), v => v < 30 || v % 7 == 0);
        }) { ValueWise = valueWise };

        var rowLengths = new ConcurrentBag<int>();
        var expected = await ReadAsync(path, options,
            new ParquetScan { Columns = projection, RowFilter = new RowFilter(Predicate(valueWise: false, rowLengths)) }, twoPass: false);
        var lengths = new ConcurrentBag<int>();
        var actual = await ReadAsync(path, options,
            new ParquetScan { Columns = projection, RowFilter = new RowFilter(Predicate(valueWise: true, lengths)) }, twoPass: false);

        Assert.Equal(expected, actual);
        Assert.NotEmpty(expected);
        Assert.Equal(Rows, rowLengths.Sum());

        // The dictionary-encoded pages' rows were answered from the dictionaries, not asked.
        Assert.True(lengths.Sum() < Rows * 3 / 4, $"asked over {lengths.Sum()} values");
    }

    // ---- Errors -----------------------------------------------------------------------------------------------------

    /// <summary>
    /// A value only an excluded row holds, on a page that fell back from the dictionary, raises when that page is
    /// answered: the column is then read as values and its live rows asked, which never include it. The batched
    /// read decodes the column again from the batch's first row, from the held chunk or (with a page index) from
    /// pages it fetches again.
    /// </summary>
    [Theory]
    [InlineData("v1", 0)]
    [InlineData("v1", 300)]
    [InlineData("v1", 2_500)]
    [InlineData("v2", 300)]
    [InlineData("v1 page index", 300)]
    [InlineData("v1 page index", 2_500)]
    [InlineData("v1 page index", 70)]
    [InlineData("v1 page index", 130)]
    [InlineData("v1", 70)]
    [InlineData("v1 no encoding_stats", 300)]
    public async Task AFallbackValueOnlyAnExcludedRowHoldsDoesNotFailTheRead(string variant, int batchSize)
    {
        string path = WriteFile(variant);
        var excluded = new Exclusions(Enumerable.Range(0, Rows).Where(IsBad).Select(r => (long)r));
        var options = new ParquetReadOptions { BatchSize = batchSize > 0 ? batchSize : null };

        var expected = await ReadAsync(path, options, new ParquetScan
        {
            Columns = ["id", "e"],
            RowFilter = new RowFilter([Over("e", RaisingOnBad, valueWise: false, new())]),
            ExcludedRows = excluded,
        }, twoPass: false);

        var asked = new ConcurrentBag<string?[]>();
        var actual = await ReadAsync(path, options, new ParquetScan
        {
            Columns = ["id", "e"],
            RowFilter = new RowFilter([Over("e", RaisingOnBad, valueWise: true, asked)]),
            ExcludedRows = excluded,
        }, twoPass: false);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
        Assert.Contains(asked, values => values.Contains("bad")); // a fallback page raised, and was declined
    }

    /// <summary>
    /// A chunk whose metadata names a dictionary encoding but which has no dictionary page (here a PLAIN chunk, with
    /// no encoding_stats to say so) is read as values and asked row by row.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(700)]
    public async Task AChunkWithNoDictionaryPageIsAskedRowByRow(int batchSize)
    {
        string path = WriteFile("v1 no encoding_stats");
        var options = new ParquetReadOptions { BatchSize = batchSize > 0 ? batchSize : null };
        Func<string?, bool?> answer = v => v == "cat-03";
        var expected = await ReadAsync(path, options, new ParquetScan
        {
            Columns = ["id"], RowFilter = new RowFilter([Over("p", answer, valueWise: false, new())]),
        }, twoPass: false);
        var asked = new ConcurrentBag<string?[]>();
        var actual = await ReadAsync(path, options, new ParquetScan
        {
            Columns = ["id"], RowFilter = new RowFilter([Over("p", answer, valueWise: true, asked)]),
        }, twoPass: false);

        Assert.Equal(Rows / 50, expected.Count);
        Assert.Equal(expected, actual);
        Assert.Equal(Rows, asked.Sum(values => values.Length));
    }

    /// <summary>A value a live row holds on a page that fell back from the dictionary raises, as row by row.</summary>
    [Theory]
    [InlineData("v1", 0)]
    [InlineData("v1", 300)]
    [InlineData("v1 page index", 300)]
    public async Task AFallbackValueALiveRowHoldsStillRaises(string variant, int batchSize)
    {
        string path = WriteFile(variant);
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => ReadAsync(
            path, new ParquetReadOptions { BatchSize = batchSize > 0 ? batchSize : null }, new ParquetScan
            {
                Columns = ["id"], RowFilter = new RowFilter([Over("e", RaisingOnBad, valueWise: true, new())]),
            }, twoPass: false));
        Assert.Contains("bad value", ex.ToString());
    }

    // ---- Helpers ----------------------------------------------------------------------------------------------------

    // Late in each row group, so on a page that fell back from the dictionary.
    private static bool IsBad(int row) => row % RowsPerGroup is 3_501 or 3_777;

    private static bool? RaisingOnBad(string? value) =>
        value == "bad" ? throw new InvalidOperationException("bad value") : value == "ok-1";

    private static RowPredicate Over(string column, Func<string?, bool?> answer, bool valueWise, ConcurrentBag<string?[]> asked) =>
        new([column], batch =>
        {
            var column = batch.Column(0);
            var values = Enumerable.Range(0, column.Length).Select(i => column switch
            {
                StringViewArray views => views.GetString(i),
                LargeStringArray large => large.GetString(i),
                _ => ((StringArray)column).GetString(i),
            }).ToArray();
            asked.Add(values);
            var builder = new BooleanArray.Builder().Reserve(values.Length);
            foreach (var value in values)
            {
                if (answer(value) is { } b)
                    builder.Append(b);
                else
                    builder.AppendNull();
            }

            return builder.Build();
        }) { ValueWise = valueWise };

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

    private static async Task<FileMetaData> ReadMetadataAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        return await reader.ReadMetadataAsync();
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
    /// id; s, 50 categories, 10% NULL, and a unique filler every fourth row; k, 0-99 and a unique filler every other
    /// row; e, "ok-0".."ok-6", a unique filler every fourth row, and "bad" late in each group. The fillers grow each
    /// chunk's dictionary past arrow-cpp's limit partway through, and the rest of the chunk is PLAIN.
    /// </summary>
    private string WriteFile(string variant)
    {
        string path = Path.Combine(_tempDir, variant.Replace(' ', '-') + ".parquet");
        if (File.Exists(path))
            return path;

        var builder = new ParquetSharp.WriterPropertiesBuilder()
            .DictionaryPagesizeLimit(6_000)
            .DataPagesize(1_024)
            .WriteBatchSize(100)
            .DataPageVersion(variant.StartsWith("v2", StringComparison.Ordinal)
                ? ParquetSharp.ParquetDataPageVersion.V2
                : ParquetSharp.ParquetDataPageVersion.V1);
        builder = variant.Contains("page index") ? builder.EnableWritePageIndex() : builder.DisableWritePageIndex();
        builder = builder.DisableDictionary("p");

        var columns = new ParquetSharp.Column[]
        {
            new ParquetSharp.Column<long>("id"),
            new ParquetSharp.Column<string>("s"),
            new ParquetSharp.Column<int>("k"),
            new ParquetSharp.Column<string>("e"),
            new ParquetSharp.Column<string>("p"),
        };

        using (var properties = builder.Build())
        using (var writer = new ParquetSharp.ParquetFileWriter(path, columns, properties))
        {
            for (int g = 0; g < Groups; g++)
            {
                var rows = Enumerable.Range(g * RowsPerGroup, RowsPerGroup).ToArray();
                using var rowGroup = writer.AppendRowGroup();
                Write(rowGroup, rows.Select(r => (long)r).ToArray());
                Write(rowGroup, rows.Select(r => r % 10 == 0 ? null
                    : r % 4 == 1 ? "fill-" + r.ToString(CultureInfo.InvariantCulture)
                    : "cat-" + (r % 50).ToString("D2", CultureInfo.InvariantCulture)).ToArray());
                Write(rowGroup, rows.Select(r => r % 2 == 1 ? 10_000 + r : r % 100).ToArray());
                Write(rowGroup, rows.Select(r => IsBad(r) ? "bad"
                    : r % 4 == 1 ? "fill-" + r.ToString(CultureInfo.InvariantCulture)
                    : "ok-" + (r % 7).ToString(CultureInfo.InvariantCulture)).ToArray());
                Write(rowGroup, rows.Select(r => "cat-" + (r % 50).ToString("D2", CultureInfo.InvariantCulture)).ToArray());
            }

            writer.Close();
        }

        if (variant.Contains("no encoding_stats"))
        {
            // Without stats, p (PLAIN, no dictionary page) also claims a dictionary encoding, as a writer that lists
            // encodings loosely might: the reader finds no dictionary page and reads its values.
            static ColumnMetaData Strip(ColumnMetaData meta)
            {
                var stripped = FooterRewrite.With(meta, nameof(ColumnMetaData.EncodingStats), null);
                return meta.PathInSchema[0] == "p"
                    ? FooterRewrite.With(stripped, nameof(ColumnMetaData.Encodings),
                        (IReadOnlyList<Encoding>)[.. meta.Encodings, Encoding.RleDictionary])
                    : stripped;
            }

            FooterRewrite.Rewrite(path, metadata => FooterRewrite.With(metadata, nameof(FileMetaData.RowGroups),
                metadata.RowGroups.Select(rowGroup => FooterRewrite.With(rowGroup, nameof(RowGroup.Columns),
                    rowGroup.Columns.Select(chunk => FooterRewrite.With(chunk, nameof(ColumnChunk.MetaData),
                        Strip(chunk.MetaData!))).ToArray())).ToArray()));
        }

        return path;
    }

    // Writes one column in slices of 100 rows: arrow-cpp measures the dictionary after each, so it falls back mid-chunk.
    private static void Write<T>(ParquetSharp.RowGroupWriter rowGroup, T[] values)
    {
        using var column = rowGroup.NextColumn().LogicalWriter<T>();
        for (int start = 0; start < values.Length; start += 100)
            column.WriteBatch(values.AsSpan(start, Math.Min(100, values.Length - start)));
    }
}
