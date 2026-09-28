// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Tests.Parquet.Interop;
using EngineeredWood.Tests.Parquet.Metadata;
using Xunit.Abstractions;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Page pruning (<see cref="ParquetFileReader.GetCandidateRowRangesAsync"/>, doc/parquet-page-index.md
/// R-2): the rows of a row group that the page index cannot rule out.
/// </summary>
/// <remarks>
/// The contract is a superset, so the main oracle evaluates each predicate row by row
/// (<see cref="ArrowRowEvaluator"/>) and requires every matching row to lie in a returned range. That
/// alone would pass for a pruner that never prunes, so the targeted cases pin exact ranges, and the
/// DataFusion case checks the kept row counts against an independent implementation.
/// </remarks>
public class PageIndexPruningTests : IDisposable
{
    private readonly string _tempDir;
    private readonly ITestOutputHelper _output;

    public PageIndexPruningTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-pagepruning-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // ───── Superset over every fixture with a page index ─────

    /// <summary>
    /// For predicates built from values each indexed column actually holds, every row that matches lies
    /// in a returned range. The fixtures come from parquet-mr, parquet-cpp and parquet-rs, and include
    /// null pages, NaN counts and truncated bounds.
    /// </summary>
    [Theory]
    [MemberData(nameof(PageIndexCodecTests.FixturesWithPageIndexes), MemberType = typeof(PageIndexCodecTests))]
    public async Task Fixture_EveryMatchingRowIsInARange(string fileName)
    {
        await using var input = new LocalRandomAccessFile(TestData.GetPath(fileName));
        using var reader = new ParquetFileReader(input, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        int checkedPredicates = 0;
        long keptRows = 0, candidateRows = 0;
        for (int g = 0; g < metadata.RowGroups.Count; g++)
        {
            RecordBatch batch;
            try
            {
                batch = await reader.ReadRowGroupAsync(g);
            }
            catch (Exception e) when (Unreadable(e))
            {
                _output.WriteLine($"row group {g} unreadable here ({e.GetType().Name}); skipped");
                continue;
            }

            using (batch)
            {
                foreach (var predicate in PredicatesFor(batch))
                {
                    BooleanArray matches;
                    try
                    {
                        matches = new ArrowRowEvaluator().EvaluatePredicate(predicate, batch);
                    }
                    catch (Exception)
                    {
                        continue; // a type the row evaluator does not take; not this test's subject
                    }

                    var ranges = await reader.GetCandidateRowRangesAsync(g, predicate);
                    AssertWellFormed(ranges, batch.Length);
                    for (int row = 0; row < batch.Length; row++)
                    {
                        if (matches.GetValue(row) == true)
                        {
                            Assert.True(ranges.Any(r => r.Start <= row && row < r.End),
                                $"{fileName} row group {g}: row {row} matches {predicate} but no range holds it");
                        }
                    }

                    checkedPredicates++;
                    candidateRows += batch.Length;
                    keptRows += ranges.Sum(r => r.Length);
                }
            }
        }

        _output.WriteLine($"{checkedPredicates} predicates; kept {keptRows:N0} of {candidateRows:N0} rows");
    }

    /// <summary>The fixture oracle above is not vacuous: on these, page pruning drops rows.</summary>
    [Theory]
    [InlineData("alltypes_tiny_pages.parquet", "id", 1234)]
    [InlineData("alltypes_tiny_pages_plain.parquet", "id", 1234)]
    public async Task Fixture_PrunesASortedColumn(string fileName, string column, int value)
    {
        await using var input = new LocalRandomAccessFile(TestData.GetPath(fileName));
        using var reader = new ParquetFileReader(input, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        long kept = 0;
        for (int g = 0; g < metadata.RowGroups.Count; g++)
            kept += (await reader.GetCandidateRowRangesAsync(g, Ex.Equal(column, LiteralValue.Of(value)))).Sum(r => r.Length);

        _output.WriteLine($"kept {kept} of {metadata.NumRows} rows");
        Assert.InRange(kept, 1, metadata.NumRows / 10);
    }

    // ───── Exact ranges on files EW writes ─────

    [Fact]
    public async Task SortedColumn_KeepsTheCoveringPages()
    {
        string path = await WriteAsync(Sorted(1000), RowCapped(100));
        using var reader = Open(path);

        Assert.Equal([new RowRange(200, 300)], await reader.GetCandidateRowRangesAsync(0, Ex.Equal("x", LiteralValue.Of(250))));
        Assert.Equal([new RowRange(900, 1000)], await reader.GetCandidateRowRangesAsync(0, Ex.GreaterThanOrEqual("x", LiteralValue.Of(950))));
        Assert.Equal([new RowRange(0, 100), new RowRange(700, 800)],
            await reader.GetCandidateRowRangesAsync(0, Ex.Or(
                Ex.LessThan("x", LiteralValue.Of(5)), Ex.Equal("x", LiteralValue.Of(777)))));
        Assert.Equal([new RowRange(300, 500)],
            await reader.GetCandidateRowRangesAsync(0, Ex.In("x", LiteralValue.Of(310), LiteralValue.Of(499))));
        Assert.Equal([new RowRange(0, 300), new RowRange(400, 1000)],
            await reader.GetCandidateRowRangesAsync(0, Ex.Not(Ex.And(
                Ex.GreaterThanOrEqual("x", LiteralValue.Of(300)), Ex.LessThan("x", LiteralValue.Of(400))))));
    }

    /// <summary>
    /// Two columns cut at different rows (int32 pages of 256 rows, int64 pages of 128): an AND keeps
    /// only the intervals where both columns' pages allow it.
    /// </summary>
    [Fact]
    public async Task TwoColumns_WithDifferentPageBoundaries()
    {
        string path = await WriteAsync(Sorted(1000), ParquetWriteOptions.Default with
        {
            DataPageSize = 1024,
            DictionaryEnabled = false,
            DataPageRowCountLimit = null,
        });
        using var reader = Open(path);
        var index = await reader.ReadPageIndexAsync(0, ["x", "y"]);
        Assert.Equal(256, index[0].OffsetIndex!.PageLocations[1].FirstRowIndex);
        Assert.Equal(128, index[1].OffsetIndex!.PageLocations[1].FirstRowIndex);

        // x >= 300 rules out x's first page [0, 256); y < 700 rules out y's pages from [768, 896) on.
        Assert.Equal([new RowRange(256, 768)],
            await reader.GetCandidateRowRangesAsync(0, Ex.And(
                Ex.GreaterThanOrEqual("x", LiteralValue.Of(300)), Ex.LessThan("y", LiteralValue.Of(700L)))));
    }

    [Fact]
    public async Task NullPages_AndNullCounts()
    {
        var values = new Int32Array.Builder();
        for (int i = 0; i < 500; i++)
        {
            if (i is >= 200 and < 300)
                values.AppendNull();
            else if (i is >= 400 && i % 2 == 0)
                values.AppendNull();
            else
                values.Append(i);
        }

        string path = await WriteAsync(Batch("c", values.Build(), nullable: true), RowCapped(100));
        using var reader = Open(path);

        // [200, 300) is a null page; [400, 500) holds some nulls.
        Assert.Equal([new RowRange(0, 200), new RowRange(300, 500)],
            await reader.GetCandidateRowRangesAsync(0, Ex.IsNotNull("c")));
        Assert.Equal([new RowRange(200, 300), new RowRange(400, 500)],
            await reader.GetCandidateRowRangesAsync(0, Ex.IsNull("c")));
        Assert.Equal([new RowRange(100, 200)],
            await reader.GetCandidateRowRangesAsync(0, Ex.Equal("c", LiteralValue.Of(150))));
    }

    /// <summary>
    /// A page's null count is the whole page's. With another column cutting that page in two, each
    /// half has as many rows as the page has nulls, and only one half holds them: taking the count as
    /// the half's would call the other half all null and drop its values.
    /// </summary>
    [Fact]
    public async Task PageNullCount_DoesNotDescribeAPartOfThePage()
    {
        var c = new Int32Array.Builder();
        var x = new Int64Array.Builder();
        for (int i = 0; i < 200; i++)
        {
            if (i is >= 50 and < 100) c.AppendNull(); else c.Append(i);
            x.Append(i);
        }

        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("c", Int32Type.Default, true))
                .Field(new Field("x", Int64Type.Default, false))
                .Build(),
            [c.Build(), x.Build()], 200);

        // 400 bytes at plain width: int32 pages of 100 rows, int64 pages of 50.
        string path = await WriteAsync(batch, ParquetWriteOptions.Default with
        {
            DataPageSize = 400,
            DictionaryEnabled = false,
            DataPageRowCountLimit = null,
        });
        using var reader = Open(path);
        var index = await reader.ReadPageIndexAsync(0, ["c", "x"]);
        Assert.Equal([0L, 100], index[0].OffsetIndex!.PageLocations.Select(p => p.FirstRowIndex));
        Assert.Equal([0L, 50, 100, 150], index[1].OffsetIndex!.PageLocations.Select(p => p.FirstRowIndex));
        Assert.Equal(50, index[0].ColumnIndex!.NullCounts![0]);

        Assert.Equal([new RowRange(0, 200)],
            await reader.GetCandidateRowRangesAsync(0, Ex.And(Ex.IsNotNull("c"), Ex.GreaterThanOrEqual("x", LiteralValue.Of(0L)))));
    }

    /// <summary>
    /// NaN sits above every value in SQL's order, so a page whose finite bounds rule out <c>d &gt; 50</c>
    /// must still be kept when it holds a NaN, and dropped when its NaN count is zero.
    /// </summary>
    [Fact]
    public async Task NaN_KeepsOnlyThePageThatHoldsOne()
    {
        var values = new DoubleArray.Builder();
        for (int i = 0; i < 400; i++)
            values.Append(i == 150 ? double.NaN : i % 10);

        string path = await WriteAsync(Batch("d", values.Build(), nullable: false), RowCapped(100) with { DictionaryEnabled = false });
        using var reader = Open(path);
        var nanCounts = (await reader.ReadPageIndexAsync(0, ["d"]))[0].ColumnIndex!.NanCounts;
        Assert.NotNull(nanCounts);

        Assert.Equal([new RowRange(100, 200)],
            await reader.GetCandidateRowRangesAsync(0, Ex.GreaterThan("d", LiteralValue.Of(50.0))));
        Assert.Equal([new RowRange(100, 200)],
            await reader.GetCandidateRowRangesAsync(0, Ex.IsNaN(Ex.Ref("d"))));
    }

    /// <summary>
    /// Strings that differ only past the 64-byte truncation keep every page, correctly; strings that
    /// differ early prune.
    /// </summary>
    [Fact]
    public async Task TruncatedStringBounds()
    {
        var early = new StringArray.Builder();
        var late = new StringArray.Builder();
        for (int i = 0; i < 400; i++)
        {
            early.Append(i.ToString("D4") + new string('p', 70));
            late.Append(new string('p', 70) + i.ToString("D4"));
        }

        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("early", StringType.Default, false))
                .Field(new Field("late", StringType.Default, false))
                .Build(),
            [early.Build(), late.Build()], 400);
        string path = await WriteAsync(batch, RowCapped(100) with { DictionaryEnabled = false });
        using var reader = Open(path);

        Assert.Equal([new RowRange(200, 300)],
            await reader.GetCandidateRowRangesAsync(0, Ex.Equal("early", LiteralValue.Of("0250" + new string('p', 70)))));
        Assert.Equal([new RowRange(0, 400)],
            await reader.GetCandidateRowRangesAsync(0, Ex.Equal("late", LiteralValue.Of(new string('p', 70) + "0250"))));
    }

    [Fact]
    public async Task RepeatedColumn_DoesNotNarrow_ButAnotherColumnStillDoes()
    {
        var list = new ListArray.Builder(Int32Type.Default);
        var elements = (Int32Array.Builder)list.ValueBuilder;
        var x = new Int32Array.Builder();
        for (int i = 0; i < 400; i++)
        {
            list.Append();
            elements.Append(i);
            x.Append(i);
        }

        var listArray = list.Build();
        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("l", listArray.Data.DataType, true))
                .Field(new Field("x", Int32Type.Default, false))
                .Build(),
            [listArray, x.Build()], 400);
        string path = await WriteAsync(batch, RowCapped(100));
        using var reader = Open(path);

        Assert.Equal([new RowRange(0, 400)],
            await reader.GetCandidateRowRangesAsync(0, Ex.Equal("l.list.element", LiteralValue.Of(250))));
        Assert.Equal([new RowRange(200, 300)],
            await reader.GetCandidateRowRangesAsync(0, Ex.And(
                Ex.Equal("l.list.element", LiteralValue.Of(250)), Ex.Equal("x", LiteralValue.Of(250)))));
    }

    [Fact]
    public async Task NoPageIndex_OneWholeRange()
    {
        string path = await WriteAsync(Sorted(1000), RowCapped(100) with { WritePageIndex = false });
        using var reader = Open(path);

        Assert.Equal([new RowRange(0, 1000)], await reader.GetCandidateRowRangesAsync(0, Ex.Equal("x", LiteralValue.Of(250))));
    }

    [Fact]
    public async Task RowGroupRuledOut_NoRanges_AndAllMatching_OneWholeRange()
    {
        string path = await WriteAsync(Sorted(1000), RowCapped(100) with { RowGroupMaxRows = 500 });
        using var reader = Open(path);

        Assert.Empty(await reader.GetCandidateRowRangesAsync(0, Ex.Equal("x", LiteralValue.Of(750))));
        Assert.Equal([new RowRange(0, 500)], await reader.GetCandidateRowRangesAsync(1, Ex.GreaterThanOrEqual("x", LiteralValue.Of(0))));
        Assert.Equal([new RowRange(200, 300)], await reader.GetCandidateRowRangesAsync(1, Ex.Equal("x", LiteralValue.Of(750))));
    }

    [Fact]
    public async Task UnknownColumn_OneWholeRange()
    {
        string path = await WriteAsync(Sorted(1000), RowCapped(100));
        using var reader = Open(path);

        Assert.Equal([new RowRange(0, 1000)], await reader.GetCandidateRowRangesAsync(0, Ex.Equal("missing", LiteralValue.Of(1))));
    }

    /// <summary>
    /// A ColumnIndex overwritten with garbage is ignored, not refused: the whole row group comes back,
    /// as it would from a reader that never looked.
    /// </summary>
    [Fact]
    public async Task CorruptColumnIndex_IsIgnored()
    {
        string path = await WriteAsync(Sorted(1000), RowCapped(100));
        ColumnChunk chunk;
        using (var probe = Open(path))
            chunk = (await probe.ReadMetadataAsync()).RowGroups[0].Columns[0];
        byte[] bytes = File.ReadAllBytes(path);
        bytes.AsSpan(checked((int)chunk.ColumnIndexOffset!.Value), chunk.ColumnIndexLength!.Value).Fill(0xFF);
        File.WriteAllBytes(path, bytes);

        using var reader = Open(path);
        Assert.Equal([new RowRange(0, 1000)], await reader.GetCandidateRowRangesAsync(0, Ex.Equal("x", LiteralValue.Of(250))));
    }

    /// <summary>
    /// A footer that places the ColumnIndex outside the file fails <see cref="ParquetFileReader.ReadPageIndexAsync"/>
    /// with a format error; candidate ranges treat it like any other unusable index.
    /// </summary>
    [Fact]
    public async Task ColumnIndexOutsideTheFile_IsIgnored()
    {
        string path = await WriteAsync(Sorted(1000), RowCapped(100));
        long length = new FileInfo(path).Length;
        FooterRewrite.Rewrite(path, metadata =>
        {
            var rowGroup = metadata.RowGroups[0];
            var x = FooterRewrite.With(rowGroup.Columns[0], nameof(ColumnChunk.ColumnIndexOffset), (long?)(length * 2));
            var columns = new[] { x, rowGroup.Columns[1] };
            return FooterRewrite.With(metadata, nameof(FileMetaData.RowGroups),
                new[] { FooterRewrite.With(rowGroup, nameof(RowGroup.Columns), columns) });
        });

        using var reader = Open(path);
        await Assert.ThrowsAsync<ParquetFormatException>(async () => await reader.ReadPageIndexAsync(0, ["x"]));
        Assert.Equal([new RowRange(0, 1000)], await reader.GetCandidateRowRangesAsync(0, Ex.Equal("x", LiteralValue.Of(250))));
    }

    /// <summary>
    /// parquet-mr 1.13 wrote these fixtures' ColumnIndex as a placeholder: every page null, null counts
    /// of -1, empty bounds, on required columns full of values. Trusted, it drops every row of
    /// <c>a IS NOT NULL</c>; it must be ignored instead.
    /// </summary>
    [Theory]
    [InlineData("datapage_v1-uncompressed-checksum.parquet")]
    [InlineData("datapage_v1-snappy-compressed-checksum.parquet")]
    public async Task ImpossibleCounts_TheIndexIsIgnored(string fileName)
    {
        await using var input = new LocalRandomAccessFile(TestData.GetPath(fileName));
        using var reader = new ParquetFileReader(input, ownsFile: false);
        var columnIndex = (await reader.ReadPageIndexAsync(0, ["a"]))[0].ColumnIndex!;
        Assert.All(columnIndex.NullPages, Assert.True);
        long rows = (await reader.ReadMetadataAsync()).RowGroups[0].NumRows;

        Assert.Equal([new RowRange(0, rows)], await reader.GetCandidateRowRangesAsync(0, Ex.IsNotNull("a")));
    }

    /// <summary>
    /// Without this, the exact-range tests could pass against a pruner that ignored the index bounds and
    /// cut by position. Lowering one page's maximum below a value that page holds makes the pruner drop
    /// the page, so the matching row is lost.
    /// </summary>
    [Fact]
    public async Task Mutation_LoweredMaximum_DropsThePage()
    {
        string path = await WriteAsync(Sorted(1000), RowCapped(100));
        using var reader = Open(path);
        var metadata = await reader.ReadMetadataAsync();
        var schema = new EngineeredWood.Parquet.Schema.SchemaDescriptor(metadata.Schema);
        var index = await reader.ReadPageIndexAsync(0, ["x"]);
        var columnIndex = index[0].ColumnIndex!;

        var maxValues = columnIndex.MaxValues.ToArray();
        maxValues[2] = BitConverter.GetBytes(240);
        var tampered = Tamper(index[0], ci => FooterRewrite.With(ci, nameof(ColumnIndex.MaxValues), maxValues));
        var ranges = PageIndexPruner.SelectRows(Ex.Equal("x", LiteralValue.Of(250)), metadata.RowGroups[0], schema, [tampered]);

        Assert.Empty(ranges);
    }

    /// <summary>
    /// A NaN count that cannot be true discredits the whole index, as an impossible null count does:
    /// the rows come back whole rather than narrowed by bounds from the same index. Rows 0-49 and the
    /// page [200, 300) are null; without tampering, <c>d = 350</c> keeps only [300, 400).
    /// </summary>
    [Theory]
    [InlineData(1, 101)] // more NaNs than the page's 100 rows
    [InlineData(0, 60)]  // 50 nulls and 60 NaNs in 100 rows
    [InlineData(2, 1)]   // a NaN in a null page
    public async Task ImpossibleNanCounts_TheIndexIsIgnored(int page, long nans)
    {
        var values = new DoubleArray.Builder();
        for (int i = 0; i < 400; i++)
        {
            if (i < 50 || i is >= 200 and < 300) values.AppendNull(); else values.Append(i);
        }

        string path = await WriteAsync(Batch("d", values.Build(), nullable: true), RowCapped(100) with { DictionaryEnabled = false });
        using var reader = Open(path);
        var metadata = await reader.ReadMetadataAsync();
        var schema = new EngineeredWood.Parquet.Schema.SchemaDescriptor(metadata.Schema);
        var index = (await reader.ReadPageIndexAsync(0, ["d"]))[0];
        var filter = Ex.Equal("d", LiteralValue.Of(350.0));
        Assert.Equal([new RowRange(300, 400)], PageIndexPruner.SelectRows(filter, metadata.RowGroups[0], schema, [index]));

        var nanCounts = index.ColumnIndex!.NanCounts!.ToArray();
        nanCounts[page] = nans;
        var tampered = Tamper(index, ci => FooterRewrite.With(ci, nameof(ColumnIndex.NanCounts), nanCounts));

        Assert.Equal([new RowRange(0, 400)], PageIndexPruner.SelectRows(filter, metadata.RowGroups[0], schema, [tampered]));
    }

    // ───── DataFusion ─────

    /// <summary>
    /// DataFusion prunes with the same index: for single-column predicates on a file EW writes, the rows
    /// it keeps equal the rows this pruner keeps.
    /// </summary>
    [SkippableFact]
    public async Task DataFusion_KeepsTheSameRows()
    {
        ExternalParquetReaders.Require();

        string path = await WriteAsync(Sorted(900), RowCapped(100));
        using var reader = Open(path);
        var cases = new (string Sql, Predicate Filter)[]
        {
            ("x = 250", Ex.Equal("x", LiteralValue.Of(250))),
            ("x >= 650", Ex.GreaterThanOrEqual("x", LiteralValue.Of(650))),
            ("x < 150 OR x > 849", Ex.Or(Ex.LessThan("x", LiteralValue.Of(150)), Ex.GreaterThan("x", LiteralValue.Of(849)))),
            ("y BETWEEN 120 AND 480", Ex.And(Ex.GreaterThanOrEqual("y", LiteralValue.Of(120L)), Ex.LessThanOrEqual("y", LiteralValue.Of(480L)))),
        };

        foreach (var (sql, filter) in cases)
        {
            long ours = (await reader.GetCandidateRowRangesAsync(0, filter)).Sum(r => r.Length);
            var (metrics, error) = ExternalParquetReaders.DataFusionPageIndexMetrics(path, sql);
            Assert.True(metrics is not null, error);
            Assert.True(metrics!.Found, $"'{sql}': DataFusion reported no page-index pruning");
            _output.WriteLine($"{sql,-28} ours {ours}, DataFusion {metrics.RowsMatched} of {metrics.RowsTotal}");
            Assert.Equal(metrics.RowsMatched, ours);
        }
    }

    // ───── Helpers ─────

    private static ParquetWriteOptions RowCapped(int rows) =>
        ParquetWriteOptions.Default with { DataPageRowCountLimit = rows };

    /// <summary>x = row as int32, y = row as int64.</summary>
    private static RecordBatch Sorted(int rows)
    {
        var x = new Int32Array.Builder();
        var y = new Int64Array.Builder();
        for (int i = 0; i < rows; i++)
        {
            x.Append(i);
            y.Append(i);
        }

        return new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("x", Int32Type.Default, false))
                .Field(new Field("y", Int64Type.Default, false))
                .Build(),
            [x.Build(), y.Build()], rows);
    }

    private static RecordBatch Batch(string name, IArrowArray column, bool nullable) =>
        new(new Apache.Arrow.Schema.Builder().Field(new Field(name, column.Data.DataType, nullable)).Build(),
            [column], column.Length);

    private async Task<string> WriteAsync(RecordBatch batch, ParquetWriteOptions options)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static ParquetFileReader Open(string path) =>
        new(new LocalRandomAccessFile(path), ownsFile: true);

    private static void AssertWellFormed(IReadOnlyList<RowRange> ranges, long rows)
    {
        long previousEnd = -1;
        foreach (var range in ranges)
        {
            Assert.True(range.Start < range.End, $"empty range {range}");
            Assert.True(range.Start > previousEnd, $"ranges overlap or touch at {range}");
            Assert.True(range.Start >= 0 && range.End <= rows, $"{range} outside {rows} rows");
            previousEnd = range.End;
        }
    }

    /// <summary>
    /// Comparisons and null tests on values each top-level flat column holds, at five positions, plus
    /// an AND and an OR across the first two such columns.
    /// </summary>
    private static IEnumerable<Predicate> PredicatesFor(RecordBatch batch)
    {
        var picks = new List<(string Column, LiteralValue Value)>();
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            string name = batch.Schema.GetFieldByIndex(c).Name;
            var array = batch.Column(c);
            yield return Ex.IsNull(name);
            yield return Ex.IsNotNull(name);

            bool picked = false;
            foreach (int row in new[] { 0, batch.Length / 3, batch.Length / 2, 2 * batch.Length / 3, batch.Length - 1 })
            {
                if (row < 0 || row >= array.Length || array.IsNull(row) || LiteralAt(array, row) is not { } value)
                    continue;
                yield return Ex.Equal(name, value);
                yield return Ex.LessThan(name, value);
                yield return Ex.GreaterThanOrEqual(name, value);
                yield return Ex.NotEqual(name, value);
                if (!picked)
                {
                    picks.Add((name, value));
                    picked = true;
                }
            }
        }

        if (picks.Count >= 2)
        {
            yield return Ex.And(Ex.GreaterThanOrEqual(picks[0].Column, picks[0].Value), Ex.LessThan(picks[1].Column, picks[1].Value));
            yield return Ex.Or(Ex.Equal(picks[0].Column, picks[0].Value), Ex.Equal(picks[1].Column, picks[1].Value));
        }
    }

    /// <summary>
    /// A row group this build cannot read (FLOAT16 on .NET Framework, for one), which the parallel column
    /// read reports inside an <see cref="AggregateException"/>.
    /// </summary>
    private static bool Unreadable(Exception e) => e switch
    {
        NotSupportedException or ParquetFormatException => true,
        AggregateException aggregate => aggregate.InnerExceptions.All(Unreadable),
        _ => false,
    };

    private static LiteralValue? LiteralAt(IArrowArray array, int row)
    {
        try
        {
            return LiteralOf(array, row);
        }
        catch (Exception e) when (e is OverflowException or ArgumentOutOfRangeException)
        {
            return null; // a decimal wider than System.Decimal, or a timestamp outside DateTimeOffset
        }
    }

    private static LiteralValue? LiteralOf(IArrowArray array, int row) => array switch
    {
        // Decimals, dates and timestamps are where bound decoding has gone wrong before (#398, #412).
        Decimal128Array a => (LiteralValue?)LiteralValue.Of(a.GetValue(row)!.Value),
        TimestampArray a => LiteralValue.Of(a.GetTimestamp(row)!.Value),
#if NET6_0_OR_GREATER
        Date32Array a => LiteralValue.Of(a.GetDateOnly(row)!.Value),
#endif
        Int8Array a => (LiteralValue?)LiteralValue.Of((int)a.GetValue(row)!.Value),
        Int16Array a => LiteralValue.Of((int)a.GetValue(row)!.Value),
        Int32Array a => LiteralValue.Of(a.GetValue(row)!.Value),
        Int64Array a => LiteralValue.Of(a.GetValue(row)!.Value),
        UInt32Array a => LiteralValue.Of(a.GetValue(row)!.Value),
        UInt64Array a => LiteralValue.Of(a.GetValue(row)!.Value),
        FloatArray a => LiteralValue.Of(a.GetValue(row)!.Value),
        DoubleArray a => LiteralValue.Of(a.GetValue(row)!.Value),
        BooleanArray a => LiteralValue.Of(a.GetValue(row)!.Value),
        StringArray a => LiteralValue.Of(a.GetString(row)),
        BinaryArray a => LiteralValue.Of(a.GetBytes(row).ToArray()),
        Apache.Arrow.Arrays.FixedSizeBinaryArray a => LiteralValue.Of(a.GetBytes(row).ToArray()),
        _ => null,
    };

    /// <summary>
    /// A page index with its ColumnIndex edited, re-encoded so that it decodes as a file's would.
    /// </summary>
    private static ColumnChunkPageIndex Tamper(ColumnChunkPageIndex original, Func<ColumnIndex, ColumnIndex> edit) =>
        new(original.Column, original.Path,
            MetadataEncoder.EncodeColumnIndex(edit(original.ColumnIndex!)),
            MetadataEncoder.EncodeOffsetIndex(original.OffsetIndex!));
}
