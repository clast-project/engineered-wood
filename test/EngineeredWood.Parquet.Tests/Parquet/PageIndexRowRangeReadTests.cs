// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Tests.Parquet.Metadata;
using Xunit.Abstractions;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Row-range reads (<see cref="ParquetFileReader.ReadRowRangesAsync"/>, doc/parquet-page-index.md R-3):
/// only the rows in the ranges, reading only their pages, and
/// <see cref="ParquetReadOptions.FilterUsePageIndex"/>, which reads through them.
/// </summary>
/// <remarks>
/// The oracle is a full read of the same row group: the rows a range read returns must be exactly
/// the full read's rows at those positions, value for value. The byte counts show the pages outside
/// the ranges are not fetched.
/// </remarks>
public class PageIndexRowRangeReadTests : IDisposable
{
    private const int Rows = 5000;

    private readonly string _tempDir;
    private readonly ITestOutputHelper _output;

    public PageIndexRowRangeReadTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-rowranges-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // ───── The rows are the full read's rows ─────

    public static TheoryData<DataPageVersion, bool, int?, long?> Layouts()
    {
        var data = new TheoryData<DataPageVersion, bool, int?, long?>();
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (bool dictionary in new[] { true, false })
        {
            data.Add(version, dictionary, null, null);
            data.Add(version, dictionary, 97, null);
            data.Add(version, dictionary, null, 4096);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task RangeRead_ReturnsExactlyTheRangesRows(DataPageVersion version, bool dictionary, int? batchSize, long? maxBytes)
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            DataPageVersion = version,
            DictionaryEnabled = dictionary,
            DataPageRowCountLimit = 250,
        });
        var options = new ParquetReadOptions { BatchSize = batchSize, MaxBatchByteSize = maxBytes };

        var expected = await FullRowsAsync(path, 0);
        foreach (var ranges in RangeSets(Rows))
            await AssertRangeReadAsync(path, options, 0, ranges, expected, batchSize);
    }

    /// <summary>Without an OffsetIndex, pages are found by scanning headers; the rows are the same.</summary>
    [Fact]
    public async Task NoPageIndex_SameRows()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            WritePageIndex = false,
            DataPageRowCountLimit = 250,
        });

        var expected = await FullRowsAsync(path, 0);
        foreach (var ranges in RangeSets(Rows))
            await AssertRangeReadAsync(path, ParquetReadOptions.Default, 0, ranges, expected, batchSize: null);
    }

    /// <summary>
    /// A row group with a nested column is decoded whole and cut to the ranges: the rows are the same,
    /// lists and structs included.
    /// </summary>
    [Fact]
    public async Task NestedColumns_SameRows()
    {
        string path = await WriteAsync(WithNested(Flat(Rows)), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });

        var expected = await FullRowsAsync(path, 0);
        foreach (var ranges in RangeSets(Rows))
        {
            await AssertRangeReadAsync(path, ParquetReadOptions.Default, 0, ranges, expected, batchSize: null);
            await AssertRangeReadAsync(path, new ParquetReadOptions { BatchSize = 97 }, 0, ranges, expected, batchSize: 97);
        }
    }

    /// <summary>
    /// Every fixture with a page index: a few ranges in each row group read the same rows as a full
    /// read. The fixtures come from parquet-mr, parquet-cpp and parquet-rs, with dictionary, delta and
    /// nested encodings EW's writer does not produce.
    /// </summary>
    [Theory]
    [MemberData(nameof(PageIndexCodecTests.FixturesWithPageIndexes), MemberType = typeof(PageIndexCodecTests))]
    public async Task Fixture_SameRows(string fileName)
    {
        string path = TestData.GetPath(fileName);
        using var probe = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true);
        var metadata = await probe.ReadMetadataAsync();

        int checkedGroups = 0;
        for (int g = 0; g < metadata.RowGroups.Count; g++)
        {
            int rows = checked((int)metadata.RowGroups[g].NumRows);
            if (rows == 0)
                continue;

            List<string> expected;
            try
            {
                expected = await FullRowsAsync(path, g);
            }
            catch (Exception e) when (Unreadable(e))
            {
                _output.WriteLine($"row group {g} unreadable here ({e.GetType().Name}); skipped");
                continue;
            }

            var ranges = rows < 5
                ? new[] { new RowRange(0, rows) }
                : new[] { new RowRange(rows / 5, 2 * rows / 5), new RowRange(3 * rows / 5, 3 * rows / 5 + 1), new RowRange(rows - 1, rows) };
            await AssertRangeReadAsync(path, ParquetReadOptions.Default, g, ranges, expected, batchSize: null);
            await AssertRangeReadAsync(path, new ParquetReadOptions { BatchSize = 7 }, g, ranges, expected, batchSize: 7);
            checkedGroups++;
        }

        _output.WriteLine($"{checkedGroups} row groups checked");
    }

    // ───── Only the ranges' pages are read ─────

    /// <summary>One 250-row page of 20 is about 5% of the data; the read fetches little more.</summary>
    [Fact]
    public async Task OnePage_FetchesAFractionOfTheBytes()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });

        long whole = await BytesReadAsync(path, reader => reader.ReadRowGroupBatchesAsync(0));
        long one = await BytesReadAsync(path, reader => reader.ReadRowRangesAsync(0, [new RowRange(1000, 1250)]));

        _output.WriteLine($"one page {one:N0} bytes, whole row group {whole:N0}");
        Assert.True(one < whole / 5, $"one page read {one:N0} of {whole:N0} bytes");
    }

    // ───── FilterUsePageIndex ─────

    [Fact]
    public async Task FilterUsePageIndex_ReadsOnlyTheCandidateRows()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        var filter = Ex.And(
            Ex.GreaterThanOrEqual("sorted", LiteralValue.Of(1100L)),
            Ex.LessThan("sorted", LiteralValue.Of(1300L)));

        var on = await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true });
        var off = await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter });
        var expected = await FullRowsAsync(path, 0);

        // The pages [1000, 1250) and [1250, 1500).
        Assert.Equal(expected.Skip(1000).Take(500), on);
        Assert.Equal(expected, off);

        long onBytes = await BytesReadAsync(path, reader => reader.ReadAllAsync(),
            new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true });
        long offBytes = await BytesReadAsync(path, reader => reader.ReadAllAsync(), new ParquetReadOptions { Filter = filter });
        _output.WriteLine($"on {onBytes:N0} bytes, off {offBytes:N0}");
        Assert.True(onBytes < offBytes / 4, $"read {onBytes:N0} of {offBytes:N0} bytes");
    }

    /// <summary>
    /// Statistics keep the row group (each half of the AND is possible somewhere in it), but no page is
    /// in both halves: the row group is skipped without reading a data page.
    /// </summary>
    [Fact]
    public async Task FilterUsePageIndex_SkipsARowGroupNoPageCanMatch()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        var filter = Ex.And(
            Ex.LessThan("sorted", LiteralValue.Of(100L)),
            Ex.GreaterThan("sorted", LiteralValue.Of(4900L)));

        Assert.Empty(await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true }));
        Assert.Equal(Rows, (await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter })).Count);
    }

    /// <summary>With several row groups, each is narrowed on its own, and ruled-out ones are skipped.</summary>
    [Fact]
    public async Task FilterUsePageIndex_SeveralRowGroups()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            DataPageRowCountLimit = 250,
            RowGroupMaxRows = 2000,
        });
        var filter = Ex.Or(
            Ex.Equal("sorted", LiteralValue.Of(10L)),
            Ex.Equal("sorted", LiteralValue.Of(4500L)));

        var on = await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true, BatchSize = 100 });
        var expected = new List<string>();
        for (int g = 0; g < 3; g++)
            expected.AddRange(await FullRowsAsync(path, g));

        // Row group 0 keeps [0, 250); row group 1 is ruled out by statistics; row group 2 (rows 4000 on)
        // keeps [500, 750) of its own, file rows [4500, 4750).
        Assert.Equal(expected.Take(250).Concat(expected.Skip(4500).Take(250)), on);
    }

    // ───── Arguments ─────

    [Theory]
    [InlineData(10, 10)]   // empty
    [InlineData(-1, 10)]   // before the row group
    [InlineData(4990, 5001)] // past it
    public async Task InvalidRange_Throws(long start, long end)
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default);
        using var reader = Open(path);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var batch in reader.ReadRowRangesAsync(0, [new RowRange(start, end)]))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task OverlappingOrUnorderedRanges_Throw()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default);
        using var reader = Open(path);

        foreach (var ranges in new[] { new[] { new RowRange(0, 100), new RowRange(50, 150) }, [new RowRange(200, 300), new RowRange(0, 100)] })
        {
            await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await foreach (var batch in reader.ReadRowRangesAsync(0, ranges))
                    batch.Dispose();
            });
        }
    }

    [Fact]
    public async Task NoRanges_NoBatches_AndTouchingRangesAreAllowed()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        using var reader = Open(path);

        await foreach (var _ in reader.ReadRowRangesAsync(0, []))
            Assert.Fail("a batch from no ranges");

        var expected = await FullRowsAsync(path, 0);
        await AssertRangeReadAsync(path, ParquetReadOptions.Default, 0, [new RowRange(0, 100), new RowRange(100, 300)], expected, batchSize: null);
    }

    // ───── Helpers ─────

    /// <summary>
    /// Single rows at both ends, a page exactly, ranges that cross pages and sit inside one, many small
    /// ranges, and the whole row group.
    /// </summary>
    private static IEnumerable<RowRange[]> RangeSets(int rows)
    {
        yield return [new RowRange(0, 1)];
        yield return [new RowRange(rows - 1, rows)];
        yield return [new RowRange(250, 500)];
        yield return [new RowRange(100, 120), new RowRange(130, 131), new RowRange(1000, 2600), new RowRange(rows - 200, rows)];
        yield return [new RowRange(0, rows)];

        var random = new Random(430);
        var many = new List<RowRange>();
        long at = random.Next(50);
        while (at < rows)
        {
            long end = Math.Min(rows, at + 1 + random.Next(40));
            many.Add(new RowRange(at, end));
            at = end + 1 + random.Next(400);
        }

        yield return [.. many];
    }

    private async Task AssertRangeReadAsync(
        string path, ParquetReadOptions options, int rowGroup, IReadOnlyList<RowRange> ranges, List<string> expected, int? batchSize)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false, options);

        var actual = new List<string>();
        int range = 0;
        long intoRange = 0;
        await foreach (var batch in reader.ReadRowRangesAsync(rowGroup, ranges))
        {
            using (batch)
            {
                Assert.True(batch.Length > 0, "an empty batch");
                if (batchSize is { } limit)
                    Assert.True(batch.Length <= limit, $"a batch of {batch.Length} rows over {limit}");

                // A batch lies inside one range: it never spans the gap to the next.
                Assert.True(range < ranges.Count, "more rows than the ranges hold");
                Assert.True(intoRange + batch.Length <= ranges[range].Length,
                    $"a batch of {batch.Length} rows crosses the end of range {ranges[range]}");
                intoRange += batch.Length;
                if (intoRange == ranges[range].Length)
                {
                    range++;
                    intoRange = 0;
                }

                for (int r = 0; r < batch.Length; r++)
                    actual.Add(ArrowValues.RenderRow(batch, r));
            }
        }

        var wanted = ranges.SelectMany(r => expected.Skip((int)r.Start).Take((int)r.Length)).ToList();
        Assert.Equal(wanted.Count, actual.Count);
        for (int i = 0; i < wanted.Count; i++)
            Assert.True(wanted[i] == actual[i], $"row {i} of the ranges {string.Join(" ", ranges)}: expected {wanted[i]}, got {actual[i]}");
    }

    private static async Task<List<string>> FullRowsAsync(string path, int rowGroup)
    {
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true);
        using var batch = await reader.ReadRowGroupAsync(rowGroup);
        return Enumerable.Range(0, batch.Length).Select(r => ArrowValues.RenderRow(batch, r)).ToList();
    }

    private static async Task<List<string>> ReadAllRowsAsync(string path, ParquetReadOptions options)
    {
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true, options);
        var rows = new List<string>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            using (batch)
            {
                for (int r = 0; r < batch.Length; r++)
                    rows.Add(ArrowValues.RenderRow(batch, r));
            }
        }

        return rows;
    }

    /// <summary>The bytes a read asks for, footer and index included.</summary>
    private static async Task<long> BytesReadAsync(
        string path, Func<ParquetFileReader, IAsyncEnumerable<RecordBatch>> read, ParquetReadOptions? options = null)
    {
        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true, options);
        await reader.ReadMetadataAsync();
        long before = counting.Bytes;
        await foreach (var batch in read(reader))
            batch.Dispose();
        return counting.Bytes - before;
    }

    private static bool Unreadable(Exception e) => e switch
    {
        NotSupportedException or ParquetFormatException => true,
        AggregateException aggregate => aggregate.InnerExceptions.All(Unreadable),
        _ => false,
    };

    /// <summary>
    /// Flat columns whose encodings differ page to page: a sorted long, a random int, a low-cardinality
    /// string (dictionary), unique strings, a double, a bool, a timestamp, and a nullable int with runs
    /// of nulls, some a whole page long.
    /// </summary>
    private static RecordBatch Flat(int rows)
    {
        var random = new Random(3);
        var sorted = new Int64Array.Builder();
        var ints = new Int32Array.Builder();
        var low = new StringArray.Builder();
        var unique = new StringArray.Builder();
        var doubles = new DoubleArray.Builder();
        var flags = new BooleanArray.Builder();
        var times = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var nullable = new Int32Array.Builder();
        var epoch = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (int i = 0; i < rows; i++)
        {
            sorted.Append(i);
            ints.Append(random.Next());
            low.Append("k" + random.Next(20));
            unique.Append($"value-{random.Next():x8}-{i}");
            doubles.Append(random.NextDouble());
            flags.Append(random.Next(3) == 0);
            times.Append(epoch.AddSeconds(i));
            if (i % 700 < 300 || (i >= 2500 && i < 2750)) nullable.AppendNull(); else nullable.Append(i);
        }

        return new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("sorted", Int64Type.Default, false))
                .Field(new Field("ints", Int32Type.Default, false))
                .Field(new Field("low", StringType.Default, false))
                .Field(new Field("unique", StringType.Default, false))
                .Field(new Field("doubles", DoubleType.Default, false))
                .Field(new Field("flags", BooleanType.Default, false))
                .Field(new Field("times", new TimestampType(TimeUnit.Microsecond, "UTC"), false))
                .Field(new Field("nullable", Int32Type.Default, true))
                .Build(),
            [sorted.Build(), ints.Build(), low.Build(), unique.Build(), doubles.Build(), flags.Build(), times.Build(), nullable.Build()],
            rows);
    }

    /// <summary><paramref name="flat"/> plus a list of ints and a struct.</summary>
    private static RecordBatch WithNested(RecordBatch flat)
    {
        var list = new ListArray.Builder(Int32Type.Default);
        var elements = (Int32Array.Builder)list.ValueBuilder;
        var inner = new Int64Array.Builder();
        for (int i = 0; i < flat.Length; i++)
        {
            if (i % 11 == 0)
            {
                list.AppendNull();
            }
            else
            {
                list.Append();
                for (int j = 0; j < i % 4; j++)
                    elements.Append(i * 10 + j);
            }

            inner.Append(-i);
        }

        var listArray = list.Build();
        var structType = new StructType([new Field("a", Int64Type.Default, false)]);
        var fields = flat.Schema.FieldsList.ToList();
        fields.Add(new Field("list", listArray.Data.DataType, true));
        fields.Add(new Field("struct", structType, false));
        var arrays = Enumerable.Range(0, flat.ColumnCount).Select(flat.Column).ToList();
        arrays.Add(listArray);
        arrays.Add(new StructArray(structType, flat.Length, [inner.Build()], ArrowBuffer.Empty, 0));
        return new RecordBatch(new Apache.Arrow.Schema(fields, null), arrays, flat.Length);
    }

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
}
