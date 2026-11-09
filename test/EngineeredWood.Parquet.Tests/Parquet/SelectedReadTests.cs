// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// <see cref="ParquetFileReader.ReadSelectedAsync"/> (#502, doc/row-level-filtering.md phase 1a): exactly the
/// rows a <see cref="RowFilter"/> and <see cref="IExcludedRows"/> select, each at its file position.
/// </summary>
/// <remarks>
/// The oracle reads the whole file without any pruning, evaluates the same predicates row by row with
/// <see cref="ArrowRowEvaluator"/> as one conjunction, removes the excluded positions, and renders the
/// projected columns. A selected read must return those rows, at those positions, in that order.
/// </remarks>
public class SelectedReadTests : IDisposable
{
    private const int Rows = 30_000;
    private const int RowsPerGroup = 10_000;

    private static readonly ArrowRowEvaluator Evaluator = new();

    private readonly string _tempDir;

    public SelectedReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-selected-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch (IOException) { }
    }

    // ---- The oracle sweep --------------------------------------------------------------------------------

    private static readonly Dictionary<string, Predicate[]> Filters = new()
    {
        ["id point"] = [Ex.Equal("id", LiteralValue.Of(12_345L))],
        ["id 10% (two conjuncts)"] = [
            Ex.GreaterThanOrEqual("id", LiteralValue.Of(5_000L)),
            Ex.LessThan("id", LiteralValue.Of(8_000L))],
        ["k 1%"] = [Ex.LessThan("k", LiteralValue.Of(10))],
        ["k 90%"] = [Ex.LessThan("k", LiteralValue.Of(900))],
        ["n > 500 (nulls)"] = [Ex.GreaterThan("n", LiteralValue.Of(500))],
        ["s = cat-03"] = [Ex.Equal("s", LiteralValue.Of("cat-03"))],
        ["k < 500, n > 100"] = [
            Ex.LessThan("k", LiteralValue.Of(500)),
            Ex.GreaterThan("n", LiteralValue.Of(100))],
        ["nothing"] = [Ex.LessThan("k", LiteralValue.Of(0))],
    };

    private static readonly Dictionary<string, string[]?> Projections = new()
    {
        ["all"] = null,
        ["id, tags"] = ["id", "tags"],
        ["pt, id, s"] = ["pt", "id", "s"],
        ["s, k"] = ["s", "k"],

        // Not swept: a dotted path into a struct ("pt.x"). Reading one leaf of a two-leaf struct throws
        // IndexOutOfRangeException in NestedAssembler on main, with or without a filter (#783). Add it back
        // once that is fixed.
    };

    public static IEnumerable<object[]> Cases()
    {
        foreach (var filter in Filters.Keys)
        {
            foreach (var projection in Projections.Keys)
            {
                foreach (int concurrency in new[] { 1, 4 })
                {
                    // Pruning and exclusions vary together with the concurrency, to keep the sweep small:
                    // serial reads prune by nothing and exclude nothing; parallel ones prune by the filter
                    // and exclude 5% of rows.
                    yield return [filter, projection, concurrency];
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ReturnsExactlyTheSelectedRowsAtTheirPositions(string filterName, string projectionName, int concurrency)
    {
        var filter = Filters[filterName];
        var projection = Projections[projectionName];
        bool parallel = concurrency > 1;
        var excluded = parallel ? TestExclusions.Random(Rows, 0.05, seed: 7) : null;

        string path = await WriteFileAsync();
        var expected = await OracleAsync(path, filter, projection, excluded);

        var options = new ParquetReadOptions { MaxConcurrentRowGroups = concurrency, BatchSize = parallel ? 3_000 : null };
        var scan = new ParquetScan
        {
            Columns = projection,
            Prune = parallel ? Ex.And(filter) : null,
            RowFilter = ToRowFilter(filter),
            ExcludedRows = excluded,
        };

        var actual = await ReadAsync(path, options, scan);

        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
            Assert.Equal(expected[i], actual[i]);
    }

    // ---- Three-valued AND ----------------------------------------------------------------------------------

    [Fact]
    public async Task NullRowsReachLaterPredicatesButAreNotReturned_FalseRowsReachNothing()
    {
        string path = await WriteFileAsync();
        var first = Ex.GreaterThan("n", LiteralValue.Of(500)); // NULL where n is null
        var seen = new ConcurrentBag<long>();
        var second = new RowPredicate(["id"], batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            for (int i = 0; i < ids.Length; i++)
                seen.Add(ids.GetValue(i)!.Value);
            return AllTrue(batch.Length);
        });

        var scan = new ParquetScan
        {
            Columns = ["id"],
            RowFilter = new RowFilter(ToRowPredicate(first), second),
        };
        var actual = await ReadAsync(path, new ParquetReadOptions(), scan);

        // The second predicate sees the rows the first answered TRUE or NULL for, and no others.
        var (n, _) = await ColumnAsync<Int32Array>(path, "n");
        var expectedSeen = Enumerable.Range(0, Rows).Where(r => n[r] is null || n[r] > 500).Select(r => (long)r).ToHashSet();
        Assert.Equal(expectedSeen, seen.ToHashSet());
        Assert.Equal(expectedSeen.Count, seen.Count);

        // Only TRUE rows come back.
        var expectedReturned = Enumerable.Range(0, Rows).Where(r => n[r] > 500).Select(r => (long)r).ToList();
        Assert.Equal(expectedReturned, actual.Select(PositionOf).ToList());
    }

    [Fact]
    public async Task ExcludedRowsNeverReachAPredicate()
    {
        string path = await WriteFileAsync();
        var excluded = TestExclusions.Random(Rows, 0.2, seed: 11);
        var seen = new ConcurrentBag<long>();
        var spy = new RowPredicate(["id"], batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            for (int i = 0; i < ids.Length; i++)
                seen.Add(ids.GetValue(i)!.Value);
            return AllTrue(batch.Length);
        });

        var actual = await ReadAsync(path, new ParquetReadOptions { MaxConcurrentRowGroups = 4 },
            new ParquetScan { Columns = ["id"], RowFilter = new RowFilter(spy), ExcludedRows = excluded });

        Assert.DoesNotContain(seen, excluded.Contains);
        Assert.Equal(Rows - excluded.Count, seen.Count);
        Assert.Equal(Rows - excluded.Count, actual.Count);
    }

    /// <summary>
    /// A row group whose every row is excluded is skipped unread: none of its column chunks is requested (by
    /// recorded byte range, as <c>RowSkipReadTests</c> checks), and none of its rows reaches a predicate. The
    /// groups around it are read, so the check is not vacuous.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AWhollyExcludedRowGroupIsNotRead(int concurrency)
    {
        string path = await WriteFileAsync();
        var excluded = new TestExclusions(Enumerable.Range(RowsPerGroup, RowsPerGroup).Select(i => (long)i));
        var seen = new ConcurrentBag<long>();
        var spy = new RowPredicate(["id"], batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            for (int i = 0; i < ids.Length; i++)
                seen.Add(ids.GetValue(i)!.Value);
            return AllTrue(batch.Length);
        });

        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        int rows = 0;
        FileMetaData metadata;
        await using (counting)
        using (var reader = new ParquetFileReader(counting, ownsFile: false, new ParquetReadOptions { MaxConcurrentRowGroups = concurrency }))
        {
            metadata = await reader.ReadMetadataAsync();
            var scan = new ParquetScan { Columns = ["id", "s"], RowFilter = new RowFilter(spy), ExcludedRows = excluded };
            await foreach (var selected in reader.ReadSelectedAsync(scan))
            {
                using (selected)
                    rows += selected.Batch.Length;
            }
        }

        Assert.Equal(Rows - RowsPerGroup, rows);
        Assert.DoesNotContain(seen, id => id >= RowsPerGroup && id < 2 * RowsPerGroup);

        var requested = counting.RequestRanges.SelectMany(r => r).ToList();
        foreach (var (offset, length) in ChunkRanges(metadata.RowGroups[1]))
        {
            // The one overlap allowed, as in RowSkipReadTests: the reader pads a dictionary chunk's read by up to
            // 100 bytes past its end (the PARQUET-816 workaround), which can run into the next group's first chunk.
            Assert.DoesNotContain(requested, r =>
                r.Offset < offset + length && offset < r.Offset + r.Length
                && !(r.Offset < offset && r.Offset + r.Length <= offset + 100));
        }

        foreach (int kept in new[] { 0, 2 })
        {
            var (offset, _) = ChunkRanges(metadata.RowGroups[kept]).First();
            Assert.Contains(requested, r => r.Offset <= offset && offset < r.Offset + r.Length);
        }
    }

    /// <summary>A row group's column chunks, from the first page (a dictionary page when there is one).</summary>
    private static IEnumerable<(long Offset, long Length)> ChunkRanges(EngineeredWood.Parquet.Metadata.RowGroup rowGroup)
    {
        foreach (var chunk in rowGroup.Columns)
        {
            var meta = chunk.MetaData!;
            long start = meta.DictionaryPageOffset is { } d && d > 0 && d < meta.DataPageOffset ? d : meta.DataPageOffset;
            yield return (start, meta.TotalCompressedSize);
        }
    }

    /// <summary>
    /// When an earlier predicate rejects every row of a batch, a later one is still asked, over no rows, as
    /// <c>EvalAnd</c> asks every operand over an empty selection. One that cannot be evaluated at all still
    /// raises then, as the whole conjunction does in a post-filter.
    /// </summary>
    [Fact]
    public async Task APredicateAfterOneThatRejectsEverythingIsStillAskedOverNoRows()
    {
        string path = await WriteFileAsync();
        var rejectAll = Ex.LessThan("k", LiteralValue.Of(0));
        var calls = new ConcurrentBag<int>();
        var spy = new RowPredicate(["id"], batch =>
        {
            calls.Add(batch.Length);
            return AllTrue(batch.Length);
        });

        var actual = await ReadAsync(path, new ParquetReadOptions { BatchSize = 5_000 },
            new ParquetScan { Columns = ["id"], RowFilter = new RowFilter(ToRowPredicate(rejectAll), spy) });

        Assert.Empty(actual);
        Assert.Equal(Rows / 5_000, calls.Count); // once per batch
        Assert.All(calls, length => Assert.Equal(0, length));
    }

    /// <summary>The differential form: a comparison with no common type (a struct against an integer) raises
    /// over an empty batch, so the conjunction raises in a post-filter, and the split read raises the same way.</summary>
    [Fact]
    public async Task AnUnevaluablePredicateRaisesEvenWhenNoRowReachesIt()
    {
        string path = await WriteFileAsync();
        var rejectAll = Ex.LessThan("k", LiteralValue.Of(0));
        var unevaluable = Ex.Equal("pt", LiteralValue.Of(1));

        // The oracle: the whole conjunction over a batch.
        await using (var file = new LocalRandomAccessFile(path))
        using (var reader = new ParquetFileReader(file, ownsFile: false))
        using (var batch = await reader.ReadRowGroupAsync(0, ["k", "pt"]))
        {
            var expected = Record.Exception(() => Evaluator.EvaluatePredicate(Ex.And(rejectAll, unevaluable), batch));
            Assert.NotNull(expected);

            var split = new RowFilter(ToRowPredicate(rejectAll), new RowPredicate(["pt"], b => Evaluator.EvaluatePredicate(unevaluable, b)));
            var actual = await Record.ExceptionAsync(() => ReadAsync(path, new ParquetReadOptions(),
                new ParquetScan { Columns = ["id"], RowFilter = split }));
            Assert.NotNull(actual);
            Assert.Equal(expected!.GetType(), actual!.GetType());
        }
    }

    // ---- Buffer ownership ----------------------------------------------------------------------------------

    /// <summary>
    /// With view output, a gather shares the source's data buffers (<c>ArrowCompute.TakeVarBinaryView</c>), and
    /// the reader builds those buffers in native memory. Values longer than a view holds inline live there, so
    /// they read back right only if neither the predicate's gathered input nor the decoded batch frees buffers
    /// the output still points into. The first predicate drops about half the rows, so the second is handed a
    /// gathered batch.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task OutOfLineViewValuesSurviveTheGathers(int concurrency)
    {
        string path = await WriteFileAsync();
        var first = Ex.LessThan("k", LiteralValue.Of(500));
        var second = new RowPredicate(["note", "k"], batch =>
        {
            // Reads every value it is handed, out-of-line bytes included, and keeps rows by their first character.
            var notes = (StringViewArray)batch.Column(0);
            var builder = new BooleanArray.Builder().Reserve(notes.Length);
            for (int i = 0; i < notes.Length; i++)
                builder.Append(notes.GetString(i)[0] <= '7');
            return builder.Build();
        });

        var options = new ParquetReadOptions
        {
            ByteArrayOutput = ByteArrayOutputKind.ViewType,
            MaxConcurrentRowGroups = concurrency,
            BatchSize = 2_000,
        };
        var actual = await ReadAsync(path, options,
            new ParquetScan { Columns = ["id", "note"], RowFilter = new RowFilter(ToRowPredicate(first), second) });

        // The same rows from a default (StringArray) read, rendered the same way (bytes).
        var (k, _) = await ColumnAsync<Int32Array>(path, "k");
        var notesByRow = await NotesAsync(path);
        var expected = Enumerable.Range(0, Rows)
            .Where(r => k[r] < 500 && notesByRow[r][0] <= '7')
            .Select(r => $"{r}:{notesByRow[r]}")
            .ToList();
        Assert.Equal(expected, actual.Select(a => $"{PositionOf(a)}:{NoteOf(a)}").ToList());
    }

    /// <summary>
    /// A predicate may answer with one of its own input columns: <c>WHERE flag</c> over a boolean column. The
    /// mask is then the reader's column (every row live) or the gathered copy's (some rows live), and disposing
    /// it as a mask must not free a column the read still returns.
    /// </summary>
    [Theory]
    [InlineData(false, 1)]
    [InlineData(false, 4)]
    [InlineData(true, 1)]
    [InlineData(true, 4)]
    public async Task APredicateMayAnswerWithItsOwnInputColumn(bool afterAnotherPredicate, int concurrency)
    {
        string path = await WriteFileAsync();
        var itself = new RowPredicate(["flag"], batch => (BooleanArray)batch.Column(0));
        var filter = afterAnotherPredicate
            ? new RowFilter(ToRowPredicate(Ex.LessThan("k", LiteralValue.Of(500))), itself)
            : new RowFilter(itself);
        var conjuncts = afterAnotherPredicate
            ? new Predicate[] { Ex.LessThan("k", LiteralValue.Of(500)), Ex.Equal("flag", LiteralValue.Of(true)) }
            : new Predicate[] { Ex.Equal("flag", LiteralValue.Of(true)) };

        var expected = await OracleAsync(path, conjuncts, ["id", "flag"], excluded: null);
        var actual = await ReadAsync(path, new ParquetReadOptions { MaxConcurrentRowGroups = concurrency, BatchSize = 4_000 },
            new ParquetScan { Columns = ["id", "flag"], RowFilter = filter });

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    private static string NoteOf(string positionAndRow) =>
        System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(positionAndRow[(positionAndRow.LastIndexOf('|') + 1)..]));

    private static async Task<string[]> NotesAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var notes = new List<string>();
        await foreach (var batch in reader.ReadAllAsync(["note"]))
        {
            using (batch)
            {
                var array = (StringArray)batch.Column(0);
                for (int i = 0; i < array.Length; i++)
                    notes.Add(array.GetString(i));
            }
        }

        return notes.ToArray();
    }

    // ---- Positions -----------------------------------------------------------------------------------------

    [Fact]
    public async Task ContiguousSurvivorsNeedNoExplicitPositions()
    {
        string path = await WriteFileAsync();
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { BatchSize = 1_000 });
        var scan = new ParquetScan
        {
            Columns = ["id"],
            RowFilter = ToRowFilter([Ex.GreaterThanOrEqual("id", LiteralValue.Of(2_500L)), Ex.LessThan("id", LiteralValue.Of(4_000L))]),
        };

        int batches = 0;
        await foreach (var selected in reader.ReadSelectedAsync(scan))
        {
            using (selected)
            {
                Assert.True(selected.Positions.IsContiguous);
                var ids = (Int64Array)selected.Batch.Column(0);
                for (int i = 0; i < ids.Length; i++)
                    Assert.Equal(ids.GetValue(i)!.Value, selected.Positions[i]);
                batches++;
            }
        }

        Assert.True(batches >= 2);
    }

    [Fact]
    public async Task ScatteredSurvivorsCarryEachRowsPosition()
    {
        string path = await WriteFileAsync();
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var scan = new ParquetScan { Columns = ["id"], RowFilter = ToRowFilter([Ex.LessThan("k", LiteralValue.Of(100))]) };

        long rows = 0;
        await foreach (var selected in reader.ReadSelectedAsync(scan))
        {
            using (selected)
            {
                Assert.False(selected.Positions.IsContiguous);
                Assert.Equal(selected.Batch.Length, selected.Positions.Explicit!.Length);
                var ids = (Int64Array)selected.Batch.Column(0);
                for (int i = 0; i < ids.Length; i++)
                    Assert.Equal(ids.GetValue(i)!.Value, selected.Positions[i]);
                rows += ids.Length;
            }
        }

        Assert.InRange(rows, 1, Rows / 2);
    }

    /// <summary>
    /// A positions-only read (no columns), as a DELETE needs: the batches have no columns, but their lengths and
    /// positions are exactly the selected rows.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task APositionsOnlyReadReturnsTheSelectedPositions(int concurrency)
    {
        string path = await WriteFileAsync();
        var excluded = TestExclusions.Random(Rows, 0.1, seed: 3);
        var filter = Ex.LessThan("k", LiteralValue.Of(100));

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { MaxConcurrentRowGroups = concurrency });
        var positions = new List<long>();
        await foreach (var selected in reader.ReadSelectedAsync(
                           new ParquetScan { Columns = [], RowFilter = ToRowFilter([filter]), ExcludedRows = excluded }))
        {
            using (selected)
            {
                Assert.Equal(0, selected.Batch.ColumnCount);
                for (int i = 0; i < selected.Batch.Length; i++)
                    positions.Add(selected.Positions[i]);
            }
        }

        var (k, _) = await ColumnAsync<Int32Array>(path, "k");
        var expected = Enumerable.Range(0, Rows).Where(r => k[r] < 100 && !excluded.Contains(r)).Select(r => (long)r);
        Assert.Equal(expected, positions);
    }

    // ---- Errors --------------------------------------------------------------------------------------------

    private sealed class PredicateFailure() : Exception("the predicate failed");

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task APredicatesExceptionPropagates(int concurrency)
    {
        string path = await WriteFileAsync();
        var failing = new RowPredicate(["id"], batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            for (int i = 0; i < ids.Length; i++)
            {
                if (ids.GetValue(i) == 25_000)
                    throw new PredicateFailure();
            }

            return AllTrue(batch.Length);
        });

        await Assert.ThrowsAsync<PredicateFailure>(() => ReadAsync(path,
            new ParquetReadOptions { MaxConcurrentRowGroups = concurrency },
            new ParquetScan { Columns = ["id"], RowFilter = new RowFilter(failing) }));
    }

    [Fact]
    public async Task AMaskOfTheWrongLengthIsRefused()
    {
        string path = await WriteFileAsync();
        var wrong = new RowPredicate(["id"], batch => AllTrue(batch.Length - 1));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => ReadAsync(path, new ParquetReadOptions(),
            new ParquetScan { Columns = ["id"], RowFilter = new RowFilter(wrong) }));
        Assert.Contains("mask", error.Message);
    }

    [Fact]
    public async Task APredicateOnALeafPathIsRefused()
    {
        string path = await WriteFileAsync();
        var leaf = new RowPredicate(["pt.x"], batch => AllTrue(batch.Length));
        await Assert.ThrowsAsync<ArgumentException>(() => ReadAsync(path, new ParquetReadOptions(),
            new ParquetScan { Columns = ["id"], RowFilter = new RowFilter(leaf) }));
    }

    [Fact]
    public async Task APredicateOnAColumnProjectedInPartIsRefused()
    {
        string path = await WriteFileAsync();
        var whole = new RowPredicate(["pt"], batch => AllTrue(batch.Length));
        await Assert.ThrowsAsync<ArgumentException>(() => ReadAsync(path, new ParquetReadOptions(),
            new ParquetScan { Columns = ["pt.x", "id"], RowFilter = new RowFilter(whole) }));
    }

    [Fact]
    public void APredicateMustNameItsColumnsOnce()
    {
        Assert.Throws<ArgumentException>(() => new RowPredicate([], b => AllTrue(b.Length)));
        Assert.Throws<ArgumentException>(() => new RowPredicate(["id", "id"], b => AllTrue(b.Length)));
        Assert.Throws<ArgumentException>(() => new RowFilter());
    }

    // ---- Helpers -------------------------------------------------------------------------------------------

    private static RowPredicate ToRowPredicate(Predicate predicate) =>
        new(ColumnsOf(predicate), batch => Evaluator.EvaluatePredicate(predicate, batch));

    private static RowFilter ToRowFilter(Predicate[] conjuncts) =>
        new(conjuncts.Select(ToRowPredicate).ToArray());

    private static string[] ColumnsOf(Predicate predicate) => predicate switch
    {
        ComparisonPredicate { Left: UnboundReference r } => [r.Name],
        _ => throw new NotSupportedException(predicate.ToString()),
    };

    private static BooleanArray AllTrue(int length)
    {
        var builder = new BooleanArray.Builder().Reserve(length);
        for (int i = 0; i < length; i++)
            builder.Append(true);
        return builder.Build();
    }

    private static long PositionOf(string positionAndRow) =>
        long.Parse(positionAndRow[..positionAndRow.IndexOf(':')], System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>"position:rendered projected row" for every selected row, in order.</summary>
    private static async Task<List<string>> ReadAsync(string path, ParquetReadOptions options, ParquetScan scan)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, options);
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

    /// <summary>The expected rows: a full, unpruned read, filtered row by row with the conjunction.</summary>
    private static async Task<List<string>> OracleAsync(
        string path, Predicate[] conjuncts, string[]? projection, TestExclusions? excluded)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var predicate = Ex.And(conjuncts);
        var rows = new List<string>();
        await foreach (var positioned in reader.ReadWithPositionsAsync(null, projection is null ? null : projection.Union(["id", "k", "n", "s", "flag"]).ToArray()))
        {
            using var batch = positioned.Batch;
            using var mask = Evaluator.EvaluatePredicate(predicate, batch);
            using var output = projection is null ? null : await ProjectAsync(path, positioned, projection);
            for (int i = 0; i < batch.Length; i++)
            {
                long position = positioned.FirstRow + i;
                if (mask.GetValue(i) != true || (excluded?.Contains(position) ?? false))
                    continue;
                rows.Add($"{position}:{ArrowValues.RenderRow(output ?? batch, i)}");
            }
        }

        return rows;
    }

    // The projection's own read of the same rows, so the oracle renders exactly the columns (and the shape:
    // a dotted path comes back as its struct) that a projected read returns.
    private static async Task<RecordBatch> ProjectAsync(string path, PositionedRecordBatch at, string[] projection)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        await foreach (var positioned in reader.ReadWithPositionsAsync(null, projection))
        {
            if (positioned.FirstRow == at.FirstRow && positioned.Batch.Length == at.Batch.Length)
                return positioned.Batch;
            positioned.Batch.Dispose();
        }

        throw new InvalidOperationException("The projected read has no batch at " + at.FirstRow);
    }

    private static async Task<(int?[] Values, int Count)> ColumnAsync<T>(string path, string column)
        where T : Int32Array
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var values = new List<int?>();
        await foreach (var batch in reader.ReadAllAsync([column]))
        {
            using (batch)
            {
                var array = (T)batch.Column(0);
                for (int i = 0; i < array.Length; i++)
                    values.Add(array.GetValue(i));
            }
        }

        return (values.ToArray(), values.Count);
    }

    /// <summary>
    /// 30,000 rows in three row groups, 1,000-row pages: <c>id</c> (sorted, equal to the row's position),
    /// <c>k</c> (uniform 0..999), <c>n</c> (nullable, 10% null), <c>s</c> (50 strings, dictionary-encoded),
    /// <c>tags</c> (a nullable list), <c>pt</c> (a struct), <c>note</c> (24-character strings: longer than a
    /// view entry holds inline) and <c>flag</c> (a nullable boolean).
    /// </summary>
    private async Task<string> WriteFileAsync()
    {
        string path = Path.Combine(_tempDir, "data.parquet");
        if (File.Exists(path))
            return path;

        var random = new Random(502);
        var id = new Int64Array.Builder();
        var k = new Int32Array.Builder();
        var n = new Int32Array.Builder();
        var s = new StringArray.Builder();
        var tags = new ListArray.Builder(Int32Type.Default);
        var tagValues = (Int32Array.Builder)tags.ValueBuilder;
        var note = new StringArray.Builder();
        var flag = new BooleanArray.Builder();
        var noteBytes = new byte[12];
        var x = new Int32Array.Builder();
        var y = new StringArray.Builder();
        for (int r = 0; r < Rows; r++)
        {
            id.Append(r);
            k.Append(random.Next(1_000));
            if (random.Next(10) == 0)
                n.AppendNull();
            else
                n.Append(random.Next(1_000));
            s.Append("cat-" + random.Next(50).ToString("D2", System.Globalization.CultureInfo.InvariantCulture));
            if (random.Next(8) == 0)
            {
                tags.AppendNull();
            }
            else
            {
                tags.Append();
                for (int t = random.Next(4); t > 0; t--)
                    tagValues.Append(random.Next(100));
            }

            random.NextBytes(noteBytes);
            note.Append(BitConverter.ToString(noteBytes).Replace("-", "")); // 24 characters: out of line in a view array
            if (random.Next(10) == 0)
                flag.AppendNull();
            else
                flag.Append(random.Next(3) == 0);
            x.Append(r % 97);
            y.Append("y" + (r % 13).ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        var pointType = new StructType([new Field("x", Int32Type.Default, false), new Field("y", StringType.Default, false)]);
        var point = new StructArray(pointType, Rows, [x.Build(), y.Build()], ArrowBuffer.Empty, nullCount: 0);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("k", Int32Type.Default, false))
            .Field(new Field("n", Int32Type.Default, true))
            .Field(new Field("s", StringType.Default, false))
            .Field(new Field("tags", new ListType(Int32Type.Default), true))
            .Field(new Field("pt", pointType, false))
            .Field(new Field("note", StringType.Default, false))
            .Field(new Field("flag", BooleanType.Default, true))
            .Build();
        using var batch = new RecordBatch(schema, [id.Build(), k.Build(), n.Build(), s.Build(), tags.Build(), point, note.Build(), flag.Build()], Rows);

        await using var output = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(output, ownsFile: false,
            ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup, DataPageRowCountLimit = 1_000 });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    /// <summary>Excluded positions, from a set.</summary>
    private sealed class TestExclusions(IEnumerable<long> positions) : IExcludedRows
    {
        private readonly HashSet<long> _positions = positions.ToHashSet();

        public int Count => _positions.Count;

        public bool Contains(long position) => _positions.Contains(position);

        public static TestExclusions Random(int rows, double fraction, int seed)
        {
            var random = new Random(seed);
            return new TestExclusions(Enumerable.Range(0, rows).Where(_ => random.NextDouble() < fraction).Select(i => (long)i));
        }

        public bool ExcludesAll(long firstRow, long rowCount)
        {
            for (long p = firstRow; p < firstRow + rowCount; p++)
            {
                if (!_positions.Contains(p))
                    return false;
            }

            return true;
        }

        public void ClearExcluded(long firstRow, int rowCount, Span<ulong> selection)
        {
            for (int i = 0; i < rowCount; i++)
            {
                if (_positions.Contains(firstRow + i))
                    selection[i >> 6] &= ~(1UL << (i & 63));
            }
        }
    }
}
