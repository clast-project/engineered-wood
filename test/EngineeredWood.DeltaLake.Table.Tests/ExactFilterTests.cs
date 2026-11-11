// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaReadOptions.FilterRows"/> (#502 phase 3a): a scan returns exactly the rows its filter answers TRUE
/// for, each with its own position and row id.
/// </summary>
/// <remarks>
/// The oracle is a superset scan of the same table with the same metadata columns, filtered row by row with the same
/// evaluator a DML predicate uses (<see cref="DeltaTable.MaskFor"/>): the exact scan must return those rows, with
/// those metadata values, in that order. Every table has several files, a deletion vector, row tracking, partitions
/// and a nullable column, and is read serially and in parallel, through the built-in reader and the codec seam.
/// </remarks>
public class ExactFilterTests : IDisposable
{
    private readonly string _tempDir;

    public ExactFilterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_exact_{Guid.NewGuid():N}");
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
        .Field(new Field("region", StringType.Default, false))
        .Field(new Field("v", Int32Type.Default, false))
        .Field(new Field("n", Int32Type.Default, true))
        .Field(new Field("note", StringType.Default, false))
        .Build();

    private static readonly Dictionary<string, Predicate> Filters = new()
    {
        ["data"] = Ex.LessThan("v", LiteralValue.Of(30)),
        ["partition"] = Ex.Equal("region", LiteralValue.Of("b")),
        ["mixed or"] = Ex.Or(Ex.Equal("region", LiteralValue.Of("a")), Ex.GreaterThan("v", LiteralValue.Of(90))),
        ["nullable"] = Ex.GreaterThan("n", LiteralValue.Of(50)),
        ["two columns"] = Ex.And(Ex.LessThan("v", LiteralValue.Of(70)), Ex.GreaterThan("n", LiteralValue.Of(20))),
        ["nothing"] = Ex.LessThan("v", LiteralValue.Of(0)),
        ["everything"] = Ex.GreaterThanOrEqual("v", LiteralValue.Of(0)),
    };

    private static readonly Dictionary<string, string[]?> Projections = new()
    {
        ["all"] = null,
        ["id, note"] = ["id", "note"],     // every filter's columns left out: read for it, then dropped
        ["region, id"] = ["region", "id"],
    };

    public static IEnumerable<object[]> Cases()
    {
        foreach (var filter in Filters.Keys)
        {
            foreach (var projection in Projections.Keys)
            {
                foreach (var (concurrency, seam) in new[] { (1, false), (4, false), (1, true) })
                    yield return [filter, projection, concurrency, seam];
            }
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task AnExactScanReturnsExactlyTheMatchingRowsWithTheirMetadata(
        string filterName, string projectionName, int concurrency, bool seam)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency, seam, ColumnMappingMode.None);
        await AssertExactAsync(table, Filters[filterName], Projections[projectionName]);
    }

    [Theory]
    [InlineData("data")]
    [InlineData("partition")]
    [InlineData("two columns")]
    public async Task ColumnMappingByNameReadsThePhysicalColumns(string filterName)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 4, seam: false, ColumnMappingMode.Name);
        await AssertExactAsync(table, Filters[filterName], ["id", "note"]);
    }

    /// <summary>
    /// A filter on a column added after some files were written: those files hold no such column, so it is NULL for
    /// all their rows and the filter, which reads no column of theirs, is answered once per file.
    /// </summary>
    [Theory]
    [InlineData(1, false, true)]
    [InlineData(4, false, true)]
    [InlineData(1, true, true)]
    [InlineData(1, true, false)] // without row tracking, the seam is asked for a projection, not every column
    public async Task AFilterOnAColumnAddedLaterMatchesNoRowOfTheOlderFiles(int concurrency, bool seam, bool rowTracking)
    {
        string name = $"t_{Guid.NewGuid():N}";
        await using var table = await CreateAsync(name, concurrency, seam, ColumnMappingMode.None, rowTracking);
        await table.AddColumnAsync(new Field("score", DoubleType.Default, true));
        var withScore = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, false))
            .Field(new Field("v", Int32Type.Default, false))
            .Field(new Field("n", Int32Type.Default, true))
            .Field(new Field("note", StringType.Default, false))
            .Field(new Field("score", DoubleType.Default, true))
            .Build();
        var scores = new DoubleArray.Builder();
        for (int i = 0; i < 40; i++)
            scores.Append(i % 2 == 0 ? i : -i);
        var batch = Rows(1_000, 40);
        await table.WriteAsync([new RecordBatch(withScore, [.. Enumerable.Range(0, batch.ColumnCount).Select(batch.Column), scores.Build()], 40)]);

        if (seam && !rowTracking)
        {
            // The superset oracle can't read this table through the seam: it names every column, and the older files
            // lack score (#790). The built-in reader's exact scan, checked against that oracle above, stands in for it.
            await using var builtIn = await DeltaTable.OpenAsync(new LocalTableFileSystem(Path.Combine(_tempDir, name)));
            foreach (var (filter, projection) in new (Predicate, string[])[]
            {
                (Ex.GreaterThan("score", LiteralValue.Of(10.0)), ["id", "region"]),
                (Ex.IsNull("score"), ["id"]),
            })
            {
                Assert.Equal(
                    await ReadRowsAsync(builtIn, filter, projection, exactly: true),
                    await ReadRowsAsync(table, filter, projection, exactly: true));
            }

            return;
        }

        await AssertExactAsync(table, Ex.GreaterThan("score", LiteralValue.Of(10.0)), ["id", "region"]);
        await AssertExactAsync(table, Ex.IsNull("score"), ["id"]);
    }

    [Fact]
    public async Task AnUnanalyzableFilterIsRefusedBeforeAnyFileIsRead()
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 1, seam: false, ColumnMappingMode.None);
        var error = await Record.ExceptionAsync(async () =>
        {
            await foreach (var b in table.ReadAsync(new DeltaReadOptions
            {
                Filter = Ex.Equal("missing_column", LiteralValue.Of(1)),
                FilterRows = true,
            }))
            {
                b.Dispose();
            }
        });
        Assert.NotNull(error);
    }

    [Fact]
    public async Task WithoutFilterRowsTheFilterStillOnlyPrunes()
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 1, seam: false, ColumnMappingMode.None);
        long superset = 0;
        await foreach (var b in table.ReadAsync(new DeltaReadOptions { Filter = Filters["data"] }))
        {
            using (b)
                superset += b.Length;
        }

        var exact = await ReadRowsAsync(table, Filters["data"], null, exactly: true);
        Assert.True(superset > exact.Count, $"{superset} rows read without FilterRows, {exact.Count} with it");
    }

    /// <summary>
    /// A projection naming a column the table lacks returns the columns it can, and none of the filter's: the count
    /// of returned columns can't tell a filter column from the missing one.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AProjectionNamingAMissingColumnStillDropsTheFilterColumns(bool seam)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 1, seam, ColumnMappingMode.None, rowTracking: false);
        var options = new DeltaReadOptions { Columns = ["id", "missing"], Filter = Filters["data"], FilterRows = true };
        long rows = 0;
        await foreach (var batch in table.ReadAsync(options))
        {
            using (batch)
            {
                Assert.Equal(["id"], batch.Schema.FieldsList.Select(f => f.Name));
                rows += batch.Length;
            }
        }

        Assert.Equal((await ReadRowsAsync(table, Filters["data"], ["id"], exactly: false)).Count, rows);
    }

    /// <summary>
    /// A filter that reads no column of a file is answered once for the file, but not for a file with no live row: a
    /// scan of that file asks the filter nothing, so a value the filter raises on mustn't fail it.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AFileWithNoLiveRowIsNotAskedTheFilter(bool exactly)
    {
        string root = Path.Combine(_tempDir, $"t_{Guid.NewGuid():N}");
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(root), TableSchema, new DeltaTableOptions(), partitionColumns: ["region"]);
        await table.WriteAsync([Rows(1, 1)]); // region b, so that the schema of an empty file has a model

        // An empty file in partition a (a DELETE removes a file it empties, so a live table has none of its own).
        string path = Path.Combine(root, "region=a", "empty.parquet");
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var empty = Rows(0, 0);
        var dataOnly = new RecordBatch(
            new Apache.Arrow.Schema(empty.Schema.FieldsList.Where(f => f.Name != "region").ToList(), null),
            [.. Enumerable.Range(0, empty.ColumnCount).Where(i => i != 1).Select(empty.Column)], 0);
        await using (var file = new LocalSequentialFile(path))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            await writer.WriteRowGroupAsync(dataOnly);
            await writer.CloseAsync();
        }

        await table.CommitDataFilesAsync([new WrittenDataFile(
            "region=a/empty.parquet", new FileInfo(path).Length, 0,
            new Dictionary<string, string> { ["region"] = "a" }, null)]);

        // Under ANSI, 'a' = 5 raises CAST_INVALID_INPUT; 'b' = 5 would too, so only region a may be asked.
        var raising = Ex.And(Ex.Equal("region", LiteralValue.Of("a")), Ex.Equal("region", LiteralValue.Of(5)));
        var options = new DeltaReadOptions { Filter = raising, FilterRows = exactly };
        await foreach (var b in table.ReadAsync(options))
            b.Dispose();
    }

    // ---- Conjuncts (4a) --------------------------------------------------------------------------------------------

    private static Predicate P(string sql) => EngineeredWood.Expressions.Sql.SparkSqlParser.ParsePredicate(sql);

    private static readonly Dictionary<string, string> ConjunctFilters = new()
    {
        ["two data"] = "v < 70 AND n > 20",
        ["three, one partition"] = "v < 70 AND n > 20 AND region <> 'c'",
        ["partition false in some files"] = "v < 70 AND region = 'b'",
        ["partition first"] = "region IN ('a', 'c') AND n > 30",
        ["unprunable partition"] = "lower(region) = 'a' AND v < 50",
        ["nested"] = "v < 70 AND (n > 20 AND (id > 100 AND region <> 'a'))",
        ["no reference"] = "v < 30 AND 1 = 1",
        ["or inside"] = "(region = 'a' OR v > 90) AND n > 10",
        ["nullable twice"] = "n > 20 AND n < 80",
    };

    public static IEnumerable<object[]> ConjunctCases()
    {
        foreach (var filter in ConjunctFilters.Keys)
        {
            foreach (var projection in Projections.Keys)
            {
                foreach (var (concurrency, seam, split) in new[]
                    { (1, false, false), (1, false, true), (4, false, true), (1, true, false) })
                    yield return [filter, projection, concurrency, seam, split];
            }
        }
    }

    /// <summary>
    /// A filter of several conjuncts returns the rows the whole expression answers TRUE for, whether the reader
    /// evaluates its conjuncts together or each on its own, with the file-constant ones answered once per file.
    /// </summary>
    [Theory]
    [MemberData(nameof(ConjunctCases))]
    public async Task AFilterOfConjunctsReturnsExactlyTheRowsTheWholeFilterMatches(
        string filterName, string projectionName, int concurrency, bool seam, bool split)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency, seam, ColumnMappingMode.None);
        table.EvaluatesConjunctsTogether = !split;
        await AssertExactAsync(table, P(ConjunctFilters[filterName]), Projections[projectionName]);
    }

    /// <summary>
    /// A row the first conjunct answers NULL for is still evaluated by the second, as in the expression's AND: here
    /// <c>n + 1 &gt; 1000</c> (which pruning does not decide) is FALSE or NULL on every row, and the cast raises (ANSI)
    /// on the rows where it is NULL.
    /// </summary>
    [Theory]
    [InlineData(1, false, false)]
    [InlineData(1, false, true)]
    [InlineData(4, false, true)]
    [InlineData(1, true, false)]
    public async Task ALaterConjunctStillEvaluatesTheRowsAnEarlierOneAnsweredNullFor(
        int concurrency, bool seam, bool split)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency, seam, ColumnMappingMode.None);
        table.EvaluatesConjunctsTogether = !split;
        var options = new DeltaReadOptions
        {
            Columns = ["id"], Filter = P("n + 1 > 1000 AND CAST(note AS INT) > 0"), FilterRows = true,
        };
        var error = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await foreach (var b in table.ReadAsync(options))
                b.Dispose();
        });
        Assert.Contains("CAST_INVALID_INPUT", error.ToString());
    }

    /// <summary>
    /// A host mask after a filter of several conjuncts is asked only about the rows the whole filter answered TRUE
    /// for: not about one a conjunct answered NULL for and a later one TRUE. The filters after the first end in a
    /// file-constant conjunct, which is answered TRUE (dropped) for the files it doesn't skip, so no predicate of
    /// its own stands between the row-level conjunct and the mask (review round 2).
    /// </summary>
    [Theory]
    [InlineData("n > 50 AND v >= 0", 1, false)]
    [InlineData("n > 50 AND v >= 0", 4, false)]
    [InlineData("n > 50 AND v >= 0", 1, true)]
    [InlineData("n > 50 AND region = 'a'", 1, false)]
    [InlineData("n > 50 AND region = 'a'", 4, false)]
    [InlineData("n > 50 AND lower(region) <> 'zz'", 1, false)]
    [InlineData("n > 50 AND lower(region) <> 'zz'", 4, false)]
    [InlineData("n > 50 AND lower(region) <> 'zz'", 1, true)]
    public async Task AMaskAfterSplitConjunctsSeesOnlyRowsTheWholeFilterAccepted(
        string filter, int concurrency, bool seam)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency, seam, ColumnMappingMode.None);
        table.EvaluatesConjunctsTogether = false;
        var sawN = new System.Collections.Concurrent.ConcurrentBag<int?>();
        var mask = new DeltaRowMask(["n", "v"], b =>
        {
            var n = (Int32Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                sawN.Add(n.GetValue(i));
            return Answer(b.Length, _ => true);
        });
        var options = new DeltaReadOptions
        {
            Columns = ["id"], Filter = P(filter), FilterRows = true, RowMasks = [mask],
        };
        await foreach (var b in table.ReadAsync(options))
            b.Dispose();

        Assert.NotEmpty(sawN);
        Assert.All(sawN, n => Assert.True(n > 50, $"the mask was asked about n = {n?.ToString() ?? "NULL"}"));
    }

    /// <summary>
    /// A conjunct over only a partition column is answered before any row of the file is read, wherever it stands,
    /// as Spark applies a partition filter: a file it rejects is skipped, so a conjunct before it that raises on that
    /// file's rows is never asked. <c>lower(region)</c> is not one file pruning decides.
    /// </summary>
    [Theory]
    [InlineData(1, false)]
    [InlineData(1, true)]
    [InlineData(4, true)]
    public async Task AFileAPartitionConjunctRejectsIsSkippedBeforeAnEarlierConjunctRaises(int concurrency, bool split)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency, seam: false, ColumnMappingMode.None);
        table.EvaluatesConjunctsTogether = !split;
        var options = new DeltaReadOptions
        {
            Columns = ["id"], Filter = P("CAST(note AS INT) > 0 AND lower(region) = 'zz'"), FilterRows = true,
        };
        int rows = 0;
        await foreach (var b in table.ReadAsync(options))
        {
            rows += b.Length;
            b.Dispose();
        }

        Assert.Equal(0, rows);
    }

    /// <summary>
    /// A file-constant conjunct that raises over the file's values, after a conjunct answered row by row, raises only
    /// if a row survives that conjunct, as the expression's AND would: <c>CAST(region AS INT)</c> raises (ANSI) for
    /// every region, and <c>lower(note)</c> is not one pruning decides.
    /// </summary>
    [Theory]
    [InlineData("lower(note) = 'zz'", false)]
    [InlineData("lower(note) = 'note-5'", true)]
    [InlineData("n + 1 > 1000", true)] // NULL rows stay live, so the conjunct is asked about them
    public async Task AFileConstantConjunctThatRaisesRaisesOnlyOverARowThatReachesIt(string first, bool raises)
    {
        foreach (bool split in new[] { false, true })
        {
            await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 1, seam: false, ColumnMappingMode.None);
            table.EvaluatesConjunctsTogether = !split;
            var options = new DeltaReadOptions
            {
                Columns = ["id"], Filter = P($"{first} AND CAST(region AS INT) > 0"), FilterRows = true,
            };
            async Task ReadAsync()
            {
                await foreach (var b in table.ReadAsync(options))
                    b.Dispose();
            }

            if (raises)
                Assert.Contains("CAST_INVALID_INPUT", (await Assert.ThrowsAnyAsync<Exception>(ReadAsync)).ToString());
            else
                await ReadAsync();
        }
    }

    /// <summary>
    /// A file-constant conjunct that cannot be evaluated at all, whatever the values, raises even when the conjunct
    /// before it rejects every row: the reader asks every predicate over no rows for that (EvalAnd's rule), and a
    /// deferred conjunct is only deferred for an error its one row's values cause.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AFileConstantConjunctThatCannotBeEvaluatedRaisesWhenNoRowReachesIt(bool split)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 1, seam: false, ColumnMappingMode.None);
        table.EvaluatesConjunctsTogether = !split;
        var options = new DeltaReadOptions
        {
            Columns = ["id"], Filter = P("lower(note) = 'zz' AND length(region, region) > 0"), FilterRows = true,
        };
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await foreach (var b in table.ReadAsync(options))
                b.Dispose();
        });
    }

    /// <summary>
    /// A conjunct that cannot be evaluated at all still raises when a file-constant conjunct skips every file it
    /// would have been asked about: the skip saves the rows, not the analysis.
    /// </summary>
    [Theory]
    [InlineData("length(note, note) > 0 AND lower(region) = 'zz'")]
    [InlineData("lower(region) = 'zz' AND length(note, note) > 0")]
    [InlineData("lower(region) = 'zz' AND length(region, region) > 0")]
    public async Task AConjunctThatCannotBeEvaluatedRaisesWhenAFileConstantConjunctSkipsTheFile(string filter)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 1, seam: false, ColumnMappingMode.None);
        var options = new DeltaReadOptions { Columns = ["id"], Filter = P(filter), FilterRows = true };
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await foreach (var b in table.ReadAsync(options))
                b.Dispose();
        });
    }

    /// <summary>
    /// A file-constant conjunct over a boolean partition column, bare, whose answer may be the shaped batch's own
    /// column: the file is kept or skipped by it, and the answer and batch are released after it is read (review
    /// round 3). The partition arrays are managed, so this checks the answers, not a double free.
    /// </summary>
    [Theory]
    [InlineData("flag AND v < 30")]
    [InlineData("v < 30 AND flag")]
    [InlineData("v < 30 AND NOT flag")]
    public async Task ABarePartitionBooleanConjunctAnswersOncePerFile(string filter)
    {
        await using var table = await CreateFlagPartitionedAsync();
        bool wantFlag = !filter.Contains("NOT", StringComparison.Ordinal);
        var expected = Enumerable.Range(0, 400).Where(i => (i / 100 % 2 == 0) == wantFlag && i % 100 < 30)
            .Select(i => (long)i).ToList();
        Assert.Equal(expected, await ReadIdsAsync(table, filter));
    }

    /// <summary>
    /// A partition-only DELETE whose mask is a bare boolean partition column: the file it matches is removed
    /// whole from its one-row answer, which is released and disposed after it is read, and the others are read.
    /// </summary>
    [Theory]
    [InlineData("flag", true)]
    [InlineData("NOT flag", false)]
    public async Task APartitionOnlyDeleteOnABareBooleanPartitionRemovesItsFiles(string filter, bool deletedFlag)
    {
        await using var table = await CreateFlagPartitionedAsync();
        var (deleted, _) = await table.DeleteAsync(P(filter));
        Assert.Equal(200, deleted);
        var expected = Enumerable.Range(0, 400).Where(i => (i / 100 % 2 == 0) != deletedFlag)
            .Select(i => (long)i).ToList();
        Assert.Equal(expected, await ReadIdsAsync(table, "v >= 0"));
    }

    private async Task<DeltaTable> CreateFlagPartitionedAsync()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("flag", BooleanType.Default, false))
            .Field(new Field("v", Int32Type.Default, false))
            .Build();
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, $"t_{Guid.NewGuid():N}")), schema,
            new DeltaTableOptions { MaxConcurrentReads = 4 }, partitionColumns: ["flag"]);
        for (int f = 0; f < 4; f++)
        {
            var id = new Int64Array.Builder();
            var flag = new BooleanArray.Builder();
            var v = new Int32Array.Builder();
            for (int i = 0; i < 100; i++)
            {
                id.Append(f * 100 + i);
                flag.Append(f % 2 == 0);
                v.Append(i);
            }

            await table.WriteAsync([new RecordBatch(schema, [id.Build(), flag.Build(), v.Build()], 100)]);
        }

        return table;
    }

    private static async Task<List<long>> ReadIdsAsync(DeltaTable table, string filter)
    {
        var actual = new List<long>();
        await foreach (var b in table.ReadAsync(new DeltaReadOptions { Columns = ["id"], Filter = P(filter), FilterRows = true }))
        {
            using (b)
            {
                var ids = (Int64Array)b.Column(0);
                for (int i = 0; i < b.Length; i++)
                    actual.Add(ids.GetValue(i)!.Value);
            }
        }

        actual.Sort();
        return actual;
    }

    /// <summary>A file-constant conjunct that raises, with no row-level conjunct before it, raises up front.</summary>
    [Fact]
    public async Task AFileConstantConjunctThatRaisesFirstRaises()
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 1, seam: false, ColumnMappingMode.None);
        var options = new DeltaReadOptions
        {
            Columns = ["id"], Filter = P("CAST(region AS INT) > 0 AND v < 70"), FilterRows = true,
        };
        var error = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await foreach (var b in table.ReadAsync(options))
                b.Dispose();
        });
        Assert.Contains("CAST_INVALID_INPUT", error.ToString());
    }

    // ---- Host row masks (3b) ---------------------------------------------------------------------------------------

    private static BooleanArray Answer(int length, Func<int, bool?> row)
    {
        var b = new BooleanArray.Builder().Reserve(length);
        for (int i = 0; i < length; i++)
        {
            if (row(i) is { } v)
                b.Append(v);
            else
                b.AppendNull();
        }

        return b.Build();
    }

    private static long Id(RecordBatch batch, int row) => ((Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"))).GetValue(row)!.Value;

    private static readonly Dictionary<string, DeltaRowMask[]> Masks = new()
    {
        // A key column, as a join filter probes it.
        ["key"] = [new DeltaRowMask(["v"], b =>
        {
            var v = (Int32Array)b.Column(0);
            return Answer(b.Length, i => v.GetValue(i)!.Value % 3 == 0);
        })],
        // Two columns, named out of table order: the batch holds them in the mask's order.
        ["two columns"] = [new DeltaRowMask(["n", "id"], b =>
        {
            Assert.Equal(["n", "id"], b.Schema.FieldsList.Select(f => f.Name));
            var n = (Int32Array)b.Column(0);
            var id = (Int64Array)b.Column(1);
            return Answer(b.Length, i => !n.IsNull(i) && id.GetValue(i)!.Value % 2 == 0);
        })],
        // A partition column: the file's value, asked once per file.
        ["partition"] = [new DeltaRowMask(["region"], b =>
        {
            var region = (StringArray)b.Column(0);
            return Answer(b.Length, i => region.GetString(i) != "b");
        })],
        // NULL drops a row, as FALSE does.
        ["null answers"] = [new DeltaRowMask(["id"], b =>
        {
            var id = (Int64Array)b.Column(0);
            return Answer(b.Length, i => id.GetValue(i)!.Value % 4 == 0 ? null : true);
        })],
        // Several masks: every one must keep the row.
        ["two masks"] =
        [
            new DeltaRowMask(["v"], b =>
            {
                var v = (Int32Array)b.Column(0);
                return Answer(b.Length, i => v.GetValue(i)!.Value < 60);
            }),
            new DeltaRowMask(["note", "region"], b =>
            {
                var note = (StringArray)b.Column(0);
                var region = (StringArray)b.Column(1);
                return Answer(b.Length, i => note.GetString(i).EndsWith("1", StringComparison.Ordinal) || region.GetString(i) == "c");
            }),
        ],
    };

    public static IEnumerable<object[]> MaskCases()
    {
        foreach (var mask in Masks.Keys)
        {
            foreach (var filter in new[] { "", "data", "nullable" })
            {
                foreach (var projection in Projections.Keys)
                {
                    foreach (var (concurrency, seam) in new[] { (1, false), (4, false), (1, true) })
                        yield return [mask, filter, projection, concurrency, seam];
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(MaskCases))]
    public async Task HostMasksKeepOnlyTheRowsEveryMaskAnswersTrueFor(
        string maskName, string filterName, string projectionName, int concurrency, bool seam)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency, seam, ColumnMappingMode.None);
        await AssertExactAsync(
            table, filterName.Length == 0 ? null : Filters[filterName], Projections[projectionName], Masks[maskName]);
    }

    [Fact]
    public async Task WithoutFilterRowsTheFilterStillOnlyPrunesButTheMasksApply()
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 1, seam: false, ColumnMappingMode.None);
        var options = new DeltaReadOptions { Filter = Filters["data"], RowMasks = Masks["key"] };
        var mask = DeltaTable.MaskFor(Filters["data"]);
        long rows = 0, outsideFilter = 0;
        await foreach (var b in table.ReadAsync(options))
        {
            using (b)
            {
                using var inFilter = mask(b);
                var v = (Int32Array)b.Column(b.Schema.GetFieldIndex("v"));
                for (int i = 0; i < b.Length; i++)
                {
                    Assert.Equal(0, v.GetValue(i)!.Value % 3);
                    if (inFilter.GetValue(i) != true)
                        outsideFilter++;
                }

                rows += b.Length;
            }
        }

        Assert.True(rows > 0);
        Assert.True(outsideFilter > 0, "the filter was applied to rows without FilterRows");
    }

    /// <summary>
    /// The filter and each mask are whole conditions: a NULL answer drops the row, so the masks after it are never
    /// asked about it (a mask dereferencing a nullable column the filter already tested would otherwise see NULLs).
    /// </summary>
    [Theory]
    [InlineData(1, false)]
    [InlineData(4, false)]
    [InlineData(1, true)]
    public async Task AMaskIsNeverAskedAboutARowAnEarlierConditionAnsweredNullFor(int concurrency, bool seam)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency, seam, ColumnMappingMode.None);
        var sawN = new System.Collections.Concurrent.ConcurrentBag<int?>();
        var sawId = new System.Collections.Concurrent.ConcurrentBag<long>();
        var afterFilter = new DeltaRowMask(["n"], b =>
        {
            var n = (Int32Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                sawN.Add(n.GetValue(i));
            return Answer(b.Length, _ => true);
        });
        var nullForEveryFourth = new DeltaRowMask(["id"], b => Answer(b.Length, i => Id(b, i) % 4 == 0 ? null : true));
        var afterMask = new DeltaRowMask(["id"], b =>
        {
            for (int i = 0; i < b.Length; i++)
                sawId.Add(Id(b, i));
            return Answer(b.Length, _ => true);
        });

        var options = new DeltaReadOptions
        {
            Columns = ["id"],
            Filter = Filters["nullable"], // n > 50: NULL for every fifth row
            FilterRows = true,
            RowMasks = [afterFilter, nullForEveryFourth, afterMask],
        };
        await foreach (var b in table.ReadAsync(options))
            b.Dispose();

        Assert.NotEmpty(sawN);
        Assert.All(sawN, n => Assert.True(n > 50, $"a mask after `n > 50` was asked about n = {n?.ToString() ?? "NULL"}"));
        Assert.NotEmpty(sawId);
        Assert.DoesNotContain(sawId, id => id % 4 == 0);
    }

    /// <summary>
    /// A mask over only a partition column, after a mask the reader answers row by row: it is asked about a file only
    /// once a row of it survives the earlier mask. Here it raises for region c, whose rows the earlier mask rejects.
    /// </summary>
    [Theory]
    [InlineData(1, false)]
    [InlineData(4, false)]
    [InlineData(1, true)]
    public async Task AFileConstantMaskIsNotAskedAboutAFileAnEarlierMaskEmptied(int concurrency, bool seam)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency, seam, ColumnMappingMode.None);
        var notC = new DeltaRowMask(["id"], b => Answer(b.Length, i => Id(b, i) % 3 != 2)); // region c is id % 3 == 2
        var onlyA = new DeltaRowMask(["region"], b =>
        {
            var region = (StringArray)b.Column(0);
            return Answer(b.Length, i => region.GetString(i) switch
            {
                "c" => throw new InvalidOperationException("asked about region c"),
                var r => r == "a",
            });
        });

        var ids = new List<long>();
        await foreach (var b in table.ReadAsync(new DeltaReadOptions { Columns = ["id"], RowMasks = [notC, onlyA] }))
        {
            using (b)
            {
                for (int i = 0; i < b.Length; i++)
                    ids.Add(Id(b, i));
            }
        }

        ids.Sort();
        Assert.Equal(Enumerable.Range(0, 1200).Select(i => (long)i).Where(i => i % 3 == 0 && i % 7 != 0), ids);
    }

    /// <summary>
    /// A mask sees each column as the scan returns it: with view output, a string column read from the file is a view
    /// array, while a partition value is a plain one, in the mask's batch as in the scan's.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AMaskSeesEachColumnAsTheScanReturnsIt(bool seam)
    {
        string name = $"t_{Guid.NewGuid():N}";
        await (await CreateAsync(name, 1, seam, ColumnMappingMode.None)).DisposeAsync();
        var fs = new LocalTableFileSystem(Path.Combine(_tempDir, name));
        await using var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions
        {
            ParquetReadOptions = ParquetReadOptions.Default with { ByteArrayOutput = ByteArrayOutputKind.ViewType },
            DataFileReader = seam ? new PassThroughReader(fs) : null,
        });

        var maskTypes = new System.Collections.Concurrent.ConcurrentBag<string>();
        var probe = new DeltaRowMask(["note", "region"], b =>
        {
            maskTypes.Add(string.Join(",", b.Schema.FieldsList.Select(f => f.Name + ":" + b.Column(f.Name).GetType().Name)));
            return Answer(b.Length, _ => true);
        });
        var scanTypes = new HashSet<string>();
        await foreach (var b in table.ReadAsync(new DeltaReadOptions { RowMasks = [probe] }))
        {
            using (b)
                scanTypes.Add(string.Join(",", new[] { "note", "region" }.Select(n => n + ":" + b.Column(n).GetType().Name)));
        }

        Assert.NotEmpty(maskTypes);
        Assert.Equal(scanTypes, maskTypes.ToHashSet());
    }

    /// <summary>
    /// Through the seam, a batch a mask rejects whole is dropped without gathering it: there is nothing to emit, and a
    /// column the gather can't take (a dictionary the host returned) must not fail a scan that returns none of its rows.
    /// </summary>
    [Fact]
    public async Task ASeamBatchAMaskRejectsWholeIsNotGathered()
    {
        // No deletion vector: the pipeline's own deletion-vector gather can't take a dictionary either.
        var fs = new LocalTableFileSystem(Path.Combine(_tempDir, $"t_{Guid.NewGuid():N}"));
        await using var table = await DeltaTable.CreateAsync(
            fs, TableSchema, new DeltaTableOptions { DataFileReader = new DictionaryNoteReader(fs) },
            partitionColumns: ["region"]);
        await table.WriteAsync([Rows(0, 300)]);

        var nothing = new DeltaRowMask(["id"], b => Answer(b.Length, _ => false));
        long rows = 0;
        await foreach (var b in table.ReadAsync(new DeltaReadOptions { Columns = ["id", "note"], RowMasks = [nothing] }))
        {
            using (b)
                rows += b.Length;
        }

        Assert.Equal(0, rows);
    }

    /// <summary>
    /// #794: a dictionary column from the seam survives the pipeline's row gathers. A deletion vector thins its
    /// batches, both for a DELETE that reads the file again (DML) and for a scan, and the gathered column keeps its
    /// dictionary encoding and its values.
    /// </summary>
    [Fact]
    public async Task ASeamDictionaryColumnSurvivesADeletionVector()
    {
        var fs = new LocalTableFileSystem(Path.Combine(_tempDir, $"t_{Guid.NewGuid():N}"));
        await using var table = await DeltaTable.CreateAsync(
            fs, TableSchema, new DeltaTableOptions { DataFileReader = new DictionaryNoteReader(fs) },
            enableDeletionVectors: true);
        await table.WriteAsync([Rows(0, 300)]);

        BooleanArray Where(RecordBatch b, Func<long, bool> doomed)
        {
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            return Answer(b.Length, i => doomed(ids.GetValue(i)!.Value));
        }

        // The first DELETE writes a deletion vector; the second reads the file through it, gathering.
        await table.DeleteAsync(b => Where(b, id => id % 10 == 0));
        await table.DeleteAsync(b => Where(b, id => id % 10 == 1));

        var notes = new Dictionary<long, string>();
        await foreach (var b in table.ReadAsync(new DeltaReadOptions { Columns = ["id", "note"] }))
        {
            using (b)
            {
                Assert.IsType<DictionaryArray>(b.Column(1));
                for (int i = 0; i < b.Length; i++)
                    notes[((Int64Array)b.Column(0)).GetValue(i)!.Value] = DictionaryValue(b.Column(1), i);
            }
        }

        var expected = Enumerable.Range(0, 300).Where(i => i % 10 > 1).ToList();
        Assert.Equal(expected.Select(i => (long)i), notes.Keys.OrderBy(k => k));
        Assert.All(expected, i => Assert.Equal("note-" + i.ToString(CultureInfo.InvariantCulture), notes[i]));
    }

    /// <summary>
    /// #794: an exact filter through the seam that keeps some rows of a batch gathers them, a dictionary column with
    /// them.
    /// </summary>
    [Fact]
    public async Task ASeamDictionaryColumnSurvivesAPartialExactFilter()
    {
        var fs = new LocalTableFileSystem(Path.Combine(_tempDir, $"t_{Guid.NewGuid():N}"));
        await using var table = await DeltaTable.CreateAsync(
            fs, TableSchema, new DeltaTableOptions { DataFileReader = new DictionaryNoteReader(fs) });
        await table.WriteAsync([Rows(0, 300)]);

        var notes = new Dictionary<long, string>();
        await foreach (var b in table.ReadAsync(new DeltaReadOptions
        {
            Columns = ["id", "note"], Filter = Filters["data"], FilterRows = true,
        }))
        {
            using (b)
            {
                for (int i = 0; i < b.Length; i++)
                    notes[((Int64Array)b.Column(0)).GetValue(i)!.Value] = DictionaryValue(b.Column(1), i);
            }
        }

        var expected = Enumerable.Range(0, 300).Where(i => i % 100 < 30).ToList();
        Assert.Equal(expected.Select(i => (long)i), notes.Keys.OrderBy(k => k));
        Assert.All(expected, i => Assert.Equal("note-" + i.ToString(CultureInfo.InvariantCulture), notes[i]));
    }

    /// <summary>
    /// #823 review: a point read (<see cref="DeltaTable.ReadRowsAsync"/>) gathers the selected rows of a dictionary
    /// column and must not release the source's dictionary under them: the gathered column shares it. Every batch is
    /// collected before any is read, so the producer has moved past (and released) each source first; the dictionary
    /// comes from an allocator that overwrites its bytes when released, so a read after release shows as wrong values.
    /// </summary>
    [Fact]
    public async Task APointReadKeepsTheDictionaryItsGatheredRowsShare()
    {
        var fs = new LocalTableFileSystem(Path.Combine(_tempDir, $"t_{Guid.NewGuid():N}"));
        var seam = new PoisoningDictionaryNoteReader(fs);
        await using var table = await DeltaTable.CreateAsync(fs, TableSchema, new DeltaTableOptions { DataFileReader = seam });
        await table.WriteAsync([Rows(0, 300)]);

        var addresses = new List<long>();
        var snapshot = table.CurrentSnapshot;
        await foreach (var b in table.ReadAsync(new DeltaReadOptions
        {
            Columns = ["id"], Metadata = DeltaRowMetadata.RowAddress, Snapshot = snapshot,
        }))
        {
            using (b)
            {
                var ids = (Int64Array)b.Column(0);
                var address = (Int64Array)b.Column(b.Schema.GetFieldIndex(TransientRowAddress.ColumnName));
                for (int i = 0; i < b.Length; i++)
                {
                    if (ids.GetValue(i)!.Value % 7 == 0)
                        addresses.Add(address.GetValue(i)!.Value);
                }
            }
        }

        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadRowsAsync(
            RowSelection.FromRowAddresses(addresses, snapshot)))
        {
            batches.Add(b);
        }

        var notes = new Dictionary<long, string>();
        foreach (var b in batches)
        {
            using (b)
            {
                var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
                var note = b.Column(b.Schema.GetFieldIndex("note"));
                for (int i = 0; i < b.Length; i++)
                    notes[ids.GetValue(i)!.Value] = DictionaryValue(note, i);
            }
        }

        var expected = Enumerable.Range(0, 300).Where(i => i % 7 == 0).ToList();
        Assert.Equal(expected.Select(i => (long)i), notes.Keys.OrderBy(k => k));
        Assert.All(expected, i => Assert.Equal("note-" + i.ToString(CultureInfo.InvariantCulture), notes[i]));
    }

    /// <summary>
    /// <see cref="DictionaryNoteReader"/> whose dictionary and indices are copied into memory that is overwritten when
    /// released, so reading it afterwards returns wrong values rather than stale right ones.
    /// </summary>
    private sealed class PoisoningDictionaryNoteReader(ITableFileSystem fs) : IDataFileReader
    {
        private readonly PoisoningAllocator _allocator = new();

        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var b in new PassThroughReader(fs).ReadAsync(relativePath, physicalColumns, cancellationToken))
            {
                int note = b.Schema.GetFieldIndex("note");
                if (note < 0)
                {
                    yield return b;
                    continue;
                }

                var source = (StringArray)b.Column(note);
                var values = new StringArray.Builder();
                var indices = new Int32Array.Builder();
                for (int i = 0; i < b.Length; i++)
                {
                    values.Append(source.GetString(i));
                    indices.Append(i);
                }

                var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
                var fields = b.Schema.FieldsList.ToList();
                var arrays = Enumerable.Range(0, b.ColumnCount).Select(b.Column).ToList();
                fields[note] = new Field("note", type, fields[note].IsNullable, fields[note].Metadata);
                arrays[note] = new DictionaryArray(type, indices.Build(_allocator), values.Build(_allocator));
                source.Dispose();
                yield return new RecordBatch(new Apache.Arrow.Schema(fields, b.Schema.Metadata), arrays, b.Length);
            }
        }

        private sealed class PoisoningAllocator : Apache.Arrow.Memory.MemoryAllocator
        {
            protected override System.Buffers.IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
            {
                bytesAllocated = length;
                return new Owner(new byte[length]);
            }

            private sealed class Owner(byte[] bytes) : System.Buffers.IMemoryOwner<byte>
            {
                public Memory<byte> Memory => bytes;

                public void Dispose() => bytes.AsSpan().Fill(0xEE);
            }
        }
    }

    // The value at `row` of a column that must still be dictionary-encoded: a gather that decoded it fails here (#823
    // review), not only one that lost or mixed up its values.
    private static string DictionaryValue(IArrowArray column, int row)
    {
        var d = Assert.IsType<DictionaryArray>(column);
        return ((StringArray)d.Dictionary).GetString(((Int32Array)d.Indices).GetValue(row)!.Value);
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(4, false)]
    [InlineData(1, true)]
    public async Task AMaskIsNeverAskedAboutADeletedRow(int concurrency, bool seam)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", concurrency, seam, ColumnMappingMode.None);
        var asked = new System.Collections.Concurrent.ConcurrentBag<long>();
        var probe = new DeltaRowMask(["id"], b =>
        {
            for (int i = 0; i < b.Length; i++)
                asked.Add(Id(b, i));
            return Answer(b.Length, _ => true);
        });
        await foreach (var b in table.ReadAsync(new DeltaReadOptions { Columns = ["note"], RowMasks = [probe] }))
            b.Dispose();

        Assert.NotEmpty(asked);
        Assert.DoesNotContain(asked, id => id % 7 == 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AMaskOnAColumnAddedLaterSeesNullForTheOlderFiles(bool seam)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 4, seam, ColumnMappingMode.None);
        await table.AddColumnAsync(new Field("score", DoubleType.Default, true));
        var withScore = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, false))
            .Field(new Field("v", Int32Type.Default, false))
            .Field(new Field("n", Int32Type.Default, true))
            .Field(new Field("note", StringType.Default, false))
            .Field(new Field("score", DoubleType.Default, true))
            .Build();
        var scores = new DoubleArray.Builder();
        for (int i = 0; i < 40; i++)
            scores.Append(i);
        var batch = Rows(1_000, 40);
        await table.WriteAsync([new RecordBatch(withScore, [.. Enumerable.Range(0, batch.ColumnCount).Select(batch.Column), scores.Build()], 40)]);

        var nullScore = new DeltaRowMask(["score"], b => Answer(b.Length, i => b.Column(0).IsNull(i)));
        var bigScore = new DeltaRowMask(["score"], b =>
        {
            var score = (DoubleArray)b.Column(0);
            return Answer(b.Length, i => score.GetValue(i) > 30);
        });
        await AssertExactAsync(table, null, ["id"], [nullScore]);
        await AssertExactAsync(table, null, ["id", "score"], [bigScore]);
    }

    [Fact]
    public async Task AMaskNamingAColumnTheTableLacksIsRefusedBeforeAnyFileIsRead()
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 1, seam: false, ColumnMappingMode.None);
        bool asked = false;
        var options = new DeltaReadOptions
        {
            RowMasks = [new DeltaRowMask(["id", "missing"], b => { asked = true; return Answer(b.Length, _ => true); })],
        };
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var b in table.ReadAsync(options))
                b.Dispose();
        });
        Assert.False(asked);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AMaskOfTheWrongLengthIsRefused(bool seam)
    {
        await using var table = await CreateAsync($"t_{Guid.NewGuid():N}", 1, seam, ColumnMappingMode.None);
        var options = new DeltaReadOptions
        {
            RowMasks = [new DeltaRowMask(["id"], b => Answer(b.Length + 1, _ => true))],
        };
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var b in table.ReadAsync(options))
                b.Dispose();
        });
    }

    [Fact]
    public void AMaskOverNoColumnOrARepeatedOneIsRefused()
    {
        Assert.Throws<ArgumentException>(() => new DeltaRowMask([], b => Answer(b.Length, _ => true)));
        Assert.Throws<ArgumentException>(() => new DeltaRowMask(["id", "id"], b => Answer(b.Length, _ => true)));
    }

    // ---- The oracle ----------------------------------------------------------------------------------------------

    private static async Task AssertExactAsync(
        DeltaTable table, Predicate? filter, string[]? projection, IReadOnlyList<DeltaRowMask>? masks = null)
    {
        var expected = await ReadRowsAsync(table, filter, projection, exactly: false, masks);
        var actual = await ReadRowsAsync(table, filter, projection, exactly: true, masks);
        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Each row as text: its projected columns, then its locator position and row id. Without <paramref name="exactly"/>,
    /// a superset scan over the projection plus every column (so the filter can be evaluated), filtered row by row by
    /// the filter and then by each of <paramref name="masks"/>, asked over its own columns of the superset batch.
    /// </summary>
    private static async Task<List<string>> ReadRowsAsync(
        DeltaTable table, Predicate? filter, string[]? projection, bool exactly, IReadOnlyList<DeltaRowMask>? masks = null)
    {
        var options = new DeltaReadOptions
        {
            Columns = exactly ? projection : null,
            Filter = filter,
            FilterRows = exactly && filter is not null,
            RowMasks = exactly ? masks : null,
            Metadata = table.CurrentSnapshot.Metadata.Configuration?.TryGetValue("delta.enableRowTracking", out var on) == true
                && on == "true"
                ? DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking
                : DeltaRowMetadata.Locator,
        };
        var rows = new List<string>();
        var mask = filter is null ? null : DeltaTable.MaskFor(filter);
        await foreach (var batch in table.ReadAsync(options))
        {
            using (batch)
            {
                if (exactly && projection is not null)
                {
                    // The columns read only for the filter are not returned: the data columns are the projection.
                    var dataColumns = batch.Schema.FieldsList
                        .Select(f => f.Name)
                        .Where(n => table.CurrentSnapshot.ArrowSchema.GetFieldIndex(n) >= 0)
                        .ToHashSet();
                    Assert.True(dataColumns.SetEquals(projection),
                        $"returned {string.Join(",", dataColumns)} for a projection of {string.Join(",", projection)}");
                }

                BooleanArray? keep = exactly || mask is null ? null : mask(batch);
                var hostAnswers = exactly || masks is null ? [] : masks.Select(m => m.Mask(new RecordBatch(
                        new Apache.Arrow.Schema(m.Columns.Select(c => batch.Schema.GetFieldByName(c)).ToList(), null),
                        m.Columns.Select(c => batch.Column(c)).ToList(), batch.Length)))
                    .ToList();
                for (int r = 0; r < batch.Length; r++)
                {
                    if (keep is not null && keep.GetValue(r) != true)
                        continue;
                    if (hostAnswers.Any(a => a.GetValue(r) != true))
                        continue;
                    var cells = new List<string>();
                    for (int c = 0; c < batch.ColumnCount; c++)
                    {
                        string name = batch.Schema.GetFieldByIndex(c).Name;
                        bool data = table.CurrentSnapshot.ArrowSchema.GetFieldIndex(name) >= 0;
                        if (data && projection is not null && !projection.Contains(name))
                            continue;
                        cells.Add(name + "=" + Render(batch.Column(c), r));
                    }

                    rows.Add(string.Join("|", cells));
                }
            }
        }

        return rows;
    }

    private static string Render(IArrowArray array, int row) => array.IsNull(row) ? "null" : array switch
    {
        Int64Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        Int32Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        DoubleArray a => a.GetValue(row)!.Value.ToString("R", CultureInfo.InvariantCulture),
        StringArray a => a.GetString(row),
        StringViewArray a => a.GetString(row),
        _ => throw new NotSupportedException(array.Data.DataType.Name),
    };

    // ---- The tables ------------------------------------------------------------------------------------------------

    /// <summary>
    /// Three files of 400 rows in row groups of 100, partitioned by <c>region</c> (a, b, c), with a deletion vector
    /// deleting every seventh row, row tracking on: <c>id</c> unique, <c>v</c> = id mod 100, <c>n</c> NULL every
    /// fifth row.
    /// </summary>
    private async Task<DeltaTable> CreateAsync(
        string name, int concurrency, bool seam, ColumnMappingMode mapping, bool rowTracking = true)
    {
        var fs = new LocalTableFileSystem(Path.Combine(_tempDir, name));
        var table = await DeltaTable.CreateAsync(
            fs, TableSchema,
            new DeltaTableOptions
            {
                ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 100, DataPageRowCountLimit = 20 },
                MaxConcurrentReads = concurrency,
                DataFileReader = seam ? new PassThroughReader(fs) : null,
            },
            partitionColumns: ["region"],
            enableDeletionVectors: true,
            enableRowTracking: rowTracking,
            columnMappingMode: mapping);
        for (int f = 0; f < 3; f++)
            await table.WriteAsync([Rows(f * 400, 400)]);

        var (deleted, _) = await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var every7th = new BooleanArray.Builder().Reserve(ids.Length);
            for (int i = 0; i < ids.Length; i++)
                every7th.Append(ids.GetValue(i)!.Value % 7 == 0);
            return every7th.Build();
        });
        Assert.True(deleted > 0);
        return table;
    }

    private static RecordBatch Rows(int first, int count)
    {
        string[] regions = ["a", "b", "c"];
        var id = new Int64Array.Builder();
        var region = new StringArray.Builder();
        var v = new Int32Array.Builder();
        var n = new Int32Array.Builder();
        var note = new StringArray.Builder();
        for (int i = first; i < first + count; i++)
        {
            id.Append(i);
            region.Append(regions[i % 3]);
            v.Append(i % 100);
            if (i % 5 == 0)
                n.AppendNull();
            else
                n.Append((i * 7) % 100);
            note.Append("note-" + i.ToString(CultureInfo.InvariantCulture));
        }

        return new RecordBatch(TableSchema, [id.Build(), region.Build(), v.Build(), n.Build(), note.Build()], count);
    }

    /// <summary>A codec seam that returns <c>note</c> dictionary-encoded, as Arrow hosts commonly do.</summary>
    private sealed class DictionaryNoteReader(ITableFileSystem fs) : IDataFileReader
    {
        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var b in new PassThroughReader(fs).ReadAsync(relativePath, physicalColumns, cancellationToken))
            {
                int note = b.Schema.GetFieldIndex("note");
                if (note < 0)
                {
                    yield return b;
                    continue;
                }

                var indices = new Int32Array.Builder();
                for (int i = 0; i < b.Length; i++)
                    indices.Append(i);
                var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
                var fields = b.Schema.FieldsList.ToList();
                var arrays = Enumerable.Range(0, b.ColumnCount).Select(b.Column).ToList();
                fields[note] = new Field("note", type, fields[note].IsNullable, fields[note].Metadata);
                arrays[note] = new DictionaryArray(type, indices.Build(), b.Column(note));
                yield return new RecordBatch(new Apache.Arrow.Schema(fields, b.Schema.Metadata), arrays, b.Length);
            }
        }
    }

    /// <summary>A codec seam that decodes with the built-in reader, every row, in file order.</summary>
    private sealed class PassThroughReader(ITableFileSystem fs) : IDataFileReader
    {
        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var file = await fs.OpenReadAsync(relativePath, cancellationToken);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            await foreach (var b in reader.ReadAllAsync(columnNames: physicalColumns, cancellationToken: cancellationToken))
                yield return b;
        }
    }
}
