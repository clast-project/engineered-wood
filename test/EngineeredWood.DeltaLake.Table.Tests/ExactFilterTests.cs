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
