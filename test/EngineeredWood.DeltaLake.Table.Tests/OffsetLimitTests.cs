// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaReadOptions.Offset"/> and <see cref="DeltaReadOptions.Limit"/> (#502 phase 5f). The oracle is the
/// same scan with neither, its rows (and their row addresses) skipped and taken in order. The table has 3 files of 4
/// row groups x 500 rows, a deletion vector deleting every 13th row of files 0 and 1 and a whole row group of file 1,
/// and a partition.
/// </summary>
public class OffsetLimitTests : IDisposable
{
    private const int Files = 3;
    private const int RowsPerFile = 2_000;
    private const int RowsPerGroup = 500;

    private readonly string _tempDir;

    public OffsetLimitTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_offsetlimit_{Guid.NewGuid():N}");
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
        .Field(new Field("p", StringType.Default, false))
        .Build();

    private static readonly long[] Offsets = [0, 1, 499, 500, 1_001, 2_000, 3_500, 7_000];
    private static readonly long?[] Limits = [null, 0, 1, 10, 501, 2_500];

    public static IEnumerable<object[]> Modes()
    {
        foreach (int concurrency in new[] { 1, 4 })
        {
            foreach (string mode in new[] { "plain", "exact filter", "mask" })
                yield return [concurrency, mode];
        }
    }

    /// <summary>Every offset and limit of the sweep returns the oracle's rows, in its order, with its row addresses.</summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task ReturnsTheScansRowsSkippedAndTaken(int concurrency, string mode)
    {
        await using var table = await CreateAsync(concurrency);
        DeltaReadOptions Options() => mode switch
        {
            "exact filter" => new DeltaReadOptions
            {
                Columns = ["id", "cat"], Filter = SparkSqlParser.ParsePredicate("cat <> 'c1'"), FilterRows = true,
                Metadata = DeltaRowMetadata.RowAddress,
            },
            "mask" => new DeltaReadOptions
            {
                Columns = ["id", "cat"], Metadata = DeltaRowMetadata.RowAddress,
                RowMasks = [new DeltaRowMask(["id"], batch =>
                {
                    var ids = (Int64Array)batch.Column(0);
                    var answer = new BooleanArray.Builder().Reserve(ids.Length);
                    for (int i = 0; i < ids.Length; i++)
                        answer.Append(ids.GetValue(i)!.Value % 3 != 0);
                    return answer.Build();
                })],
            },
            _ => new DeltaReadOptions { Columns = ["id", "cat", "p"], Metadata = DeltaRowMetadata.RowAddress },
        };

        var all = await ReadAsync(table, Options());
        Assert.True(all.Count > 3_000);
        foreach (long offset in Offsets)
        {
            foreach (long? limit in Limits)
            {
                var expected = all.Skip((int)Math.Min(offset, all.Count)).Take(limit is { } l ? (int)l : int.MaxValue).ToList();
                var actual = await ReadAsync(table, Options() with { Offset = offset, Limit = limit });
                Assert.True(expected.SequenceEqual(actual),
                    $"{mode}, concurrency {concurrency}, offset {offset}, limit {limit?.ToString(CultureInfo.InvariantCulture) ?? "none"}: "
                    + $"{actual.Count} rows, expected {expected.Count}; first {actual.FirstOrDefault()} vs {expected.FirstOrDefault()}");
            }
        }
    }

    /// <summary>
    /// With no row condition, the units a limit or an offset leaves out are not read: a limit inside the first row group
    /// reads one unit, and an offset covering the first file reads none of its row groups.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task WithNoRowConditionOnlyTheNeededRowGroupsAreRead(int concurrency)
    {
        await using var table = await CreateAsync(concurrency);
        var options = new DeltaReadOptions { Columns = ["id"] };

        int before = table.ScanUnitsRead;
        var ten = await ReadAsync(table, options with { Limit = 10 });
        Assert.Equal(10, ten.Count);
        Assert.Equal(1, table.ScanUnitsRead - before);

        // File 0 holds 2,000 rows less every 13th deleted: an offset of 2,000 is past all of them.
        before = table.ScanUnitsRead;
        var paged = await ReadAsync(table, options with { Offset = 2_000, Limit = 10 });
        Assert.Equal(10, paged.Count);
        Assert.Equal(1, table.ScanUnitsRead - before);

        before = table.ScanUnitsRead;
        int unlimitedUnits = (await ReadAsync(table, options)).Count >= 0 ? table.ScanUnitsRead - before : 0;
        before = table.ScanUnitsRead;
        await ReadAsync(table, options with { Offset = 4_000 });
        Assert.True(table.ScanUnitsRead - before < unlimitedUnits,
            $"an offset of 4,000 read {table.ScanUnitsRead - before} units of {unlimitedUnits}");
    }

    /// <summary>
    /// With an exact filter, a parallel scan's limit reads one row group first, then two more for each that ends, so a
    /// limit the first row group meets reads at most three, not the window's eight.
    /// </summary>
    [Fact]
    public async Task AFilteredLimitRampsItsWindow()
    {
        await using var table = await CreateAsync(concurrency: 8);
        var options = new DeltaReadOptions
        {
            Columns = ["id"], Filter = SparkSqlParser.ParsePredicate("cat <> 'c1'"), FilterRows = true, Limit = 1,
        };

        int before = table.ScanUnitsRead;
        Assert.Single(await ReadAsync(table, options));
        int units = table.ScanUnitsRead - before;
        Assert.True(units is >= 1 and <= 3, $"read {units} units");
    }

    [Fact]
    public async Task ALimitOfZeroReadsNothing()
    {
        await using var table = await CreateAsync(concurrency: 4);
        int before = table.ScanUnitsRead;
        Assert.Empty(await ReadAsync(table, new DeltaReadOptions { Columns = ["id"], Offset = 5, Limit = 0 }));
        Assert.Equal(before, table.ScanUnitsRead);
    }

    [Fact]
    public async Task ANegativeOffsetOrLimitAndASupersetFilterAreRefused()
    {
        await using var table = await CreateAsync(concurrency: 1);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ReadAsync(table, new DeltaReadOptions { Offset = -1 }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ReadAsync(table, new DeltaReadOptions { Limit = -1 }));
        var superset = SparkSqlParser.ParsePredicate("id < 100L");
        await Assert.ThrowsAsync<ArgumentException>(() => ReadAsync(table, new DeltaReadOptions { Filter = superset, Limit = 5 }));
        await Assert.ThrowsAsync<ArgumentException>(() => ReadAsync(table, new DeltaReadOptions { Filter = superset, Offset = 5 }));

        // Without an offset or a limit, the superset filter is as before.
        Assert.NotEmpty(await ReadAsync(table, new DeltaReadOptions { Filter = superset }));
    }

    // ---- The table --------------------------------------------------------------------------------------------------

    private static bool IsDeleted(long id) =>
        (id < 2 * RowsPerFile && id % 13 == 0) || id is >= 2_500 and < 3_000;

    private async Task<DeltaTable> CreateAsync(int concurrency)
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, $"t_{Guid.NewGuid():N}")), TableSchema,
            new DeltaTableOptions
            {
                ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup, DataPageRowCountLimit = 100 },
                MaxConcurrentReads = concurrency,
                TargetFileSize = long.MaxValue,
            },
            partitionColumns: ["p"],
            enableDeletionVectors: true);
        for (int f = 0; f < Files; f++)
        {
            var id = new Int64Array.Builder();
            var cat = new StringArray.Builder();
            var p = new StringArray.Builder();
            for (long i = f * RowsPerFile; i < (f + 1) * RowsPerFile; i++)
            {
                id.Append(i);
                if (i % 11 == 0)
                    cat.AppendNull();
                else
                    cat.Append("c" + (i % 5).ToString(CultureInfo.InvariantCulture));
                p.Append("q" + f.ToString(CultureInfo.InvariantCulture));
            }

            await table.WriteAsync([new RecordBatch(TableSchema, [id.Build(), cat.Build(), p.Build()], RowsPerFile)]);
        }

        var (deleted, _) = await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var answer = new BooleanArray.Builder().Reserve(ids.Length);
            for (int i = 0; i < ids.Length; i++)
                answer.Append(IsDeleted(ids.GetValue(i)!.Value));
            return answer.Build();
        });
        Assert.True(deleted > 500);
        return table;
    }

    /// <summary>Each row rendered, every column (metadata included), in the scan's order.</summary>
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

        return rows;
    }
}
