// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaReadOptions.Filter"/> reaches the pages of a surviving row group when the table's
/// <see cref="ParquetReadOptions.FilterUsePageIndex"/> is set. Every table here is ONE file of 300 rows,
/// <c>id</c> 0..299 in row groups of 100 and pages of 10, so a point filter keeps one page of one row
/// group. Row position equals <c>id</c>, and the pages a filter keeps are separated by pages it skips,
/// so every position-keyed step (deletion vector, row index, row id) sees rows AFTER a skipped range.
/// </summary>
public class PageIndexPushdownTests : IDisposable
{
    private const int RowsPerGroup = 100;
    private const int RowsPerPage = 10;
    private const int Rows = 300;

    /// <summary>Row group 0 keeps the pages [10, 20) and [70, 80); groups 1 and 2 are ruled out.</summary>
    private static readonly Predicate TwoPages = Ex.Or(Ex.Equal("id", 15L), Ex.Equal("id", 75L));

    private readonly string _tempDir;

    public PageIndexPushdownTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_pipd_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema IdSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    /// <summary>A batch size smaller than a page, so a kept range comes back as several batches.</summary>
    private static DeltaTableOptions SmallPages(bool pageIndex = true) => new()
    {
        ParquetWriteOptions = ParquetWriteOptions.Default with
        {
            RowGroupMaxRows = RowsPerGroup,
            DataPageRowCountLimit = RowsPerPage,
        },
        ParquetReadOptions = ParquetReadOptions.Default with { FilterUsePageIndex = pageIndex, BatchSize = 4 },
    };

    private async Task<DeltaTable> CreateOneFileTable(
        bool pageIndex = true, ColumnMappingMode mode = ColumnMappingMode.None, bool dv = false, bool rowTracking = false)
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, SmallPages(pageIndex),
            columnMappingMode: mode, enableDeletionVectors: dv, enableRowTracking: rowTracking);
        await table.WriteAsync([new RecordBatch(IdSchema, [Ids(0, Rows)], Rows)]);
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        return table;
    }

    private static Int64Array Ids(int start, int count) =>
        new Int64Array.Builder().AppendRange(Enumerable.Range(start, count).Select(v => (long)v)).Build();

    private static async Task<List<long>> ReadIds(DeltaTable table, Predicate? filter)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Filter = filter }))
        {
            var column = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        return ids;
    }

    private static List<long> Range(int start, int count) =>
        Enumerable.Range(start, count).Select(v => (long)v).ToList();

    private static List<long> TwoPagesRows => [.. Range(10, RowsPerPage), .. Range(70, RowsPerPage)];

    [Fact]
    public async Task PageIndex_NarrowsWithinARowGroup()
    {
        await using var table = await CreateOneFileTable();

        Assert.Equal(TwoPagesRows, await ReadIds(table, TwoPages));
        Assert.Equal(Range(0, Rows), await ReadIds(table, null));
    }

    [Fact]
    public async Task PageIndex_Off_ReadsTheWholeRowGroup()
    {
        await using var table = await CreateOneFileTable(pageIndex: false);

        Assert.Equal(Range(0, RowsPerGroup), await ReadIds(table, TwoPages));
    }

    /// <summary>
    /// The deletion vector is keyed by file position: a deleted row inside each kept range must go, and
    /// one in a skipped range must not shift the rows after it.
    /// </summary>
    [Fact]
    public async Task DeletionVector_StillAppliesAtTheRowsFilePosition()
    {
        await using var table = await CreateOneFileTable(dv: true);
        await table.DeleteAsync(Ex.In("id", [LiteralValue.Of(12L), LiteralValue.Of(40L), LiteralValue.Of(77L), LiteralValue.Of(79L)]));
        Assert.NotNull(table.CurrentSnapshot.ActiveFiles.Values.Single().DeletionVector);

        var expected = TwoPagesRows;
        expected.RemoveAll(id => id is 12 or 77 or 79);
        Assert.Equal(expected, await ReadIds(table, TwoPages));
    }

    [Fact]
    public async Task MetadataColumns_CarryTheFilePosition_AfterASkippedPageRange()
    {
        await using var table = await CreateOneFileTable(rowTracking: true);

        var options = new DeltaReadOptions
        {
            Filter = TwoPages,
            Metadata = DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
        };
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(options))
        {
            var id = (Int64Array)batch.Column("id");
            var rowIndex = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIndexSuffix);
            var rowId = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIdSuffix);
            for (int i = 0; i < batch.Length; i++)
            {
                Assert.Equal(id.GetValue(i), rowIndex.GetValue(i));
                Assert.Equal(id.GetValue(i), rowId.GetValue(i)); // baseRowId 0 + position
                ids.Add(id.GetValue(i)!.Value);
            }
        }
        Assert.Equal(TwoPagesRows, ids);
    }

    /// <summary>
    /// After a compaction, row ids are MATERIALIZED in the file and no longer equal the position: the
    /// deleted row shifted every row after it. A page-pruned read must still pair each row with its own.
    /// </summary>
    [Fact]
    public async Task MaterializedRowIds_StayWithTheirRows()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, SmallPages(),
            enableDeletionVectors: true, enableRowTracking: true);
        await using var _ = table;
        await table.WriteAsync([new RecordBatch(IdSchema, [Ids(0, Rows / 2)], Rows / 2)]);
        await table.WriteAsync([new RecordBatch(IdSchema, [Ids(Rows / 2, Rows / 2)], Rows / 2)]);
        await table.DeleteAsync(Ex.Equal("id", 3L));
        Assert.NotNull(await table.CompactAsync());
        Assert.Single(table.CurrentSnapshot.ActiveFiles);

        // The compacted file's page boundaries are the writer's, so the oracle is a full read: every row the
        // pruned read returns must carry the row index and row id the full read gives it.
        var full = await RowsWithMetadata(table, filter: null);
        var pruned = await RowsWithMetadata(table, TwoPages);

        Assert.All(full, row => Assert.Equal(row.Id, row.RowId)); // the original id, kept through the rewrite
        Assert.Contains(full, row => row.RowIndex != row.Id);     // row 3's removal shifted the rows after it
        Assert.All(pruned, row => Assert.Contains(row, full));
        Assert.Contains(pruned, row => row.Id == 15);
        Assert.Contains(pruned, row => row.Id == 75);
        Assert.True(pruned.Count < full.Count / 2, $"kept {pruned.Count} of {full.Count} rows");
    }

    private static async Task<List<(long Id, long RowIndex, long RowId)>> RowsWithMetadata(DeltaTable table, Predicate? filter)
    {
        var options = new DeltaReadOptions
        {
            Filter = filter,
            Metadata = DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
        };
        var rows = new List<(long, long, long)>();
        await foreach (var batch in table.ReadAsync(options))
        {
            var id = (Int64Array)batch.Column("id");
            var rowIndex = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIndexSuffix);
            var rowId = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIdSuffix);
            for (int i = 0; i < batch.Length; i++)
                rows.Add((id.GetValue(i)!.Value, rowIndex.GetValue(i)!.Value, rowId.GetValue(i)!.Value));
        }
        return rows;
    }

    [Theory]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task ColumnMapping_TranslatesLogicalNames(ColumnMappingMode mode)
    {
        await using var table = await CreateOneFileTable(mode: mode);

        Assert.Equal(TwoPagesRows, await ReadIds(table, TwoPages));
    }

    /// <summary>The partition column is not in the file: a filter on it keeps everything the file holds.</summary>
    [Fact]
    public async Task PartitionedTable_NarrowsByTheFileColumns()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("part", StringType.Default, false))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, SmallPages(), partitionColumns: ["part"]);
        var parts = new StringArray.Builder().AppendRange(Enumerable.Repeat("a", Rows)).Build();
        await table.WriteAsync([new RecordBatch(schema, [parts, Ids(0, Rows)], Rows)]);
        Assert.Single(table.CurrentSnapshot.ActiveFiles);

        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions
        {
            Filter = Ex.And(Ex.Equal("part", "a"), TwoPages),
        }))
        {
            var part = (StringArray)batch.Column("part");
            var id = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
            {
                Assert.Equal("a", part.GetString(i));
                ids.Add(id.GetValue(i)!.Value);
            }
        }
        Assert.Equal(TwoPagesRows, ids);
    }
}
