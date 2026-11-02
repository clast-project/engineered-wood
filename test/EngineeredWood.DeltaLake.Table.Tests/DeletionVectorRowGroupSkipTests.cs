// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A scan or a predicate DELETE skips, unread, the row groups a file's deletion vector deletes in full (#618).
/// Every table here is ONE file of 500 rows, <c>id</c> 0..499 in row groups of 100, so a row's position, its
/// <c>id</c> and (row tracking on, base row id 0) its row id are all the same number. The vector is set up by
/// the functional DELETE, which reads every row. The oracle is that identity: whatever is skipped, each row read
/// must carry its own position and row id. Byte counts against a vector one row short of the group show the
/// group was not read.
/// </summary>
public class DeletionVectorRowGroupSkipTests : IDisposable
{
    private const int RowsPerGroup = 100;
    private const int Rows = 500;

    private readonly string _tempDir;

    public DeletionVectorRowGroupSkipTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dvskip_{Guid.NewGuid():N}");
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
        .Field(new Field("payload", StringType.Default, false))
        .Build();

    /// <summary>
    /// The deleted range [from, to): exactly one group, two adjacent groups, a group plus one row of the next, a
    /// group less one row, and every group but the last.
    /// </summary>
    [Theory]
    [InlineData(100, 200)]
    [InlineData(100, 300)]
    [InlineData(100, 201)]
    [InlineData(101, 200)]
    [InlineData(0, 400)]
    public async Task AScanReturnsEachSurvivingRowAtItsOwnPositionAndRowId(int from, int to)
    {
        var (table, _) = await CreateAsync("t", from, to);
        await using (table)
        {
            var rows = await ReadAsync(table, filter: null);
            Assert.Equal(Survivors(from, to), rows);
        }
    }

    /// <summary>Statistics pruning and the vector's skip in the same file: each removes its own groups.</summary>
    [Fact]
    public async Task AFilteredScanComposesWithTheSkip()
    {
        var (table, _) = await CreateAsync("t", 300, 400);
        await using (table)
        {
            var rows = await ReadAsync(table, Ex.GreaterThanOrEqual("id", 250L));
            Assert.Equal(Enumerable.Range(250, 50).Concat(Enumerable.Range(400, 100)).Select(i => (long)i), rows);
        }
    }

    /// <summary>
    /// A fully deleted group costs no data read: against a vector one row short of covering it, which must read
    /// the group, the scan reads at least most of a group's bytes fewer.
    /// </summary>
    [Fact]
    public async Task AFullyDeletedGroupIsNotRead()
    {
        var (skipped, skippedFs) = await CreateAsync("skipped", 100, 300);
        var (read, readFs) = await CreateAsync("read", 101, 300);
        await using (skipped)
        await using (read)
        {
            skippedFs.Reset();
            readFs.Reset();
            await ReadAsync(skipped, filter: null);
            await ReadAsync(read, filter: null);

            // Two of five groups skipped against one... less one row: about a fifth of the data, as the payloads
            // dominate; a half is a safe floor.
            long groupBytes = readFs.DataBytesRead / 4;
            Assert.True(skippedFs.DataBytesRead < readFs.DataBytesRead - groupBytes / 2,
                $"skipped read {skippedFs.DataBytesRead} bytes, the unskipped one {readFs.DataBytesRead}");
        }
    }

    /// <summary>
    /// A predicate DELETE over a file whose vector already deletes a whole group skips that group, deletes
    /// exactly the live rows that match, and leaves the same rows a scan sees.
    /// </summary>
    [Fact]
    public async Task APredicateDeleteSkipsTheGroupAndDeletesOnlyLiveRows()
    {
        var (skipped, skippedFs) = await CreateAsync("skipped", 100, 200);
        var (read, readFs) = await CreateAsync("read", 101, 200);
        await using (skipped)
        await using (read)
        {
            // Statistics keep groups 1..4; the vector removes group 1 of them in the first table only.
            var predicate = Ex.GreaterThanOrEqual("id", 150L);
            skippedFs.Reset();
            readFs.Reset();
            var (skippedDeleted, _) = await skipped.DeleteAsync(predicate);
            var (readDeleted, _) = await read.DeleteAsync(predicate);

            Assert.Equal(300, skippedDeleted); // 200..499: 150..199 were already gone
            Assert.Equal(300, readDeleted);    // 200..499, and 100 itself is below 150
            Assert.True(skippedFs.DataBytesRead < readFs.DataBytesRead,
                $"skipped read {skippedFs.DataBytesRead} bytes, the unskipped one {readFs.DataBytesRead}");

            Assert.Equal(Enumerable.Range(0, 100).Select(i => (long)i), await ReadAsync(skipped, filter: null));
            Assert.Equal(new long[] { 100 }.Concat(Enumerable.Range(0, 100).Select(i => (long)i)).OrderBy(v => v),
                await ReadAsync(read, filter: null));
        }
    }

    // ---- helpers -----------------------------------------------------------------------------------------

    private static IEnumerable<long> Survivors(int from, int to) =>
        Enumerable.Range(0, Rows).Where(i => i < from || i >= to).Select(i => (long)i);

    /// <summary>One file, row tracking on, with the rows [from, to) deleted by a vector.</summary>
    private async Task<(DeltaTable Table, DataFileCountingFileSystem Fs)> CreateAsync(string name, int from, int to)
    {
        var fs = new DataFileCountingFileSystem(new LocalTableFileSystem(Path.Combine(_tempDir, name)));
        var table = await DeltaTable.CreateAsync(
            fs, TableSchema,
            new DeltaTableOptions
            {
                ParquetWriteOptions = ParquetWriteOptions.Default with
                {
                    RowGroupMaxRows = RowsPerGroup,
                    DataPageRowCountLimit = 10,
                },
                ParquetReadOptions = ParquetReadOptions.Default with { BatchSize = 32 },
            },
            enableDeletionVectors: true, enableRowTracking: true);
        await table.WriteAsync([Batch()]);
        Assert.Single(table.CurrentSnapshot.ActiveFiles);

        var range = Ex.And(Ex.GreaterThanOrEqual("id", (long)from), Ex.LessThan("id", (long)to));
        var (deleted, _) = await table.DeleteAsync(DeltaTable.MaskFor(range));
        Assert.Equal(to - from, deleted);
        Assert.NotNull(Assert.Single(table.CurrentSnapshot.ActiveFiles).Value.DeletionVector);
        return (table, fs);
    }

    /// <summary>
    /// The ids read, after checking each row's absolute position and row id against its id (all three are the
    /// row's position in the one file).
    /// </summary>
    private static async Task<List<long>> ReadAsync(DeltaTable table, Predicate? filter)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions
        {
            Filter = filter,
            Metadata = DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
        }))
        {
            var id = (Int64Array)batch.Column("id");
            var index = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIndexSuffix);
            var rowId = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIdSuffix);
            for (int i = 0; i < batch.Length; i++)
            {
                long v = id.GetValue(i)!.Value;
                Assert.Equal(v, index.GetValue(i));
                Assert.Equal(v, rowId.GetValue(i));
                ids.Add(v);
            }
        }

        ids.Sort();
        return ids;
    }

    /// <summary>Distinct, poorly compressible payloads, so the data pages outweigh the footer and indexes.</summary>
    private static RecordBatch Batch()
    {
        var ids = new Int64Array.Builder();
        var payloads = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            ids.Append(i);
            payloads.Append(string.Concat(Enumerable.Range(0, 6).Select(k => Guid.NewGuid().ToString("N"))));
        }
        return new RecordBatch(TableSchema, [ids.Build(), payloads.Build()], Rows);
    }
}
