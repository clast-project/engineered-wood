// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A scan, a predicate DELETE, OPTIMIZE and an inferred change feed skip, unread, the row groups of a file none
/// of their rows can come from: those a deletion vector deletes in full, and for the feed of a vector edit, those
/// the edit does not touch (#618).
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
            AssertFewerBytes(skippedFs, readFs);

            Assert.Equal(Enumerable.Range(0, 100).Select(i => (long)i), await ReadAsync(skipped, filter: null));
            Assert.Equal(new long[] { 100 }.Concat(Enumerable.Range(0, 100).Select(i => (long)i)).OrderBy(v => v),
                await ReadAsync(read, filter: null));
        }
    }

    /// <summary>
    /// OPTIMIZE skips the groups a source's vector deletes in full: the compacted file holds exactly the
    /// survivors, each with the row id it had, and the rewrite reads fewer bytes than one over a vector a row
    /// short of each group.
    /// </summary>
    [Fact]
    public async Task OptimizeSkipsTheGroupAndKeepsEachSurvivorsRowId()
    {
        var (skipped, skippedFs) = await CreateAsync("skipped", 100, 300);
        var (read, readFs) = await CreateAsync("read", 101, 300);
        await using (skipped)
        await using (read)
        {
            foreach (var table in new[] { skipped, read })
            {
                await table.WriteAsync([Batch(Rows, 10)]); // a second small file, so the group has two to compact
                Assert.Equal(2, table.CurrentSnapshot.ActiveFiles.Count);
            }

            skippedFs.Reset();
            readFs.Reset();
            Assert.NotNull(await skipped.CompactAsync());
            Assert.NotNull(await read.CompactAsync());
            AssertFewerBytes(skippedFs, readFs);

            Assert.Null(Assert.Single(skipped.CurrentSnapshot.ActiveFiles).Value.DeletionVector);
            var appended = Enumerable.Range(Rows, 10).Select(i => (long)i);
            Assert.Equal(Survivors(100, 300).Concat(appended), await ReadAsync(skipped, filter: null, positionsAreIds: false));
            Assert.Equal(Survivors(101, 300).Concat(appended), await ReadAsync(read, filter: null, positionsAreIds: false));
        }
    }

    /// <summary>
    /// Through a host's <see cref="IDataFileReader"/>, which cannot skip, OPTIMIZE positions the source's batches by
    /// counting them: a fully deleted batch (row group 1) still counts towards the next, partly deleted one (row 250
    /// of group 2), so every survivor keeps its own row id.
    /// </summary>
    [Fact]
    public async Task OptimizeThroughTheCodecSeamCountsAFullyDeletedBatchTowardsTheNext()
    {
        GroupPerBatchReader? seam = null;
        var (table, _) = await CreateAsync("t", 100, 200, seam: fs => seam = new GroupPerBatchReader(fs));
        await using (table)
        {
            var (deleted, _) = await table.DeleteAsync(DeltaTable.MaskFor(Ex.Equal("id", 250L)));
            Assert.Equal(1, deleted);
            await table.WriteAsync([Batch(Rows, 10)]); // a second small file, so the group has two to compact

            seam!.Batches.Clear();
            Assert.NotNull(await table.CompactAsync());
            Assert.True(seam.Batches.Count >= 5, $"the seam returned {seam.Batches.Count} batches");
            Assert.Contains(seam.Batches, b => b == RowsPerGroup); // one per row group, the deleted one too

            var expected = Survivors(100, 200).Where(id => id != 250).Concat(Enumerable.Range(Rows, 10).Select(i => (long)i));
            Assert.Equal(expected, await ReadAsync(table, filter: null, positionsAreIds: false));
        }
    }

    /// <summary>A host codec returning each row group as its own batch, recording the batch lengths.</summary>
    private sealed class GroupPerBatchReader(IO.ITableFileSystem fs) : IDataFileReader
    {
        public List<int> Batches { get; } = [];

        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var file = await fs.OpenReadAsync(relativePath, cancellationToken);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            await foreach (var batch in reader.ReadAllAsync(physicalColumns, cancellationToken))
            {
                Batches.Add(batch.Length);
                yield return batch;
            }
        }
    }

    /// <summary>
    /// The inferred feed of a vector edit with no change file reads only the groups holding the rows the edit
    /// deleted: here one group of the four live ones (the old vector already deletes group 1 in full).
    /// </summary>
    [Fact]
    public async Task AVectorEditsInferredFeedReadsOnlyTheGroupsItTouched()
    {
        var (table, fs) = await CreateAsync("t", 100, 200, cdf: true);
        await using (table)
        {
            string path = Assert.Single(table.CurrentSnapshot.ActiveFiles).Value.Path;
            var txn = table.StartTransaction();
            await txn.StageRowDeletesAsync(RowSelection.ByPath(
                new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal) { [path] = new long[] { 350, 351 } }));
            long version = await txn.CommitAsync();
            await AssertNoChangeFileAsync(fs, version);

            fs.Reset();
            await ReadAsync(table, filter: null);
            long scanBytes = fs.DataBytesRead;

            fs.Reset();
            Assert.Equal(new[] { (350L, "delete"), (351L, "delete") }, await ReadFeedAsync(table, version));
            Assert.True(fs.DataBytesRead * 2 < scanBytes, $"the feed read {fs.DataBytesRead} bytes, a scan {scanBytes}");
        }
    }

    /// <summary>
    /// The inferred feed of an overwrite reports the removed file's survivors as deleted, each with its row id,
    /// skipping the groups the file's vector deletes in full.
    /// </summary>
    [Fact]
    public async Task AnOverwritesInferredFeedSkipsTheRemovedFilesDeletedGroups()
    {
        var (skipped, skippedFs) = await CreateAsync("skipped", 100, 300, cdf: true);
        var (read, readFs) = await CreateAsync("read", 101, 300, cdf: true);
        await using (skipped)
        await using (read)
        {
            long skippedVersion = await skipped.WriteAsync([Batch(Rows, 1)], DeltaWriteMode.Overwrite);
            long readVersion = await read.WriteAsync([Batch(Rows, 1)], DeltaWriteMode.Overwrite);
            await AssertNoChangeFileAsync(skippedFs, skippedVersion);

            skippedFs.Reset();
            readFs.Reset();
            var skippedFeed = await ReadFeedAsync(skipped, skippedVersion);
            var readFeed = await ReadFeedAsync(read, readVersion);

            var expected = Survivors(100, 300).Select(id => (id, "delete"))
                .Append(((long)Rows, "insert"))
                .OrderBy(c => c.Item2, StringComparer.Ordinal).ThenBy(c => c.Item1);
            Assert.Equal(expected, skippedFeed);
            Assert.Equal(Survivors(101, 300).Count() + 1, readFeed.Count);
            AssertFewerBytes(skippedFs, readFs);
        }
    }

    // ---- runs inside one row group (#618 step 4) ----------------------------------------------------------

    private const int OneGroupRows = 20_000;

    /// <summary>
    /// A long deleted run inside the file's one row group is cut out of the scan: each survivor still comes back
    /// at its own position, with its own row id. The run in the middle, from the start, and to the end.
    /// </summary>
    [Theory]
    [InlineData(3_000, 12_000)]
    [InlineData(0, 15_000)]
    [InlineData(5_000, OneGroupRows)]
    public async Task AScanInsideOneGroupReturnsEachSurvivorAtItsPositionAndRowId(int from, int to)
    {
        var (table, _) = await CreateOneGroupAsync("t", from, to);
        await using (table)
            Assert.Equal(Survivors(from, to, OneGroupRows), await ReadAsync(table, filter: null));
    }

    /// <summary>
    /// The cut run's pages are not read: against a twin deleting as many rows in runs too short to cut (500 rows,
    /// under a 1,024-row probe block), the scan reads at least 10% fewer bytes.
    /// </summary>
    [Fact]
    public async Task ALongRunInsideOneGroupIsNotRead()
    {
        var shortRuns = Ex.Or(Enumerable.Range(0, 18).Select(k => (Predicate)Ex.And(
            Ex.GreaterThanOrEqual("id", k * 1_000L), Ex.LessThan("id", k * 1_000L + 500))).ToArray());
        var (run, runFs) = await CreateOneGroupAsync("run", 3_000, 12_000);
        var (scattered, scatteredFs) = await CreateOneGroupAsync("scattered", 0, 0, deleted: (shortRuns, 9_000));
        await using (run)
        await using (scattered)
        {
            runFs.Reset();
            scatteredFs.Reset();
            Assert.Equal(Survivors(3_000, 12_000, OneGroupRows), await ReadAsync(run, filter: null));
            Assert.Equal(OneGroupRows - 9_000, (await ReadAsync(scattered, filter: null)).Count);
            AssertFewerBytes(runFs, scatteredFs);
        }
    }

    /// <summary>
    /// A predicate DELETE over a group whose vector already deletes a long run inside it deletes exactly the live
    /// rows that match, keeping the vector's.
    /// </summary>
    [Fact]
    public async Task APredicateDeleteInsideOneGroupDeletesOnlyLiveRows()
    {
        var (table, _) = await CreateOneGroupAsync("t", 3_000, 12_000);
        await using (table)
        {
            var (deleted, _) = await table.DeleteAsync(Ex.GreaterThanOrEqual("id", 10_000L));
            Assert.Equal(OneGroupRows - 12_000, deleted);
            Assert.Equal(Enumerable.Range(0, 3_000).Select(i => (long)i), await ReadAsync(table, filter: null));
        }
    }

    /// <summary>OPTIMIZE over a group with a long deleted run keeps exactly the survivors, each with its row id.</summary>
    [Fact]
    public async Task OptimizeInsideOneGroupKeepsEachSurvivorsRowId()
    {
        var (table, _) = await CreateOneGroupAsync("t", 3_000, 12_000);
        await using (table)
        {
            await table.WriteAsync([Batch(OneGroupRows, 10)]);
            Assert.NotNull(await table.CompactAsync());
            Assert.Null(Assert.Single(table.CurrentSnapshot.ActiveFiles).Value.DeletionVector);
            Assert.Equal(
                Survivors(3_000, 12_000, OneGroupRows).Concat(Enumerable.Range(OneGroupRows, 10).Select(i => (long)i)),
                await ReadAsync(table, filter: null, positionsAreIds: false));
        }
    }

    /// <summary>
    /// The inferred feed of a vector edit inside one row group reads only around the rows the edit deleted: the
    /// rest of the group is a run it needs none of, and is cut.
    /// </summary>
    [Fact]
    public async Task AVectorEditsInferredFeedInsideOneGroupReadsAroundTheRowsItTouched()
    {
        var (table, fs) = await CreateOneGroupAsync("t", 3_000, 12_000, cdf: true);
        await using (table)
        {
            string path = Assert.Single(table.CurrentSnapshot.ActiveFiles).Value.Path;
            var txn = table.StartTransaction();
            await txn.StageRowDeletesAsync(RowSelection.ByPath(
                new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal) { [path] = new long[] { 15_000, 15_001 } }));
            long version = await txn.CommitAsync();
            await AssertNoChangeFileAsync(fs, version);

            fs.Reset();
            await ReadAsync(table, filter: null);
            long scanBytes = fs.DataBytesRead;

            fs.Reset();
            Assert.Equal(new[] { (15_000L, "delete"), (15_001L, "delete") }, await ReadFeedAsync(table, version));
            Assert.True(fs.DataBytesRead * 4 < scanBytes, $"the feed read {fs.DataBytesRead} bytes, a scan {scanBytes}");
        }
    }

    // ---- helpers -----------------------------------------------------------------------------------------

    /// <summary>
    /// The skipping twin read at least 10% fewer data bytes. Not merely fewer: the payloads are random, so two
    /// twins reading the same groups differ by a few bytes either way, and a bare comparison passes half the time
    /// without any skip at all. A skipped group here is a fifth of the file.
    /// </summary>
    private static void AssertFewerBytes(DataFileCountingFileSystem skipped, DataFileCountingFileSystem read) =>
        Assert.True(skipped.DataBytesRead * 10 < read.DataBytesRead * 9,
            $"skipped read {skipped.DataBytesRead} bytes, the unskipped one {read.DataBytesRead}");

    private static IEnumerable<long> Survivors(int from, int to, int rows = Rows) =>
        Enumerable.Range(0, rows).Where(i => i < from || i >= to).Select(i => (long)i);

    /// <summary>
    /// One file of <see cref="OneGroupRows"/> rows in ONE row group of 100-row pages, with [from, to) deleted by a
    /// vector (or <paramref name="deleted"/>), so the probe's blocks are 1,024 rows.
    /// </summary>
    private Task<(DeltaTable Table, DataFileCountingFileSystem Fs)> CreateOneGroupAsync(
        string name, int from, int to, bool cdf = false, (Predicate Rows, int Count)? deleted = null) =>
        CreateAsync(name, from, to, cdf, rows: OneGroupRows, groupRows: OneGroupRows, pageRows: 100, deleted: deleted);

    /// <summary>One file, row tracking on, with the rows [from, to) deleted by a vector; <paramref name="seam"/>, when
    /// given, builds the host codec every data file is read through.</summary>
    private async Task<(DeltaTable Table, DataFileCountingFileSystem Fs)> CreateAsync(
        string name, int from, int to, bool cdf = false, Func<IO.ITableFileSystem, IDataFileReader>? seam = null,
        int rows = Rows, int groupRows = RowsPerGroup, int pageRows = 10, (Predicate Rows, int Count)? deleted = null)
    {
        var fs = new DataFileCountingFileSystem(new LocalTableFileSystem(Path.Combine(_tempDir, name)));
        var table = await DeltaTable.CreateAsync(
            fs, TableSchema,
            new DeltaTableOptions
            {
                ParquetWriteOptions = ParquetWriteOptions.Default with
                {
                    RowGroupMaxRows = groupRows,
                    DataPageRowCountLimit = pageRows,
                },
                ParquetReadOptions = ParquetReadOptions.Default with { BatchSize = 32 },
                DataFileReader = seam?.Invoke(fs),
            },
            enableDeletionVectors: true, enableRowTracking: true,
            configuration: cdf ? new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" } : null);
        await table.WriteAsync([Batch(0, rows)]);
        Assert.Single(table.CurrentSnapshot.ActiveFiles);

        var (doomed, count) = deleted ?? (Ex.And(Ex.GreaterThanOrEqual("id", (long)from), Ex.LessThan("id", (long)to)), to - from);
        var (deletedCount, _) = await table.DeleteAsync(DeltaTable.MaskFor(doomed));
        Assert.Equal(count, deletedCount);
        Assert.NotNull(Assert.Single(table.CurrentSnapshot.ActiveFiles).Value.DeletionVector);
        return (table, fs);
    }

    /// <summary>
    /// The ids read, after checking each row's row id, and its absolute position unless
    /// <paramref name="positionsAreIds"/> is false, against its id (all three are the row's position in the one
    /// file it was written to).
    /// </summary>
    private static async Task<List<long>> ReadAsync(DeltaTable table, Predicate? filter, bool positionsAreIds = true)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions
        {
            Filter = filter,
            Metadata = DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
        }))
        {
            using var owned = batch;
            var id = (Int64Array)batch.Column("id");
            var index = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIndexSuffix);
            var rowId = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIdSuffix);
            for (int i = 0; i < batch.Length; i++)
            {
                long v = id.GetValue(i)!.Value;
                if (positionsAreIds)
                    Assert.Equal(v, index.GetValue(i));
                Assert.Equal(v, rowId.GetValue(i));
                ids.Add(v);
            }
        }

        ids.Sort();
        return ids;
    }

    /// <summary>The feed of one version, as (id, change type), each row's row id checked against its id.</summary>
    private static async Task<List<(long Id, string Type)>> ReadFeedAsync(DeltaTable table, long version)
    {
        var changes = new List<(long, string)>();
        await foreach (var batch in table.ReadChangesAsync(new DeltaChangeReadOptions
        {
            StartVersion = version, EndVersion = version, Metadata = DeltaRowMetadata.RowTracking,
        }))
        {
            using var owned = batch;
            var ids = (Int64Array)batch.Column("id");
            var types = (StringArray)batch.Column("_change_type");
            var rowIds = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIdSuffix);
            for (int i = 0; i < batch.Length; i++)
            {
                Assert.Equal(ids.GetValue(i), rowIds.GetValue(i));
                changes.Add((ids.GetValue(i)!.Value, types.GetString(i)!));
            }
        }
        return changes.OrderBy(c => c.Item2, StringComparer.Ordinal).ThenBy(c => c.Item1).ToList();
    }

    /// <summary>A version with a change file is read from it, never inferred from its data files.</summary>
    private static async Task AssertNoChangeFileAsync(DataFileCountingFileSystem fs, long version)
    {
        var actions = await new TransactionLog(fs).ReadCommitAsync(version);
        Assert.DoesNotContain(actions, a => a is CdcFile);
    }

    /// <summary>Distinct, poorly compressible payloads, so the data pages outweigh the footer and indexes.</summary>
    private static RecordBatch Batch(int start, int count)
    {
        var ids = new Int64Array.Builder();
        var payloads = new StringArray.Builder();
        for (int i = start; i < start + count; i++)
        {
            ids.Append(i);
            payloads.Append(string.Concat(Enumerable.Range(0, 6).Select(k => Guid.NewGuid().ToString("N"))));
        }
        return new RecordBatch(TableSchema, [ids.Build(), payloads.Build()], count);
    }
}
