// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A read releases every native buffer it decoded once its batches are disposed, including those it dropped while
/// bringing a file's batches to the table's shape (#855): a widened column's narrow values, a dropped column's whole
/// decode. Measured before any collection, since a dropped buffer is otherwise freed only when the collector finalizes
/// it, out of sight of the scan's memory budget.
/// </summary>
[Collection(NativeMemoryCollection.Name)]
public class RawBatchReleaseTests : IDisposable
{
    private const int Rows = 8_000, RowGroup = 2_000;
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;

    public RawBatchReleaseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-raw-release-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    [Theory]
    [InlineData(null)]
    [InlineData(RowGroup / 4)]
    public async Task AScanOfAWidenedColumn_ReleasesItsNarrowValues(int? batchSize)
    {
        await using (var table = await CreateAsync(new() { [EngineeredWood.DeltaLake.Schema.TypeWidening.EnableKey] = "true" }))
        {
            var wide = new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int64Type.Default, true))
                .Field(new Field("tag", StringType.Default, true))
                .Field(new Field("gone", StringType.Default, true)).Build();
            await table.WriteAsync(
                [new RecordBatch(wide, wide.FieldsList.Select(f => ArrowCompute.MakeNullArray(f.DataType, 1)), 1)],
                mergeSchema: true);
        }

        Assert.Equal(Rows + 1, await ScanReleasingAsync(batchSize, null, dropped: false));
    }

    [Theory]
    [InlineData("name", null)]
    [InlineData("name", RowGroup / 4)]
    [InlineData("id", null)]
    [InlineData("id", RowGroup / 4)]
    public async Task AScanOfADroppedColumn_ReleasesItsDecode(string mapping, int? batchSize)
    {
        await using (var table = await CreateAsync(new() { ["delta.columnMapping.mode"] = mapping }))
            await table.DropColumnAsync("gone");

        Assert.Equal(Rows, await ScanReleasingAsync(batchSize, null));
    }

    // An exact filter reads through the built-in reader's selected read (#502), a pipeline of its own. One that keeps
    // some rows gathers them; one that keeps every row hands on the decode itself.
    [Theory]
    [InlineData("name", false)]
    [InlineData("name", true)]
    [InlineData("id", false)]
    [InlineData("id", true)]
    public async Task AFilteredScanOfADroppedColumn_ReleasesItsDecode(string mapping, bool everyRow)
    {
        await using (var table = await CreateAsync(new() { ["delta.columnMapping.mode"] = mapping }))
            await table.DropColumnAsync("gone");

        var filter = everyRow
            ? Ex.Or(Ex.Or(Ex.Equal("tag", "t0"), Ex.Equal("tag", "t1")), Ex.Or(Ex.Equal("tag", "t2"), Ex.Equal("tag", "t3")))
            : Ex.Equal("tag", "t1");
        Assert.Equal(everyRow ? Rows : Rows / 4, await ScanReleasingAsync(null, filter));
    }

    // Through the codec seam a filter's column is read with the projection and dropped after the filter, a step of its
    // own. A filter that keeps every row hands on the decode itself, so the dropped column is the reader's.
    [Fact]
    public async Task ASeamScanFilteredOnAnUnprojectedColumn_ReleasesThatColumn()
    {
        await (await CreateAsync([])).DisposeAsync();

        long rows = 0;
        await AssertReleasedAsync(async table =>
        {
            var options = new DeltaReadOptions
            {
                Columns = ["id"],
                Filter = Ex.Or(Ex.Or(Ex.Equal("tag", "t0"), Ex.Equal("tag", "t1")), Ex.Or(Ex.Equal("tag", "t2"), Ex.Equal("tag", "t3"))),
                FilterRows = true,
            };
            await foreach (var b in table.ReadAsync(options))
            {
                Assert.Equal(["id"], b.Schema.FieldsList.Select(f => f.Name));
                rows += b.Length;
                b.Dispose();
            }
        }, batchSize: null, seam: true);

        Assert.Equal(Rows, rows);
    }

    // A filter that keeps only some rows gathers them, and a gather of a view column shares the out-of-line string data
    // with the column it gathers from, whose views and validity it supersedes. Those are released all the same, in the
    // built-in reader's selected read and in the codec seam's filter alike (#857 review).
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ASelectiveFilterOverViewColumns_ReleasesTheSupersededViews(bool seam)
    {
        await (await CreateAsync([])).DisposeAsync();

        long rows = 0;
        await AssertReleasedAsync(async table =>
        {
            // On the int column: a string filter can't be evaluated over views.
            await foreach (var b in table.ReadAsync(new DeltaReadOptions { Filter = Ex.IsNotNull("id"), FilterRows = true }))
            {
                var gone = (StringViewArray)b.Column(b.Schema.GetFieldIndex("gone"));
                var ids = (Int32Array)b.Column(b.Schema.GetFieldIndex("id"));
                for (int i = 0; i < b.Length; i++)
                {
                    if (ids.GetValue(i) is int id)
                        Assert.Equal("dropped value " + id, gone.GetString(i));
                }

                rows += b.Length;
                b.Dispose();
            }
        }, batchSize: null, seam: seam, byteArrays: ByteArrayOutputKind.ViewType);

        // Every id but each seventh row's (NULL).
        Assert.Equal(Rows - (Rows + 6) / 7, rows);
    }

    // Reshaping rebuilds a struct around the children it keeps: a widened child's narrow values and a dropped child's
    // decode are released inside the struct too, in a batched read whose struct starts mid-decode (#857 review).
    [Theory]
    [InlineData(null)]
    [InlineData(RowGroup / 4)]
    public async Task AScanOfAStructWithAWidenedChild_ReleasesItsNarrowValues(int? batchSize)
    {
        await (await CreateStructAsync(new() { [EngineeredWood.DeltaLake.Schema.TypeWidening.EnableKey] = "true" })).DisposeAsync();
        await using (var table = await DeltaTable.OpenAsync(_fs))
        {
            var wide = StructSchema(new Field("a", Int64Type.Default, true), new Field("b", StringType.Default, true),
                new Field("c", StringType.Default, true));
            await table.WriteAsync(
                [new RecordBatch(wide, wide.FieldsList.Select(f => ArrowCompute.MakeNullArray(f.DataType, 1)), 1)],
                mergeSchema: true);
        }

        Assert.Equal(Rows + 1, await ScanStructReleasingAsync(batchSize, ["a", "b", "c"]));
    }

    [Theory]
    [InlineData(null)]
    [InlineData(RowGroup / 4)]
    public async Task AScanOfAStructWithADroppedChild_ReleasesItsDecode(int? batchSize)
    {
        await using (var table = await CreateStructAsync(new() { ["delta.columnMapping.mode"] = "name" }))
            await table.DropFieldAsync(["s", "c"]);

        Assert.Equal(Rows, await ScanStructReleasingAsync(batchSize, ["a", "b"]));
    }

    private static Apache.Arrow.Schema StructSchema(params Field[] children) => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int32Type.Default, true))
        .Field(new Field("s", new StructType(children), true)).Build();

    // A nullable struct (every fifth row NULL) of an int `a` and two strings: `b` names the row, `c` is the one dropped.
    private async Task<DeltaTable> CreateStructAsync(Dictionary<string, string> configuration)
    {
        var schema = StructSchema(new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true),
            new Field("c", StringType.Default, true));
        var ids = new Int32Array.Builder();
        var a = new Int32Array.Builder();
        var b = new StringArray.Builder();
        var c = new StringArray.Builder();
        var validity = new ArrowBuffer.BitmapBuilder();
        for (int i = 0; i < Rows; i++)
        {
            ids.Append(i);
            a.Append(i);
            b.Append("row " + i);
            c.Append("a dropped child's value " + i);
            validity.Append(i % 5 != 0);
        }

        var s = new StructArray(schema.FieldsList[1].DataType, Rows, [a.Build(), b.Build(), c.Build()],
            validity.Build(), Rows / 5);
        var table = await DeltaTable.CreateAsync(
            _fs, schema,
            new DeltaTableOptions { ParquetWriteOptions = new ParquetWriteOptions { RowGroupMaxRows = RowGroup } },
            configuration: configuration);
        await table.WriteAsync([new RecordBatch(schema, [ids.Build(), s], Rows)]);
        return table;
    }

    // Scans the struct table, checking each row's struct against its id: the children kept read as written.
    private async Task<long> ScanStructReleasingAsync(int? batchSize, string[] children)
    {
        long rows = 0;
        await AssertReleasedAsync(async table =>
        {
            await foreach (var batch in table.ReadAsync())
            {
                var ids = (Int32Array)batch.Column(0);
                var s = (StructArray)batch.Column(1);
                Assert.Equal(children, ((StructType)s.Data.DataType).Fields.Select(f => f.Name));
                var b = (StringArray)s.Fields[1];
                for (int i = 0; i < batch.Length; i++)
                {
                    if (ids.GetValue(i) is not int id)
                        continue; // the row the widening wrote
                    Assert.Equal(id % 5 == 0, s.IsNull(i));
                    if (!s.IsNull(i))
                    {
                        Assert.Equal((long)id, Convert.ToInt64(((IArrowArray)s.Fields[0]) switch
                        {
                            Int64Array l => l.GetValue(i),
                            Int32Array n => n.GetValue(i),
                            _ => null,
                        }));
                        Assert.Equal("row " + id, b.GetString(i));
                    }
                }

                rows += batch.Length;
                batch.Dispose();
            }
        }, batchSize);
        return rows;
    }

    // The feed reshapes both kinds of file itself: the data file an insert is inferred from and the change file an
    // update wrote, each still carrying the column dropped since.
    [Fact]
    public async Task AChangeFeedOfADroppedColumn_ReleasesItsDecode()
    {
        await using (var table = await CreateAsync(new()
                     {
                         ["delta.columnMapping.mode"] = "name",
                         [CdfConfig.EnableKey] = "true",
                     }))
        {
            await table.UpdateAsync(Ex.Equal("tag", "t1"), SetGone);
            await table.DropColumnAsync("gone");
        }

        long rows = 0;
        await AssertReleasedAsync(async table =>
        {
            await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 0, EndVersion = table.CurrentSnapshot.Version }))
            {
                Assert.Equal(-1, b.Schema.GetFieldIndex("gone"));
                rows += b.Length;
                b.Dispose();
            }
        }, batchSize: null);

        // The insert, then the update's pre- and post-images.
        Assert.Equal(Rows + 2 * (Rows / 4), rows);
    }

    private async Task<DeltaTable> CreateAsync(Dictionary<string, string> configuration)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true))
            .Field(new Field("tag", StringType.Default, true))
            .Field(new Field("gone", StringType.Default, true)).Build();
        var ids = new Int32Array.Builder();
        var tags = new StringArray.Builder();
        var gone = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            if (i % 7 == 0) ids.AppendNull(); else ids.Append(i);
            tags.Append("t" + (i % 4));
            gone.Append("dropped value " + i);
        }

        var table = await DeltaTable.CreateAsync(
            _fs, schema,
            new DeltaTableOptions { ParquetWriteOptions = new ParquetWriteOptions { RowGroupMaxRows = RowGroup } },
            configuration: configuration);
        await table.WriteAsync([new RecordBatch(schema, [ids.Build(), tags.Build(), gone.Build()], Rows)]);
        return table;
    }

    private static RecordBatch SetGone(RecordBatch batch)
    {
        int index = batch.Schema.GetFieldIndex("gone");
        var x = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            x.Append("updated");
        var columns = Enumerable.Range(0, batch.ColumnCount)
            .Select(c => c == index ? x.Build() : batch.Column(c)).ToArray();
        return new RecordBatch(batch.Schema, columns, batch.Length);
    }

    private async Task<long> ScanReleasingAsync(int? batchSize, Expressions.Predicate? filter, bool dropped = true)
    {
        long rows = 0;
        await AssertReleasedAsync(async table =>
        {
            var options = filter is null ? null : new DeltaReadOptions { Filter = filter, FilterRows = true };
            await foreach (var b in table.ReadAsync(options))
            {
                Assert.Equal(dropped, b.Schema.GetFieldIndex("gone") < 0);
                rows += b.Length;
                b.Dispose();
            }
        }, batchSize);
        return rows;
    }

    // Runs `read` over the table and checks that every native byte it allocated is free again, with no collection in
    // between to finalize what it dropped.
    private async Task AssertReleasedAsync(
        Func<DeltaTable, Task> read, int? batchSize, bool seam = false, ByteArrayOutputKind byteArrays = ByteArrayOutputKind.Default)
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        long before = NativeMemoryTracker.LiveBytes;
        long allocated = NativeMemoryTracker.TotalAllocated;
        await using (var table = await DeltaTable.OpenAsync(_fs, new DeltaTableOptions
                     {
                         ParquetReadOptions = new ParquetReadOptions { BatchSize = batchSize, ByteArrayOutput = byteArrays },
                         MaxConcurrentReads = 1,
                         DataFileReader = seam ? new SeamReader(_fs) : null,
                     }))
        {
            await read(table);
        }

        Assert.True(NativeMemoryTracker.TotalAllocated > allocated, "The read decoded nothing natively.");
        Assert.Equal(before, NativeMemoryTracker.LiveBytes);
    }

    // A codec-seam host over the built-in reader, which hands each batch over as the seam's contract says.
    private sealed class SeamReader(ITableFileSystem fs) : IDataFileReader
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
