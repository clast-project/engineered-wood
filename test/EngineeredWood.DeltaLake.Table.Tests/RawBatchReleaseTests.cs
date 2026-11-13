// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>Reads whose native memory is counted: none may run beside them.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class NativeMemoryCollection
{
    public const string Name = "Native memory";
}

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

    // An exact filter reads through the built-in reader's selected read (#502), a pipeline of its own.
    [Theory]
    [InlineData("name")]
    [InlineData("id")]
    public async Task AFilteredScanOfADroppedColumn_ReleasesItsDecode(string mapping)
    {
        await using (var table = await CreateAsync(new() { ["delta.columnMapping.mode"] = mapping }))
            await table.DropColumnAsync("gone");

        Assert.Equal(Rows / 4, await ScanReleasingAsync(null, Ex.Equal("tag", "t1")));
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
    private async Task AssertReleasedAsync(Func<DeltaTable, Task> read, int? batchSize)
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
                         ParquetReadOptions = new ParquetReadOptions { BatchSize = batchSize },
                         MaxConcurrentReads = 1,
                     }))
        {
            await read(table);
        }

        Assert.True(NativeMemoryTracker.TotalAllocated > allocated, "The read decoded nothing natively.");
        Assert.Equal(before, NativeMemoryTracker.LiveBytes);
    }
}
