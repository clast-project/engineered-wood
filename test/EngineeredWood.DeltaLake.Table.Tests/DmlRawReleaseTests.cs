// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// DELETE, UPDATE and MERGE release each batch they read once nothing reads it any more (#865): a batch they gather
/// rows from once its gathers are built, a batch they keep whole once the rewrite holding it is written. Before, nothing
/// released either, so every rewritten file left its whole decode for the collector, out of sight of the memory budget.
/// What a change file still holds is kept: a gather of a view column shares the read's buffers.
/// </summary>
[Collection(NativeMemoryCollection.Name)]
public class DmlRawReleaseTests : IDisposable
{
    private const int Rows = 8_000;
    // The rows each operation changes: in the third of eight 1,000-row batches when read batched, so that batch is
    // gathered from and the others are kept whole (copy-on-write) or left in place (deletion vectors).
    private const int First = 2_000, Last = 2_500;
    private readonly string _tempDir;

    public DmlRawReleaseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-dml-release-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    /// <summary>DELETE by predicate, DELETE by position (<see cref="RowSelection"/>), UPDATE, MERGE.</summary>
    public enum Op { Delete, DeleteRows, Update, Merge }

    public enum Layout
    {
        /// <summary>One batch per row group, the file's one.</summary>
        Whole,
        /// <summary>1,000-row batches, with row tracking (each batch carries its ids and versions).</summary>
        BatchedTracked,
        /// <summary>1,000-row batches of string views: a gather shares the read's data buffers.</summary>
        BatchedViews,
    }

    public static TheoryData<Op, bool, bool, Layout> Cases()
    {
        var data = new TheoryData<Op, bool, bool, Layout>();
        foreach (var op in new[] { Op.Delete, Op.DeleteRows, Op.Update, Op.Merge })
            foreach (bool dv in new[] { false, true })
                foreach (bool cdf in new[] { false, true })
                    foreach (var layout in new[] { Layout.Whole, Layout.BatchedTracked, Layout.BatchedViews })
                        data.Add(op, dv, cdf, layout);
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ADmlLeavesNoDecodeBehind(Op op, bool deletionVectors, bool cdf, Layout layout)
    {
        var dir = Path.Combine(_tempDir, "t");
        Directory.CreateDirectory(dir);
        var fs = new LocalTableFileSystem(dir);
        var options = new DeltaTableOptions
        {
            MaxConcurrentReads = 1,
            ParquetReadOptions = new ParquetReadOptions
            {
                BatchSize = layout == Layout.Whole ? null : 1_000,
                ByteArrayOutput = layout == Layout.BatchedViews ? ByteArrayOutputKind.ViewType : ByteArrayOutputKind.Default,
            },
        };
        await CreateAsync(fs, deletionVectors, cdf, tracked: layout == Layout.BatchedTracked);

        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        long before = NativeMemoryTracker.LiveBytes;
        long allocated = NativeMemoryTracker.TotalAllocated;
        long version;
        await using (var table = await DeltaTable.OpenAsync(fs, options))
        {
            var mode = deletionVectors ? RowDeleteMode.DeletionVector : RowDeleteMode.CopyOnWrite;
            switch (op)
            {
                case Op.Delete:
                    (long deleted, version) = await table.DeleteAsync(Changed, mode);
                    Assert.Equal(Last - First, deleted);
                    break;
                case Op.DeleteRows:
                    var file = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;
                    var positions = Enumerable.Range(First, Last - First).Select(i => (long)i).ToList();
                    (long deletedRows, version) = await table.DeleteRowsAsync(
                        RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>> { [file] = positions }), mode);
                    Assert.Equal(Last - First, deletedRows);
                    break;
                case Op.Update:
                    // Identity: the post-images are the gathered rows themselves.
                    (long updated, version) = await table.UpdateAsync(Changed, b => b, mode: mode);
                    Assert.Equal(Last - First, updated);
                    break;
                default:
                    MergeMetrics metrics;
                    (metrics, version) = await table.MergeAsync([Source(First, Last)], new MergeOptions
                    {
                        Keys = ["id"],
                        WhenMatched = [MergeAction.Update],
                        DeleteMode = mode,
                    });
                    Assert.Equal(Last - First, metrics.NumTargetRowsUpdated);
                    break;
            }
        }
        // A vector delete by position reads the file only for its change rows.
        if (op != Op.DeleteRows || !deletionVectors || cdf)
            Assert.True(NativeMemoryTracker.TotalAllocated > allocated, "The operation decoded nothing natively.");
        Assert.Equal(0, NativeMemoryTracker.LiveBytes - before);

        // Nothing released was still read: the table and its change files hold every value as it was.
        await using var check = await DeltaTable.OpenAsync(fs);
        var seen = new HashSet<int>();
        await foreach (var batch in check.ReadAllAsync())
        {
            using (batch)
            {
                foreach (var (id, gone) in Values(batch))
                {
                    Assert.Equal(Gone(id), gone);
                    Assert.True(seen.Add(id));
                }
            }
        }
        bool deletes = op is Op.Delete or Op.DeleteRows;
        Assert.Equal(deletes ? Rows - (Last - First) : Rows, seen.Count);

        if (!cdf)
            return;
        var changes = new Dictionary<string, int>();
        await foreach (var batch in check.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            using (batch)
            {
                var types = (StringArray)batch.Column(batch.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
                int row = 0;
                foreach (var (id, gone) in Values(batch))
                {
                    Assert.InRange(id, First, Last - 1);
                    Assert.Equal(Gone(id), gone);
                    string type = types.GetString(row++)!;
                    changes[type] = (changes.TryGetValue(type, out int count) ? count : 0) + 1;
                }
            }
        }
        var expected = deletes
            ? new Dictionary<string, int> { [CdfConfig.Delete] = Last - First }
            : new Dictionary<string, int> { [CdfConfig.UpdatePreimage] = Last - First, [CdfConfig.UpdatePostimage] = Last - First };
        Assert.Equal(expected.OrderBy(kv => kv.Key), changes.OrderBy(kv => kv.Key));
    }

    // With deletion vectors an UPDATE or MERGE writes only its post-images, so it processes each batch as it is read and
    // keeps no more of the file than a read does: about a row group, here a sixteenth of the file (#728). It held the
    // whole file before.
    [Theory]
    [InlineData(Op.Update)]
    [InlineData(Op.Merge)]
    public async Task ADeletionVectorDmlHoldsNoMoreThanARead(Op op)
    {
        const int rows = 64_000;
        var dir = Path.Combine(_tempDir, "groups");
        Directory.CreateDirectory(dir);
        var fs = new LocalTableFileSystem(dir);
        var write = new DeltaTableOptions { ParquetWriteOptions = new ParquetWriteOptions { RowGroupMaxRows = rows / 16 } };
        await using (var created = await DeltaTable.CreateAsync(fs, TableSchema, write, enableDeletionVectors: true))
            await created.WriteAsync([Source(0, rows)]);

        long readPeak = await PeakOfAsync(fs, async table =>
        {
            await foreach (var batch in table.ReadAllAsync())
                batch.Dispose();
        });
        long dmlPeak = await PeakOfAsync(fs, async table =>
        {
            if (op == Op.Update)
            {
                var (updated, _) = await table.UpdateAsync(Changed, b => b, mode: RowDeleteMode.DeletionVector);
                Assert.Equal(Last - First, updated);
            }
            else
            {
                var (metrics, _) = await table.MergeAsync([Source(First, Last)], new MergeOptions
                {
                    Keys = ["id"], WhenMatched = [MergeAction.Update], DeleteMode = RowDeleteMode.DeletionVector,
                });
                Assert.Equal(Last - First, metrics.NumTargetRowsUpdated);
            }
        });

        // Measured: the same peak as the read (465,016 B); 5,168,136 B when the file was held whole.
        Assert.True(dmlPeak < 2 * readPeak, $"Peak {dmlPeak:N0} B; a read of the table peaks at {readPeak:N0} B.");
    }

    // The highest native memory in use while `act` runs on the table, over what was in use before it.
    private static async Task<long> PeakOfAsync(LocalTableFileSystem fs, Func<DeltaTable, Task> act)
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        NativeMemoryTracker.Reset();
        await using (var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions { MaxConcurrentReads = 1 }))
            await act(table);
        return NativeMemoryTracker.PeakBytes;
    }

    // An UPDATE's updater may hand back arrays of its own, which are the caller's: handing the post-images to the change
    // files releases only what the UPDATE itself read. Here the updater's column was read natively, as views, by the
    // caller, who still holds it.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnUpdatersOwnArraysStayTheCallers(bool deletionVectors)
    {
        var viewOptions = new DeltaTableOptions
        {
            MaxConcurrentReads = 1,
            ParquetReadOptions = new ParquetReadOptions { BatchSize = 1_000, ByteArrayOutput = ByteArrayOutputKind.ViewType },
        };
        var sourceDir = Path.Combine(_tempDir, "source");
        Directory.CreateDirectory(sourceDir);
        var sourceFs = new LocalTableFileSystem(sourceDir);
        await using (var created = await DeltaTable.CreateAsync(sourceFs, TableSchema))
            await created.WriteAsync([Source(First, Last)]);
        RecordBatch? callers = null;
        await using (var sourceTable = await DeltaTable.OpenAsync(sourceFs, viewOptions))
        {
            await foreach (var batch in sourceTable.ReadAllAsync())
            {
                Assert.Null(callers);
                callers = batch;
            }
        }
        var gone = callers!.Column(callers.Schema.GetFieldIndex("gone"));

        var dir = Path.Combine(_tempDir, "target");
        Directory.CreateDirectory(dir);
        var fs = new LocalTableFileSystem(dir);
        await CreateAsync(fs, deletionVectors, cdf: true, tracked: false);

        long before = NativeMemoryTracker.LiveBytes;
        await using (var table = await DeltaTable.OpenAsync(fs, viewOptions))
        {
            // The changed rows are one read batch's, so the updater sees them all at once, in order.
            var (updated, _) = await table.UpdateAsync(Changed, matched =>
            {
                Assert.Equal(gone.Length, matched.Length);
                int g = matched.Schema.GetFieldIndex("gone");
                var columns = Enumerable.Range(0, matched.ColumnCount)
                    .Select(i => i == g ? gone : matched.Column(i)).ToList();
                return new RecordBatch(matched.Schema, columns, matched.Length);
            }, mode: deletionVectors ? RowDeleteMode.DeletionVector : RowDeleteMode.CopyOnWrite);
            Assert.Equal(Last - First, updated);
        }

        // Nothing of the caller's was released (that would show as less than before), and nothing the UPDATE read is left.
        Assert.Equal(0, NativeMemoryTracker.LiveBytes - before);
        using (callers)
        {
            foreach (var (id, value) in Values(callers))
                Assert.Equal(Gone(id), value);
        }
    }

    private static BooleanArray Changed(RecordBatch batch)
    {
        var ids = (Int32Array)batch.Column(batch.Schema.GetFieldIndex("id"));
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < ids.Length; i++)
            mask.Append(ids.GetValue(i) is >= First and < Last);
        return mask.Build();
    }

    private static string Gone(int id) => "a value the rewrite carries through " + id;

    private static IEnumerable<(int Id, string Gone)> Values(RecordBatch batch)
    {
        var ids = (Int32Array)batch.Column(batch.Schema.GetFieldIndex("id"));
        var gone = batch.Column(batch.Schema.GetFieldIndex("gone"));
        for (int i = 0; i < batch.Length; i++)
        {
            string? value = gone switch
            {
                StringArray s => s.GetString(i),
                StringViewArray v => v.GetString(i),
                _ => throw new InvalidOperationException($"Unexpected {gone.GetType().Name}"),
            };
            yield return (ids.GetValue(i)!.Value, value!);
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int32Type.Default, false))
        .Field(new Field("tag", StringType.Default, true))
        .Field(new Field("gone", StringType.Default, true))
        .Build();

    private static RecordBatch Source(int first, int last)
    {
        var ids = new Int32Array.Builder();
        var tags = new StringArray.Builder();
        var gone = new StringArray.Builder();
        for (int i = first; i < last; i++)
        {
            ids.Append(i);
            tags.Append("t" + (i % 4));
            gone.Append(Gone(i));
        }
        return new RecordBatch(TableSchema, [ids.Build(), tags.Build(), gone.Build()], last - first);
    }

    // One file of `Rows` rows in one row group.
    private static async Task CreateAsync(LocalTableFileSystem fs, bool deletionVectors, bool cdf, bool tracked)
    {
        var configuration = new Dictionary<string, string>();
        if (cdf)
            configuration[CdfConfig.EnableKey] = "true";
        await using var table = await DeltaTable.CreateAsync(
            fs, TableSchema, enableDeletionVectors: deletionVectors, enableRowTracking: tracked,
            configuration: configuration);
        await table.WriteAsync([Source(0, Rows)]);
    }
}
