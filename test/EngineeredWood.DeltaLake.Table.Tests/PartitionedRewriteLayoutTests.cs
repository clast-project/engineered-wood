// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The on-disk layout a PARTITIONED table's data files must have, whichever writer produced them: the partition
/// columns are NOT in the file (their values live in <c>add.partitionValues</c>, and every reader re-materializes
/// them) and they are not in the file's statistics either. The append and compaction paths build their batches
/// from partition-split or raw-parquet rows, so they never carried the columns; the copy-on-write REWRITES
/// (predicate UPDATE, row-id DELETE/UPDATE) build theirs from the READ path, which materializes partition
/// columns — and used to write them straight back into the rewritten file.
///
/// The stray column was inert rather than corrupting (EW's reader projects the non-partition columns by name,
/// and Spark resolves by name too), which is exactly why nothing failed while it was there — hence these tests
/// assert the file layout directly instead of only round-tripping rows.
/// </summary>
public class PartitionedRewriteLayoutTests : IDisposable
{
    private readonly string _tempDir;

    public PartitionedRewriteLayoutTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_partlayout_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema Schema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("region", StringType.Default, true))
        .Field(new Field("val", Int64Type.Default, true))
        .Build();

    private static RecordBatch Batch(long startId, int count, string region)
    {
        var ids = new Int64Array.Builder();
        var regions = new StringArray.Builder();
        var vals = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            regions.Append(region);
            vals.Append(100 + startId + i);
        }
        return new RecordBatch(Schema, [ids.Build(), regions.Build(), vals.Build()], count);
    }

    private Task<DeltaTable> CreateAsync(ColumnMappingMode mapping = ColumnMappingMode.None)
        => DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), Schema,
            partitionColumns: ["region"], columnMappingMode: mapping).AsTask();

    /// <summary>Every active file's PHYSICAL column names, as stored in the parquet footer.</summary>
    private async Task<List<List<string>>> ActiveFileColumnsAsync()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.OpenAsync(fs);
        var result = new List<List<string>>();
        foreach (var addFile in table.CurrentSnapshot.ActiveFiles.Values)
        {
            await using var file = await fs.OpenReadAsync(DeltaPath.Decode(addFile.Path));
            using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
            await foreach (var batch in reader.ReadAllAsync())
            {
                result.Add(batch.Schema.FieldsList.Select(f => f.Name).ToList());
                break;
            }
        }
        return result;
    }

    /// <summary>Every active file's statistics JSON.</summary>
    private async Task<List<string>> ActiveFileStatsAsync()
    {
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        return table.CurrentSnapshot.ActiveFiles.Values.Select(a => a.GetStatsJson() ?? "").ToList();
    }

    private async Task<List<(long Id, string Region, long Val)>> ReadRowsAsync()
    {
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var rows = new List<(long, string, long)>();
        await foreach (var b in table.ReadAllAsync())
        {
            var id = (Int64Array)b.Column("id");
            var region = (StringArray)b.Column("region");
            var val = (Int64Array)b.Column("val");
            for (int i = 0; i < b.Length; i++)
                rows.Add((id.GetValue(i)!.Value, region.GetString(i), val.GetValue(i)!.Value));
        }
        rows.Sort();
        return rows;
    }

    private async Task<List<long>> RowIdsOfAsync(DeltaTable table, params long[] ids)
    {
        var wanted = new HashSet<long>(ids);
        var result = new List<long>();
        await foreach (var b in table.ReadAllWithRowIdsAsync(null, null))
        {
            var id = (Int64Array)b.Column("id");
            var rid = (Int64Array)b.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < b.Length; i++)
                if (wanted.Contains(id.GetValue(i)!.Value))
                    result.Add(rid.GetValue(i)!.Value);
        }
        return result;
    }

    private static RecordBatch SetVal(RecordBatch b, long newVal)
    {
        int idx = b.Schema.GetFieldIndex("val");
        var columns = new IArrowArray[b.ColumnCount];
        for (int i = 0; i < b.ColumnCount; i++)
        {
            if (i != idx) { columns[i] = b.Column(i); continue; }
            var builder = new Int64Array.Builder();
            for (int r = 0; r < b.Length; r++) builder.Append(newVal);
            columns[i] = builder.Build();
        }
        return new RecordBatch(b.Schema, columns, b.Length);
    }

    private static Func<RecordBatch, BooleanArray> IdIs(long target) => b =>
    {
        var id = (Int64Array)b.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < b.Length; i++) mask.Append(id.GetValue(i) == target);
        return mask.Build();
    };

    [Fact]
    public async Task Append_WritesNoPartitionColumn()
    {
        // The reference layout the rewrites have to match.
        await using (var table = await CreateAsync())
            await table.WriteAsync([Batch(1, 3, "east")]);

        Assert.Equal([["id", "val"]], await ActiveFileColumnsAsync());
        Assert.All(await ActiveFileStatsAsync(), s => Assert.DoesNotContain("region", s));
    }

    [Fact]
    public async Task PredicateUpdate_RewritesWithoutThePartitionColumn()
    {
        await using (var table = await CreateAsync())
        {
            await table.WriteAsync([Batch(1, 3, "east")]);
            await table.UpdateAsync(IdIs(2), b => SetVal(b, 999));
        }

        Assert.Equal([["id", "val"]], await ActiveFileColumnsAsync());
        Assert.All(await ActiveFileStatsAsync(), s => Assert.DoesNotContain("region", s));
        Assert.Equal([(1L, "east", 101L), (2L, "east", 999L), (3L, "east", 103L)], await ReadRowsAsync());
    }

    [Fact]
    public async Task RowIdCopyOnWriteDelete_RewritesWithoutThePartitionColumn()
    {
        await using (var table = await CreateAsync())
        {
            await table.WriteAsync([Batch(1, 3, "east")]);
            await table.DeleteRowsAsync(
                RowSelection.FromRowAddresses(await RowIdsOfAsync(table, 2), table.CurrentSnapshot),
                RowDeleteMode.CopyOnWrite);
        }

        Assert.Equal([["id", "val"]], await ActiveFileColumnsAsync());
        Assert.All(await ActiveFileStatsAsync(), s => Assert.DoesNotContain("region", s));
        Assert.Equal([(1L, "east", 101L), (3L, "east", 103L)], await ReadRowsAsync());
    }

    [Fact]
    public async Task RowIdCopyOnWriteUpdate_RewritesWithoutThePartitionColumn()
    {
        await using (var table = await CreateAsync())
        {
            await table.WriteAsync([Batch(1, 3, "east")]);
            var selection = RowSelection.FromRowAddresses(
                await RowIdsOfAsync(table, 3), table.CurrentSnapshot);
            await table.UpdateRowsAsync(selection, (_, batches, _) =>
            {
                var outp = new List<RecordBatch>(batches.Count);
                foreach (var b in batches)
                {
                    var id = (Int64Array)b.Column("id");
                    var val = (Int64Array)b.Column("val");
                    var nb = new Int64Array.Builder();
                    for (int i = 0; i < b.Length; i++)
                        nb.Append(id.GetValue(i) == 3 ? 777 : val.GetValue(i)!.Value);
                    var columns = new IArrowArray[b.ColumnCount];
                    for (int c = 0; c < b.ColumnCount; c++)
                        columns[c] = b.Schema.FieldsList[c].Name == "val" ? nb.Build() : b.Column(c);
                    outp.Add(new RecordBatch(b.Schema, columns, b.Length));
                }
                return outp;
            });
        }

        Assert.Equal([["id", "val"]], await ActiveFileColumnsAsync());
        Assert.All(await ActiveFileStatsAsync(), s => Assert.DoesNotContain("region", s));
        Assert.Equal([(1L, "east", 101L), (2L, "east", 102L), (3L, "east", 777L)], await ReadRowsAsync());
    }

    [Fact]
    public async Task MappedTable_RewriteWritesOnlyTheDataColumns()
    {
        // Under column mapping the file's names are physical, so assert on the COUNT (two data columns, never
        // three) — a leaked partition column would arrive under its own physical name.
        await using (var table = await CreateAsync(ColumnMappingMode.Name))
        {
            await table.WriteAsync([Batch(1, 3, "east")]);
            await table.UpdateAsync(IdIs(2), b => SetVal(b, 999));
        }

        Assert.All(await ActiveFileColumnsAsync(), cols => Assert.Equal(2, cols.Count));
        Assert.Equal([(1L, "east", 101L), (2L, "east", 999L), (3L, "east", 103L)], await ReadRowsAsync());
    }

    [Fact]
    public async Task OldFileCarryingThePartitionColumn_StillReadsCorrectly()
    {
        // Tables written before the fix have the stray column baked into their data files. The read path
        // projects the non-partition columns by name, so those files keep reading correctly — and a rewrite
        // of one drops the column without disturbing the rows.
        await using (var table = await CreateAsync())
        {
            await table.WriteAsync([Batch(1, 3, "east")]);

            // Re-create the old shape: write a file that DOES carry the partition column, and reference it.
            var fs = new LocalTableFileSystem(_tempDir);
            var addFile = table.CurrentSnapshot.ActiveFiles.Values.Single();
            string dir = addFile.Path[..(addFile.Path.LastIndexOf('/') + 1)];
            string name = $"{Guid.NewGuid():N}.parquet";
            long size;
            await using (var file = await fs.CreateAsync(DeltaPath.Decode(dir) + name))
            {
                await using var writer = new Parquet.ParquetFileWriter(file, ownsFile: false);
                await writer.WriteRowGroupAsync(Batch(10, 2, "east")); // id, region, val — the stray column
                await writer.DisposeAsync();
                size = file.Position;
            }
            await new EngineeredWood.DeltaLake.Log.TransactionLog(fs).WriteCommitAsync(
                table.CurrentSnapshot.Version + 1,
                [
                    new EngineeredWood.DeltaLake.Actions.AddFile
                    {
                        Path = dir + name,
                        PartitionValues = new Dictionary<string, string> { ["region"] = "east" },
                        Size = size,
                        ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        DataChange = true,
                    },
                ]);
        }

        Assert.Equal(
            [(1L, "east", 101L), (2L, "east", 102L), (3L, "east", 103L),
             (10L, "east", 110L), (11L, "east", 111L)],
            await ReadRowsAsync());

        // Rewriting that legacy file normalizes it.
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)))
            await table.UpdateAsync(IdIs(10), b => SetVal(b, 888));

        Assert.All(await ActiveFileColumnsAsync(), cols => Assert.Equal(["id", "val"], cols));
        Assert.Equal(
            [(1L, "east", 101L), (2L, "east", 102L), (3L, "east", 103L),
             (10L, "east", 888L), (11L, "east", 111L)],
            await ReadRowsAsync());
    }
}
