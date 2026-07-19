// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The active file set is keyed by (path, deletionVector), so any RemoveFile targeting a file that carries a
/// DV must carry that DV too. A remove that omits it does not reconcile: the file stays ACTIVE alongside its
/// replacement, and every subsequent read returns its rows twice.
/// </summary>
public class RemoveReconciliationTests : IDisposable
{
    private readonly string _tempDir;

    public RemoveReconciliationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rmrec_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema Schema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params long[] ids)
    {
        var b = new Int64Array.Builder();
        foreach (var id in ids)
            b.Append(id);
        return new RecordBatch(schema, [b.Build()], ids.Length);
    }

    // DeleteAsync always marks rows with a deletion vector rather than rewriting the file, so the AddFile
    // survives the delete carrying a DV — exactly the shape that exposes an unqualified remove.
    private static async Task<DeltaTable> CreateDvTableAsync(string dir)
    {
        var fs = new LocalTableFileSystem(dir);
        var options = new DeltaTableOptions { CheckpointInterval = 0 };
        return await DeltaTable.CreateAsync(fs, Schema(), options);
    }

    private static async Task<List<long>> ReadIdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var b in table.ReadAllAsync())
        {
            var col = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    [Fact]
    public async Task Overwrite_AfterDeletionVectorDelete_DoesNotDuplicateRows()
    {
        await using var table = await CreateDvTableAsync(_tempDir);
        var schema = table.ArrowSchema;

        await table.WriteAsync([Rows(schema, 1, 2, 3)]);

        // DV delete — the AddFile stays, now carrying a deletion vector.
        await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                mask.Append(ids.GetValue(i) == 1);
            return mask.Build();
        });
        Assert.Equal([2L, 3L], await ReadIdsAsync(table));
        Assert.NotNull(table.CurrentSnapshot.ActiveFiles.Values.Single().DeletionVector);

        // Overwrite must remove that DV-carrying file. If the remove omits the DV it doesn't reconcile,
        // and the old file's surviving rows (2, 3) come back alongside the new ones.
        await table.WriteAsync([Rows(schema, 99)], DeltaWriteMode.Overwrite);

        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        Assert.Equal([99L], await ReadIdsAsync(table));
    }

    [Fact]
    public async Task Update_AfterDeletionVectorDelete_DoesNotDuplicateRows()
    {
        await using var table = await CreateDvTableAsync(_tempDir);
        var schema = table.ArrowSchema;

        await table.WriteAsync([Rows(schema, 1, 2, 3)]);

        await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                mask.Append(ids.GetValue(i) == 1);
            return mask.Build();
        });

        // Copy-on-write UPDATE rewrites the file; the source remove must be DV-qualified.
        await table.UpdateAsync(
            batch =>
            {
                var ids = (Int64Array)batch.Column(0);
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    mask.Append(ids.GetValue(i) == 2);
                return mask.Build();
            },
            batch =>
            {
                var b = new Int64Array.Builder();
                for (int i = 0; i < batch.Length; i++)
                    b.Append(20);
                return new RecordBatch(batch.Schema, [b.Build()], batch.Length);
            });

        Assert.Equal([3L, 20L], await ReadIdsAsync(table));
    }

    [Fact]
    public async Task Compaction_AfterDeletionVectorDelete_DoesNotDuplicateRows()
    {
        await using var table = await CreateDvTableAsync(_tempDir);
        var schema = table.ArrowSchema;

        await table.WriteAsync([Rows(schema, 1, 2)]);
        await table.WriteAsync([Rows(schema, 3, 4)]);

        await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                mask.Append(ids.GetValue(i) == 1);
            return mask.Build();
        });

        var before = await ReadIdsAsync(table);
        Assert.Equal([2L, 3L, 4L], before);

        await table.CompactAsync(new CompactionOptions
        {
            MinFileSize = long.MaxValue,
            TargetFileSize = long.MaxValue,
        });

        // Compaction is a rearrangement — the row set must be identical, with the DV-carrying source removed.
        Assert.Equal(before, await ReadIdsAsync(table));
    }
}
