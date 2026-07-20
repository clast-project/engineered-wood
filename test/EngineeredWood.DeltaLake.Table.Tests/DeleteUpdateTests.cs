// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class DeleteUpdateTests : IDisposable
{
    private readonly string _tempDir;

    public DeleteUpdateTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_du_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<DeltaTable> CreateAndPopulateTable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

        var table = await DeltaTable.CreateAsync(fs, schema, enableDeletionVectors: true);

        var ids = new Int64Array.Builder()
            .Append(1).Append(2).Append(3).Append(4).Append(5).Build();
        var values = new StringArray.Builder()
            .Append("a").Append("b").Append("c").Append("d").Append("e").Build();
        var batch = new RecordBatch(schema, [ids, values], 5);

        await table.WriteAsync([batch]);
        return table;
    }

    [Fact]
    public async Task Delete_ByPredicate_CreatesDeleteionVector()
    {
        await using var table = await CreateAndPopulateTable();

        // Delete rows where id > 3
        var (deleted, version) = await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var builder = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                builder.Append(ids.GetValue(i)!.Value > 3);
            return builder.Build();
        });

        Assert.Equal(2L, deleted); // Rows 4 and 5
        Assert.Equal(2L, version);

        // The file should now have a DV
        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        Assert.NotNull(addFile.DeletionVector);
        Assert.Equal(2L, addFile.DeletionVector!.Cardinality);

        // Read back — should only have 3 rows
        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;
        Assert.Equal(3, totalRows);
    }

    [Fact]
    public async Task Delete_NoMatchingRows_NoCommit()
    {
        await using var table = await CreateAndPopulateTable();

        var (deleted, version) = await table.DeleteAsync(batch =>
        {
            var builder = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                builder.Append(false); // No rows match
            return builder.Build();
        });

        Assert.Equal(0L, deleted);
        Assert.Equal(1L, version); // No new commit
    }

    [Fact]
    public async Task Delete_AllRows_EmptyTable()
    {
        await using var table = await CreateAndPopulateTable();

        var (deleted, _) = await table.DeleteAsync(batch =>
        {
            var builder = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                builder.Append(true);
            return builder.Build();
        });

        Assert.Equal(5L, deleted);

        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;
        Assert.Equal(0, totalRows);
    }

    [Fact]
    public async Task Delete_MultipleDeletes_MergesDVs()
    {
        await using var table = await CreateAndPopulateTable();

        // First delete: id == 1
        await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var builder = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                builder.Append(ids.GetValue(i)!.Value == 1);
            return builder.Build();
        });

        // Second delete: id == 5
        await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var builder = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                builder.Append(ids.GetValue(i)!.Value == 5);
            return builder.Build();
        });

        // Should have 3 rows remaining
        int totalRows = 0;
        var readIds = new List<long>();
        await foreach (var b in table.ReadAllAsync())
        {
            totalRows += b.Length;
            var ids = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                readIds.Add(ids.GetValue(i)!.Value);
        }

        Assert.Equal(3, totalRows);
        Assert.Equal(new long[] { 2, 3, 4 }, readIds.OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task Update_ModifiesMatchingRows()
    {
        await using var table = await CreateAndPopulateTable();

        // Update: set value = "UPDATED" where id > 3
        var (updated, version) = await table.UpdateAsync(
            predicate: batch =>
            {
                var ids = (Int64Array)batch.Column(0);
                var builder = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    builder.Append(ids.GetValue(i)!.Value > 3);
                return builder.Build();
            },
            updater: batch =>
            {
                // Keep id column, replace value column
                var newValues = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    newValues.Append("UPDATED");
                return new RecordBatch(batch.Schema,
                    [batch.Column(0), newValues.Build()], batch.Length);
            });

        Assert.Equal(2L, updated);
        Assert.Equal(2L, version);

        // Read back and verify
        var allValues = new Dictionary<long, string>();
        await foreach (var b in table.ReadAllAsync())
        {
            var ids = (Int64Array)b.Column(0);
            var values = (StringArray)b.Column(1);
            for (int i = 0; i < b.Length; i++)
                allValues[ids.GetValue(i)!.Value] = values.GetString(i);
        }

        Assert.Equal(5, allValues.Count);
        Assert.Equal("a", allValues[1]);
        Assert.Equal("b", allValues[2]);
        Assert.Equal("c", allValues[3]);
        Assert.Equal("UPDATED", allValues[4]);
        Assert.Equal("UPDATED", allValues[5]);
    }

    [Fact]
    public async Task Update_NoMatchingRows_NoCommit()
    {
        await using var table = await CreateAndPopulateTable();

        var (updated, version) = await table.UpdateAsync(
            predicate: batch =>
            {
                var builder = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    builder.Append(false);
                return builder.Build();
            },
            updater: batch => batch);

        Assert.Equal(0L, updated);
        Assert.Equal(1L, version);
    }

    [Fact]
    public async Task Update_AllRows()
    {
        await using var table = await CreateAndPopulateTable();

        var (updated, _) = await table.UpdateAsync(
            predicate: batch =>
            {
                var builder = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    builder.Append(true);
                return builder.Build();
            },
            updater: batch =>
            {
                var newValues = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    newValues.Append("ALL");
                return new RecordBatch(batch.Schema,
                    [batch.Column(0), newValues.Build()], batch.Length);
            });

        Assert.Equal(5L, updated);

        await foreach (var b in table.ReadAllAsync())
        {
            var values = (StringArray)b.Column(1);
            for (int i = 0; i < b.Length; i++)
                Assert.Equal("ALL", values.GetString(i));
        }
    }

    [Fact]
    public async Task DeleteThenRead_DataIntegrity()
    {
        await using var table = await CreateAndPopulateTable();

        // Delete row with id=3
        await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var builder = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                builder.Append(ids.GetValue(i)!.Value == 3);
            return builder.Build();
        });

        // Verify remaining data
        var allData = new List<(long Id, string Value)>();
        await foreach (var b in table.ReadAllAsync())
        {
            var ids = (Int64Array)b.Column(0);
            var values = (StringArray)b.Column(1);
            for (int i = 0; i < b.Length; i++)
                allData.Add((ids.GetValue(i)!.Value, values.GetString(i)));
        }

        Assert.Equal(4, allData.Count);
        allData.Sort((a, b) => a.Id.CompareTo(b.Id));
        Assert.Equal(1L, allData[0].Id);
        Assert.Equal("a", allData[0].Value);
        Assert.Equal(2L, allData[1].Id);
        Assert.Equal("b", allData[1].Value);
        Assert.Equal(4L, allData[2].Id);
        Assert.Equal("d", allData[2].Value);
        Assert.Equal(5L, allData[3].Id);
        Assert.Equal("e", allData[3].Value);
    }
}
