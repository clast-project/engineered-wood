// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Batches that differ only in an Arrow nullability flag share a data file (#651). The write takes every field's flag
/// from the table schema, as compaction does since #650, instead of starting a new file at each flip.
/// </summary>
public class WriteNullabilityTests : IDisposable
{
    private readonly string _tempDir;

    public WriteNullabilityTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_write_nullability_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static RecordBatch B(long id, bool nullable) => new(
        new Apache.Arrow.Schema([new Field("id", Int64Type.Default, nullable)], null),
        [new Int64Array.Builder().Append(id).Build()], 1);

    private static RecordBatch Nested(long x, bool childNullable)
    {
        var type = new StructType([new Field("x", Int64Type.Default, childNullable)]);
        return new RecordBatch(
            new Apache.Arrow.Schema([new Field("p", type, true)], null),
            [new StructArray(type, 1, [new Int64Array.Builder().Append(x).Build()], ArrowBuffer.Empty, 0)], 1);
    }

    private static async Task<List<long>> IdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var column = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    /// <summary>The issue's repro, against a table that declares the column nullable and one that declares it NOT
    /// NULL.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task OneWrite_OfAlternatingFlags_WritesOneFile(bool tableNullable)
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), B(0, tableNullable).Schema);

        await table.WriteAsync([B(1, false), B(2, true), B(3, false)]);

        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        Assert.Equal([1L, 2L, 3L], await IdsAsync(table));
    }

    [Fact]
    public async Task ANestedFieldsFlag_DoesNotSplitTheFileEither()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), Nested(0, childNullable: true).Schema);

        await table.WriteAsync([Nested(1, false), Nested(2, true)]);

        Assert.Single(table.CurrentSnapshot.ActiveFiles);
    }

    /// <summary>A transaction's append goes through the same write.</summary>
    [Fact]
    public async Task ATransactionsWrite_WritesOneFile()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), B(0, true).Schema);
        await using (var txn = table.StartTransaction())
        {
            await txn.WriteAsync([B(1, false), B(2, true)]);
            await txn.CommitAsync();
        }

        Assert.Single(table.CurrentSnapshot.ActiveFiles);
    }

    /// <summary>The host-commit seam that writes files without committing them.</summary>
    [Fact]
    public async Task WriteDataFilesAsync_WritesOneFile()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), B(0, true).Schema);

        var files = await table.WriteDataFilesAsync([B(1, false), B(2, true), B(3, false)]);

        Assert.Single(files);
    }

    /// <summary>
    /// A copy-on-write rewrite takes the table's flags too. The updated rows are written first, so an updater that
    /// declares <c>v</c> NOT NULL made the rewritten file's <c>v</c> required, and the untouched row's NULL after it
    /// was refused as a NULL in a required column.
    /// </summary>
    [Fact]
    public async Task AnUpdatersFlags_DoNotRefuseTheUntouchedRowsNull()
    {
        var schema = new Apache.Arrow.Schema(
            [new Field("id", Int64Type.Default, true), new Field("v", Int64Type.Default, true)], null);
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build(),
             new Int64Array.Builder().AppendNull().Append(5).Build()], 2)]);

        var notNull = new Apache.Arrow.Schema(
            [new Field("id", Int64Type.Default, false), new Field("v", Int64Type.Default, false)], null);
        var (updated, _) = await table.UpdateAsync(
            batch =>
            {
                var ids = (Int64Array)batch.Column("id");
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    mask.Append(ids.GetValue(i) == 2);
                return mask.Build();
            },
            batch =>
            {
                var v = new Int64Array.Builder();
                for (int i = 0; i < batch.Length; i++)
                    v.Append(6);
                return new RecordBatch(notNull, [batch.Column("id"), v.Build()], batch.Length);
            });

        Assert.Equal(1, updated);
        var rows = new SortedDictionary<long, long?>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var v = (Int64Array)batch.Column("v");
            for (int i = 0; i < batch.Length; i++)
                rows[ids.GetValue(i)!.Value] = v.GetValue(i);
        }
        Assert.Equal([new KeyValuePair<long, long?>(1, null), new KeyValuePair<long, long?>(2, 6)], rows);
    }
}
