// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The isolation level bounds row-level concurrency.
///
/// <para>Reconciling two deletes because their rows are disjoint is a WriteSerializable behaviour. Under
/// <see cref="IsolationLevel.Serializable"/> commit order IS the logical order, so a second writer touching a
/// file the first also touched conflicts at FILE granularity however disjoint the rows are — admitting that
/// interleaving is precisely what the stricter level exists to forbid.</para>
/// </summary>
public class RowLevelIsolationTests : IDisposable
{
    private readonly string _tempDir;

    public RowLevelIsolationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rlisolation_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
            ids.Append(startId + i);
        return new RecordBatch(BuildSchema(), [ids.Build()], count);
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    /// <summary>One file of ids 0..9, deletion vectors and row tracking on — the shape row-level DML needs.</summary>
    private async Task<DeltaTable> CreateAsync()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema(),
            enableDeletionVectors: true, enableRowTracking: true);
        await table.WriteAsync([Batch(0, 10)]);
        return table;
    }

    /// <summary>Positions in the single file, keyed by its ordinal in the path-sorted active set.</summary>
    private static Dictionary<int, IReadOnlyCollection<long>> AtOrdinalZero(params long[] positions)
        => new() { [0] = positions };

    private async Task<int> RowCountAsync()
    {
        await using var table = await OpenAsync();
        int n = 0;
        await foreach (var b in table.ReadAllAsync())
            n += b.Length;
        return n;
    }

    /// <summary>
    /// Two deletes of DIFFERENT rows in the same file. Under WriteSerializable they reconcile and both land —
    /// the row-level behaviour. Under Serializable the second must conflict instead.
    /// </summary>
    [Fact]
    public async Task DisjointRowDeletes_ReconcileUnderWriteSerializable_ButConflictUnderSerializable()
    {
        // WriteSerializable: both deletes survive.
        await using (var created = await CreateAsync())
        {
            var pinned = created.CurrentSnapshot;
            await using var table = await OpenAsync();
            var txn = table.StartTransaction(IsolationLevel.WriteSerializable);
            await txn.StageRowDeletesAsync(AtOrdinalZero(2));

            await using (var other = await OpenAsync())
            {
                await other.DeleteByRowIdsViaVectorsAsync(
                    new[] { TransientRowAddress.Pack(0, 7) });
            }

            await txn.CommitAsync();
            Assert.Equal(8, await RowCountAsync());
        }

        // Serializable, same shape on a fresh table: the second one conflicts and only the first delete landed.
        Directory.Delete(_tempDir, recursive: true);
        Directory.CreateDirectory(_tempDir);
        await using (var created = await CreateAsync())
        {
            await using var table = await OpenAsync();
            var txn = table.StartTransaction(IsolationLevel.Serializable);
            await txn.StageRowDeletesAsync(AtOrdinalZero(2));

            await using (var other = await OpenAsync())
            {
                await other.DeleteByRowIdsViaVectorsAsync(
                    new[] { TransientRowAddress.Pack(0, 7) });
            }

            await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
            Assert.Equal(9, await RowCountAsync());
        }
    }
}
