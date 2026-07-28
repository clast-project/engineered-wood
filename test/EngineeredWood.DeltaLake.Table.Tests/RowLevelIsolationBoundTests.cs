// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The ISOLATION BOUND on row-level concurrency: reconciling two deletes of different rows in one file is a
/// <see cref="IsolationLevel.WriteSerializable"/> behaviour, so the LEVEL belongs in the condition and not
/// only the presence of deletion-vector edits.
/// </summary>
public class RowLevelIsolationBoundTests : IDisposable
{
    private readonly string _tempDir;

    public RowLevelIsolationBoundTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_isobound_{Guid.NewGuid():N}");
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

    private ValueTask<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

    private async Task<DeltaTable> CreateAsync()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema(),
            enableDeletionVectors: true, enableRowTracking: true);
        await table.WriteAsync([Batch(0, 10)]);
        return table;
    }

    /// <summary>Absolute positions in the fixture's single file, keyed by its ordinal (0).</summary>
    private static IReadOnlyDictionary<int, IReadOnlyCollection<long>> Positions(params long[] positions) =>
        new Dictionary<int, IReadOnlyCollection<long>> { [0] = positions };

    /// <summary>The same positions as transient row addresses, for the autocommit DV delete.</summary>
    private static IReadOnlyCollection<long> Addresses(params long[] positions) =>
        positions.Select(p => TransientRowAddress.Pack(0, p)).ToList();

    private async Task<int> RowCountAsync()
    {
        await using var table = await OpenAsync();
        int n = 0;
        await foreach (var batch in table.ReadAllAsync())
            n += batch.Length;
        return n;
    }

    /// <summary>
    /// Two deletes of DIFFERENT rows in the same file reconcile under WriteSerializable — both land. Under
    /// Serializable they must NOT: commit order is the logical order there, so a second writer touching a file
    /// the first also touched conflicts at FILE granularity however disjoint the rows are. Reconciling under
    /// Serializable admits exactly the interleaving that level exists to forbid.
    /// </summary>
    [Fact]
    public async Task DisjointRowDeletes_ReconcileUnderWriteSerializable_ButConflictUnderSerializable()
    {
        // WriteSerializable: both deletes survive.
        await using (await CreateAsync())
        {
            await using var table = await OpenAsync();
            // Started BEFORE the concurrent delete, so the transaction's base is the pre-delete version.
            var txn = table.StartTransaction(IsolationLevel.WriteSerializable);
            await txn.StageRowDeletesAsync(Positions(2));

            await using (var other = await OpenAsync())
            {
                await other.DeleteByRowIdsViaVectorsAsync(Addresses(7));
            }

            await txn.CommitAsync();
            Assert.Equal(8, await RowCountAsync());
        }

        // Serializable, same shape on a fresh table: the second one conflicts.
        Directory.Delete(_tempDir, recursive: true);
        Directory.CreateDirectory(_tempDir);
        await using (await CreateAsync())
        {
            await using var table = await OpenAsync();
            var txn = table.StartTransaction(IsolationLevel.Serializable);
            await txn.StageRowDeletesAsync(Positions(2));

            await using (var other = await OpenAsync())
            {
                await other.DeleteByRowIdsViaVectorsAsync(Addresses(7));
            }

            await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
            Assert.Equal(9, await RowCountAsync());   // only the concurrent delete landed
        }
    }
}
