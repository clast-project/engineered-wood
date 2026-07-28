// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// What a host-staged transaction READ, and how the isolation level bounds the row-level relaxation.
///
/// <para>Two things a host could not express before. It could not declare its reads at all — the predicate
/// overloads of <see cref="DeltaTransaction.DeleteAsync"/> record their own, but a host that ran its own scan
/// and staged the result had no way to say what that scan depended on, so its transaction was treated as
/// having read only the files it removes. And the row-level reconciliation that lets two deletes of DIFFERENT
/// rows in one file both land ran at EVERY isolation level, including
/// <see cref="IsolationLevel.Serializable"/>, where admitting that interleaving is precisely what the level
/// forbids.</para>
/// </summary>
public class ReadDeclarationTests : IDisposable
{
    private readonly string _tempDir;

    public ReadDeclarationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_readdecl_{Guid.NewGuid():N}");
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

    private async Task<DeltaTable> CreateAsync()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema(),
            enableDeletionVectors: true, enableRowTracking: true);
        await table.WriteAsync([Batch(0, 10)]);
        return table;
    }

    private async Task<int> RowCountAsync()
    {
        await using var table = await OpenAsync();
        int n = 0;
        await foreach (var b in table.ReadAllAsync())
            n += b.Length;
        return n;
    }

    /// <summary>
    /// A declared read predicate reaches the read set: under Serializable a concurrent APPEND that could
    /// satisfy it conflicts, even though the append itself touched nothing this transaction wrote.
    /// </summary>
    [Fact]
    public async Task DeclaredReadPredicate_ConflictsWithAMatchingConcurrentAppend_UnderSerializable()
    {
        await using var created = await CreateAsync();
        await using var table = await OpenAsync();
        var txn = table.StartTransaction(IsolationLevel.Serializable);
        var files = await table.WriteDataFilesAsync([Batch(200, 1)]);
        txn.StageDataFiles(files);
        txn.StageReadPredicate(Ex.GreaterThanOrEqual("id", 100L));

        // A blind append landing in the range the transaction says it read.
        await using (var other = await OpenAsync())
        {
            await other.WriteAsync([Batch(150, 1)]);
        }

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
    }

    /// <summary>
    /// The same declaration under WriteSerializable does NOT conflict: a blind append is exempt there, which
    /// is the whole difference between the two levels. Shows the predicate is honoured rather than ignored —
    /// the outcome differs only by isolation.
    /// </summary>
    [Fact]
    public async Task DeclaredReadPredicate_IsExemptFromABlindAppend_UnderWriteSerializable()
    {
        await using var created = await CreateAsync();
        await using var table = await OpenAsync();
        var txn = table.StartTransaction(IsolationLevel.WriteSerializable);
        var files = await table.WriteDataFilesAsync([Batch(200, 1)]);
        txn.StageDataFiles(files);
        txn.StageReadPredicate(Ex.GreaterThanOrEqual("id", 100L));

        await using (var other = await OpenAsync())
        {
            await other.WriteAsync([Batch(150, 1)]);
        }

        await txn.CommitAsync();
        Assert.Equal(12, await RowCountAsync());
    }

    /// <summary>
    /// A whole-table read is the honest answer when a scan had no pushable predicate, and it is strictly
    /// stronger: under Serializable ANY concurrent add is relevant, including one no predicate would match.
    /// </summary>
    [Fact]
    public async Task DeclaredWholeTableRead_ConflictsWithAnyConcurrentAppend_UnderSerializable()
    {
        await using var created = await CreateAsync();
        await using var table = await OpenAsync();
        var txn = table.StartTransaction(IsolationLevel.Serializable);
        var files = await table.WriteDataFilesAsync([Batch(200, 1)]);
        txn.StageDataFiles(files);
        txn.StageWholeTableRead();

        await using (var other = await OpenAsync())
        {
            await other.WriteAsync([Batch(900, 1)]);   // matches no predicate anyone would write
        }

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
    }

    /// <summary>Declaring nothing keeps a staged append blind — the baseline the two above are measured against.</summary>
    [Fact]
    public async Task DeclaringNoReads_StaysBlind_EvenUnderSerializable()
    {
        await using var created = await CreateAsync();
        await using var table = await OpenAsync();
        var txn = table.StartTransaction(IsolationLevel.Serializable);
        var files = await table.WriteDataFilesAsync([Batch(200, 1)]);
        txn.StageDataFiles(files);

        await using (var other = await OpenAsync())
        {
            await other.WriteAsync([Batch(150, 1)]);
        }

        await txn.CommitAsync();
        Assert.Equal(12, await RowCountAsync());
    }
}
