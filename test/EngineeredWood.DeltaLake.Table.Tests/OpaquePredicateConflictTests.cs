// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The delegate overloads of <c>DeleteAsync</c>/<c>UpdateAsync</c> (#491). Their condition is an opaque
/// function, so it cannot be tested against a concurrent add's statistics the way an analyzable predicate
/// is. They used to record no read predicate at all, which made a matching concurrent add invisible to
/// them at every isolation level; they now take every concurrent add to match, still behind the isolation
/// level's blind-append gate, while a concurrent remove stays scoped to the files they actually read.
/// </summary>
public class OpaquePredicateConflictTests : IDisposable
{
    private readonly string _tempDir;

    public OpaquePredicateConflictTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_opaque_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdRegionSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("region", StringType.Default, false))
        .Build();

    private static RecordBatch Batch(long[] ids, string[] regions)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        var regionBuilder = new StringArray.Builder();
        foreach (string r in regions)
            regionBuilder.Append(r);
        return new RecordBatch(IdRegionSchema, [idArray, regionBuilder.Build()], ids.Length);
    }

    /// <summary>The opaque condition: <c>region = 'us'</c>, as a delegate.</summary>
    private static BooleanArray RegionIsUs(RecordBatch batch)
    {
        var regions = (StringArray)batch.Column("region");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            mask.Append(regions.GetString(i) == "us");
        return mask.Build();
    }

    private static Func<RecordBatch, RecordBatch> SetRegion(string region) => batch =>
    {
        var regions = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            regions.Append(region);
        return new RecordBatch(IdRegionSchema, [batch.Column("id"), regions.Build()], batch.Length);
    };

    private static async Task<List<(long Id, string Region)>> ReadRows(DeltaTable table)
    {
        var rows = new List<(long, string)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var regions = (StringArray)batch.Column("region");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, regions.GetString(i)));
        }

        rows.Sort();
        return rows;
    }

    /// <summary>
    /// Under <see cref="IsolationLevel.Serializable"/> a concurrent blind append of a matching row aborts
    /// the delete — the case where raising the isolation level used to have no effect on this overload.
    /// </summary>
    [Fact]
    public async Task Serializable_DelegateDelete_ConcurrentBlindAppend_Aborts()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch([1, 2], ["us", "eu"])]);

        await using var tx = table.StartTransaction(IsolationLevel.Serializable);
        await tx.DeleteAsync(RegionIsUs);

        await table.WriteAsync([Batch([3], ["us"])]); // concurrent blind append

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () => await tx.CommitAsync());
        Assert.Equal(DeltaErrorCodes.ConcurrentAppend, ex.ErrorCode);
        Assert.Equal([(1L, "us"), (2L, "eu"), (3L, "us")], await ReadRows(table));
    }

    /// <summary>The same under Serializable for the transactional delegate UPDATE.</summary>
    [Fact]
    public async Task Serializable_DelegateUpdate_ConcurrentBlindAppend_Aborts()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch([1, 2], ["us", "eu"])]);

        await using var tx = table.StartTransaction(IsolationLevel.Serializable);
        await tx.UpdateAsync(RegionIsUs, SetRegion("xx"));

        await table.WriteAsync([Batch([3], ["us"])]); // concurrent blind append

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () => await tx.CommitAsync());
        Assert.Equal(DeltaErrorCodes.ConcurrentAppend, ex.ErrorCode);
        Assert.Equal([(1L, "us"), (2L, "eu"), (3L, "us")], await ReadRows(table));
    }

    /// <summary>
    /// Under the default <see cref="IsolationLevel.WriteSerializable"/> a concurrent BLIND append is still
    /// exempt: the delete rebases and lands, and the appended row survives.
    /// </summary>
    [Fact]
    public async Task WriteSerializable_DelegateDelete_ConcurrentBlindAppend_Lands()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch([1, 2], ["us", "eu"])]);

        await using var tx = table.StartTransaction();
        await tx.DeleteAsync(RegionIsUs);

        await table.WriteAsync([Batch([3], ["us"])]); // concurrent blind append

        await tx.CommitAsync();

        Assert.Equal([(2L, "eu"), (3L, "us")], await ReadRows(table));
    }

    /// <summary>
    /// Under WriteSerializable a concurrent commit that is NOT a blind append — its writer read the table
    /// before adding — is examined, and its add is taken to match the opaque condition.
    /// </summary>
    [Fact]
    public async Task WriteSerializable_DelegateDelete_ConcurrentNonBlindAdd_Aborts()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch([1, 2], ["us", "eu"])]);

        await using var tx = table.StartTransaction();
        await tx.DeleteAsync(RegionIsUs);

        await using (var other = table.StartTransaction())
        {
            other.IsBlindAppend = false; // an INSERT ... SELECT FROM t, say
            await other.WriteAsync([Batch([3], ["us"])]);
            await other.CommitAsync();
        }

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () => await tx.CommitAsync());
        Assert.Equal(DeltaErrorCodes.ConcurrentAppend, ex.ErrorCode);
    }

    /// <summary>
    /// Not a whole-table read: a concurrent delete of a file the delegate DELETE never read does not abort
    /// it. Without deletion vectors each delete removes whole files and adds nothing, so the only thing the
    /// concurrent commit could conflict on is the remove.
    /// </summary>
    [Fact]
    public async Task DelegateDelete_ConcurrentRemoveOfUnreadFile_Lands()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);
        await table.WriteAsync([Batch([1], ["us"])]); // file 1
        await table.WriteAsync([Batch([2], ["eu"])]); // file 2

        await using var tx = table.StartTransaction(IsolationLevel.Serializable);
        await tx.DeleteAsync(RegionIsUs);

        await table.DeleteAsync(batch =>
        {
            var regions = (StringArray)batch.Column("region");
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                mask.Append(regions.GetString(i) == "eu");
            return mask.Build();
        });

        await tx.CommitAsync();

        Assert.Empty(await ReadRows(table));
    }

    /// <summary>
    /// A delegate DELETE that matched nothing still read the table, so a transaction staging one alongside
    /// an append is not a blind append.
    /// </summary>
    [Fact]
    public async Task DelegateDelete_MatchingNothing_StillRecordsARead()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch([2], ["eu"])]);

        await using var tx = table.StartTransaction(IsolationLevel.Serializable);
        Assert.Equal(0, await tx.DeleteAsync(RegionIsUs));
        await tx.WriteAsync([Batch([4], ["eu"])]);

        await table.WriteAsync([Batch([3], ["us"])]); // concurrent blind append

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () => await tx.CommitAsync());
        Assert.Equal(DeltaErrorCodes.ConcurrentAppend, ex.ErrorCode);
    }
}
