// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Concurrency;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// An UPDATE on a table with deletion vectors (#704): the rows it updates are masked by the file's deletion vector
/// and their post-images written to a new file, as Spark's UPDATE does
/// (<see cref="Interop.UpdateDeletionVectorInteropTests"/> compares the two).
/// </summary>
public class UpdateDeletionVectorTests : IDisposable
{
    private readonly string _tempDir;

    public UpdateDeletionVectorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_update_dv_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private Task<DeltaTable> CreateAsync(
        bool deletionVectors = true, bool cdf = false, bool rowTracking = false,
        IReadOnlyList<string>? partitionColumns = null)
    {
        var configuration = new Dictionary<string, string>();
        if (cdf)
            configuration[CdfConfig.EnableKey] = "true";
        return DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), TableSchema, partitionColumns: partitionColumns,
            enableDeletionVectors: deletionVectors, enableRowTracking: rowTracking,
            configuration: configuration).AsTask();
    }

    private static RecordBatch Rows(params (long Id, string Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        foreach (var (id, value) in rows)
        {
            ids.Append(id);
            values.Append(value);
        }
        return new RecordBatch(TableSchema, [ids.Build(), values.Build()], rows.Length);
    }

    private static readonly RecordBatch Five = Rows((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"));

    /// <summary>The updater: every matched row's value upper-cased.</summary>
    private static RecordBatch Upper(RecordBatch batch) => SetValues(batch, v => v.ToUpperInvariant());

    private static RecordBatch SetValues(RecordBatch batch, Func<string, string> change)
    {
        var values = (StringArray)batch.Column("value");
        var builder = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            builder.Append(change(values.GetString(i)));
        return new RecordBatch(batch.Schema, [batch.Column("id"), builder.Build()], batch.Length);
    }

    /// <summary>Ids 2 and 3: the rows Spark's own UPDATE was measured on.</summary>
    private static readonly Predicate TwoAndThree = Ex.Or(
        Ex.Equal("id", LiteralValue.Of(2L)), Ex.Equal("id", LiteralValue.Of(3L)));

    private static readonly List<(long, string)> Updated = [(1, "a"), (2, "B"), (3, "C"), (4, "d"), (5, "e")];

    private static async Task<List<(long Id, string Value)>> ReadAsync(DeltaTable table)
    {
        var rows = new List<(long, string)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var values = (StringArray)batch.Column("value");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, values.GetString(i)));
        }
        return rows.OrderBy(r => r.Item1).ToList();
    }

    private async Task<IReadOnlyDictionary<string, string>> LastMetricsAsync(DeltaTable table)
    {
        IReadOnlyDictionary<string, string>? metrics = null;
        long version = -1;
        await foreach (var entry in table.GetHistoryAsync())
        {
            if (entry.Version > version)
            {
                version = entry.Version;
                metrics = entry.OperationMetrics;
            }
        }
        return metrics!;
    }

    private static Actions.AddFile FileAt(DeltaTable table, string path) =>
        table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.Path == path);

    private int FileCount(string pattern) =>
        Directory.GetFiles(_tempDir, pattern, SearchOption.AllDirectories).Length;

    [Fact]
    public async Task OnADeletionVectorTable_TheUpdatedRowsAreMaskedAndOnlyThePostImagesWritten()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        var original = table.CurrentSnapshot.ActiveFiles.Values.Single();

        var (rows, _) = await table.UpdateAsync(TwoAndThree, Upper);

        Assert.Equal(2, rows);
        Assert.Equal(Updated, await ReadAsync(table));
        var files = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        // The original file stays, masking the two updated rows; one post-image file joins it.
        var masked = files.Single(f => f.Path == original.Path);
        Assert.Equal(2, masked.DeletionVector!.Cardinality);
        Assert.Contains("\"tightBounds\":false", masked.Stats);
        Assert.Equal(5, masked.GetNumRecords());
        var post = files.Single(f => f.Path != original.Path);
        Assert.Null(post.DeletionVector);
        Assert.Equal(2, post.GetNumRecords());

        var committed = await LastMetricsAsync(table);
        Assert.Equal("2", committed["numUpdatedRows"]);
        Assert.Equal("0", committed["numCopiedRows"]);
        Assert.Equal("1", committed["numAddedFiles"]);
        // Unlike its DELETE and MERGE, Spark's UPDATE counts the masked file as removed, with its bytes.
        Assert.Equal("1", committed["numRemovedFiles"]);
        Assert.Equal(original.Size.ToString(System.Globalization.CultureInfo.InvariantCulture), committed["numRemovedBytes"]);
        Assert.Equal("1", committed["numDeletionVectorsAdded"]);
        Assert.Equal("0", committed["numDeletionVectorsRemoved"]);
        Assert.Equal("0", committed["numDeletionVectorsUpdated"]);
    }

    [Fact]
    public async Task TheFunctionalPredicateOverload_AlsoMasks()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        var original = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        await table.UpdateAsync(
            batch =>
            {
                var ids = (Int64Array)batch.Column("id");
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    mask.Append(ids.GetValue(i) is 2 or 3);
                return mask.Build();
            },
            Upper);

        Assert.Equal(Updated, await ReadAsync(table));
        Assert.Equal(2, FileAt(table, original).DeletionVector!.Cardinality);
    }

    /// <summary>Re-adds the table's only file with <paramref name="stats"/> as its statistics.</summary>
    private static async Task ReplaceStatsAsync(DeltaTable table, string? stats)
    {
        var file = table.CurrentSnapshot.ActiveFiles.Values.Single();
        await using var transaction = table.StartTransaction();
        transaction.StageActions(
        [
            new Actions.RemoveFile
            {
                Path = file.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = false,
            },
            file with { Stats = stats, TypedStats = null, DataChange = false },
        ]);
        await transaction.CommitAsync();
    }

    /// <summary>The protocol requires a file with a deletion vector to carry its physical row count, so a file whose
    /// statistics lack one gets it from the read: its live rows and the rows its existing vector already hid.</summary>
    [Theory]
    [InlineData(null, false)]
    [InlineData("{\"minValues\":{\"id\":1}}", false)]
    [InlineData(null, true)]
    public async Task AMaskedFileWithoutARowCount_IsGivenOne(string? stats, bool existingVector)
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        if (existingVector)
            await table.DeleteAsync(Ex.Equal("id", LiteralValue.Of(5L)));
        await ReplaceStatsAsync(table, stats);
        Assert.Null(table.CurrentSnapshot.ActiveFiles.Values.Single().GetNumRecords());

        // The functional overload: stats-based pruning must not skip a file whose statistics were replaced.
        await table.UpdateAsync(
            batch =>
            {
                var ids = (Int64Array)batch.Column("id");
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    mask.Append(ids.GetValue(i) is 2 or 3);
                return mask.Build();
            },
            Upper);

        var masked = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.DeletionVector is not null);
        Assert.Equal(5, masked.GetNumRecords());
        Assert.Contains("\"tightBounds\":false", masked.Stats);
        if (stats is not null)
            Assert.Contains("\"minValues\":{\"id\":1}", masked.Stats);
    }

    [Fact]
    public async Task AFileWhoseEveryLiveRowIsUpdated_IsRemovedWhole()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((2, "b"), (3, "c"))]);
        var touched = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;
        await table.WriteAsync([Rows((1, "a"), (4, "d"), (5, "e"))]);

        await table.UpdateAsync(TwoAndThree, Upper);

        Assert.Equal(Updated, await ReadAsync(table));
        Assert.DoesNotContain(table.CurrentSnapshot.ActiveFiles.Values, f => f.Path == touched);
        Assert.All(table.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
        var committed = await LastMetricsAsync(table);
        Assert.Equal("1", committed["numRemovedFiles"]);
        Assert.Equal("0", committed["numCopiedRows"]);
        Assert.Equal("0", committed["numDeletionVectorsAdded"]);
    }

    [Fact]
    public async Task AFileWithADeletionVector_HasItUnionedWithTheUpdatedRows()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        await table.DeleteAsync(Ex.Equal("id", LiteralValue.Of(5L)));
        var path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        await table.UpdateAsync(TwoAndThree, Upper);

        Assert.Equal([(1, "a"), (2, "B"), (3, "C"), (4, "d")], await ReadAsync(table));
        Assert.Equal(3, FileAt(table, path).DeletionVector!.Cardinality);
        Assert.Equal("1", (await LastMetricsAsync(table))["numDeletionVectorsUpdated"]);
    }

    [Fact]
    public async Task ARowAnEarlierDeletionVectorHid_IsNotUpdated()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        await table.DeleteAsync(Ex.Equal("id", LiteralValue.Of(2L)));

        var (rows, _) = await table.UpdateAsync(TwoAndThree, Upper);

        Assert.Equal(1, rows);
        Assert.Equal([(1, "a"), (3, "C"), (4, "d"), (5, "e")], await ReadAsync(table));
    }

    [Fact]
    public async Task ARepeatedUpdate_MasksThePostImageFileInItsTurn()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        await table.UpdateAsync(TwoAndThree, Upper);

        await table.UpdateAsync(Ex.Equal("id", LiteralValue.Of(2L)), b => SetValues(b, v => v + "!"));

        Assert.Equal([(1, "a"), (2, "B!"), (3, "C"), (4, "d"), (5, "e")], await ReadAsync(table));
        Assert.Equal(2, table.CurrentSnapshot.ActiveFiles.Values.Count(f => f.DeletionVector is not null));
    }

    [Fact]
    public async Task CopyOnWrite_RewritesTheFileEvenWithDeletionVectors()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        var original = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        await table.UpdateAsync(TwoAndThree, Upper, mode: RowDeleteMode.CopyOnWrite);

        Assert.Equal(Updated, await ReadAsync(table));
        Assert.DoesNotContain(table.CurrentSnapshot.ActiveFiles.Values, f => f.Path == original);
        Assert.All(table.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
        var committed = await LastMetricsAsync(table);
        Assert.Equal("3", committed["numCopiedRows"]);
        Assert.Equal("1", committed["numRemovedFiles"]);
    }

    [Fact]
    public async Task WithoutDeletionVectors_TheTableIsRewritten()
    {
        await using var table = await CreateAsync(deletionVectors: false);
        await table.WriteAsync([Five]);

        await table.UpdateAsync(TwoAndThree, Upper);

        Assert.Equal(Updated, await ReadAsync(table));
        Assert.All(table.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
        Assert.Equal("3", (await LastMetricsAsync(table))["numCopiedRows"]);
    }

    [Fact]
    public async Task DeletionVectorsAskedForOnATableWithoutThem_AreRefusedAndWriteNothing()
    {
        await using var table = await CreateAsync(deletionVectors: false);
        // Two files, so that the refusal on the second takes back the post-images written for the first.
        await table.WriteAsync([Rows((1, "a"), (2, "b"))]);
        await table.WriteAsync([Rows((3, "c"), (4, "d"), (5, "e"))]);
        long version = table.CurrentSnapshot.Version;
        int parquet = FileCount("*.parquet");

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            table.UpdateAsync(TwoAndThree, Upper, mode: RowDeleteMode.DeletionVector).AsTask());

        Assert.Equal(version, table.CurrentSnapshot.Version);
        Assert.Equal(parquet, FileCount("*.parquet"));
        Assert.Equal(0, FileCount("*.bin"));
        Assert.Equal([(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], await ReadAsync(table));
    }

    [Fact]
    public async Task DeletionVectorsAskedForOnATableWithoutThem_StillRemoveAWholeFile()
    {
        // No vector is needed when every live row of the file is updated, as for a DELETE.
        await using var table = await CreateAsync(deletionVectors: false);
        await table.WriteAsync([Rows((2, "b"), (3, "c"))]);

        await table.UpdateAsync(TwoAndThree, Upper, mode: RowDeleteMode.DeletionVector);

        Assert.Equal([(2, "B"), (3, "C")], await ReadAsync(table));
    }

    [Fact]
    public async Task OnARowTrackingTable_EveryRowKeepsItsStableId()
    {
        await using var table = await CreateAsync(rowTracking: true);
        await table.WriteAsync([Five]);
        var before = await RowTrackingAsync(table);

        await table.UpdateAsync(TwoAndThree, Upper);

        var after = await RowTrackingAsync(table);
        long updateVersion = table.CurrentSnapshot.Version;
        foreach (long id in new long[] { 1, 2, 3, 4, 5 })
            Assert.Equal(before[id].RowId, after[id].RowId);
        // The updated rows' commit version advances; an untouched one keeps its own.
        Assert.Equal(updateVersion, after[2].Version);
        Assert.Equal(updateVersion, after[3].Version);
        Assert.Equal(before[1].Version, after[1].Version);
        Assert.NotNull(table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.DeletionVector is not null));
    }

    private static async Task<Dictionary<long, (long RowId, long Version)>> RowTrackingAsync(DeltaTable table)
    {
        var result = new Dictionary<long, (long, long)>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var ids = (Int64Array)batch.Column("id");
            var rowIds = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            var versions = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowCommitVersionColumnName));
            for (int i = 0; i < batch.Length; i++)
                result[ids.GetValue(i)!.Value] = (rowIds.GetValue(i)!.Value, versions.GetValue(i)!.Value);
        }
        return result;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheChangeFeed_IsTheSameAsACopyOnWriteUpdatesFeed(bool rowTracking)
    {
        await using var table = await CreateAsync(cdf: true, rowTracking: rowTracking);
        await table.WriteAsync([Five]);

        var (_, version) = await table.UpdateAsync(TwoAndThree, Upper);

        Assert.NotNull(table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.DeletionVector is not null));
        var changes = new List<(string Type, long Id, string Value)>();
        await foreach (var batch in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var ids = (Int64Array)batch.Column("id");
            var values = (StringArray)batch.Column("value");
            var types = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                changes.Add((types.GetString(i), ids.GetValue(i)!.Value, values.GetString(i)));
        }

        Assert.Equal(
            [
                ("update_postimage", 2L, "B"), ("update_postimage", 3L, "C"),
                ("update_preimage", 2L, "b"), ("update_preimage", 3L, "c"),
            ],
            changes.OrderBy(c => c.Type, StringComparer.Ordinal).ThenBy(c => c.Id).ToList());
    }

    [Fact]
    public async Task OnAPartitionedTable_AnUpdateThatChangesThePartitionMovesOnlyThatRow()
    {
        await using var table = await CreateAsync(partitionColumns: ["value"]);
        await table.WriteAsync([Rows((1, "p"), (2, "p"), (3, "p"))]);
        var original = table.CurrentSnapshot.ActiveFiles.Values.Single();

        await table.UpdateAsync(Ex.Equal("id", LiteralValue.Of(2L)), b => SetValues(b, _ => "q"));

        Assert.Equal([(1, "p"), (2, "q"), (3, "p")], await ReadAsync(table));
        Assert.Equal(1, FileAt(table, original.Path).DeletionVector!.Cardinality);
        Assert.Equal("q", table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.Path != original.Path).PartitionValues["value"]);
    }

    [Fact]
    public async Task AnUpdateMatchingNothing_WritesNoVector()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        long version = table.CurrentSnapshot.Version;

        var (rows, committed) = await table.UpdateAsync(Ex.Equal("id", LiteralValue.Of(42L)), Upper);

        Assert.Equal(0, rows);
        Assert.Equal(version, committed);
        Assert.All(table.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
        Assert.Equal(0, FileCount("*.bin"));
    }

    [Fact]
    public async Task InATransaction_ASecondOperationOnAFileTheUpdateMasked_IsRefused()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);

        await using var txn = table.StartTransaction();
        await txn.UpdateAsync(TwoAndThree, Upper);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            txn.DeleteAsync(Ex.Equal("id", LiteralValue.Of(4L))).AsTask());
    }

    [Fact]
    public async Task InATransaction_TheModeIsHonoured()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        var original = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        await using (var txn = table.StartTransaction())
        {
            await txn.UpdateAsync(TwoAndThree, Upper, mode: RowDeleteMode.CopyOnWrite);
            await txn.CommitAsync();
        }

        Assert.Equal(Updated, await ReadAsync(table));
        Assert.DoesNotContain(table.CurrentSnapshot.ActiveFiles.Values, f => f.Path == original);
        Assert.All(table.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
    }

    [Fact]
    public async Task AnAbortedUpdate_TakesBackItsVectorAndPostImages()
    {
        await using var table = await CreateAsync();
        table.DeletionVectorInlineThreshold = 0; // a vector in a file of its own, for the abort to take back
        await table.WriteAsync([Five]);
        int parquet = FileCount("*.parquet");

        await using (var txn = table.StartTransaction())
        {
            await txn.UpdateAsync(TwoAndThree, Upper);
            Assert.Equal(1, FileCount("*.bin"));
            Assert.Equal(parquet + 1, FileCount("*.parquet"));
            await txn.AbortAsync();
        }

        Assert.Equal(parquet, FileCount("*.parquet"));
        Assert.Equal(0, FileCount("*.bin"));
        Assert.Equal([(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], await ReadAsync(table));
    }

    [Fact]
    public async Task AConcurrentDeleteOfAnotherRowOfTheMaskedFile_Conflicts()
    {
        // An update cannot be rebased row by row: had the concurrent delete removed an updated row, a union of the
        // vectors would bring its post-image back. So the UPDATE conflicts on the file, as a rewrite does.
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);

        await using var txn = table.StartTransaction();
        await txn.UpdateAsync(TwoAndThree, Upper);
        await table.DeleteAsync(Ex.Equal("id", LiteralValue.Of(4L)));

        await Assert.ThrowsAnyAsync<DeltaConflictException>(() => txn.CommitAsync().AsTask());
        Assert.Equal([(1, "a"), (2, "b"), (3, "c"), (5, "e")], await ReadAsync(table));
    }

    [Fact]
    public async Task ALosingDeleteOfARowTheUpdateMasked_Conflicts()
    {
        // The delete rebases by uniting vectors, and its row is already in the winner's: the row lives on as the
        // UPDATE's post-image, so uniting would silently drop the delete.
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);

        await using var txn = table.StartTransaction();
        await txn.DeleteAsync(Ex.Equal("id", LiteralValue.Of(2L)));
        await table.UpdateAsync(TwoAndThree, Upper);

        await Assert.ThrowsAnyAsync<DeltaConflictException>(() => txn.CommitAsync().AsTask());
        Assert.Equal(Updated, await ReadAsync(table));
    }

    [Fact]
    public async Task ALosingDeleteOfAnotherRowOfTheMaskedFile_Lands()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);

        await using (var txn = table.StartTransaction())
        {
            await txn.DeleteAsync(Ex.Equal("id", LiteralValue.Of(4L)));
            await table.UpdateAsync(TwoAndThree, Upper);
            await txn.CommitAsync();
        }

        Assert.Equal([(1, "a"), (2, "B"), (3, "C"), (5, "e")], await ReadAsync(table));
    }

    [Fact]
    public async Task AConcurrentAppend_IsRebasedPast()
    {
        // A blind append touches no file the UPDATE read: the auto-committing UPDATE rebases and lands.
        await using var table = await CreateAsync(rowTracking: true);
        await table.WriteAsync([Five]);
        await using var other = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        bool appended = false;
        await table.UpdateAsync(TwoAndThree, batch =>
        {
            if (!appended)
            {
                appended = true;
                other.WriteAsync([Rows((6, "f"))]).AsTask().GetAwaiter().GetResult();
            }
            return Upper(batch);
        });

        Assert.Equal([(1, "a"), (2, "B"), (3, "C"), (4, "d"), (5, "e"), (6, "f")], await ReadAsync(table));
        var ids = await RowTrackingAsync(table);
        Assert.Equal(6, ids.Values.Select(v => v.RowId).Distinct().Count());
    }
}
