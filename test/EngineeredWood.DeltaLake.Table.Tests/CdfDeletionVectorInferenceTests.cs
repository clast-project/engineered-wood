// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The feed of a version that carries no change file is inferred from its adds and removes. A deletion-vector
/// DELETE commits <c>remove</c>(file, old vector) + <c>add</c>(the SAME file, new vector), and read side by side
/// that pair reported every survivor deleted and re-inserted (#529): <c>delete 1–5, insert 1,2,4,5</c> for a
/// delete of row 3. What changed is the difference between the two vectors, which is how Spark's reader
/// reports it (<c>CDCReader.processDeletionVectorActions</c>). Spark's vector DML writes no change file, so any
/// such commit from Spark depends on this.
/// </summary>
public class CdfDeletionVectorInferenceTests : IDisposable
{
    private readonly string _tempDir;

    public CdfDeletionVectorInferenceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdfdv_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append("v" + (startId + i));
        }
        return new RecordBatch(BuildSchema(), [ids.Build(), values.Build()], count);
    }

    private Task<DeltaTable> CreateAsync(bool rowTracking = false) => DeltaTable.CreateAsync(
        Fs, BuildSchema(), enableDeletionVectors: true, enableRowTracking: rowTracking,
        configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" }).AsTask();

    private static async Task<Dictionary<long, (string Path, long Position)>> LocateRowsAsync(DeltaTable table)
    {
        var ordered = table.CurrentSnapshot.ActiveFiles.Values
            .Select(a => a.Path).OrderBy(p => p, StringComparer.Ordinal).ToList();
        var located = new Dictionary<long, (string, long)>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }))
        {
            var ids = (Int64Array)batch.Column("id");
            var rids = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
            {
                long rid = rids.GetValue(i)!.Value;
                located[ids.GetValue(i)!.Value] =
                    (ordered[TransientRowAddress.FileOrdinal(rid)], TransientRowAddress.Position(rid));
            }
        }
        return located;
    }

    private static RowSelection Sel(params (string Path, long Position)[] rows)
    {
        var byPath = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
        foreach (var (path, position) in rows)
        {
            if (!byPath.TryGetValue(path, out var set))
                byPath[path] = set = new HashSet<long>();
            ((HashSet<long>)set).Add(position);
        }
        return RowSelection.ByPath(byPath);
    }

    private async Task<List<(long Id, string Type, string? Value)>> ReadFeedAsync(long version)
    {
        await using var check = await OpenAsync();
        var changes = new List<(long, string, string?)>();
        await foreach (var batch in check.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var ids = (Int64Array)batch.Column("id");
            var values = (StringArray)batch.Column("value");
            var types = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                changes.Add((ids.GetValue(i)!.Value, types.GetString(i), values.GetString(i)));
        }
        return changes.OrderBy(c => c.Item2, StringComparer.Ordinal).ThenBy(c => c.Item1).ToList();
    }

    private async Task<long> StageVectorDeleteAsync(DeltaTable table, params long[] ids)
    {
        var at = await LocateRowsAsync(table);
        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel([.. ids.Select(id => at[id])]));
        return await txn.CommitAsync();
    }

    private async Task AssertNoChangeFileAsync(long version)
    {
        var actions = await new TransactionLog(Fs).ReadCommitAsync(version);
        Assert.DoesNotContain(actions, a => a is CdcFile); // else the version is read cdc-only, not inferred
    }

    [Fact]
    public async Task AVectorDelete_ReportsOnlyTheDeletedRow()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Batch(1, 5)]);

        long version = await StageVectorDeleteAsync(table, 3);
        await AssertNoChangeFileAsync(version);

        // Was: delete 1, 2, 3, 4, 5 and insert 1, 2, 4, 5.
        Assert.Equal([(3L, "delete", (string?)"v3")], await ReadFeedAsync(version));
    }

    [Fact]
    public async Task ASecondVectorDelete_ReportsOnlyTheRowsTheOldVectorDidNotHide()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Batch(1, 5)]);
        await StageVectorDeleteAsync(table, 2);

        long version = await StageVectorDeleteAsync(table, 4, 5);
        await AssertNoChangeFileAsync(version);

        Assert.Equal(
            [(4L, "delete", (string?)"v4"), (5L, "delete", "v5")],
            await ReadFeedAsync(version));
    }

    [Fact]
    public async Task OtherFilesInTheVersion_AreStillReadWhole()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Batch(1, 3)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[2]));
        txn.StageDataFiles(await table.WriteDataFilesAsync([Batch(10, 2)]));
        long version = await txn.CommitAsync();
        await AssertNoChangeFileAsync(version);

        Assert.Equal(
            [(2L, "delete", (string?)"v2"), (10L, "insert", "v10"), (11L, "insert", "v11")],
            await ReadFeedAsync(version));
    }

    /// <summary>The other direction of the diff: a pair whose new vector hides FEWER rows than the old, as a
    /// RESTORE to before a vector delete commits, re-inserts the rows it un-hides and deletes nothing.</summary>
    [Fact]
    public async Task AVectorThatUnhidesRows_ReportsThemAsInserts()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Batch(1, 5)]);
        await StageVectorDeleteAsync(table, 2, 4);

        var hidden = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.NotNull(hidden.DeletionVector);
        var txn = table.StartTransaction();
        txn.StageActions(
        [
            new RemoveFile
            {
                Path = hidden.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                DeletionVector = hidden.DeletionVector,
                PartitionValues = hidden.PartitionValues,
                Size = hidden.Size,
            },
            hidden with { DeletionVector = null, DataChange = true },
        ]);
        long version = await txn.CommitAsync();
        await AssertNoChangeFileAsync(version);

        Assert.Equal(
            [(2L, "insert", (string?)"v2"), (4L, "insert", "v4")],
            await ReadFeedAsync(version));
    }

    [Fact]
    public async Task OnARowTrackingTable_TheDeletedRowKeepsItsId()
    {
        await using var table = await CreateAsync(rowTracking: true);
        await table.WriteAsync([Batch(1, 5)]);

        var rowIds = new Dictionary<long, long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var id = (Int64Array)batch.Column("id");
            var rid = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
                rowIds[id.GetValue(i)!.Value] = rid.GetValue(i)!.Value;
        }

        long version = await StageVectorDeleteAsync(table, 4);
        await AssertNoChangeFileAsync(version);

        await using var check = await OpenAsync();
        var feed = new List<(long Id, long? RowId, string Type)>();
        await foreach (var batch in check.ReadChangesAsync(new DeltaChangeReadOptions
                       {
                           StartVersion = version, EndVersion = version, Metadata = DeltaRowMetadata.RowTracking,
                       }))
        {
            var id = (Int64Array)batch.Column("id");
            var rid = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            var types = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                feed.Add((id.GetValue(i)!.Value, rid.GetValue(i), types.GetString(i)));
        }

        Assert.Equal([(4L, (long?)rowIds[4], "delete")], feed);
    }

    /// <summary>A delete read from a vector edit takes its partition values from the remove, which EW's vector
    /// writers once left off (the protocol makes them optional on a remove) — so the paired add's are the
    /// fallback, and a non-nullable partition column came back NULL or threw.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task OnAPartitionedTable_TheDeletedRowKeepsItsPartitionValue(bool removeCarriesPartitionValues)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            Fs, schema, partitionColumns: ["region"], enableDeletionVectors: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
        var ids = new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build();
        var regions = new StringArray.Builder().AppendRange(["eu", "eu", "eu"]).Build();
        await table.WriteAsync([new RecordBatch(schema, [ids, regions], 3)]);

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        var txn = table.StartTransaction();
        if (removeCarriesPartitionValues)
        {
            await txn.StageRowDeletesAsync(Sel((file.Path, 1)));
        }
        else
        {
            // What an older writer commits: the same edit, with the remove's partition values left off.
            var (staged, _) = await table.ComputeDeletionVectorActionsAsync(
                new Dictionary<int, IReadOnlyCollection<long>> { [0] = [1L] });
            txn.StageActions([.. staged.Select(a => a is RemoveFile r ? r with { ExtendedFileMetadata = null, PartitionValues = null, Size = null } : a)]);
        }
        long version = await txn.CommitAsync();
        await AssertNoChangeFileAsync(version);

        var remove = Assert.Single((await new TransactionLog(Fs).ReadCommitAsync(version)).OfType<RemoveFile>());
        if (removeCarriesPartitionValues)
        {
            // Declared, as on every other remove EW writes: the flag is what tells a reader the map is there.
            Assert.True(remove.ExtendedFileMetadata);
            Assert.Equal("eu", remove.PartitionValues?["region"]);
            Assert.Equal(file.Size, remove.Size);
        }
        else
            Assert.Null(remove.PartitionValues);

        await using var check = await OpenAsync();
        var feed = new List<(long, string?, string)>();
        await foreach (var batch in check.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var id = (Int64Array)batch.Column("id");
            var region = (StringArray)batch.Column("region");
            var types = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                feed.Add((id.GetValue(i)!.Value, region.GetString(i), types.GetString(i)));
        }
        Assert.Equal([(2L, (string?)"eu", "delete")], feed);
    }

    /// <summary>VACUUM removes a data file together with its vectors. A version naming such a file is skipped,
    /// as it was before vector edits were paired — reading the vector first threw instead.</summary>
    [Fact]
    public async Task AVacuumedFile_IsSkippedRatherThanThrowing()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Batch(1, 3)]);
        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        string original = file.Path;

        // A vector this small is written inline; force a file, since that is what VACUUM can delete.
        var vector = await new DeletionVectorWriter(Fs) { InlineThreshold = 0 }.CreateAsync([1L], 1);
        Assert.Equal("u", vector.StorageType);
        var txn = table.StartTransaction();
        txn.StageActions(
        [
            new RemoveFile
            {
                Path = file.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                PartitionValues = file.PartitionValues,
                Size = file.Size,
            },
            file with { DeletionVector = vector, DataChange = true },
        ]);
        long vectorDelete = await txn.CommitAsync();
        // Removes the vector-carrying file, unpaired.
        long overwrite = await table.WriteAsync([Batch(10, 1)], DeltaWriteMode.Overwrite);

        File.Delete(Path.Combine(_tempDir, DeltaPath.Decode(original)));
        var vectors = Directory.GetFiles(_tempDir, "deletion_vector_*.bin");
        Assert.NotEmpty(vectors);
        foreach (var vectorFile in vectors)
            File.Delete(vectorFile);

        Assert.Empty(await ReadFeedAsync(vectorDelete));
        Assert.Equal([(10L, "insert", (string?)"v10")], await ReadFeedAsync(overwrite));
    }
}
