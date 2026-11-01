// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// RESTORE (#567), as Spark 4.0.3 / Delta 4.0.0 does it (measured, and pinned against Spark by
/// <see cref="Interop.RestoreInteropTests"/>): a new commit diffing the active files by path and deletion vector,
/// the target's metadata with nothing moving backwards, the protocol never downgraded, row ids kept, a missing file
/// refused unless the check is turned off, and no commit at all for the current version.
/// </summary>
public class RestoreTests : IDisposable
{
    private readonly string _tempDir;

    public RestoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_restore_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Field(new Field("v", StringType.Default, true))
        .Build();

    private static RecordBatch Rows(params long[] ids)
    {
        var id = new Int64Array.Builder();
        var v = new StringArray.Builder();
        foreach (long i in ids)
        {
            id.Append(i);
            v.Append($"v{i}");
        }
        return new RecordBatch(TableSchema, [id.Build(), v.Build()], ids.Length);
    }

    private static Func<RecordBatch, BooleanArray> IdIs(params long[] values) => batch =>
    {
        var ids = new HashSet<long>(values);
        var column = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            mask.Append(column.GetValue(i) is { } value && ids.Contains(value));
        return mask.Build();
    };

    private Task<DeltaTable> CreateAsync(
        bool deletionVectors = true, bool rowTracking = false, IReadOnlyList<string>? clustering = null,
        IReadOnlyDictionary<string, string>? configuration = null) =>
        DeltaTable.CreateAsync(
            Fs, TableSchema, new DeltaTableOptions { CheckpointInterval = 0 },
            clusteringColumns: clustering, enableDeletionVectors: deletionVectors, enableRowTracking: rowTracking,
            configuration: configuration).AsTask();

    private static async Task<List<string>> ReadAsync(DeltaTable table, long? version = null)
    {
        var rows = new List<string>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { AtVersion = version }))
        {
            var id = (Int64Array)batch.Column("id");
            var v = (StringArray)batch.Column("v");
            for (int i = 0; i < batch.Length; i++)
                rows.Add($"{id.GetValue(i)}:{v.GetString(i)}");
        }
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private async Task<List<JsonElement>> CommitAsync(long version)
    {
        await Task.Yield();
        string text = File.ReadAllText(Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json"));
        return text.Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(line => JsonDocument.Parse(line).RootElement.Clone())
            .ToList();
    }

    private static IEnumerable<JsonElement> Of(IEnumerable<JsonElement> actions, string kind) =>
        actions.Where(a => a.TryGetProperty(kind, out _)).Select(a => a.GetProperty(kind));

    private static async Task<DeltaFormatException> RefusedAsync(string code, Func<Task> action)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(action);
        Assert.Equal(code, ex.ErrorCode);
        return ex;
    }

    /// <summary>
    /// Across appends, a deletion-vector DELETE and an UPDATE, the restored table reads exactly as the target
    /// version did, and the commit is Spark's: op RESTORE, the version parameter, the six metrics, every add with
    /// dataChange=true and its original statistics, every remove carrying the deletion vector it was live under.
    /// </summary>
    [Fact]
    public async Task Restore_UndoesDmlAndRecordsSparksCommit()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows(1, 2, 3)]);                                         // v1
        await table.WriteAsync([Rows(4, 5)]);                                            // v2
        await table.DeleteAsync(IdIs(2), RowDeleteMode.DeletionVector);                  // v3
        await table.UpdateAsync(IdIs(4), b => b);                                        // v4
        var expected = await ReadAsync(table, 2);

        var result = await table.RestoreAsync(2);

        Assert.True(result.Committed);
        Assert.Equal(5, result.Version);
        Assert.Equal(2, result.RestoredVersion);
        Assert.Equal(expected, await ReadAsync(table));
        Assert.Equal(2, result.NumOfFilesAfterRestore);

        var actions = await CommitAsync(5);
        var commitInfo = Of(actions, "commitInfo").Single();
        Assert.Equal("RESTORE", commitInfo.GetProperty("operation").GetString());
        Assert.False(commitInfo.GetProperty("isBlindAppend").GetBoolean());
        Assert.Equal("2", commitInfo.GetProperty("operationParameters").GetProperty("version").GetString());
        var metrics = commitInfo.GetProperty("operationMetrics");
        foreach (string metric in new[]
                 {
                     "numRestoredFiles", "numRemovedFiles", "restoredFilesSize", "removedFilesSize",
                     "numOfFilesAfterRestore", "tableSizeAfterRestore",
                 })
        {
            Assert.True(metrics.TryGetProperty(metric, out _), metric);
        }
        Assert.Equal(result.NumRestoredFiles.ToString(), metrics.GetProperty("numRestoredFiles").GetString());
        Assert.Equal(result.NumRemovedFiles.ToString(), metrics.GetProperty("numRemovedFiles").GetString());

        Assert.Single(Of(actions, "metaData"));
        var adds = Of(actions, "add").ToList();
        Assert.Equal(result.NumRestoredFiles, adds.Count);
        Assert.All(adds, add =>
        {
            Assert.True(add.GetProperty("dataChange").GetBoolean());
            Assert.True(add.TryGetProperty("stats", out _));
            Assert.False(add.TryGetProperty("deletionVector", out _)); // v2 had none
        });
        var removes = Of(actions, "remove").ToList();
        Assert.Equal(result.NumRemovedFiles, removes.Count);
        Assert.All(removes, r => Assert.True(r.GetProperty("dataChange").GetBoolean()));
        Assert.Contains(removes, r => r.TryGetProperty("deletionVector", out _));
        Assert.Empty(Of(actions, "cdc"));
    }

    /// <summary>
    /// A file whose deletion vector is all that changed is removed under its current vector and re-added under the
    /// target's, as Spark keys the diff: one restored file, one removed, the same path.
    /// </summary>
    [Fact]
    public async Task Restore_WhenOnlyADeletionVectorChanged_SwapsTheVector()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows(1, 2, 3)]);                                         // v1
        await table.DeleteAsync(IdIs(1), RowDeleteMode.DeletionVector);                  // v2
        await table.DeleteAsync(IdIs(2), RowDeleteMode.DeletionVector);                  // v3

        var result = await table.RestoreAsync(2);

        Assert.Equal(1, result.NumRestoredFiles);
        Assert.Equal(1, result.NumRemovedFiles);
        Assert.Equal(["2:v2", "3:v3"], await ReadAsync(table));
        var actions = await CommitAsync(result.Version);
        var add = Of(actions, "add").Single();
        var remove = Of(actions, "remove").Single();
        Assert.Equal(add.GetProperty("path").GetString(), remove.GetProperty("path").GetString());
        Assert.Equal(1, add.GetProperty("deletionVector").GetProperty("cardinality").GetInt64());
        Assert.Equal(2, remove.GetProperty("deletionVector").GetProperty("cardinality").GetInt64());

        // And back to before any delete: the add carries no vector at all.
        var none = await table.RestoreAsync(1);
        Assert.Equal(["1:v1", "2:v2", "3:v3"], await ReadAsync(table));
        Assert.False(Of(await CommitAsync(none.Version), "add").Single().TryGetProperty("deletionVector", out _));
    }

    /// <summary>
    /// Restoring to the current version commits nothing; restoring to an older version with the same files still
    /// commits, as Spark does, with zero file metrics.
    /// </summary>
    [Fact]
    public async Task Restore_ToTheCurrentVersion_CommitsNothing()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows(1)]);                                               // v1
        await table.WriteAsync([Rows(2)]);                                               // v2
        await table.RestoreAsync(1);                                                     // v3

        var same = await table.RestoreAsync(3);
        Assert.False(same.Committed);
        Assert.Equal(3, same.Version);
        Assert.Equal(3, table.CurrentSnapshot.Version);

        var equal = await table.RestoreAsync(1);
        Assert.True(equal.Committed);
        Assert.Equal(4, equal.Version);
        Assert.Equal(0, equal.NumRestoredFiles);
        Assert.Equal(0, equal.NumRemovedFiles);
    }

    /// <summary>A version past the latest, or negative, is refused with Spark's message.</summary>
    [Fact]
    public async Task Restore_ToAVersionThatDoesNotExist_IsRefused()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows(1)]);

        var ex = await RefusedAsync(
            DeltaTableErrorCodes.CannotRestoreTableVersion, async () => await table.RestoreAsync(9));
        Assert.Contains("Available versions: [0, 1]", ex.Message);
        await RefusedAsync(DeltaTableErrorCodes.CannotRestoreTableVersion, async () => await table.RestoreAsync(-1));
        Assert.Equal(1, table.CurrentSnapshot.Version);
    }

    /// <summary>
    /// The target's schema and properties come back (a column added since is gone, a constraint added since is
    /// dropped), but the protocol keeps what was added since: never downgraded.
    /// </summary>
    [Fact]
    public async Task Restore_RestoresSchemaAndPropertiesButNeverDowngradesTheProtocol()
    {
        await using var table = await CreateAsync(deletionVectors: false);
        await table.WriteAsync([Rows(1)]);                                               // v1
        var before = table.CurrentSnapshot.Protocol;
        await table.AddColumnAsync(new Field("extra", Int32Type.Default, true));         // v2
        await table.AddConstraintAsync("positive", "id > 0");                            // v3

        var result = await table.RestoreAsync(1);

        var snapshot = table.CurrentSnapshot;
        Assert.DoesNotContain(snapshot.Schema.Fields, f => f.Name == "extra");
        Assert.DoesNotContain(snapshot.Metadata.Configuration ?? new Dictionary<string, string>(),
            p => p.Key.StartsWith("delta.constraints.", StringComparison.Ordinal));
        Assert.True(snapshot.Protocol.MinWriterVersion >= 3);
        Assert.True(snapshot.Protocol.MinWriterVersion > before.MinWriterVersion);
        Assert.Equal(["1:v1"], await ReadAsync(table));
        Assert.Equal(0, result.NumRemovedFiles);
    }

    /// <summary>
    /// A file VACUUM deleted makes the restore refuse, naming it. With <see cref="RestoreOptions.IgnoreMissingFiles"/>
    /// the check is skipped and the file is still re-added, as Spark and delta-rs do.
    /// </summary>
    [Fact]
    public async Task Restore_OfAVacuumedFile_IsRefusedUnlessTheCheckIsSkipped()
    {
        await using var table = await CreateAsync(deletionVectors: false);
        await table.WriteAsync([Rows(1)]);                                               // v1
        await table.WriteAsync([Rows(2)]);                                               // v2
        await table.DeleteAsync(IdIs(1));                                                // v3
        string vacuumed = (await table.GetSnapshotAtVersionAsync(2)).ActiveFiles.Values
            .Select(f => f.Path).Except(table.CurrentSnapshot.ActiveFiles.Values.Select(f => f.Path)).Single();
        File.Delete(Path.Combine(_tempDir, vacuumed));

        var ex = await RefusedAsync(
            DeltaTableErrorCodes.RestoreMissingFiles, async () => await table.RestoreAsync(2));
        Assert.Contains(vacuumed, ex.Message);
        Assert.Contains("Not all files from version 2 are available", ex.Message);
        Assert.Equal(3, table.CurrentSnapshot.Version);

        var result = await table.RestoreAsync(2, new RestoreOptions { IgnoreMissingFiles = true });
        Assert.Equal(1, result.NumRestoredFiles);
        Assert.Contains(table.CurrentSnapshot.ActiveFiles.Values, f => f.Path == vacuumed);
    }

    /// <summary>A missing deletion-vector file is refused too: Spark does not check it, and the restored table
    /// would then fail every read.</summary>
    [Fact]
    public async Task Restore_ToAVersionWhoseDeletionVectorIsGone_IsRefused()
    {
        await using var table = await CreateAsync();
        // Every other row of 5000, so the vector is too large to inline and gets a file of its own.
        long[] ids = Enumerable.Range(0, 5000).Select(i => (long)i).ToArray();
        await table.WriteAsync([Rows(ids)]);                                             // v1
        await table.DeleteAsync(IdIs(ids.Where(i => i % 2 == 1).ToArray()), RowDeleteMode.DeletionVector); // v2
        string dv = Directory.GetFiles(_tempDir, "deletion_vector_*.bin", SearchOption.AllDirectories).Single();
        await table.DeleteAsync(IdIs(0), RowDeleteMode.DeletionVector);                  // v3
        File.Delete(dv);

        var ex = await RefusedAsync(
            DeltaTableErrorCodes.RestoreMissingFiles, async () => await table.RestoreAsync(2));
        Assert.Contains(Path.GetFileName(dv), ex.Message);
    }

    /// <summary>
    /// Restored files keep their baseRowId and defaultRowCommitVersion, so every row keeps its id; the high-water
    /// mark does not move back, and is not re-emitted when nothing new was assigned. Across an OPTIMIZE.
    /// </summary>
    [Fact]
    public async Task Restore_KeepsRowIdsAndNeverLowersTheHighWaterMark()
    {
        await using var table = await CreateAsync(deletionVectors: false, rowTracking: true);
        await table.WriteAsync([Rows(1, 2)]);                                            // v1
        await table.WriteAsync([Rows(3)]);                                               // v2
        var target = await table.GetSnapshotAtVersionAsync(2);
        await table.UpdateAsync(IdIs(3), b => b);                                        // v3
        await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue });
        long highWaterMark = table.CurrentSnapshot.RowIdHighWaterMark;

        var result = await table.RestoreAsync(2);

        var restored = table.CurrentSnapshot.ActiveFiles.Values.ToDictionary(f => f.Path);
        foreach (var file in target.ActiveFiles.Values)
        {
            Assert.Equal(file.BaseRowId, restored[file.Path].BaseRowId);
            Assert.Equal(file.DefaultRowCommitVersion, restored[file.Path].DefaultRowCommitVersion);
        }
        Assert.Equal(highWaterMark, table.CurrentSnapshot.RowIdHighWaterMark);
        Assert.Empty(Of(await CommitAsync(result.Version), "domainMetadata"));
    }

    /// <summary>
    /// A file from before row tracking was enabled has no row ids, and is given fresh ones past the high-water mark,
    /// with the restore as its row commit version, as Spark's commit assigns them.
    /// </summary>
    [Fact]
    public async Task Restore_AssignsFreshRowIdsToAFileThatHadNone()
    {
        var fs = Fs;
        var log = new TransactionLog(fs);
        await using (var table = await CreateAsync(deletionVectors: false))
            await table.WriteAsync([Rows(1, 2)]);                                         // v1, no row ids
        await using (var table = await DeltaTable.OpenAsync(fs))
        {
            var snapshot = table.CurrentSnapshot;
            var configuration = (snapshot.Metadata.Configuration ?? new Dictionary<string, string>())
                .ToDictionary(p => p.Key, p => p.Value);
            configuration["delta.enableRowTracking"] = "true";
            configuration[DeltaLake.RowTracking.RowTrackingConfig.MaterializedRowIdColumnNameKey] = "_row_id_x";
            configuration[DeltaLake.RowTracking.RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey] = "_row_cv_x";
            await log.WriteCommitAsync(2,
            [
                new ProtocolAction
                {
                    MinReaderVersion = 1,
                    MinWriterVersion = 7,
                    WriterFeatures = ["domainMetadata", "rowTracking", "appendOnly", "invariants"],
                },
                snapshot.Metadata with { Configuration = configuration },
            ]);                                                                           // v2
        }
        await using var tracked = await DeltaTable.OpenAsync(fs);
        await tracked.WriteAsync([Rows(3)]);                                             // v3, ids from 0
        await tracked.DeleteAsync(IdIs(1));                                              // v4
        long mark = tracked.CurrentSnapshot.RowIdHighWaterMark;

        var result = await tracked.RestoreAsync(1);

        var add = tracked.CurrentSnapshot.ActiveFiles.Values.Single();
        Assert.Equal(mark, add.BaseRowId);
        Assert.Equal(result.Version, add.DefaultRowCommitVersion);
        Assert.Equal(mark + 2, tracked.CurrentSnapshot.RowIdHighWaterMark);
        // The target lacked the materialized column names; they are kept, as Spark keeps them.
        Assert.Equal("_row_id_x", tracked.CurrentSnapshot.Metadata.Configuration![
            DeltaLake.RowTracking.RowTrackingConfig.MaterializedRowIdColumnNameKey]);
    }

    /// <summary>delta.appendOnly does not stop a RESTORE removing files, as in Spark, whose RESTORE commit skips
    /// the check.</summary>
    [Fact]
    public async Task Restore_OnAnAppendOnlyTable_StillRemovesFiles()
    {
        await using var table = await CreateAsync(
            deletionVectors: false, configuration: new Dictionary<string, string> { ["delta.appendOnly"] = "true" });
        await table.WriteAsync([Rows(1)]);
        await table.WriteAsync([Rows(2)]);

        var result = await table.RestoreAsync(1);

        Assert.Equal(1, result.NumRemovedFiles);
        Assert.Equal(["1:v1"], await ReadAsync(table));
    }

    /// <summary>
    /// The restore commits no change data; the feed derives the changes from its adds and removes, as Spark's does:
    /// the rows the restore brings back as inserts, the rows it drops as deletes.
    /// </summary>
    [Fact]
    public async Task Restore_ChangeFeedReportsTheDifference()
    {
        await using var table = await CreateAsync(
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
        await table.WriteAsync([Rows(1, 2)]);                                            // v1
        await table.WriteAsync([Rows(3)]);                                               // v2
        await table.DeleteAsync(IdIs(1), RowDeleteMode.DeletionVector);                  // v3

        var result = await table.RestoreAsync(1);

        Assert.Empty(Of(await CommitAsync(result.Version), "cdc"));
        var changes = new List<string>();
        await foreach (var batch in table.ReadChangesAsync(
                           new DeltaChangeReadOptions { StartVersion = result.Version, EndVersion = result.Version }))
        {
            var id = (Int64Array)batch.Column("id");
            var type = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                changes.Add($"{type.GetString(i)}:{id.GetValue(i)}");
        }
        changes.Sort(StringComparer.Ordinal);
        Assert.Equal(["delete:3", "insert:1"], changes);
    }

    /// <summary>Liquid clustering's columns are restored with the rest; it is the only domain Spark restores.</summary>
    [Fact]
    public async Task Restore_RestoresTheClusteringColumnsAndNoOtherDomain()
    {
        await using var table = await CreateAsync(deletionVectors: false, clustering: ["id"]);
        await table.WriteAsync([Rows(1)]);                                               // v1
        var clusteringThen = table.CurrentSnapshot.DomainMetadata["delta.clustering"].Configuration;
        await table.SetClusteringColumnsAsync(["v"]);                                    // v2
        await table.SetDomainMetadataAsync("app.custom", "{\"a\":1}");                   // v3

        await table.RestoreAsync(1);

        Assert.Equal(clusteringThen, table.CurrentSnapshot.DomainMetadata["delta.clustering"].Configuration);
        Assert.True(table.CurrentSnapshot.DomainMetadata.ContainsKey("app.custom"));
    }

    /// <summary>
    /// An identity column's high-water mark does not move back with the schema, so the next write cannot reissue a
    /// value rows written since already hold (Spark's copySchemaWithMergedHighWaterMarks).
    /// </summary>
    [Fact]
    public async Task Restore_NeverLowersAnIdentityHighWaterMark()
    {
        var fs = Fs;
        var log = new TransactionLog(fs);
        string idMeta = JsonSerializer.Serialize(IdentityColumn.CreateMetadata());
        await log.WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["identityColumns"] },
            new MetadataAction
            {
                Id = "restore-identity",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":"""
                    + idMeta + """},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        ]);
        await using var table = await DeltaTable.OpenAsync(fs);
        var valueSchema = new Apache.Arrow.Schema.Builder().Field(new Field("value", StringType.Default, true)).Build();
        RecordBatch Values(params string[] values) =>
            new(valueSchema, [new StringArray.Builder().AppendRange(values).Build()], values.Length);
        await table.WriteAsync([Values("a")]);                                            // v1, id 1
        await table.WriteAsync([Values("b", "c")]);                                       // v2, ids 2-3

        await table.RestoreAsync(1);
        await table.WriteAsync([Values("d")]);

        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var id = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(id.GetValue(i)!.Value);
        }
        ids.Sort();
        Assert.Equal([1L, 4L], ids);
    }

    /// <summary>
    /// A restore by timestamp lands on the latest version committed at or before it, an exact match included,
    /// using the commit file's modification time for a commit that records no timestamp of its own, as Spark resolves
    /// every commit on a table without in-commit timestamps; one before the first commit or after the last is
    /// refused.
    /// </summary>
    [Fact]
    public async Task Restore_ByTimestamp_ResolvesAgainstCommitTimes()
    {
        await using (var setup = await CreateAsync(deletionVectors: false))
        {
            await setup.WriteAsync([Rows(1)]);                                            // v1
            await setup.WriteAsync([Rows(2)]);                                            // v2
            await setup.WriteAsync([Rows(3)]);                                            // v3
        }
        // No commitInfo, so only the files' times are left; spread apart so the resolution is unambiguous.
        for (int v = 0; v < 4; v++)
        {
            string path = Path.Combine(_tempDir, "_delta_log", $"{v:D20}.json");
            File.WriteAllLines(path, File.ReadAllLines(path).Where(l => !l.StartsWith("{\"commitInfo\"", StringComparison.Ordinal)));
            File.SetLastWriteTimeUtc(path, new DateTime(2026, 1, 1, 0, v, 0, DateTimeKind.Utc));
        }
        foreach (string crc in Directory.GetFiles(Path.Combine(_tempDir, "_delta_log"), "*.crc"))
            File.Delete(crc); // they record the commit timestamps just removed
        await using var table = await DeltaTable.OpenAsync(Fs);
        DateTimeOffset At(int minute, int seconds = 0) => new(2026, 1, 1, 0, minute, seconds, TimeSpan.Zero);

        var exact = await table.RestoreAsync(At(2));
        Assert.Equal(2, exact.RestoredVersion);
        Assert.Equal(["1:v1", "2:v2"], await ReadAsync(table));
        var parameters = Of(await CommitAsync(exact.Version), "commitInfo").Single().GetProperty("operationParameters");
        Assert.True(parameters.TryGetProperty("timestamp", out _));
        Assert.False(parameters.TryGetProperty("version", out _));

        var between = await table.RestoreAsync(At(1, 30));
        Assert.Equal(1, between.RestoredVersion);

        await RefusedAsync(DeltaTableErrorCodes.CannotRestoreTimestampEarlier,
            async () => await table.RestoreAsync(At(0).AddMilliseconds(-1)));
        await RefusedAsync(DeltaTableErrorCodes.CannotRestoreTimestampGreater,
            async () => await table.RestoreAsync(DateTimeOffset.UtcNow.AddDays(1)));
    }

    /// <summary>
    /// Any commit landing while the restore runs fails it, a blind append included: its actions are a diff against
    /// the state it read, and moving them past any other commit would leave the table at neither version. Spark's
    /// RESTORE makes one attempt for the same reason. Nothing of the restore is committed.
    /// </summary>
    [Fact]
    public async Task Restore_FailsOnAnyConcurrentCommit()
    {
        await using (var setup = await CreateAsync(deletionVectors: false))
        {
            await setup.WriteAsync([Rows(1)]);                                            // v1
            await setup.WriteAsync([Rows(2)]);                                            // v2
        }

        var racing = new RacingFileSystem(Fs, async () =>
        {
            await using var other = await DeltaTable.OpenAsync(Fs);
            await other.WriteAsync([Rows(9)]);                                            // takes v3
        });
        await using var table = await DeltaTable.OpenAsync(racing);

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await table.RestoreAsync(1));

        await using var reopened = await DeltaTable.OpenAsync(Fs);
        Assert.Equal(3, reopened.CurrentSnapshot.Version);
        Assert.Equal(["1:v1", "2:v2", "9:v9"], await ReadAsync(reopened));
    }

    /// <summary>
    /// Even a concurrent commit that changes no data fails the restore, which the conflict checker alone would let
    /// through: the restore is not rebase-safe, and the committer's refusal to rebase it is not retried.
    /// </summary>
    [Fact]
    public async Task Restore_FailsOnAConcurrentCommitThatChangesNoData()
    {
        await using (var setup = await CreateAsync(deletionVectors: false))
        {
            await setup.WriteAsync([Rows(1)]);                                            // v1
            await setup.WriteAsync([Rows(2)]);                                            // v2
            await setup.SetDomainMetadataAsync("app.first", "{}");                        // v3, declares the feature
        }

        var racing = new RacingFileSystem(Fs, async () =>
        {
            await using var other = await DeltaTable.OpenAsync(Fs);
            await other.SetDomainMetadataAsync("app.unrelated", "{}");                    // takes v4
        });
        await using var table = await DeltaTable.OpenAsync(racing);

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () => await table.RestoreAsync(1));
        Assert.Equal(DeltaErrorCodes.RebaseUnsafe, ex.ErrorCode);

        await using var reopened = await DeltaTable.OpenAsync(Fs);
        Assert.Equal(4, reopened.CurrentSnapshot.Version);
        Assert.Equal(["1:v1", "2:v2"], await ReadAsync(reopened));
    }

    private async Task<long> CommitTimestampAsync(long version) =>
        Of(await CommitAsync(version), "commitInfo").Single().GetProperty("timestamp").GetInt64();

    private async Task WriteRawAsync(long version, params DeltaAction[] actions) =>
        await new TransactionLog(Fs).WriteCommitAsync(version, actions);

    private static MetadataAction RawMetadata(IReadOnlyDictionary<string, string>? configuration = null) => new()
    {
        Id = "restore-raw",
        Format = Format.Parquet,
        SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""",
        PartitionColumns = [],
        Configuration = configuration ?? new Dictionary<string, string>(),
    };

    /// <summary>
    /// A timestamp landing on a version the log can no longer reconstruct (its predecessor's commit is gone, and the
    /// checkpoint is later) is refused as that version, not quietly moved back to an older one that can be.
    /// </summary>
    [Fact]
    public async Task Restore_ByTimestamp_OnAnUnreadableVersion_IsRefused()
    {
        await using (var table = await CreateAsync(deletionVectors: false))
        {
            for (long i = 1; i <= 5; i++)
            {
                await Task.Delay(5);
                await table.WriteAsync([Rows(i)]);                                        // v1..v5
            }
            await table.CheckpointAsync();                                                // at v5
            await Task.Delay(5);
            await table.WriteAsync([Rows(6)]);                                            // v6
        }
        long atV4 = await CommitTimestampAsync(4);
        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{3:D20}.json"));

        await using var reopened = await DeltaTable.OpenAsync(Fs);
        await RefusedAsync(DeltaTableErrorCodes.CannotRestoreTableVersion,
            async () => await reopened.RestoreAsync(DateTimeOffset.FromUnixTimeMilliseconds(atV4)));
        Assert.Equal(6, reopened.CurrentSnapshot.Version);
    }

    /// <summary>
    /// After log cleanup a version may survive only as a checkpoint: readable by version, but with no commit to carry
    /// a timestamp. As in Spark (measured: 4.0.3 / Delta 4.0.0), it takes no part in resolving a timestamp, so the
    /// earliest restorable time is the first surviving commit's and anything earlier is refused.
    /// </summary>
    [Fact]
    public async Task Restore_ByTimestamp_IgnoresACheckpointOnlyVersion()
    {
        await using (var table = await CreateAsync(deletionVectors: false))
        {
            for (long i = 1; i <= 4; i++)
            {
                await Task.Delay(5);
                await table.WriteAsync([Rows(i)]);                                        // v1..v4
            }
            await table.CheckpointAsync();                                                // at v4
            for (long i = 5; i <= 6; i++)
            {
                await Task.Delay(5);
                await table.WriteAsync([Rows(i)]);                                        // v5, v6
            }
        }
        long atV4 = await CommitTimestampAsync(4);
        long atV5 = await CommitTimestampAsync(5);
        for (int v = 0; v <= 4; v++)
            File.Delete(Path.Combine(_tempDir, "_delta_log", $"{v:D20}.json"));

        await using var reopened = await DeltaTable.OpenAsync(Fs);
        var ex = await RefusedAsync(DeltaTableErrorCodes.CannotRestoreTimestampEarlier,
            async () => await reopened.RestoreAsync(DateTimeOffset.FromUnixTimeMilliseconds(atV4)));
        Assert.Contains(DateTimeOffset.FromUnixTimeMilliseconds(atV5)
            .ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture), ex.Message);

        var result = await reopened.RestoreAsync(DateTimeOffset.FromUnixTimeMilliseconds(atV5));
        Assert.Equal(5, result.RestoredVersion);
        Assert.Equal(["1:v1", "2:v2", "3:v3", "4:v4", "5:v5"], await ReadAsync(reopened));

        // By version, the checkpoint-only version is restorable.
        Assert.Equal(4, (await reopened.RestoreAsync(4)).RestoredVersion);
    }

    /// <summary>
    /// A version that enabled UniForm cannot be restored while this library does not maintain UniForm's metadata:
    /// the restored properties would re-enable it, leaving Iceberg readers on stale metadata.
    /// </summary>
    [Fact]
    public async Task Restore_ThatReEnablesUniForm_IsRefused()
    {
        await WriteRawAsync(0,
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            RawMetadata(new Dictionary<string, string> { [Schema.UniversalFormat.EnabledFormatsKey] = "iceberg" }));
        await WriteRawAsync(1, RawMetadata());
        await using var table = await DeltaTable.OpenAsync(Fs);

        await RefusedAsync(DeltaTableErrorCodes.UniversalFormatNotMaintained, async () => await table.RestoreAsync(0));
        Assert.Equal(1, table.CurrentSnapshot.Version);
    }

    /// <summary>
    /// A restore that turns in-commit timestamps back on is where they begin again: its commit carries one, and the
    /// metadata names it as the enablement point, as Spark stamps a mid-life enablement.
    /// </summary>
    [Fact]
    public async Task Restore_ThatReEnablesInCommitTimestamps_StampsTheEnablement()
    {
        await WriteRawAsync(0,
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["inCommitTimestamp"] },
            RawMetadata(new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "true" }));
        await WriteRawAsync(1, RawMetadata());
        await using var table = await DeltaTable.OpenAsync(Fs);

        var result = await table.RestoreAsync(0);

        var actions = await CommitAsync(result.Version);
        var commitInfo = Of(actions, "commitInfo").Single();
        long inCommit = commitInfo.GetProperty("inCommitTimestamp").GetInt64();
        Assert.Equal("RESTORE", commitInfo.GetProperty("operation").GetString());
        Assert.True(commitInfo.TryGetProperty("operationMetrics", out _));
        Assert.Equal("0", commitInfo.GetProperty("operationParameters").GetProperty("version").GetString());
        Assert.False(commitInfo.GetProperty("isBlindAppend").GetBoolean());
        var configuration = Of(actions, "metaData").Single().GetProperty("configuration");
        Assert.Equal(result.Version.ToString(), configuration.GetProperty("delta.inCommitTimestampEnablementVersion").GetString());
        Assert.Equal(inCommit.ToString(), configuration.GetProperty("delta.inCommitTimestampEnablementTimestamp").GetString());
    }

    /// <summary>
    /// On a column-mapped table an identity column renamed since the target version is still the same column: its
    /// high-water mark is matched by physical name and carried back, so values written since are never reissued.
    /// </summary>
    [Fact]
    public async Task Restore_AcrossAMappedRename_KeepsTheIdentityHighWaterMark()
    {
        await WriteRawAsync(0,
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["columnMapping"],
                WriterFeatures = ["columnMapping", "identityColumns"],
            },
            new MetadataAction
            {
                Id = "restore-mapped-identity",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{"delta.identity.start":1,"delta.identity.step":1,"delta.identity.allowExplicitInsert":false,"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a"}},{"name":"value","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-b"}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    [ColumnMapping.ModeKey] = "name",
                    [ColumnMapping.MaxColumnIdKey] = "2",
                },
            });
        await using var table = await DeltaTable.OpenAsync(Fs);
        var valueSchema = new Apache.Arrow.Schema.Builder().Field(new Field("value", StringType.Default, true)).Build();
        RecordBatch Values(params string[] values) =>
            new(valueSchema, [new StringArray.Builder().AppendRange(values).Build()], values.Length);
        await table.WriteAsync([Values("a")]);                                            // v1, id 1
        await table.RenameColumnAsync("id", "ident");                                     // v2
        await table.WriteAsync([Values("b", "c")]);                                       // v3, ids 2-3

        await table.RestoreAsync(1);
        await table.WriteAsync([Values("d")]);

        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var id = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(id.GetValue(i)!.Value);
        }
        ids.Sort();
        Assert.Equal([1L, 4L], ids);
    }

    /// <summary>
    /// Restoring the version another handle already committed commits nothing, and this handle then reports that
    /// version, as the result does.
    /// </summary>
    [Fact]
    public async Task Restore_ToAVersionAnotherHandleCommitted_PublishesIt()
    {
        await using var table = await CreateAsync(deletionVectors: false);
        await table.WriteAsync([Rows(1)]);                                               // v1
        await using (var other = await DeltaTable.OpenAsync(Fs))
            await other.WriteAsync([Rows(2)]);                                            // v2

        var result = await table.RestoreAsync(2);

        Assert.False(result.Committed);
        Assert.Equal(2, result.Version);
        Assert.Equal(2, table.CurrentSnapshot.Version);
        Assert.Equal(["1:v1", "2:v2"], await ReadAsync(table));
    }

    /// <summary>An absolute-path deletion vector (storage type <c>p</c>) is checked for existence too.</summary>
    [Fact]
    public async Task Restore_ToAVersionWhoseAbsoluteDeletionVectorIsGone_IsRefused()
    {
        var dv = new DeletionVector
        {
            StorageType = "p",
            PathOrInlineDv = "elsewhere/absolute_dv.bin",
            Offset = 1,
            SizeInBytes = 34,
            Cardinality = 1,
        };
        File.WriteAllBytes(Path.Combine(_tempDir, "a.parquet"), [0]);
        await WriteRawAsync(0,
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["deletionVectors"],
                WriterFeatures = ["deletionVectors"],
            },
            RawMetadata(new Dictionary<string, string> { ["delta.enableDeletionVectors"] = "true" }));
        await WriteRawAsync(1, new AddFile
        {
            Path = "a.parquet",
            PartitionValues = new Dictionary<string, string>(),
            Size = 1,
            ModificationTime = 0,
            DataChange = true,
            Stats = """{"numRecords":2}""",
            DeletionVector = dv,
        });
        await WriteRawAsync(2, new RemoveFile
        {
            Path = "a.parquet",
            DataChange = true,
            DeletionVector = dv,
        });
        await using var table = await DeltaTable.OpenAsync(Fs);

        var ex = await RefusedAsync(DeltaTableErrorCodes.RestoreMissingFiles, async () => await table.RestoreAsync(1));
        Assert.Contains("elsewhere/absolute_dv.bin", ex.Message);
    }

    /// <summary>
    /// Resolving a timestamp near the start of history reads the commits only up to the one past it, not the whole
    /// log.
    /// </summary>
    [Fact]
    public async Task GetSnapshotAtTimestamp_StopsReadingAtTheBoundary()
    {
        await using (var table = await CreateAsync(deletionVectors: false))
        {
            for (long i = 1; i <= 10; i++)
            {
                await Task.Delay(5);
                await table.WriteAsync([Rows(i)]);                                        // v1..v10
            }
        }
        long atV1 = await CommitTimestampAsync(1);
        var counting = new RacingFileSystem(Fs, race: null);
        await using var reader = await DeltaTable.OpenAsync(counting);
        int before = counting.CommitReads;

        var snapshot = await reader.GetSnapshotAtTimestampAsync(DateTimeOffset.FromUnixTimeMilliseconds(atV1));

        Assert.Equal(1, snapshot.Version);
        // v0..v2 for the timestamps, then v0..v1 for the snapshot; reading the whole log would be 11 + 2.
        Assert.True(counting.CommitReads - before <= 5, $"{counting.CommitReads - before} commit reads");
    }

    /// <summary>
    /// Runs <paramref name="race"/> once, just before the first commit file this handle tries to create, so that
    /// another writer takes the version first.
    /// </summary>
    private sealed class RacingFileSystem(ITableFileSystem inner, Func<Task>? race) : ITableFileSystem
    {
        private int _raced;
        private int _commitReads;

        /// <summary>How many commit files have been read whole.</summary>
        public int CommitReads => Volatile.Read(ref _commitReads);

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async ValueTask<bool> TryWriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            if (race is not null && path.EndsWith(".json", StringComparison.Ordinal) && path.Contains("_delta_log")
                && Interlocked.Exchange(ref _raced, 1) == 0)
            {
                await race();
            }
            return await inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        }

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
            => inner.OpenReadAsync(path, cancellationToken);

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
        {
            if (path.EndsWith(".json", StringComparison.Ordinal) && path.Contains("_delta_log"))
                Interlocked.Increment(ref _commitReads);
            return inner.ReadAllBytesAsync(path, cancellationToken);
        }

        public ValueTask<ISequentialFile> CreateAsync(
            string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);

        public ValueTask WriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);
    }
}
