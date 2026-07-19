// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class VacuumTests : IDisposable
{
    private readonly string _tempDir;

    public VacuumTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_vacuum_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task Vacuum_DryRun_ListsFilesToDelete()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write data then overwrite (leaving orphaned file)
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch1]);

        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(2).Build()], 1);
        await table.WriteAsync([batch2], DeltaWriteMode.Overwrite);

        // Vacuum with zero retention (all unreferenced files eligible)
        var result = await table.VacuumAsync(
            retentionPeriod: TimeSpan.Zero,
            dryRun: true);

        // Should find the orphaned file from the first write
        Assert.NotEmpty(result.FilesToDelete);
        Assert.Equal(0, result.FilesDeleted); // Dry run → nothing deleted
    }

    [Fact]
    public async Task Vacuum_DeletesOrphanedFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write then overwrite
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch1]);

        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(2).Build()], 1);
        await table.WriteAsync([batch2], DeltaWriteMode.Overwrite);

        // Vacuum with zero retention — actually delete
        var result = await table.VacuumAsync(
            retentionPeriod: TimeSpan.Zero,
            dryRun: false);

        Assert.NotEmpty(result.FilesToDelete);
        Assert.Equal(result.FilesToDelete.Count, result.FilesDeleted);

        // Verify deleted files no longer exist
        foreach (string path in result.FilesToDelete)
        {
            Assert.False(File.Exists(Path.Combine(_tempDir, path)));
        }

        // Table data should still be readable (only the active file remains)
        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;
        Assert.Equal(1, totalRows);
    }

    [Fact]
    public async Task Vacuum_RespectsRetention()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch1]);

        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(2).Build()], 1);
        await table.WriteAsync([batch2], DeltaWriteMode.Overwrite);

        // Vacuum with 7-day retention — files just created won't qualify
        var result = await table.VacuumAsync(
            retentionPeriod: TimeSpan.FromDays(7),
            dryRun: true);

        // Recently created files should not be eligible for deletion
        Assert.Empty(result.FilesToDelete);
    }

    // ── Spec alignment: sweep everything, keep what the current version references. ──

    private static Apache.Arrow.Schema IdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch IdBatch(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    /// <summary>
    /// The leak this rewrite exists to close. Vacuum used to filter the listing to <c>.parquet</c>, so
    /// an abandoned <c>deletion_vector_*.bin</c> was never a candidate and stayed forever.
    /// </summary>
    [Fact]
    public async Task Vacuum_OrphanedDeletionVectorFile_IsCollected()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([IdBatch(1)]);

        // A .bin no action references — exactly what a DV rewrite or an aborted delete leaves behind.
        string orphan = Path.Combine(_tempDir, "deletion_vector_11111111-2222-3333-4444-555555555555.bin");
        File.WriteAllBytes(orphan, new byte[] { 1, 2, 3, 4 });

        // Backdate the orphan so the sweep is deterministic. With zero retention the cutoff IS "now",
        // and vacuum keeps files whose mtime is not strictly older than it (matching Spark). On .NET
        // Framework DateTime.UtcNow is quantised to the ~15.6 ms system tick while the NTFS timestamp
        // is finer, so a file written microseconds ago compares NOT-older-than the cutoff and is kept —
        // measured at 182/200 iterations on net472 vs 0/200 on net8.0. The sibling tests dodge this only
        // because a second write plus a commit gives the clock time to tick over; this one orphans and
        // vacuums back to back, so it lost the race every time the JIT was already warm (i.e. whenever
        // the rest of the class ran first — which is why it passed in isolation and failed in the suite).
        // Backdating removes the race without weakening what is under test: that an orphaned .bin is a
        // vacuum CANDIDATE at all. Retention timing itself is covered by Vacuum_DryRun_ListsFilesToDelete.
        File.SetLastWriteTimeUtc(orphan, DateTime.UtcNow.AddMinutes(-1));

        var result = await table.VacuumAsync(retentionPeriod: TimeSpan.Zero, dryRun: false);

        Assert.Contains(result.FilesToDelete,
            p => p.EndsWith("deletion_vector_11111111-2222-3333-4444-555555555555.bin", StringComparison.Ordinal));
        Assert.False(File.Exists(orphan));
    }

    /// <summary>
    /// The other side of that coin, and the more dangerous direction. A DV referenced by the CURRENT
    /// version must survive: deleting it silently un-deletes every row it masks, with nothing logged.
    /// </summary>
    [Fact]
    public async Task Vacuum_LiveDeletionVectorFile_IsPreserved()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([IdBatch(1, 2, 3, 4, 5)]);

        var addFile = table.CurrentSnapshot.ActiveFiles.Values.Single();

        // InlineThreshold 0 forces a FILE-backed vector, so there is a .bin on disk to protect.
        var dvWriter = new DeletionVectorWriter(fs) { InlineThreshold = 0 };
        var dv = await dvWriter.CreateAsync([1L, 3L], cardinality: 2);

        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(table.CurrentSnapshot.Version + 1, new DeltaAction[]
        {
            new RemoveFile
            {
                Path = addFile.Path,
                DataChange = false,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            },
            new AddFile
            {
                Path = addFile.Path,
                PartitionValues = addFile.PartitionValues,
                Size = addFile.Size,
                ModificationTime = addFile.ModificationTime,
                DataChange = false,
                Stats = addFile.Stats,
                DeletionVector = dv,
            },
        });
        await table.RefreshAsync();

        var result = await table.VacuumAsync(retentionPeriod: TimeSpan.Zero, dryRun: false);

        string dvPath = Path.Combine(_tempDir, $"deletion_vector_{DvUuid(dv)}.bin");
        Assert.True(File.Exists(dvPath), "vacuum deleted a live deletion vector");
        Assert.DoesNotContain(result.FilesToDelete, p => p.EndsWith(".bin", StringComparison.Ordinal));

        // And the mask still applies — the real assertion behind the file check.
        var rows = new List<long>();
        await foreach (var b in table.ReadAllAsync())
        {
            var ids = (Int64Array)b.Column("id");
            for (int i = 0; i < b.Length; i++)
                rows.Add(ids.GetValue(i)!.Value);
        }

        Assert.Equal([1L, 3L, 5L], rows);
    }

    /// <summary>
    /// Change-data-feed files are referenced by <c>cdc</c> actions, which never appear in the
    /// snapshot's active files — so a keep-set built from adds alone does not cover them. They are
    /// <c>.parquet</c>, so the OLD implementation deleted them once past retention, destroying readable
    /// CDF history. <c>_change_data/</c> is now excluded from the sweep outright.
    /// </summary>
    [Fact]
    public async Task Vacuum_ChangeDataFeedFiles_ArePreserved()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([IdBatch(1)]);

        string cdcDir = Path.Combine(_tempDir, "_change_data");
        Directory.CreateDirectory(cdcDir);
        string cdcFile = Path.Combine(cdcDir, "cdc-0000.parquet");
        File.WriteAllBytes(cdcFile, new byte[] { 9, 9, 9, 9 });

        await table.VacuumAsync(retentionPeriod: TimeSpan.Zero, dryRun: false);

        Assert.True(File.Exists(cdcFile), "vacuum deleted change-data-feed history");
    }

    /// <summary>
    /// <c>delta.deletedFileRetentionDuration</c> supplies the default retention when the caller passes
    /// none — measured against delta-spark 4.0.0, where a RETAIN-less VACUUM on a table with the
    /// property at <c>interval 0 seconds</c> collects a just-orphaned file immediately.
    /// </summary>
    [Fact]
    public async Task Vacuum_NoExplicitRetention_UsesDeletedFileRetentionDurationProperty()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([IdBatch(1)]);
        await table.WriteAsync([IdBatch(2)], DeltaWriteMode.Overwrite);

        // Default library retention is 7 days, so without the property this would collect nothing.
        var before = await table.VacuumAsync(dryRun: true);
        Assert.Empty(before.FilesToDelete);

        // No SetTablePropertiesAsync yet — commit the metaData directly.
        var current = table.CurrentSnapshot.Metadata;
        // net472's Dictionary has no IReadOnlyDictionary constructor overload.
        var configuration = new Dictionary<string, string>();
        foreach (var kv in current.Configuration ?? new Dictionary<string, string>())
            configuration[kv.Key] = kv.Value;
        configuration["delta.deletedFileRetentionDuration"] = "interval 0 seconds";

        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(table.CurrentSnapshot.Version + 1, new DeltaAction[]
        {
            new MetadataAction
            {
                Id = current.Id,
                Format = current.Format,
                SchemaString = current.SchemaString,
                PartitionColumns = current.PartitionColumns,
                Configuration = configuration,
                CreatedTime = current.CreatedTime,
            },
        });
        await table.RefreshAsync();

        var after = await table.VacuumAsync(dryRun: true);
        Assert.NotEmpty(after.FilesToDelete);
    }

    /// <summary>
    /// Absolute-path (<c>p</c>) vectors cannot be resolved against the table root, so vacuum cannot
    /// prove they lie outside the directory it is about to sweep. It refuses rather than guessing.
    /// </summary>
    [Fact]
    public async Task Vacuum_AbsolutePathDeletionVector_IsRefused()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([IdBatch(1, 2, 3)]);

        var addFile = table.CurrentSnapshot.ActiveFiles.Values.Single();
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(table.CurrentSnapshot.Version + 1, new DeltaAction[]
        {
            new RemoveFile { Path = addFile.Path, DataChange = false },
            new AddFile
            {
                Path = addFile.Path,
                PartitionValues = addFile.PartitionValues,
                Size = addFile.Size,
                ModificationTime = addFile.ModificationTime,
                DataChange = false,
                DeletionVector = new DeletionVector
                {
                    StorageType = "p",
                    PathOrInlineDv = "/somewhere/else/deletion_vector_x.bin",
                    SizeInBytes = 4,
                    Cardinality = 1,
                },
            },
        });
        await table.RefreshAsync();

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await table.VacuumAsync(retentionPeriod: TimeSpan.Zero, dryRun: true));
    }

    /// <summary>Canonical (big-endian) UUID rendering of a DV's Z85-encoded path, for locating its file.</summary>
    private static string DvUuid(DeletionVector dv)
    {
        byte[] bytes = Base85.Decode(dv.PathOrInlineDv.Substring(dv.PathOrInlineDv.Length - 20));
        var sb = new System.Text.StringBuilder(36);
        for (int i = 0; i < 16; i++)
        {
            if (i is 4 or 6 or 8 or 10)
                sb.Append('-');
            sb.Append(bytes[i].ToString("x2"));
        }

        return sb.ToString();
    }
}
