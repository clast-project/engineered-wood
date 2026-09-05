// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// EVERY commit path leaves a version checksum behind.
///
/// <para>This is the assertion the feature actually stands on, and the reason it is a coverage test rather
/// than one case per operation. A checksum is how another engine detects that its incrementally computed
/// view of a table has drifted from the log; checksums produced on some commit paths and not others give
/// it a set it cannot reason about, and the gap is invisible until something else surfaces it. The commit
/// paths in this library do not share one implementation — the optimistic-concurrency loop lives in the
/// log layer, while creation, the overwrite family, OPTIMIZE, VACUUM and the metadata-only changes commit
/// through <c>TransactionLog</c> directly — so "every path" has to be stated as a test, not assumed from
/// the code's shape. The interval checkpoint and the log cleanup have both drifted out of one path or
/// another before; this is that lesson applied in advance.</para>
///
/// <para>Version 0 is included deliberately: creating a table is a commit like any other.</para>
/// </summary>
public class VersionChecksumCoverageTests : IDisposable
{
    private readonly string _tempDir;

    public VersionChecksumCoverageTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_crc_cov_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema TestSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("region", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(params long[] ids)
    {
        var idBuilder = new Int64Array.Builder();
        idBuilder.AppendRange(ids);
        var regionBuilder = new StringArray.Builder();
        foreach (long id in ids)
            regionBuilder.Append(id % 2 == 0 ? "us" : "eu");
        return new RecordBatch(TestSchema, [idBuilder.Build(), regionBuilder.Build()], ids.Length);
    }

    private string LogDir => Path.Combine(_tempDir, "_delta_log");

    private bool HasChecksum(long version) =>
        File.Exists(Path.Combine(LogDir, $"{DeltaVersion.Format(version)}.crc"));

    private JsonElement Checksum(long version)
    {
        using var doc = JsonDocument.Parse(
            File.ReadAllText(Path.Combine(LogDir, $"{DeltaVersion.Format(version)}.crc")));
        return doc.RootElement.Clone();
    }

    /// <summary>Every version the log holds a commit file for.</summary>
    private IEnumerable<long> CommittedVersions() =>
        Directory.GetFiles(LogDir, "*.json")
            .Select(Path.GetFileName)
            .Select(name =>
            {
                bool parsed = DeltaVersion.TryParseCommitVersion(name!, out long version);
                return (parsed, version);
            })
            .Where(result => result.parsed)
            .Select(result => result.version)
            .OrderBy(v => v);

    private void AssertEveryCommittedVersionHasAChecksum()
    {
        var missing = CommittedVersions().Where(v => !HasChecksum(v)).ToList();
        Assert.True(
            missing.Count == 0,
            $"versions {string.Join(", ", missing)} committed without a checksum; "
            + "every commit path must go through DeltaTable.AfterCommitAsync or LogCommitter");
    }

    // ── every path ─────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EveryCommitPath_LeavesAChecksum()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        // Checkpointing off throughout: it would reclaim commits and checksums together and hide the
        // very gaps this test exists to find.
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        // v0 — CREATE TABLE, which commits through TransactionLog directly.
        await using var table = await DeltaTable.CreateAsync(fs, TestSchema, options);
        Assert.True(HasChecksum(0), "the creation commit left no checksum");

        // v1 — blind append (the optimistic-concurrency loop, in the log layer).
        await table.WriteAsync([Batch(1, 2, 3)]);

        // v2 — overwrite (a single-attempt commit through TransactionLog, not the OCC loop).
        await table.WriteAsync([Batch(4, 5)], DeltaWriteMode.Overwrite);

        // v3 — a metadata-only change.
        await table.AddColumnAsync(new Field("extra", StringType.Default, true));

        // v4/v5 — domain metadata, set then removed.
        await table.SetDomainMetadataAsync("app.domain", """{"v":1}""");
        await table.RemoveDomainMetadataAsync("app.domain");

        // v6 — DELETE (a data commit through the OCC loop, with removes). Every row of the one live file,
        // so it is a whole-file remove: a partial delete would need deletion vectors, which is a different
        // feature and not what this sweep is about.
        await table.DeleteAsync(batch =>
        {
            var all = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                all.Append(true);
            return all.Build();
        });

        // v7, v8 — two small files for OPTIMIZE to have something to compact.
        await table.WriteAsync([Batch(6)]);
        await table.WriteAsync([Batch(7)]);

        // v9 — OPTIMIZE, which commits through TransactionLog directly.
        long? compacted = await table.CompactAsync(
            new CompactionOptions { TargetFileSize = 1024 * 1024, MinFileSize = 1024 * 1024 });

        // VACUUM's two commitInfo-only commits.
        await table.VacuumAsync(TimeSpan.Zero);

        AssertEveryCommittedVersionHasAChecksum();

        // And the sweep really did cover the paths it claims to: a table that committed only three or
        // four versions would pass the assertion above while proving nothing.
        Assert.True(table.CurrentSnapshot.Version >= 10,
            $"expected the sweep to reach at least version 10, got {table.CurrentSnapshot.Version}");
        Assert.NotNull(compacted);
    }

    [Fact]
    public async Task ChecksumsOff_LeavesNoneOnAnyPath()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var options = new DeltaTableOptions { CheckpointInterval = 0, WriteVersionChecksums = false };

        await using var table = await DeltaTable.CreateAsync(fs, TestSchema, options);
        await table.WriteAsync([Batch(1, 2)]);
        await table.WriteAsync([Batch(3)], DeltaWriteMode.Overwrite);
        await table.SetDomainMetadataAsync("app.domain", """{"v":1}""");
        await table.VacuumAsync(TimeSpan.Zero);

        Assert.Empty(Directory.GetFiles(LogDir, "*.crc"));
    }

    // ── what the file says ─────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Checksum_DescribesTheStateAtItsOwnVersion()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, TestSchema, new DeltaTableOptions { CheckpointInterval = 0 });

        await table.WriteAsync([Batch(1, 2, 3)]);
        await table.WriteAsync([Batch(4, 5)]);

        // v0: the table exists and holds nothing.
        Assert.Equal(0, Checksum(0).GetProperty("numFiles").GetInt64());
        Assert.Equal(0, Checksum(0).GetProperty("tableSizeBytes").GetInt64());

        // v2: both appends are live, and the size is the sum of the two files' own `size` fields — the
        // same number a reader gets by adding up the log, which is what makes the field checkable.
        var snapshot = table.CurrentSnapshot;
        Assert.Equal(snapshot.ActiveFiles.Count, Checksum(2).GetProperty("numFiles").GetInt64());
        Assert.Equal(
            snapshot.ActiveFiles.Values.Sum(f => f.Size),
            Checksum(2).GetProperty("tableSizeBytes").GetInt64());
    }

    [Fact]
    public async Task Checksum_AfterAnOverwrite_CountsOnlyTheSurvivingFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, TestSchema, new DeltaTableOptions { CheckpointInterval = 0 });

        await table.WriteAsync([Batch(1, 2, 3)]);
        await table.WriteAsync([Batch(4)], DeltaWriteMode.Overwrite);

        // The overwrite's commit contains a remove AND an add. Counting what the commit said rather than
        // what survives reconciliation is the mistake an incrementally maintained tally makes; a checksum
        // computed from the snapshot cannot make it.
        Assert.Equal(1, Checksum(2).GetProperty("numFiles").GetInt64());
        Assert.Equal(
            table.CurrentSnapshot.ActiveFiles.Values.Single().Size,
            Checksum(2).GetProperty("tableSizeBytes").GetInt64());
    }

    [Fact]
    public async Task LogCleanup_ReclaimsOurOwnChecksums_AlongWithTheCommitsTheyDescribe()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, TestSchema,
            new DeltaTableOptions { CheckpointInterval = 2 });

        // Backdated past the default 30-day retention rather than shortening it, so the horizon is
        // reachable without sleeping through it — the same device LogCleanupTriggerTests uses. Cleanup
        // runs off the back of a checkpoint, so the writes below both produce checksums and reclaim them.
        await table.WriteAsync([Batch(0)]);
        var old = DateTime.UtcNow.AddDays(-60);
        foreach (string file in Directory.GetFiles(LogDir))
            File.SetLastWriteTimeUtc(file, old);

        for (int i = 1; i < 6; i++)
            await table.WriteAsync([Batch(i)]);

        // The point is that OUR checksums are collected too. Cleanup already deleted delta-spark's; a
        // library that produced its own and left them growing without bound would have re-created the
        // exact condition cleanup exists to end.
        var orphaned = Directory.GetFiles(LogDir, "*.crc")
            .Select(f => Path.GetFileNameWithoutExtension(f)!)
            .Select(long.Parse)
            .Where(v => !File.Exists(Path.Combine(LogDir, $"{DeltaVersion.Format(v)}.json"))
                        && !File.Exists(Path.Combine(
                            LogDir, $"{DeltaVersion.Format(v)}.checkpoint.parquet")))
            .ToList();

        Assert.True(orphaned.Count == 0,
            $"checksums {string.Join(", ", orphaned)} outlived both their commit and their checkpoint");
    }
}
