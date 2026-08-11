// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <c>delta.logRetentionDuration</c> — deleting the commit files a checkpoint has made redundant.
///
/// <para>The property was accepted and stored and read by nobody, so <c>_delta_log</c> grew for the life of
/// a table. Every test here drives <see cref="LogCleanup"/> directly with an injected clock, so a retention
/// horizon can be crossed without sleeping and the assertions are about the RULE rather than about timing.
/// </para>
///
/// <para>The rule has two halves and both are load-bearing: a file may go only when a checkpoint covers it
/// AND it is older than the horizon. Each half has a test that fails if the other is dropped.</para>
/// </summary>
public class LogCleanupTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TransactionLog _log;
    private static readonly DateTimeOffset Now = new(2026, 8, 8, 12, 0, 0, TimeSpan.Zero);

    public LogCleanupTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_logclean_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
        _log = new TransactionLog(_fs);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private const string SchemaJson =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""";

    /// <summary>Writes versions 0..<paramref name="through"/>; v0 carries protocol + metadata.</summary>
    private async ValueTask WriteVersionsAsync(long through)
    {
        await _log.WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "cleanup-test",
                Format = Format.Parquet,
                SchemaString = SchemaJson,
                PartitionColumns = [],
                CreatedTime = 1700000000000,
            },
        ]);

        for (long v = 1; v <= through; v++)
            await _log.WriteCommitAsync(v, [Add($"f{v}.parquet")]);
    }

    private static AddFile Add(string path) => new()
    {
        Path = path,
        PartitionValues = new Dictionary<string, string>(),
        Size = 100,
        ModificationTime = 1700000001000,
        DataChange = true,
    };

    /// <summary>
    /// Backdates every log file so the horizon is unambiguously crossed, one minute apart in version order.
    ///
    /// <para>⚠ The SPACING matters and a first version of this stamped them all identically, which made
    /// three tests fail: Delta treats a commit that is not STRICTLY newer than its predecessor as needing
    /// timestamp adjustment, so a uniformly-stamped log is one long dependency chain and cleanup correctly
    /// retains all of it. Real logs have distinct times; the fixture has to as well, or it tests a shape
    /// that does not occur and hides the rule underneath it.</para>
    /// </summary>
    private void Backdate(DateTime start)
    {
        var files = Directory.GetFiles(Path.Combine(_tempDir, "_delta_log"));
        Array.Sort(files, StringComparer.Ordinal);
        for (int i = 0; i < files.Length; i++)
            File.SetLastWriteTimeUtc(files[i], start.AddMinutes(i));
    }

    private string LogFile(string name) => Path.Combine(_tempDir, "_delta_log", name);

    private static Dictionary<string, string> Retention(string interval) =>
        new() { ["delta.logRetentionDuration"] = interval };

    // ── the base case ──────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The headline: commits a checkpoint subsumes, older than the horizon, are deleted — and THE TABLE IS
    /// STILL READABLE, which is the assertion that separates cleanup from corruption. Everything v0 carried
    /// (protocol, metadata) now comes from the checkpoint.
    /// </summary>
    [Fact]
    public async Task Cleanup_DeletesExpiredCommits_AndTheTableStillReads()
    {
        await WriteVersionsAsync(through: 5);

        // ⚠ A REAL checkpoint, not just the parameter. A first version of this passed
        // latestCheckpointVersion: 5 without writing one, deleted the prefix, and then failed its own
        // readability assertion with "version 0 is missing and no checkpoint covers it" — the fixture had
        // manufactured exactly the corruption the rule exists to prevent. In production LogCommitter only
        // calls cleanup immediately after writing one, so the argument is always truthful there.
        var built = await Snapshot.SnapshotBuilder.BuildAsync(
            _log, new Checkpoint.CheckpointReader(_fs), atVersion: null);
        await new Checkpoint.CheckpointWriter(_fs).WriteCheckpointAsync(built);

        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        int deleted = await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 5, Now);

        Assert.Equal(5, deleted);                              // v0..v4; v5 is the checkpoint's own version
        Assert.False(File.Exists(LogFile("00000000000000000000.json")));
        Assert.False(File.Exists(LogFile("00000000000000000004.json")));
        Assert.True(File.Exists(LogFile("00000000000000000005.json")));

        // The metadata action lived in v0. If the survivors cannot describe the table, this throws.
        var snapshot = await Snapshot.SnapshotBuilder.BuildAsync(
            _log, new Checkpoint.CheckpointReader(_fs), atVersion: null);
        Assert.Equal(5, snapshot.Version);
    }

    // ── half one: a checkpoint must cover it ───────────────────────────────────────────────────────────

    /// <summary>
    /// ⚠ THE SAFETY RULE. With no checkpoint, every commit is the ONLY copy of its actions — deleting one
    /// does not make the table older, it makes it unreadable. Delta returns early here too.
    /// </summary>
    [Fact]
    public async Task Cleanup_DeletesNothing_WhenThereIsNoCheckpoint()
    {
        await WriteVersionsAsync(through: 5);
        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        int deleted = await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: null, Now);

        Assert.Equal(0, deleted);
        Assert.Equal(6, Directory.GetFiles(Path.Combine(_tempDir, "_delta_log"), "*.json").Length);
    }

    /// <summary>
    /// The checkpoint's own version is what the survivors replay FROM, so it is never deletable — nor is
    /// anything above it, however old the files are.
    /// </summary>
    [Fact]
    public async Task Cleanup_NeverDeletesAtOrAboveTheCheckpointVersion()
    {
        await WriteVersionsAsync(through: 5);
        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        int deleted = await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 2, Now);

        Assert.Equal(2, deleted); // v0, v1 only
        Assert.True(File.Exists(LogFile("00000000000000000002.json")));
        Assert.True(File.Exists(LogFile("00000000000000000005.json")));
    }

    // ── half two: it must be older than the horizon ────────────────────────────────────────────────────

    /// <summary>
    /// THE CONTROL FOR THE HORIZON. Same table, same checkpoint, files NOT backdated — so the only thing
    /// that changed is age. Without this, the tests above pass equally if retention were ignored entirely,
    /// which is the very bug being fixed.
    /// </summary>
    [Fact]
    public async Task Cleanup_KeepsFilesInsideTheRetentionWindow()
    {
        await WriteVersionsAsync(through: 5);

        int deleted = await LogCleanup.RunAsync(
            _log, Retention("interval 30 days"), latestCheckpointVersion: 5, DateTimeOffset.UtcNow);

        Assert.Equal(0, deleted);
        Assert.True(File.Exists(LogFile("00000000000000000000.json")));
    }

    /// <summary>
    /// An unset property must behave as Delta's 30 days rather than as "no retention" — the difference
    /// between a default and a missing check is a table whose whole log disappears at its first checkpoint.
    /// </summary>
    [Fact]
    public async Task Cleanup_WithNoProperty_UsesTheThirtyDayDefault()
    {
        await WriteVersionsAsync(through: 5);

        int kept = await LogCleanup.RunAsync(
            _log, configuration: null, latestCheckpointVersion: 5, DateTimeOffset.UtcNow);
        Assert.Equal(0, kept); // minutes old, so inside 30 days

        // ...and the same table with everything pushed past the default horizon IS collected, which is what
        // makes the assertion above about the horizon rather than about the property being missing.
        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        int deleted = await LogCleanup.RunAsync(
            _log, configuration: null, latestCheckpointVersion: 5, DateTimeOffset.UtcNow);
        Assert.Equal(5, deleted);
    }

    /// <summary>
    /// ⚠ A NON-POSITIVE OR UNPARSEABLE VALUE FALLS BACK TO THE DEFAULT rather than being honoured. An odd
    /// property must not read as an instruction to delete a table's entire log, and `interval 0 seconds`
    /// literally means "keep nothing" if taken at face value.
    /// </summary>
    [Theory]
    [InlineData("interval 0 seconds")]
    [InlineData("interval -5 days")]
    [InlineData("not an interval")]
    [InlineData("interval 1 months")] // months are calendar-relative; the parser refuses them
    public async Task Cleanup_RefusesAnOddRetention_AndKeepsTheDefault(string raw)
    {
        await WriteVersionsAsync(through: 5);

        int deleted = await LogCleanup.RunAsync(
            _log, Retention(raw), latestCheckpointVersion: 5, DateTimeOffset.UtcNow);

        Assert.Equal(0, deleted);
    }

    // ── the opt-out and the boundary ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// <c>delta.enableExpiredLogCleanup = false</c> is Delta's own switch, honoured so a table whose log
    /// retention something else owns keeps every file.
    /// </summary>
    [Fact]
    public async Task Cleanup_IsDisabled_ByEnableExpiredLogCleanupFalse()
    {
        await WriteVersionsAsync(through: 5);
        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        var config = Retention("interval 1 days");
        config["delta.enableExpiredLogCleanup"] = "false";

        int deleted = await LogCleanup.RunAsync(_log, config, latestCheckpointVersion: 5, Now);

        Assert.Equal(0, deleted);
    }

    /// <summary>
    /// ⚠ A value PRESENT and unparseable disables cleanup, which is the opposite of what "default true"
    /// would give. Absent means enabled — that is Delta's default and this asserts it elsewhere — but
    /// someone who wrote <c>no</c> or <c>off</c> into this property was reaching for the switch, and the
    /// only safe way to be wrong about that is to keep their files. Same principle as an odd
    /// <c>logRetentionDuration</c> falling back to 30 days rather than being taken at face value: an
    /// unreadable property must never be the thing that authorises deleting a table's log.
    /// </summary>
    [Theory]
    [InlineData("no")]
    [InlineData("off")]
    [InlineData("0")]
    [InlineData("")]
    public async Task Cleanup_IsDisabled_ByAnUnparseableEnableExpiredLogCleanup(string raw)
    {
        await WriteVersionsAsync(through: 5);
        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        var config = Retention("interval 1 days");
        config["delta.enableExpiredLogCleanup"] = raw;

        int deleted = await LogCleanup.RunAsync(_log, config, latestCheckpointVersion: 5, Now);

        Assert.Equal(0, deleted);
        Assert.True(File.Exists(LogFile("00000000000000000000.json")));
    }

    /// <summary>
    /// ⚠ A FILESYSTEM THAT CANNOT DATE ITS FILES GETS NO CLEANUP. An ITableFileSystem whose backing listing
    /// carries no modification time may return a placeholder, and a host was found reporting a constant
    /// epoch for every file — under which every commit looks decades old and is therefore expired the
    /// instant it is written. An absent timestamp must decline the pass, not default to one, because the
    /// opposite choice deletes live history silently.
    /// </summary>
    [Fact]
    public async Task Cleanup_DeletesNothing_WhenTheListingCannotDateTheFiles()
    {
        await WriteVersionsAsync(through: 5);
        foreach (var file in Directory.GetFiles(Path.Combine(_tempDir, "_delta_log")))
            File.SetLastWriteTimeUtc(file, new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        int deleted = await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 5, Now);

        Assert.Equal(0, deleted);
        Assert.True(File.Exists(LogFile("00000000000000000000.json")));
    }

    /// <summary>
    /// ⚠ THE ADJUSTMENT ANCHOR. Delta presents commit timestamps as strictly increasing with version,
    /// adjusting a file that is not newer than its predecessor. A reader doing time-travel-BY-TIMESTAMP
    /// therefore depends on the file a survivor was adjusted from — delete it and the same timestamp query
    /// starts answering with a different version.
    ///
    /// <para>This library's own time travel reads IN-COMMIT timestamps and is immune, but a Delta reader on
    /// the same table is not, so the anchor is not ours to break. Here the first SURVIVOR (v3) is NOT newer
    /// than v2, so v2 is retained as its anchor — while v0 and v1, which nothing surviving depends on, still
    /// go. Retaining the CHAIN rather than the whole log is what keeps cleanup useful on a table whose
    /// timestamps are not strictly increasing.</para>
    /// </summary>
    [Fact]
    public async Task Cleanup_RetainsTheAnchor_TheFirstSurvivorDependsOn()
    {
        await WriteVersionsAsync(through: 5);
        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        // v3 survives (checkpoint at 3) and is OLDER than v2 — exactly the shape that needs adjusting.
        File.SetLastWriteTimeUtc(
            LogFile("00000000000000000003.json"), new DateTime(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        int deleted = await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 3, Now);

        // v2 is RETAINED as v3's anchor; v0 and v1 are free of it and still go. The walk-back keeps the
        // chain the survivor depends on, not the whole log — a rule that retained everything would make
        // cleanup useless on any table whose timestamps are not strictly increasing.
        Assert.Equal(2, deleted);
        Assert.True(File.Exists(LogFile("00000000000000000002.json")));
        Assert.False(File.Exists(LogFile("00000000000000000000.json")));
    }

    /// <summary>
    /// THE CONTROL FOR THE ANCHOR RULE: the identical shape with the survivor NEWER than its predecessor
    /// deletes the anchor too. Without it the test above passes equally if v2 were being retained for some
    /// unrelated reason.
    /// </summary>
    [Fact]
    public async Task Cleanup_Deletes_WhenTheFirstSurvivorIsNewerThanTheLastExpiredFile()
    {
        await WriteVersionsAsync(through: 5);
        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        File.SetLastWriteTimeUtc(
            LogFile("00000000000000000003.json"), new DateTime(2020, 6, 1, 0, 0, 0, DateTimeKind.Utc));

        int deleted = await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 3, Now);

        Assert.Equal(3, deleted); // v0, v1, v2
    }

    // ── V2 checkpoint sidecars ─────────────────────────────────────────────────────────────────────────
    //
    // On a table big enough to use them, the sidecars ARE the log: the file actions live there and the
    // checkpoint body is an index over them. Reclaiming checkpoints without their sidecars would free the
    // index and leave the bulk, permanently — VacuumExecutor excludes _delta_log, so nothing else would
    // ever collect them.

    /// <summary>Writes v0 with a protocol that permits V2 checkpoints, then versions 1..<paramref name="through"/>.</summary>
    private async ValueTask WriteV2CapableVersionsAsync(long through)
    {
        await _log.WriteCommitAsync(0,
        [
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["v2Checkpoint"],
                WriterFeatures = ["v2Checkpoint"],
            },
            new MetadataAction
            {
                Id = "cleanup-sidecar-test",
                Format = Format.Parquet,
                SchemaString = SchemaJson,
                PartitionColumns = [],
                CreatedTime = 1700000000000,
            },
        ]);

        for (long v = 1; v <= through; v++)
            await _log.WriteCommitAsync(v, [Add($"f{v}.parquet")]);
    }

    /// <summary>
    /// Writes a V2 checkpoint at the current version whose file actions are FORCED into sidecars —
    /// <c>SidecarThreshold = 0</c> rather than a hundred-file fixture, so the layout under test is the real
    /// one without the table that normally produces it.
    /// </summary>
    private async ValueTask<Snapshot.Snapshot> WriteV2CheckpointWithSidecarsAsync()
    {
        var snapshot = await Snapshot.SnapshotBuilder.BuildAsync(
            _log, new Checkpoint.CheckpointReader(_fs), atVersion: null);
        await new Checkpoint.V2CheckpointWriter(_fs) { SidecarThreshold = 0 }
            .WriteCheckpointAsync(snapshot);
        return snapshot;
    }

    private string[] SidecarNames() =>
        Directory.Exists(SidecarDir)
            ? [.. Directory.GetFiles(SidecarDir).Select(Path.GetFileName).OrderBy(n => n, StringComparer.Ordinal)!]
            : [];

    private string SidecarDir => Path.Combine(_tempDir, "_delta_log", "_sidecars");

    /// <summary>
    /// The headline for sidecars: an expired checkpoint's sidecars go with it, and the surviving
    /// checkpoint's do not. Two checkpoints, each with its own set, and only one survives the horizon.
    /// </summary>
    [Fact]
    public async Task Cleanup_DeletesSidecars_NoSurvivingCheckpointReferences()
    {
        await WriteV2CapableVersionsAsync(through: 2);
        await WriteV2CheckpointWithSidecarsAsync();               // checkpoint at v2, sidecar set A
        string[] setA = SidecarNames();
        Assert.NotEmpty(setA);

        for (long v = 3; v <= 5; v++)
            await _log.WriteCommitAsync(v, [Add($"f{v}.parquet")]);
        await WriteV2CheckpointWithSidecarsAsync();               // checkpoint at v5, sidecar set B
        string[] setB = [.. SidecarNames().Except(setA)];
        Assert.NotEmpty(setB);

        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        BackdateSidecars(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        int deleted = await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 5, Now);

        string[] remaining = SidecarNames();
        foreach (string a in setA)
            Assert.DoesNotContain(a, remaining);
        foreach (string b in setB)
            Assert.Contains(b, remaining);

        // The v5 checkpoint still resolves through its sidecars, which is the whole point of keeping them.
        var rebuilt = await Snapshot.SnapshotBuilder.BuildAsync(
            _log, new Checkpoint.CheckpointReader(_fs), atVersion: null);
        Assert.Equal(5, rebuilt.Version);
        Assert.Equal(5, rebuilt.FileCount);
        Assert.True(deleted > setA.Length, "the count should cover the commits AND the expired sidecars");
    }

    /// <summary>
    /// ⚠ THE AGE CONDITION, and it is not decoration. A writer publishes its sidecars BEFORE the checkpoint
    /// that names them — it must, since the checkpoint records their paths — so a sweep that deleted
    /// everything unreferenced would destroy a checkpoint that is seconds from existing. A recent
    /// unreferenced sidecar therefore survives, and an old one does not: the same file, told apart only by
    /// its age.
    /// </summary>
    [Fact]
    public async Task Cleanup_KeepsAnUnreferencedSidecar_ThatIsYoungerThanTheHorizon()
    {
        await WriteV2CapableVersionsAsync(through: 5);
        await WriteV2CheckpointWithSidecarsAsync();

        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        BackdateSidecars(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        // Two sidecars nothing references: one long expired, one a moment old — as a concurrent writer's
        // would be, published ahead of the checkpoint about to name it.
        Directory.CreateDirectory(SidecarDir);
        string stale = Path.Combine(SidecarDir, "aaaaaaaa-stale.parquet");
        string fresh = Path.Combine(SidecarDir, "bbbbbbbb-fresh.parquet");
        File.WriteAllBytes(stale, [1]);
        File.WriteAllBytes(fresh, [1]);
        File.SetLastWriteTimeUtc(stale, new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        File.SetLastWriteTimeUtc(fresh, Now.UtcDateTime.AddMinutes(-1));

        await LogCleanup.RunAsync(_log, Retention("interval 1 days"), latestCheckpointVersion: 5, Now);

        string[] remaining = SidecarNames();
        Assert.DoesNotContain("aaaaaaaa-stale.parquet", remaining);
        Assert.Contains("bbbbbbbb-fresh.parquet", remaining);
    }

    /// <summary>
    /// FAILS CLOSED. A surviving checkpoint that cannot be read leaves the referenced set incomplete, and
    /// sweeping against an incomplete set deletes live data — so an unreadable survivor abandons the sweep
    /// entirely rather than proceeding with what it managed to collect.
    /// </summary>
    [Fact]
    public async Task Cleanup_SweepsNoSidecars_WhenASurvivingCheckpointCannotBeRead()
    {
        await WriteV2CapableVersionsAsync(through: 5);
        await WriteV2CheckpointWithSidecarsAsync();
        string[] before = SidecarNames();
        Assert.NotEmpty(before);

        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        BackdateSidecars(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        // An unreferenced, long-expired sidecar: without the corruption below it WOULD be swept, so its
        // survival is what proves the sweep declined rather than merely found nothing to do.
        string orphan = Path.Combine(SidecarDir, "cccccccc-orphan.parquet");
        File.WriteAllBytes(orphan, [1]);
        File.SetLastWriteTimeUtc(orphan, new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        string checkpoint = Directory
            .GetFiles(Path.Combine(_tempDir, "_delta_log"), "*.checkpoint.*").Single();
        File.WriteAllText(checkpoint, "not a checkpoint");

        await LogCleanup.RunAsync(_log, Retention("interval 1 days"), latestCheckpointVersion: 5, Now);

        string[] remaining = SidecarNames();
        Assert.Contains("cccccccc-orphan.parquet", remaining);
        foreach (string name in before)
            Assert.Contains(name, remaining);
    }

    /// <summary>
    /// ⚠ THE AWKWARD HISTORY: sidecars exist and the SURVIVING checkpoint is classic. A table that used V2
    /// checkpoints and later wrote a classic one — a <c>delta.checkpointPolicy</c> change, or another
    /// engine — keeps its sidecar directory, so the sweep runs and has to ask a checkpoint with no
    /// sidecar column what it references. The answer is none, every old sidecar is unreferenced, and all
    /// of them go.
    ///
    /// <para>This is also the case that makes the projected read matter rather than being an optimisation:
    /// a classic checkpoint carries every file action in its body, so answering by materialising it would
    /// be the table's whole file list allocated to discover a null result, on a commit path. The body is
    /// asked for its <c>sidecar</c> column, has none, and is not read.</para>
    /// </summary>
    [Fact]
    public async Task Cleanup_SweepsSidecars_WhenTheSurvivingCheckpointIsClassic()
    {
        await WriteV2CapableVersionsAsync(through: 2);
        await WriteV2CheckpointWithSidecarsAsync();               // V2 checkpoint at v2, with sidecars
        Assert.NotEmpty(SidecarNames());

        for (long v = 3; v <= 5; v++)
            await _log.WriteCommitAsync(v, [Add($"f{v}.parquet")]);

        // Classic: the table declares the v2Checkpoint FEATURE but no checkpointPolicy, which is exactly
        // the combination CheckpointFormat.Automatic resolves to a classic checkpoint.
        var built = await Snapshot.SnapshotBuilder.BuildAsync(
            _log, new Checkpoint.CheckpointReader(_fs), atVersion: null);
        await new Checkpoint.CheckpointWriter(_fs).WriteCheckpointAsync(built);
        Assert.True(File.Exists(LogFile("00000000000000000005.checkpoint.parquet")));

        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        BackdateSidecars(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 5, Now);

        Assert.Empty(SidecarNames());

        var rebuilt = await Snapshot.SnapshotBuilder.BuildAsync(
            _log, new Checkpoint.CheckpointReader(_fs), atVersion: null);
        Assert.Equal(5, rebuilt.Version);
        Assert.Equal(5, rebuilt.FileCount);
    }

    /// <summary>
    /// A V2 body may be Parquet as well as NDJSON — delta-spark picks between them with a session config,
    /// so which one a table carries is not something a reader can predict. The Parquet path is the one
    /// that projects a single column, so it needs its own case; the tests above all exercise NDJSON,
    /// which is what this writer produces by default.
    /// </summary>
    [Fact]
    public async Task Cleanup_ReadsSidecarReferences_FromAParquetV2Body()
    {
        await WriteV2CapableVersionsAsync(through: 2);
        var snapshot = await Snapshot.SnapshotBuilder.BuildAsync(
            _log, new Checkpoint.CheckpointReader(_fs), atVersion: null);
        await new Checkpoint.V2CheckpointWriter(_fs)
        {
            SidecarThreshold = 0,
            Body = Checkpoint.V2CheckpointBody.Parquet,
        }.WriteCheckpointAsync(snapshot);
        string[] referenced = SidecarNames();
        Assert.NotEmpty(referenced);

        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        BackdateSidecars(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        // An expired sidecar the surviving Parquet-bodied checkpoint does NOT reference.
        string orphan = Path.Combine(SidecarDir, "dddddddd-orphan.parquet");
        File.WriteAllBytes(orphan, [1]);
        File.SetLastWriteTimeUtc(orphan, new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 2, Now);

        string[] remaining = SidecarNames();
        Assert.DoesNotContain("dddddddd-orphan.parquet", remaining);
        foreach (string name in referenced)
            Assert.Contains(name, remaining);
    }

    /// <summary>
    /// A classic-checkpoint table pays a listing and stops. Asserted because the sweep must not become a
    /// reason for the common case to start reading checkpoint bodies it has no use for.
    /// </summary>
    [Fact]
    public async Task Cleanup_OnATableWithNoSidecars_LeavesTheCountUnchanged()
    {
        await WriteVersionsAsync(through: 5);
        var built = await Snapshot.SnapshotBuilder.BuildAsync(
            _log, new Checkpoint.CheckpointReader(_fs), atVersion: null);
        await new Checkpoint.CheckpointWriter(_fs).WriteCheckpointAsync(built);

        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        int deleted = await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 5, Now);

        Assert.Equal(5, deleted); // v0..v4 and nothing else — no sidecar directory to sweep
        Assert.Empty(SidecarNames());
    }

    private void BackdateSidecars(DateTime stamp)
    {
        if (!Directory.Exists(SidecarDir))
            return;
        foreach (string file in Directory.GetFiles(SidecarDir))
            File.SetLastWriteTimeUtc(file, stamp);
    }

    // ── version checksum files ─────────────────────────────────────────────────────────────────────────
    //
    // This library writes none. delta-spark writes one beside every commit by default, so a table shared
    // with it carries them, and a cleanup that does not recognise the name leaves one file per commit
    // behind forever — the exact condition cleanup exists to end, surviving in a name it does not parse.

    /// <summary>Writes a <c>&lt;version&gt;.crc</c> beside each of versions 0..<paramref name="through"/>.</summary>
    private void WriteChecksumFiles(long through)
    {
        for (long v = 0; v <= through; v++)
            File.WriteAllText(LogFile($"{DeltaVersion.Format(v)}.crc"), """{"tableSizeBytes":1,"numFiles":1}""");
    }

    private string[] ChecksumNames() =>
        [.. Directory.GetFiles(Path.Combine(_tempDir, "_delta_log"), "*.crc")
            .Select(Path.GetFileName).OrderBy(n => n, StringComparer.Ordinal)!];

    /// <summary>
    /// A checksum file whose commit has just been deleted goes with it; the surviving version's stays.
    /// The pairing is the assertion — a <c>.crc</c> must never outlive or predecease its own commit.
    /// </summary>
    [Fact]
    public async Task Cleanup_DeletesChecksumFiles_ForTheCommitsItRemoved()
    {
        await WriteVersionsAsync(through: 5);
        WriteChecksumFiles(through: 5);

        var built = await Snapshot.SnapshotBuilder.BuildAsync(
            _log, new Checkpoint.CheckpointReader(_fs), atVersion: null);
        await new Checkpoint.CheckpointWriter(_fs).WriteCheckpointAsync(built);

        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        int deleted = await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 5, Now);

        Assert.Equal(10, deleted);                      // v0..v4 commits AND their five checksums
        Assert.Equal(["00000000000000000005.crc"], ChecksumNames());
        Assert.True(File.Exists(LogFile("00000000000000000005.json")));
    }

    /// <summary>
    /// ⚠ THE REASON THEY ARE SWEPT SEPARATELY: a file outside the replay chain must not be able to gate
    /// it. The prefix walk stops at the first file it may not delete, because a hole in the COMMITS is
    /// corruption. A checksum file is not part of that chain, and delta-spark writes one AFTER the commit
    /// it describes — so a single <c>.crc</c> can be newer than the horizon while every commit around it
    /// is long expired. Folded into the walk it would halt cleanup at its own version and strand every
    /// later commit; swept separately it halts nothing.
    ///
    /// <para>Here v1's checksum is minutes old and everything else is years old. The commits must still
    /// go, and v1's checksum must survive on its own age.</para>
    /// </summary>
    [Fact]
    public async Task Cleanup_IsNotHaltedByARecentChecksum_AmongExpiredCommits()
    {
        await WriteVersionsAsync(through: 5);
        WriteChecksumFiles(through: 5);

        var built = await Snapshot.SnapshotBuilder.BuildAsync(
            _log, new Checkpoint.CheckpointReader(_fs), atVersion: null);
        await new Checkpoint.CheckpointWriter(_fs).WriteCheckpointAsync(built);

        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        File.SetLastWriteTimeUtc(
            LogFile($"{DeltaVersion.Format(1)}.crc"), Now.UtcDateTime.AddMinutes(-1));

        int deleted = await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 5, Now);

        // Every expired commit goes, including the ones AFTER the recent checksum.
        for (long v = 0; v <= 4; v++)
        {
            Assert.False(
                File.Exists(LogFile($"{DeltaVersion.Format(v)}.json")),
                $"v{v} outlived the horizon because a checksum file halted the walk");
        }

        // v1's checksum is younger than the horizon, so it stays on its own age — the same rule the
        // sidecar sweep applies, and the reason the count is 9 rather than 10.
        Assert.Equal(9, deleted);
        Assert.Equal(
            ["00000000000000000001.crc", "00000000000000000005.crc"], ChecksumNames());
    }

    /// <summary>
    /// A checksum for a version whose commit SURVIVES is kept even when the file itself is long expired —
    /// the boundary is the commit it describes, not its own age, which is what keeps the two in step.
    /// </summary>
    [Fact]
    public async Task Cleanup_KeepsAChecksum_WhoseCommitSurvives()
    {
        await WriteVersionsAsync(through: 5);
        WriteChecksumFiles(through: 5);

        var built = await Snapshot.SnapshotBuilder.BuildAsync(
            _log, new Checkpoint.CheckpointReader(_fs), atVersion: null);
        await new Checkpoint.CheckpointWriter(_fs).WriteCheckpointAsync(built);

        Backdate(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        // A checkpoint at v3 covers v0..v2 only, so v3, v4 and v5 keep their commits — and their checksums.
        await LogCleanup.RunAsync(
            _log, Retention("interval 1 days"), latestCheckpointVersion: 3, Now);

        Assert.Equal(
            [
                "00000000000000000003.crc",
                "00000000000000000004.crc",
                "00000000000000000005.crc",
            ],
            ChecksumNames());
    }
}
