// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Concurrency;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// What a conflict TELLS you, as opposed to what the loop does about it. Every commit failure used to
/// arrive as one exception carrying a message, so a host could only match on prose or treat all
/// conflicts alike; these pin the classification each condition now carries — its
/// <see cref="DeltaConflictException.ErrorCode"/>, its <see cref="ConflictRecovery"/>, and which of the
/// two versions it can name.
///
/// <para>Driven end-to-end through <see cref="LogCommitter"/> rather than by constructing exceptions,
/// because the claim under test is that the RIGHT code reaches the caller from the right condition —
/// a hand-built exception would prove only that the constructor assigns its arguments.</para>
/// </summary>
public class ConflictClassificationTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TransactionLog _log;

    public ConflictClassificationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_conflictcls_{Guid.NewGuid():N}");
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

    private async ValueTask<Snapshot.Snapshot> CreateTableAsync()
    {
        await _log.WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "conflict-cls",
                Format = Format.Parquet,
                SchemaString = SchemaJson,
                PartitionColumns = [],
                CreatedTime = 1700000000000,
            },
        ]);
        return await SnapshotAsync();
    }

    private ValueTask<Snapshot.Snapshot> SnapshotAsync() =>
        SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs), atVersion: null);

    private static AddFile Add(string path, long minId = 0, long maxId = 9) => new()
    {
        Path = path,
        PartitionValues = new Dictionary<string, string>(),
        Size = 100,
        ModificationTime = 1700000001000,
        DataChange = true,
        Stats = $$$"""{"numRecords":10,"minValues":{"id":{{{minId}}}},"maxValues":{"id":{{{maxId}}}}}""",
    };

    private static RemoveFile Remove(string path) => new()
    {
        Path = path,
        DeletionTimestamp = 1700000002000,
        DataChange = true,
    };

    private LogCommitter Committer() => new(_log, new LogCommitOptions { CheckpointInterval = 0 });

    private async Task<DeltaConflictException> ConflictFrom(LogCommitRequest request) =>
        await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await Committer().CommitAsync(request));

    // ── the lost version slot: the one Replay ─────────────────────────────────────────────────────

    [Fact]
    public async Task LostVersionSlot_IsConcurrentWrite_AndReplayable()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]);

        var conflict = await ConflictFrom(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
            MaxAttempts = 1, // no retry, so the raw slot race reaches the caller
        });

        Assert.Equal(DeltaErrorCodes.ConcurrentWrite, conflict.ErrorCode);
        // The only kind whose staged work survives: nothing was validated, the version was just taken.
        Assert.Equal(ConflictRecovery.Replay, conflict.Recovery);
        Assert.Equal(1, conflict.AttemptedVersion);
        // Nothing examined the concurrent commit, so there is no verdict about which one is to blame.
        Assert.Null(conflict.ConflictingVersion);
    }

    // ── the checker's five verdicts ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentMetadataChange_IsMetadataChanged()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1,
        [
            snapshot.Metadata with
            {
                Configuration = new Dictionary<string, string> { ["delta.appendOnly"] = "true" },
            },
        ]);

        var conflict = await ConflictFrom(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
        });

        Assert.Equal(DeltaErrorCodes.MetadataChanged, conflict.ErrorCode);
        Assert.Equal(ConflictRecovery.Replan, conflict.Recovery);
        Assert.Equal(1, conflict.ConflictingVersion);
    }

    [Fact]
    public async Task ConcurrentProtocolChange_IsProtocolChanged()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1,
            [new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 3 }]);

        var conflict = await ConflictFrom(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
        });

        Assert.Equal(DeltaErrorCodes.ProtocolChanged, conflict.ErrorCode);
        Assert.Equal(ConflictRecovery.Replan, conflict.Recovery);
    }

    [Fact]
    public async Task ConcurrentDeleteOfAFileWeRead_IsConcurrentDeleteRead()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("read.parquet")]);
        snapshot = await SnapshotAsync();
        await _log.WriteCommitAsync(2, [Remove("read.parquet")]);

        var conflict = await ConflictFrom(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
            Reads = new ReadSet { Files = new HashSet<string>(StringComparer.Ordinal) { "read.parquet" } },
        });

        Assert.Equal(DeltaErrorCodes.ConcurrentDeleteRead, conflict.ErrorCode);
        Assert.Equal(2, conflict.ConflictingVersion);
    }

    [Fact]
    public async Task ConcurrentDeleteOfAFileWeAlsoDelete_IsConcurrentDeleteDelete()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("doomed.parquet")]);
        snapshot = await SnapshotAsync();
        await _log.WriteCommitAsync(2, [Remove("doomed.parquet")]);

        var conflict = await ConflictFrom(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Remove("doomed.parquet")],
            PlannedRemovePaths = new HashSet<string>(StringComparer.Ordinal) { "doomed.parquet" },
        });

        // Deliberately NOT the same code as a row-level collision: this is file granularity, and the
        // table layer's DELTA_ROW_LEVEL_CONFLICT means the reconciliation was tried and the ROWS
        // genuinely overlapped.
        Assert.Equal(DeltaErrorCodes.ConcurrentDeleteDelete, conflict.ErrorCode);
    }

    [Fact]
    public async Task ConcurrentAppendMatchingOurPredicate_IsConcurrentAppend()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet", minId: 0, maxId: 9)]);

        var conflict = await ConflictFrom(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
            Isolation = IsolationLevel.Serializable,
            Reads = new ReadSet { Predicates = [Ex.LessThan("id", LiteralValue.Of(50L))] },
        });

        Assert.Equal(DeltaErrorCodes.ConcurrentAppend, conflict.ErrorCode);
        Assert.Equal(1, conflict.ConflictingVersion);
    }

    // ── not a verdict: the actions simply cannot move ─────────────────────────────────────────────

    [Fact]
    public async Task NotRebaseSafe_IsRebaseUnsafe_AndStillReplan()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]);

        var conflict = await ConflictFrom(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
            RebaseSafe = false,
        });

        Assert.Equal(DeltaErrorCodes.RebaseUnsafe, conflict.ErrorCode);
        // Replan, not Replay — the checker found nothing wrong, so the WORK is fine and it is these
        // particular actions that cannot move version. Rebuilding them is what fixes it.
        Assert.Equal(ConflictRecovery.Replan, conflict.Recovery);
        Assert.Equal(1, conflict.ConflictingVersion);
    }

    // ── the mapping itself ────────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(ConflictType.MetadataChanged, DeltaErrorCodes.MetadataChanged)]
    [InlineData(ConflictType.ProtocolChanged, DeltaErrorCodes.ProtocolChanged)]
    [InlineData(ConflictType.ConcurrentDeleteRead, DeltaErrorCodes.ConcurrentDeleteRead)]
    [InlineData(ConflictType.ConcurrentDeleteDelete, DeltaErrorCodes.ConcurrentDeleteDelete)]
    [InlineData(ConflictType.ConcurrentAppend, DeltaErrorCodes.ConcurrentAppend)]
    public void EveryVerdict_MapsToItsCode(ConflictType type, string expected) =>
        Assert.Equal(expected, new ConflictResult(type, 1, "m").ErrorCode);

    /// <summary>
    /// The guard that keeps the two vocabularies in step. <see cref="ConflictType"/> is closed but not
    /// frozen — a new rule would add a member, and a member with no code would reach callers as a null
    /// <see cref="DeltaConflictException.ErrorCode"/>, silently un-matchable. This fails the moment
    /// that happens instead.
    /// </summary>
    [Fact]
    public void EveryConflictType_ExceptNone_HasACode()
    {
        foreach (ConflictType type in Enum.GetValues(typeof(ConflictType)))
        {
            if (type == ConflictType.None)
                continue;
            string code = new ConflictResult(type, 1, "m").ErrorCode;
            Assert.StartsWith("DELTA_", code, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void None_HasNoCode_AndSaysSo()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => ConflictResult.None.ErrorCode);
        Assert.Contains("not a conflict", ex.Message, StringComparison.Ordinal);
    }

    // ── the legacy constructor ────────────────────────────────────────────────────────────────────

    [Fact]
    public void MessageOnlyConstructor_LeavesTheCodeNull_AndDefaultsToReplan()
    {
        // Kept because it is public API. A caller constructing one has no code to give, and the
        // conservative recovery is the one that does not invite replaying stale work.
        var ex = new DeltaConflictException("something moved");

        Assert.Null(ex.ErrorCode);
        Assert.Equal(ConflictRecovery.Replan, ex.Recovery);
        Assert.Null(ex.AttemptedVersion);
        Assert.Null(ex.ConflictingVersion);
    }
}
