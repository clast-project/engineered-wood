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
/// The log layer committing on its OWN — no table, no Arrow, no data plane. Every test here makes its data
/// files out of thin air (a Delta <c>add</c> is a path and some numbers; nothing opens the parquet) and
/// drives <see cref="LogCommitter"/> directly, which is exactly the position a host with its own codec and
/// execution engine is in.
///
/// <para>Under test is the loop AROUND <see cref="ConflictChecker"/> rather than the checker's verdicts,
/// which <see cref="ConflictCheckerTests"/> pins on their own: which collisions rebase and which abort,
/// what happens to the caller's actions in between, and what ends up on disk.</para>
///
/// <para>A "concurrent writer" here is just a commit written to the log before the one under test — the
/// committer cannot tell that from a real race, and it makes the interleaving exact instead of hoping two
/// tasks collide.</para>
/// </summary>
public class LogCommitterTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TransactionLog _log;

    public LogCommitterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_committer_{Guid.NewGuid():N}");
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

    // ── fixtures ───────────────────────────────────────────────────────────────────────────────────────

    private const string SchemaJson =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""";

    /// <summary>Writes version 0 (protocol + metadata) and returns the snapshot of it.</summary>
    private async ValueTask<Snapshot.Snapshot> CreateTableAsync(
        IReadOnlyList<string>? writerFeatures = null)
    {
        await _log.WriteCommitAsync(0,
        [
            writerFeatures is null
                ? new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }
                : new ProtocolAction
                {
                    MinReaderVersion = 3,
                    MinWriterVersion = 7,
                    ReaderFeatures = [],
                    WriterFeatures = writerFeatures,
                },
            new MetadataAction
            {
                Id = "committer-test",
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

    /// <summary>An <c>add</c> carrying id-range statistics, so a read predicate can be pruned against it.</summary>
    private static AddFile Add(string path, long minId = 0, long maxId = 9, bool dataChange = true) => new()
    {
        Path = path,
        PartitionValues = new Dictionary<string, string>(),
        Size = 100,
        ModificationTime = 1700000001000,
        DataChange = dataChange,
        Stats = $$$"""{"numRecords":10,"minValues":{"id":{{{minId}}}},"maxValues":{"id":{{{maxId}}}}}""",
    };

    private static RemoveFile Remove(string path, bool dataChange = true) => new()
    {
        Path = path,
        DeletionTimestamp = 1700000002000,
        DataChange = dataChange,
    };

    /// <summary>No checkpointing unless a test asks for it, so the log stays readable in the assertions.</summary>
    private LogCommitter Committer(LogCommitOptions? options = null) =>
        new(_log, options ?? new LogCommitOptions { CheckpointInterval = 0 });

    private string LogFile(string name) => Path.Combine(_tempDir, "_delta_log", name);

    // ── the base case ──────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Commit_LandsAtTheNextVersion_AndReturnsTheSnapshotOfIt()
    {
        var snapshot = await CreateTableAsync();

        var result = await Committer().CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("a.parquet")],
        });

        Assert.True(result.Committed);
        Assert.Equal(1, result.Version);
        // Returning a snapshot is half the point: it already reflects the commit, so a caller planning its
        // next one does not have to re-read the log to get a current view.
        Assert.Equal(1, result.Snapshot.Version);
        Assert.Contains("a.parquet", result.Snapshot.ActiveFiles.Values.Select(f => f.Path));
    }

    [Fact]
    public async Task Commit_WithNoActions_IsANoOp()
    {
        var snapshot = await CreateTableAsync();

        var result = await Committer().CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [],
        });

        Assert.False(result.Committed);
        Assert.Equal(0, result.Version);
        // Nothing to commit is not an empty version: version 1 must not exist.
        Assert.Equal(0, await _log.GetLatestVersionAsync());
    }

    [Fact]
    public async Task Commit_StampsCommitInfoWithTheOperation()
    {
        var snapshot = await CreateTableAsync();

        await Committer().CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("a.parquet")],
            Operation = "DELETE",
        });

        var info = Assert.Single((await _log.ReadCommitAsync(1)).OfType<CommitInfo>());
        Assert.Equal("DELETE", info.Values["operation"].GetString());
    }

    // ── rebase ─────────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Commit_RebasesPastANonConflictingConcurrentCommit()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]); // someone took our version

        // We read nothing and remove nothing, so their append cannot have invalidated us: the SAME actions
        // land one version later.
        var result = await Committer().CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
            Reads = ReadSet.Blind,
        });

        Assert.Equal(2, result.Version);
        var active = result.Snapshot.ActiveFiles.Values.Select(f => f.Path).ToList();
        Assert.Contains("ours.parquet", active);
        Assert.Contains("theirs.parquet", active); // the rebase kept their work rather than replacing it
    }

    [Fact]
    public async Task Commit_RebasesOntoTheLatestVersion_NotTheNextOne()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs-1.parquet")]);
        await _log.WriteCommitAsync(2, [Add("theirs-2.parquet")]);

        var result = await Committer().CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
        });

        // One retry, not two: the rebase jumps straight to the LATEST version rather than walking up one
        // collision at a time.
        Assert.Equal(3, result.Version);
    }

    [Fact]
    public async Task Commit_WhenNotRebaseSafe_AbortsOnACollisionItWouldHaveForgiven()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]);

        // Same blind read-set as above — the checker finds nothing wrong — but these actions encode the
        // version they were planned for, so landing them one version later would be quietly incorrect.
        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await Committer().CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("ours.parquet")],
                RebaseSafe = false,
            }));

        Assert.Contains("cannot be safely rebased", conflict.Message, StringComparison.Ordinal);
        Assert.Equal(1, await _log.GetLatestVersionAsync());
    }

    [Fact]
    public async Task Commit_WithOneAttempt_PropagatesTheRawCollision()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]);

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await Committer().CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("ours.parquet")],
                MaxAttempts = 1,
            }));

        // Unexamined: the version it wanted, not a verdict about what the other commit contained.
        Assert.Equal(1, conflict.AttemptedVersion);
    }

    [Fact]
    public async Task Commit_RecomputeRebase_RederivesTheActionsAgainstTheVersionItLandsOn()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]);

        // An overwrite: "remove every active file, add mine". Re-committing that verbatim would leave
        // theirs.parquet alive — it was not active when the removes were computed.
        async ValueTask<IReadOnlyList<DeltaAction>> BuildAsync(Snapshot.Snapshot s, CancellationToken ct)
        {
            var actions = new List<DeltaAction>();
            foreach (var f in s.ActiveFiles.Values)
                actions.Add(Remove(f.Path));
            actions.Add(Add("ours.parquet"));
            return actions;
        }

        var result = await Committer().CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = await BuildAsync(snapshot, default),
            Rebase = new RecomputeRebaseHandler(BuildAsync),
        });

        Assert.Equal(2, result.Version);
        Assert.Equal(["ours.parquet"], result.Snapshot.ActiveFiles.Values.Select(f => f.Path).ToList());
    }

    [Fact]
    public async Task Commit_RebaseHandler_SeesBothTheStagedAndTheAttemptedActions()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]);
        var observer = new RecordingRebaseHandler();

        await Committer().CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
            Rebase = observer,
        });

        var context = Assert.Single(observer.Contexts);
        Assert.Equal(0, context.BaseSnapshot.Version);
        Assert.Equal(1, context.LatestVersion);
        Assert.Equal(2, context.NextAttemptVersion);
        Assert.Equal(0, context.Attempt);
        // The commits it has to judge itself against, and the two views of its own work.
        Assert.Equal([1], context.Concurrent.Select(c => c.Version).ToList());
        Assert.Same(context.StagedActions, context.AttemptedActions);
        // NeedsLatestSnapshot is false on this handler, so the loop took the cheap path.
        Assert.Null(context.LatestSnapshot);
    }

    [Fact]
    public async Task Commit_RebaseHandler_ThrowingAConflict_Aborts()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]);

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await Committer().CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("ours.parquet")],
                Rebase = new ThrowingRebaseHandler("this rebase cannot be expressed"),
            }));

        // Not retried, and not translated: a rebase that failed will not succeed by being asked again.
        Assert.Equal("this rebase cannot be expressed", conflict.Message);
        Assert.Equal(1, await _log.GetLatestVersionAsync());
    }

    // ── verdicts the loop acts on ──────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Commit_AbortsOnAConcurrentMetadataChange()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1,
        [
            snapshot.Metadata with
            {
                Configuration = new Dictionary<string, string> { ["delta.appendOnly"] = "true" },
            },
        ]);

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await Committer().CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("ours.parquet")],
                Reads = ReadSet.Blind, // even a blind append cannot rebase past a metadata change
            }));

        Assert.Contains("changed the table metadata", conflict.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Commit_AbortsWhenAConcurrentCommitRemovedAFileItRead()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("read-by-us.parquet")]);
        snapshot = await SnapshotAsync();
        await _log.WriteCommitAsync(2, [Remove("read-by-us.parquet")]);

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await Committer().CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("ours.parquet")],
                Reads = new ReadSet { Files = new HashSet<string>(StringComparer.Ordinal) { "read-by-us.parquet" } },
            }));

        Assert.Contains("which this transaction read", conflict.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Commit_AbortsWhenAConcurrentCommitRemovedAFileItAlsoRemoves()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("doomed.parquet")]);
        snapshot = await SnapshotAsync();
        await _log.WriteCommitAsync(2, [Remove("doomed.parquet")]);

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await Committer().CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Remove("doomed.parquet")],
                PlannedRemovePaths = new HashSet<string>(StringComparer.Ordinal) { "doomed.parquet" },
            }));

        Assert.Contains("which this transaction also removes", conflict.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Commit_PrunesAConcurrentAppendAgainstTheReadPredicate()
    {
        var snapshot = await CreateTableAsync();
        // A concurrent append of ids 100..109 — outside what we read, so it cannot have changed our answer.
        // The committer builds a DeltaFilePruner from the base schema to establish that; with no pruner the
        // checker has to assume every add matches.
        await _log.WriteCommitAsync(1, [Add("theirs.parquet", minId: 100, maxId: 109)]);

        var result = await Committer().CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
            Isolation = IsolationLevel.Serializable,
            Reads = new ReadSet { Predicates = [Ex.LessThan("id", LiteralValue.Of(50L))] },
        });

        Assert.Equal(2, result.Version);
    }

    [Fact]
    public async Task Commit_AbortsWhenAConcurrentAppendMatchesTheReadPredicate()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet", minId: 0, maxId: 9)]);

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await Committer().CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("ours.parquet")],
                Isolation = IsolationLevel.Serializable,
                Reads = new ReadSet { Predicates = [Ex.LessThan("id", LiteralValue.Of(50L))] },
            }));

        Assert.Contains("matches this transaction's read predicates", conflict.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Commit_UnderWriteSerializable_ForgivesTheSameConcurrentBlindAppend()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet", minId: 0, maxId: 9)]);

        // Identical to the test above but for the isolation level — which is the whole difference between
        // the two levels, and it has to survive the trip through the request.
        var result = await Committer().CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
            Isolation = IsolationLevel.WriteSerializable,
            Reads = new ReadSet { Predicates = [Ex.LessThan("id", LiteralValue.Of(50L))] },
        });

        Assert.Equal(2, result.Version);
    }

    // ── the protocol gate ──────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Commit_RefusesATableWithWriterFeaturesItDoesNotImplement()
    {
        var snapshot = await CreateTableAsync(writerFeatures: ["someFutureFeature"]);

        var failure = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await Committer().CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("ours.parquet")],
            }));

        Assert.Equal(DeltaErrorCodes.UnsupportedFeaturesForWrite, failure.ErrorCode);
        // Refused BEFORE writing. The gate exists to keep us out of the log, not to report afterwards.
        Assert.Equal(0, await _log.GetLatestVersionAsync());
    }

    [Fact]
    public async Task Commit_CanBeAskedToSkipTheProtocolGate()
    {
        var snapshot = await CreateTableAsync(writerFeatures: ["someFutureFeature"]);

        var result = await Committer(new LogCommitOptions
        {
            CheckpointInterval = 0,
            ValidateWriteProtocol = false,
        }).CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
        });

        Assert.Equal(1, result.Version);
    }

    // ── preconditions ──────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Commit_AsksThePreconditionBeforeWritingAnything()
    {
        var snapshot = await CreateTableAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Committer().CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("ours.parquet")],
                Precondition = (_, _) => throw new InvalidOperationException("no"),
            }));

        Assert.Equal(0, await _log.GetLatestVersionAsync());
    }

    [Fact]
    public async Task Commit_ReAsksThePreconditionOnEveryRetry()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]);
        var asks = new List<int?>();

        var result = await Committer().CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
            Precondition = (_, seen) => asks.Add(seen?.Count),
        });

        Assert.Equal(2, result.Version);
        // The first ask has no concurrent commits to look at; the retry's does, and that is the point of
        // re-asking — a precondition is a fact about the table, not about the staged output.
        Assert.Equal([null, 1], asks);
    }

    [Fact]
    public async Task Commit_PreconditionFailureOnARetry_IsNotReportedAsAConflict()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]);

        // The distinction matters to a caller that retries on conflict: no amount of retrying makes an
        // already-recorded batch un-record itself.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Committer().CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("ours.parquet")],
                Precondition = (_, seen) =>
                {
                    if (seen is not null)
                        throw new InvalidOperationException("already applied");
                },
            }));
    }

    // ── the durability signal ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Commit_SignalsDurabilityOnce_EvenAfterALosingAttempt()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]);
        int durable = 0;

        await Committer().CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
            OnCommitDurable = () => durable++,
        });

        Assert.Equal(1, durable);
    }

    [Fact]
    public async Task Commit_DoesNotSignalDurabilityWhenItAborts()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("read-by-us.parquet")]);
        snapshot = await SnapshotAsync();
        await _log.WriteCommitAsync(2, [Remove("read-by-us.parquet")]);
        int durable = 0;

        await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await Committer().CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("ours.parquet")],
                Reads = new ReadSet { Files = new HashSet<string>(StringComparer.Ordinal) { "read-by-us.parquet" } },
                OnCommitDurable = () => durable++,
            }));

        Assert.Equal(0, durable);
    }

    // ── checkpointing ──────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Commit_WritesACheckpointOnTheInterval()
    {
        var snapshot = await CreateTableAsync();
        var committer = Committer(new LogCommitOptions { CheckpointInterval = 2 });

        for (int i = 0; i < 3; i++)
        {
            var result = await committer.CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add($"f{i}.parquet")],
            });
            snapshot = result.Snapshot;
        }

        // Versions 1, 2, 3 — only 2 is a multiple of the interval.
        Assert.False(File.Exists(LogFile("00000000000000000001.checkpoint.parquet")));
        Assert.True(File.Exists(LogFile("00000000000000000002.checkpoint.parquet")));
        Assert.False(File.Exists(LogFile("00000000000000000003.checkpoint.parquet")));
        // _last_checkpoint is what makes it findable without listing the whole log.
        Assert.True(File.Exists(LogFile("_last_checkpoint")));
    }

    [Fact]
    public async Task Commit_CheckpointsTheVersionItLandedOn_NotTheOneItAttempted()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("theirs.parquet")]);

        // Attempts 1, collides, rebases onto 2. The interval has to be judged against what happened, since
        // that is how the log is numbered — and a rebased commit is exactly where the two differ.
        await Committer(new LogCommitOptions { CheckpointInterval = 2 }).CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
        });

        Assert.True(File.Exists(LogFile("00000000000000000002.checkpoint.parquet")));
    }

    [Fact]
    public async Task Commit_CanOptOutOfCheckpointing()
    {
        var snapshot = await CreateTableAsync();

        await Committer(new LogCommitOptions { CheckpointInterval = 1 }).CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = snapshot,
            Actions = [Add("ours.parquet")],
            WriteCheckpointOnInterval = false,
        });

        Assert.False(File.Exists(LogFile("00000000000000000001.checkpoint.parquet")));
    }

    [Fact]
    public async Task Commit_CheckpointIsReadable_AndTheTableOpensFromIt()
    {
        var snapshot = await CreateTableAsync();
        var committer = Committer(new LogCommitOptions { CheckpointInterval = 1 });

        for (int i = 0; i < 2; i++)
        {
            var result = await committer.CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add($"f{i}.parquet")],
            });
            snapshot = result.Snapshot;
        }

        // The point of checkpointing at all: a fresh reader reconstructs the same state without replaying
        // every commit. Checked by rebuilding from scratch, which prefers the newest checkpoint.
        var reopened = await SnapshotAsync();
        Assert.Equal(2, reopened.Version);
        Assert.Equal(
            ["f0.parquet", "f1.parquet"],
            reopened.ActiveFiles.Values.Select(f => f.Path).OrderBy(p => p, StringComparer.Ordinal).ToList());
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────────

    private sealed class RecordingRebaseHandler : ICommitRebaseHandler
    {
        public List<CommitRebaseContext> Contexts { get; } = [];

        public bool NeedsLatestSnapshot => false;

        public ValueTask<CommitRebase> RebaseAsync(
            CommitRebaseContext context, CancellationToken cancellationToken)
        {
            Contexts.Add(context);
            return new ValueTask<CommitRebase>(new CommitRebase(context.StagedActions));
        }
    }

    private sealed class ThrowingRebaseHandler : ICommitRebaseHandler
    {
        private readonly string _message;

        public ThrowingRebaseHandler(string message) => _message = message;

        public bool NeedsLatestSnapshot => false;

        public ValueTask<CommitRebase> RebaseAsync(
            CommitRebaseContext context, CancellationToken cancellationToken)
            => throw new DeltaConflictException(_message);
    }
}
