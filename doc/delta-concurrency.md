# Delta optimistic concurrency

**Status: complete**, apart from one tail item recorded at the end. This is the outcome record for
EngineeredWood's Delta concurrency support — the framing, what the machinery does, the design facts
that were established by *measurement* rather than reasoning, and the entry points. Several of the
facts below were wrong when reasoned from first principles; the "Measure, don't assume" section says
which.

## The framing

This is **optimistic-concurrency (OCC) correctness**, not multi-statement transactions. The heart is
the classic Delta OptimisticTransaction: a transaction records the version it read from, does its
work, and at commit validates that nothing it read was invalidated by a commit that landed in
between — aborting only on a real conflict, otherwise rebasing onto the newer version and
committing.

Before this work EngineeredWood was **safe but fragile**: `TransactionLog.WriteCommitAsync` used an
atomic write-temp-then-rename and threw `DeltaConflictException` on a name collision, but nothing
caught that exception. No retry, no read-set validation, no rebase. The atomic rename prevented lost
updates, but any concurrent commit failed an in-flight write even when there was no real conflict.
This work upgraded fragile-but-safe to robust-and-correct; it was never fixing active corruption.

## The three layers

- **Layer 1 — OCC correctness.** Read-version tracking, the `ConflictChecker`, bounded rebase-retry.
  This is what makes concurrent writers safe.
- **Layer 2 — ConflictChecker parity.** The verdict rules themselves.
- **Layer 3 — row-level concurrency** (a Databricks extension beyond OSS): two writers touching
  **disjoint rows of the same file** both land instead of the second aborting. OSS Spark and delta-rs
  conflict at file granularity here, so a file-level abort is spec-correct — just not maximally
  permissive.

All three are implemented. Layer 3 covers both of its sub-problems: DELETE/DELETE deletion-vector
union, and remap-across-rewrite by stable row id.

## What the machinery does

**Deletion-vector union (disjoint deletes of one file).** DELETE is DV-based and does not rewrite the
file, so row positions are stable. `ComputeDeleteActionsAsync` records each file's newly-deleted
positions as a `DeleteDvEdit`; on a collision `CommitOccAsync` calls `ResolveRowLevelDeletesAsync`,
which re-stages each edited file as `RemoveFile(path, currentDV) + AddFile(path, currentDV ∪ ourRows)`
against the *latest* snapshot. Disjoint rows → both land. Same row → abort. `ConflictChecker.Check`
takes `rowLevelResolvedPaths` so the reconciled file's own remove/re-add is not counted as a conflict.

**Remap across a rewrite.** A losing DELETE whose target file was concurrently compacted or
UPDATE-rewritten is relocated by **stable row id** onto the new file rather than aborting
(`RemapRowLevelDeletesAsync`). The row's commit version is the concurrent-modification discriminator:
a row that was merely relocated lands, while one concurrently updated or deleted raises a row-level
conflict. `ReadFileAsync` supplies each surviving row's absolute in-file position via the
`strippedAbsPositionsOut` out-param, which is what lets a delete's target positions be correlated
with the resolved stable ids.

**Row-tracking rebase.** An append or an UPDATE's copy-on-write post-image add re-derives its
`baseRowId` from the latest snapshot's high-water mark and its `defaultRowCommitVersion` from the
attempt version, then rebuilds the `delta.rowTracking` domain (`RebaseRowTrackingAddIds`). Adds
excluded from that re-derivation — a DV re-union re-add of an existing file, or a remap re-add on a
concurrently-rewritten file — already carry the correct id.

**Row-level reconciliation is bounded by the isolation level.** The gate is per RECONCILIATION KIND,
not per transaction. After `ResolveRowLevelDeletesAsync`, a `Serializable` commit narrows
`resolvedPaths` to the paths no concurrent commit removed with `dataChange=true`
(`KeepOnlyDataPreservingResolutions`). So the DV union — which would otherwise silence a
`dataChange=true` remove of a file we read, a conflict at both levels — stops applying, while the
remap across a `dataChange=false` compaction keeps working (that changes no contents and is already
read-exempt at both levels). Narrowing restores the ordinary checks for a path rather than forcing a
conflict, so a delete whose files nobody touched still rebases.

Deliberately **not** done: blinding the whole `ReadSet` when a row delete is staged. WriteSerializable
exempts a concurrent *blind append* and nothing else; blinding also drops the `Predicates` of
unrelated statements in the same transaction, admitting a concurrentAppend that both levels catch —
pinned by `StagedRowDelete_DoesNotExemptTheTransactionsReadPredicates`.

## Derived from the actions, or declared

`ConflictChecker.Check` takes the actions the transaction is about to commit, and reads two things off
them: whether it changes the metadata, and which paths it removes. Both used to be stated separately —
`plannedRemovePaths` as a parameter, `metadataChanged` not at all.

The test for which way a fact should travel is whether the actions record it:

| fact | source | why |
|---|---|---|
| removes | derived | a `RemoveFile` *is* the removal |
| metadata change | derived | a `MetadataAction` *is* the change |
| reads | **declared** (`ReadSet`) | a commit does not record what it looked at |
| `isBlindAppend` | **declared** | a property of the transaction's reads, same reason |

Deriving is not merely tidier. A restated fact can contradict the thing it restates, and this one did:
`plannedRemovePaths`'s documentation had to warn that naming a merely-*read* path there would report a
concurrent delete of it as delete/delete rather than the concurrentDeleteRead it is. The table layer
carried a paragraph explaining that its read-set had to be a *different object* from the removed-path set
for that reason. Derived from the actions, the mistake is unrepresentable — a read path is not a
`RemoveFile` — and both the warning and the paragraph are gone.

The converse holds for reads. `ReadSet.Blind` is the *default*, so it means "this caller said nothing",
not "this caller declares it read nothing"; sourcing a spec field off it would turn every silent caller
into an assertive one. That is why `isBlindAppend` is asked for rather than inferred here, even though a
later reader may end up inferring it (below).

## How "blind append" is decided

Blind-append is a property of the *writer's transaction* — `readPredicates.isEmpty &&
readFiles.isEmpty` — not of the actions it emitted, so only the writer knows it, and Delta records the
answer in `commitInfo.isBlindAppend`. `ConflictChecker.IsBlindAppend` answers in three steps:

1. **A declared boolean flag wins**, including a `false` on a commit that contains nothing but adds.
   Delta itself only ever reads the flag.
2. **Absent or malformed, a `cdc` action means not blind.** A change-data file records row-level
   changes, so the statement located those rows and therefore read. This is the only *positive*
   evidence in the fallback; everything else is an absence.
3. **Otherwise infer from the shape**: at least one add, and no remove, metadata, or protocol action.

Step 3 errs unsafely on its own — `INSERT INTO t SELECT … FROM t` and an insert-only `MERGE` both emit
adds only and both plainly read — which is why step 1 exists and why Delta makes no inference at all.
We keep the fallback rather than following Delta into `getOrElse(false)` because two populations of
unflagged commit are not going away: everything committed before this library emitted the flag, and
**everything delta-rs writes**. delta-rs declares `isBlindAppend` on no commit shape at all —
measured, not read off its source, by `DeltaRsBlindAppendGroundTruthTests` — so on a table it maintains
the fallback is the whole answer, and step 2 is what keeps an insert-only `MERGE` there from being
exempted from a concurrent-append check.

The rule has exactly one implementation. `DeltaTable.CheckLogicalRebaseAsync` calls the same method,
after a period where it kept a second copy that disagreed.

### And one term that judges *us*

Being a blind append is not enough to earn the exemption; **this** transaction has to qualify for it too.
Delta's gate:

```scala
val addedFilesToCheckForConflicts = isolationLevel match {
  case WriteSerializable if !currentTransactionInfo.metadataChanged =>
    winningCommitSummary.changedDataAddedFiles
  case Serializable | WriteSerializable =>
    winningCommitSummary.changedDataAddedFiles ++ winningCommitSummary.blindAppendAddedFiles
  case SnapshotIsolation =>
    Seq.empty
}
```

Under `WriteSerializable`, a transaction that itself changes the metadata falls through to the
`Serializable` branch and examines concurrent blind appends too. The exemption is justified by a blind
append not having depended on anything we did — but a schema change is not local to the files we read,
and an append written against the *old* schema need not still be valid under the new one.

This is a different rule from "a concurrent `metaData` action conflicts unconditionally" (rules 1 and 2
above), which judges the *winning* commit. This one judges ours, and only widens which adds get examined
— it never manufactures a conflict on its own.

**Metadata, not protocol.** Delta's `metadataChanged` is `newMetadata.nonEmpty`, assigned by a loop whose
only case is `case m: Metadata`; a `Protocol` action never sets it (checked at `v4.0.0`). Including
protocol would be *stricter* than Delta — a transaction that only enables a table feature would start
conflicting with concurrent appends — and gratuitous strictness about concurrency is its own defect.

`ConflictChecker.ExamineConcurrentAdds` is the whole gate, and both paths call it. Sharing only the
blind-append *rule* while each path decided what to do with the answer is precisely how the previous
divergence went live, so the gate is shared too. `BlindAppendRebaseParityTests` pins the agreement.

`SnapshotIsolation` has no counterpart in our `IsolationLevel`, which is why the Scala reads as three
cases and ours as two.

## Entry points

All of the OCC core lives in **`EngineeredWood.DeltaLake`** (the log layer), and is public: a host with
its own data plane can commit with real optimistic concurrency without taking the table layer.

- `Concurrency/ConflictChecker.cs` — pure, no I/O. Rules in order: metadata change, protocol change,
  delete/delete, concurrentDeleteRead (`dataChange=false` compaction exempt), concurrentAppend (blind
  append exempt under WriteSerializable). Modeled on Spark's `ConflictChecker`.
- `IsolationLevel.cs` — public enum, `WriteSerializable` (default) / `Serializable`. The two differ in
  exactly two places: whether a concurrent blind append matching read predicates conflicts, and the
  row-level reconciliation narrowing above.
- `Log/LogCommitter.cs` — the loop itself. Protocol gate, attempt, read `readVersion+1..latest` on a
  collision, rebase hook, conflict verdict, retry, post-commit snapshot refresh, checkpoint on
  interval. It never inspects the actions beyond handing them to the log.
- `Log/ICommitRebaseHandler.cs` — the seam for actions whose CONTENT is coupled to the version they
  land at. `RecomputeRebaseHandler` covers the common shape (re-derive from the newest snapshot); the
  table layer's `DeltaTable.OccRebaseHandler` implements the two hard ones, DV union/remap and
  row-tracking id re-derivation.
- `DeltaConflictException.cs` / `ConflictRecovery.cs` — what a conflict TELLS a caller.
  `ErrorCode` names the condition as a `DELTA_*` constant (six of them delta-spark's own names, checked
  against `error/delta-error-classes.json` in delta-spark 4.0.0); `Recovery` says whether the staged
  actions survive (`Replay`, only the lost version slot) or the plan has to be rebuilt (`Replan`,
  everything else); `ConflictingVersion` names the commit responsible, which the checker always knew
  and used to discard. `ConflictType` stays the checker's own closed vocabulary and is mapped to a code
  at one point, `ConflictResult.ErrorCode`.

In **`EngineeredWood.DeltaLake.Table`**:

- `DeltaTransaction.cs` — public; a thin recorder of staged actions plus the read-set. Several
  operations can be staged on one transaction; the accumulated `_operations` drives the commitInfo
  label (single-op → that op, mixed → `WRITE`).
- `DeltaTable.cs` — `StartTransaction()`, `CommitTransactionAsync` → `CommitOccAsync` (which builds the
  request and hands it to `LogCommitter`), and the compute halves each shared by an auto-committer and
  the transaction: `ComputeDeleteActionsAsync`, `ComputeWriteActionsAsync`, `ComputeUpdateActionsAsync`.
  `ValidateWritable(snapshot, isAppend)` is the shared write-precondition gate.

## Design facts worth keeping

- A DELETE's read-set is exactly the files it rewrites, so the removed paths serve as **both** the
  concurrentDeleteRead read-set and the delete/delete planned-removes.
- On a no-conflict rebase the staged actions are re-committed **verbatim** — valid precisely because
  "no conflict" means nothing the transaction read or removed was touched. No action re-resolution is
  needed at file-level granularity; that is only for row-level.
- The checker takes the concurrent commits as a parameter, so it stays pure and testable; the commit
  loop owns reading `readVersion+1..latest` from the log.
- Buffered and auto-commit DML share ONE remap implementation, so a host's multi-statement transaction
  composes through a concurrent OPTIMIZE instead of aborting. Conflicts stay distinguishable by
  message: concurrently deleted (id absent), concurrently updated (id present, commit version
  advanced), and no-row-tracking (stable ids unavailable).

## Measure, don't assume

This effort repeatedly found that reasoning about another implementation's behaviour was wrong, and
that measuring corrected it. Two standing examples:

- The VACUUM plan said the keep-set should protect unexpired `RemoveFile` tombstones. Measurement
  against Spark showed it **must not** — `VACUUM … RETAIN 0 HOURS` deletes a file orphaned seconds
  earlier with the tombstone still fresh, which is precisely what ends time travel past the retention
  window. Implementing tombstone protection made two existing tests fail, which is how the error
  surfaced.
- OSS Spark will **not** demonstrate Layer 3's both-land target behaviour, because it aborts too. So
  "does Spark also let them both land?" is a misleading measurement. The measurable cross-engine
  claims are that the *resulting* commit — a unioned or remapped deletion vector — is spec-legal and
  reads correctly, and that the row-tracking-through-rewrite artifacts match the protocol. Both were
  measured on Spark 4.0.1.

A rebased commit introduces **no new on-disk artifact**: it is byte-identical to a sequential one,
same add/remove actions with a higher version number. The only new policy is the blind-append conflict
rule, which lives in the checker.

## Constraints

- **The overwrite family does not rebase.** Full, partition-scoped and dynamic overwrite still
  single-attempt, because the remove-set is a read of the whole active-file set. Making it rebase-safe
  needs that read-set expressed as a partition predicate. This is the one remaining tail item, and it
  is unrelated to row tracking or the buffered seam.
- **`ArrowRowEvaluator` covers a bounded type set.** Bool, (u)int, float, double, string, binary, plus
  Date32/Date64/Timestamp and Decimal32/64/128/256. `DeltaFilePruner` (stats pruning) is independent
  and broader, so pruning can work where row evaluation does not.
- **Decimal arrays come back narrowed.** The Delta/Parquet reader narrows a decimal column to the
  smallest `Decimal{32,64,128,256}Array` that fits its precision, so a `decimal(12,2)` reads back as
  `Decimal64Array`. Anything consuming these must handle all four widths.

## Running the tests

- The concurrency unit and integration tests are pure and local — no external toolchain:
  `dotnet test test/EngineeredWood.DeltaLake.Tests -f net10.0 --filter "FullyQualifiedName~ConflictCheckerTests|FullyQualifiedName~LogCommitterTests"` (the verdicts and the loop, both log-layer)
  and `dotnet test test/EngineeredWood.DeltaLake.Table.Tests -f net10.0 --filter "FullyQualifiedName~DeltaTransactionTests"`
- Full validation uses the Delta interop tiers (delta-rs + PySpark). Setup and the
  `EW_REQUIRE_DELTA_INTEROP` / `EW_REQUIRE_SPARK_INTEROP` flags are in [`running-tests.md`](running-tests.md).
  Tier 3 needs `JAVA_HOME` (JDK 17+) and, on Windows, `HADOOP_HOME` with winutils on `PATH`.
- The Spark tier is slow. Filter out `SparkInteropTests` when iterating on non-interop code.

Row tracking has its own record in [`row-tracking-conformance-brief.md`](row-tracking-conformance-brief.md);
gaps and limitations are in [`known-issues.md`](known-issues.md).
