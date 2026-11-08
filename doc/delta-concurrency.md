# Delta optimistic concurrency

How EngineeredWood commits Delta transactions concurrently with other writers: what is checked, what is
retried, and where the code lives. One tail item remains (see [Constraints](#constraints)).

## The model

This is **optimistic concurrency (OCC)**, Delta's OptimisticTransaction: a transaction records the version it
read, does its work, and at commit checks whether any commit that landed in between invalidated what it read.
If so it aborts with `DeltaConflictException`; otherwise it rebases onto the newer version and commits.

Three layers, all implemented:

- **OCC.** Read-version tracking, `ConflictChecker`, bounded rebase-and-retry.
- **ConflictChecker parity** with Delta's verdict rules.
- **Row-level concurrency** (a Databricks extension beyond OSS): two writers touching **disjoint rows of the
  same file** both land. OSS Spark and delta-rs conflict at file granularity here, which is spec-correct but
  less permissive.

## The conflict rules

`ConflictChecker.Check` is pure (no I/O). For each concurrent commit, in order:

1. A `metaData` action conflicts (`DELTA_METADATA_CHANGED`).
2. A `protocol` action conflicts (`DELTA_PROTOCOL_CHANGED`).
3. A `domainMetadata` action for a domain this transaction writes or read conflicts
   (`DELTA_DOMAIN_METADATA_CONFLICT`). The row-tracking domain is exempt; it is re-derived on rebase.
4. **delete/delete:** a remove of a file this transaction also removes (`DELTA_CONCURRENT_DELETE_DELETE`).
5. **concurrentDeleteRead:** a `dataChange=true` remove of a file this transaction read
   (`DELTA_CONCURRENT_DELETE_READ`). A `dataChange=false` compaction is exempt.
6. **concurrentAppend:** a `dataChange=true` add matching this transaction's read predicates
   (`DELTA_CONCURRENT_APPEND`), subject to the blind-append gate below.

What the transaction *removes* and whether it *changes metadata* are derived from its own actions. What it
*read* (`ReadSet`) and whether it is a blind append are declared, because a commit does not record what it
looked at. `ReadSet.Blind` is the default and means "said nothing", not "read nothing".

### Isolation levels

`IsolationLevel` is `WriteSerializable` (default) or `Serializable`. They differ in two places: whether a
concurrent blind append matching the read predicates conflicts, and the row-level reconciliation limit below.
The table's `delta.isolationLevel` sets the level for every DML and append commit and for a transaction
started without one; a caller may ask for a stronger level, never a weaker one. Metadata-only commits and
OPTIMIZE do not consult it. An unrecognised value fails data-changing commits with
`DELTA_INVALID_ISOLATION_LEVEL`.

### Blind appends

A blind append (a writer whose transaction read nothing) is exempt from rule 6 under `WriteSerializable`.
`ConflictChecker.IsBlindAppend` decides whether a concurrent commit was one:

1. **A declared `commitInfo.isBlindAppend` wins**, including `false` on an adds-only commit.
2. **Absent or malformed, a `cdc` action means not blind**: the statement located rows, so it read.
3. **Otherwise infer from the shape**: at least one add, and no remove, metadata or protocol action.

Step 3 alone is unsafe (`INSERT INTO t SELECT … FROM t` and an insert-only `MERGE` emit only adds), which is
why the flag wins. The fallback is kept because unflagged commits are common: delta-rs never writes the flag
(measured by `DeltaRsBlindAppendGroundTruthTests`). A host staging an append after its own scan should set
`DeltaTransaction.IsBlindAppend = false`.

**This transaction must also qualify.** Delta's gate:

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

Under `WriteSerializable`, a transaction that itself changes the metadata examines concurrent blind appends
too: an append written against the old schema need not be valid under the new one. Only a `metaData` action
counts, not `protocol`, matching Delta (checked at `v4.0.0`). `ConflictChecker.ExamineConcurrentAdds` is
this gate, shared by every commit path (`BlindAppendRebaseParityTests`). Our `IsolationLevel` has no
`SnapshotIsolation`.

## Row-level reconciliation

These run in the table layer's rebase handler before the conflict check.

**Deletion-vector union.** A DV DELETE does not rewrite the file, so positions are stable. Each staged
delete records its newly deleted positions per file (`DeleteDvEdit`). On a collision,
`ResolveRowLevelDeletesAsync` re-stages each edited file against the latest snapshot as
`remove(path, currentDV) + add(path, currentDV ∪ ourRows)`. Disjoint rows: both land. Same row: abort with
`DELTA_ROW_LEVEL_CONFLICT`. The reconciled paths are passed to `ConflictChecker.Check` as
`rowLevelResolvedPaths` so their remove/re-add is not counted as a conflict.

**Remap across a rewrite.** A losing DELETE whose file was concurrently compacted or UPDATE-rewritten is
relocated by **stable row id** onto the new file (`RemapRowLevelDeletesAsync`). The row's commit version
tells the cases apart: a row merely moved lands; one concurrently updated or deleted is a row-level
conflict. Remap needs row tracking. Buffered and auto-commit DML share this implementation, so a host's
multi-statement transaction survives a concurrent OPTIMIZE.

**Row-tracking rebase.** An append's or UPDATE/MERGE post-image's add re-derives `baseRowId` from the latest
high-water mark and `defaultRowCommitVersion` from the attempt version, and the `delta.rowTracking` domain
is rebuilt (`RebaseRowTrackingAddIds`). DV re-adds and remap re-adds already carry correct ids.

**Serializable limits reconciliation.** Under `Serializable`, a resolution is refused if any resolved path
saw a `dataChange=true` add or remove in its lineage through the concurrent commits
(`EnsureNoDataChangeInResolvedLineage`; lineage is followed through `dataChange=false` rewrites). So DV
union no longer silences a data-changing remove of a file we read, while remap across a plain compaction
still works. It throws rather than narrowing the resolved set.

A staged row delete does **not** blind the transaction's `ReadSet`; that would drop the predicates of other
statements in the same transaction.

## Rebase

On a no-conflict rebase the staged actions are re-committed **verbatim**, except where a rebase handler must
re-derive version-coupled content (row ids, DV unions, remaps). A rebased commit is byte-identical in shape
to a sequential one; there is no new on-disk artifact.

A DELETE's read set is the files it rewrites, so its removed paths serve both rule 4 and rule 5. Its
concurrentAppend read set is its condition. The `Expressions.Predicate` overloads test a concurrent add's
stats against it. The delegate overloads cannot inspect theirs, so they set `ReadSet.OpaquePredicate`: every
concurrent `dataChange` add matches (still behind the blind-append gate) while removes stay scoped to the
files read. Consequently two delegate UPDATEs on different files conflict, as Spark's do on an unpartitioned
table.

## Entry points

The OCC core lives in **`EngineeredWood.DeltaLake`** and is public, so a host with its own data plane can
commit with full OCC without the table layer (see [`embedding-host-guide.md`](embedding-host-guide.md)).

- `Concurrency/ConflictChecker.cs`: the rules above, modelled on Spark's `ConflictChecker`. Takes the
  concurrent commits as a parameter, so it stays pure.
- `IsolationLevel.cs`, `IsolationLevelProperty.cs`: the levels and `delta.isolationLevel`.
- `Log/LogCommitter.cs`: the loop. Protocol gate, attempt, read `readVersion+1..latest` on a collision,
  rebase hook, verdict, retry (`LogCommitRequest.MaxAttempts`), post-commit snapshot refresh, interval
  checkpoint. Once the commit file lands only the refresh can throw; checksum, checkpoint and log-cleanup
  failures go to `OnPostCommitMaintenanceFailure` instead (as delta-spark's post-commit hooks do). An
  explicit `CheckpointAsync` still throws.
- `Log/ICommitRebaseHandler.cs`: the seam for actions whose content depends on their landing version.
  `RecomputeRebaseHandler` re-derives from the newest snapshot; the table layer's `OccRebaseHandler` does DV
  union/remap and row-id re-derivation. Without a handler, `RebaseSafe = false` aborts with
  `DELTA_REBASE_UNSAFE`.
- `DeltaConflictException.cs`, `ConflictRecovery.cs`: `ErrorCode` (a `DELTA_*` constant; those with a
  delta-spark counterpart use its name, checked against delta-spark 4.0.0's `error/delta-error-classes.json`),
  `Recovery` (`Replay`: only the version slot was lost; `Replan`: everything else), and
  `ConflictingVersion`.

In **`EngineeredWood.DeltaLake.Table`**:

- `DeltaTransaction.cs`: records staged actions and the read set. Several operations can be staged; the
  commitInfo operation is the single kind staged, or `WRITE` when mixed, unless `Operation` is set.
- `DeltaTable.cs`: `StartTransaction` / `StartTransactionAsync`, `CommitTransactionAsync` → `CommitOccAsync`
  (builds the request for `LogCommitter`), and the compute halves shared by auto-commit and transactional
  paths (`ComputeDeleteActionsAsync`, `ComputeWriteActionsAsync`, `ComputeUpdateActionsAsync`).

## Cross-engine validation

OSS Spark aborts where row-level concurrency lets both writers land, so "does Spark also land both?" is not
a meaningful test. What was measured (Spark 4.0.1) is that the resulting commits (a unioned or remapped
deletion vector, row tracking through a rewrite) are spec-legal and read correctly.

Measurement against Spark also showed VACUUM must **not** protect unexpired `remove` tombstones:
`VACUUM … RETAIN 0 HOURS` deletes a file orphaned seconds earlier.

## Constraints

- **The overwrite family does not rebase.** Full, partition-scoped and dynamic overwrites make one attempt,
  because their remove set is a read of the whole active set (or partition). Making them rebase-safe needs
  that read expressed as a partition predicate; this is the remaining tail item. A predicate overwrite
  (`OverwriteWhereAsync`, Spark's `replaceWhere`) is a predicate DELETE plus an append and rebases like any
  staged DML.
- **`ArrowRowEvaluator` covers a bounded type set:** bool, (u)int, float, double, string, binary,
  Date32/Date64/Timestamp, Decimal32/64/128/256. `DeltaFilePruner` (stats pruning) is broader, so pruning
  can work where row evaluation does not.
- **Decimal width depends on the reader.** The built-in reader returns every Delta decimal as `Decimal128`.
  A host `IDataFileReader` keeps whatever type it produces, so code consuming its batches must handle all
  four widths.

## Running the tests

- Unit and integration tests are local, no external toolchain:
  `dotnet test test/EngineeredWood.DeltaLake.Tests -f net10.0 --filter "FullyQualifiedName~ConflictCheckerTests|FullyQualifiedName~LogCommitterTests"`
  and `dotnet test test/EngineeredWood.DeltaLake.Table.Tests -f net10.0 --filter "FullyQualifiedName~DeltaTransactionTests"`.
- Full validation uses the Delta interop tiers (delta-rs + PySpark); setup and the
  `EW_REQUIRE_DELTA_INTEROP` / `EW_REQUIRE_SPARK_INTEROP` flags are in [`running-tests.md`](running-tests.md).
  The Spark tier needs `JAVA_HOME` (JDK 17+) and, on Windows, `HADOOP_HOME` with winutils on `PATH`. It is
  slow; filter out `SparkInteropTests` when iterating.

Row tracking: [`row-tracking-conformance-brief.md`](row-tracking-conformance-brief.md). Gaps:
[`known-issues.md`](known-issues.md).
