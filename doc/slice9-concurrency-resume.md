# Resuming the Delta optimistic-concurrency work (PR #4 slice 9)

Handoff for continuing or completing EngineeredWood's Delta Lake concurrency support. Read this
first; it records the correct framing, what has landed, what remains, and the facts that were
established by *measurement* so they are not re-derived (several were wrong when reasoned from first
principles — see "Measure, don't assume" below).

## The correct framing

This is **optimistic-concurrency (OCC) correctness**, not multi-statement transactions. The PR's own
test naming (`BufferedTransactionTests`) over-emphasises fused multi-statement commits; that is one
*consumer* of the machinery, not its purpose. The heart is the classic Delta OptimisticTransaction: a
transaction records the version it read from, does its work, and at commit validates that nothing it
read was invalidated by a commit that landed in between — aborting only on a real conflict, otherwise
rebasing onto the newer version and committing.

### What master did before this work (verified)

- `DeleteAsync`/`WriteCoreAsync` compute actions against `CurrentSnapshot` and commit at
  `readVersion + 1`.
- `TransactionLog.WriteCommitAsync` uses an atomic write-temp-then-rename and throws
  `DeltaConflictException` on a name collision.
- **Nothing caught that exception.** No retry, no read-set validation, no rebase.

So master was **safe but fragile**: the atomic rename prevents lost updates, but any concurrent commit
failed an in-flight write even when there was no real conflict, and there was no notion of "did my
read stay valid". This work upgrades fragile-but-safe to robust-and-correct. It is *not* fixing active
data corruption.

## The three layers (and where "layer 1" really sits)

- **Layer 1 — OCC correctness** (the important part): read-version tracking + the ConflictChecker +
  bounded rebase-retry. This is what makes concurrent writers safe. **Steps 1 and 2 below are done.**
- **Layer 2 — full ConflictChecker parity**: the verdict rules. Landed as the checker in step 1.
- **Layer 3 — row-level concurrency** (Databricks extension): two deletes on the *same file* touching
  *disjoint rows* both land, instead of the second aborting. Needs DV re-union (`resolveAgainst:`) and
  remap-across-rewrite by stable row id. **Not started.** OSS Spark and delta-rs also conflict at file
  granularity here, so the current file-level abort is *correct*, just not maximally permissive.

The buffered multi-statement surface (`Compute*` schema ALTERs, identity chaining,
`ReconcileBatchToFields`, `ReadRowsByRowIds`) is **not needed for OCC correctness** and is deferred.

## What has landed

| Step | Commit | What |
|---|---|---|
| 1 | `705a4e2` | `ConflictChecker` (pure verdict function) + `IsolationLevel` + `ConflictCheckerTests` (9). |
| 2 | `b8e4fc6` | `DeltaTransaction` + `StartTransaction()` + the OCC commit loop + `DeltaTransactionTests` (4). |

Entry points:

- `src/EngineeredWood.DeltaLake.Table/Concurrency/ConflictChecker.cs` — pure, no I/O. Rules, in order:
  metadata change, protocol change, delete/delete, concurrentDeleteRead (dataChange=false compaction
  exempt), concurrentAppend (blind append exempt under WriteSerializable). Modeled on Spark's
  `ConflictChecker`.
- `src/EngineeredWood.DeltaLake.Table/IsolationLevel.cs` — public enum, `WriteSerializable` (default) /
  `Serializable`. The two differ only on whether a concurrent blind append matching read predicates
  conflicts.
- `src/EngineeredWood.DeltaLake.Table/DeltaTransaction.cs` — public; thin recorder of staged actions +
  read-set. `ReadVersion`, `IsolationLevel`, `DeleteAsync`, `CommitAsync`.
- `src/EngineeredWood.DeltaLake.Table/DeltaTable.cs` — `StartTransaction()`, the internal
  `CommitTransactionAsync` (the OCC loop), and `ComputeDeleteActionsAsync` (the extracted compute half
  of `DeleteAsync`, shared with the transaction).

Design facts worth keeping:

- A DELETE's read-set is exactly the files it rewrites, so the removed paths serve as **both** the
  concurrentDeleteRead read-set and the delete/delete planned-removes.
- On a no-conflict rebase the staged actions are re-committed **verbatim** — valid precisely because
  "no conflict" means nothing the transaction read or removed was touched. No action re-resolution is
  needed at file-level granularity (that is only for row-level, layer 3).
- The checker takes the concurrent commits as a parameter (stays pure/testable); the loop in
  `CommitTransactionAsync` owns reading `readVersion+1..latest` from the log.

## Deliberate limitations currently in place

1. **DELETE-only on the transaction.** Appends/updates cannot be staged on a `DeltaTransaction` yet
   (only the auto-committer writes). Adding them means extracting compute halves like
   `ComputeDeleteActionsAsync` — but `WriteCoreAsync` is large and complex (identity, row-tracking,
   column mapping, partition split, overwrite modes, repartition), so extract carefully.
2. **Row-tracking tables abort on rebase.** A rebased add's `baseRowId` would need recomputing against
   the advanced high-water mark. They still commit on an uncontended first attempt; on any rebase they
   abort with a clear message. Fail-safe.
3. **concurrentAppend is inert for DELETE.** `DeleteAsync` takes a functional
   `Func<RecordBatch, BooleanArray>` predicate, not an analyzable `Expressions.Predicate`, so the
   transaction cannot feed a read predicate to `DeltaFilePruner`. `ReadSet.Predicates` is left empty
   (NOT faked with `TruePredicate`, which would wrongly conflict disjoint deletes). Under the default
   WriteSerializable this is moot. The checker already supports the predicate path (unit-tested); it
   lights up for free once DELETE gains an expression-predicate overload.

## Remaining work, in suggested order

1. **Step 3 — route the auto-committers through the OCC loop.** Make single-shot `DeleteAsync` (and
   ideally the append path) get rebase-retry for free instead of throwing on any collision. `DeleteAsync`
   already delegates to `ComputeDeleteActionsAsync`, so wiring it to `CommitTransactionAsync`-style
   logic is small; the append path is the harder one (see limitation 1). Add a
   concurrent-disjoint-writes-both-land test.
2. **Appends/updates on the transaction** (closes limitation 1). Enables "two concurrent INSERTs both
   land" on the transaction API.
3. **Expression-predicate DELETE overload** (closes limitation 3) — makes concurrentAppend precise.
4. **Layer 3 — row-level concurrency.** The Databricks extension. Largest remaining piece; needs
   row-tracking-through-rewrite in EW's own rewrite path (confirm master's state first — the write path
   assigns baseRowId per file but the copy-on-write *rewrite* path likely does not preserve original
   ids). Corresponds to the 7 parked `RowLevelConcurrencyTests`.

The parked ledger is `test/EngineeredWood.DeltaLake.Table.Tests/PendingCoverageTests.cs`. The 7
`LogicalRebase` stubs are retired (now live in `ConflictCheckerTests` + `DeltaTransactionTests`); the
`RowLevelConcurrency`, `BufferedTxn`, `SetSchema`, and `CommitDataFiles` stubs remain.

## Measure, don't assume

This effort repeatedly found that reasoning about another implementation's behaviour was wrong, and
measuring corrected it. During slice 9 specifically: the VACUUM landing-notes plan said to protect
unexpired tombstones — Spark measurement showed it must not. Before starting layer 3, **verify against
Spark** (tier 3) rather than trust the PR notes or these notes. The interop tiers make measuring cheap;
use them.

## Running the tests

- Concurrency unit + integration tests are pure/local — no external toolchain:
  `dotnet test test/EngineeredWood.DeltaLake.Table.Tests -f net10.0 --filter "FullyQualifiedName~ConflictCheckerTests|FullyQualifiedName~DeltaTransactionTests"`
- Full validation uses the Delta interop tiers (delta-rs + PySpark). Setup and the
  `EW_REQUIRE_DELTA_INTEROP` / `EW_REQUIRE_SPARK_INTEROP` CI flags are in `doc/running-tests.md`. Tier 3
  needs `JAVA_HOME` (JDK 17+) and, on Windows, `HADOOP_HOME` with winutils on `PATH`.
- The Spark tier is slow (~25s); running the whole `DeltaLake.Table` suite twice across TFMs can exceed
  a 10-minute command budget. Filter out `SparkInteropTests` when iterating on non-interop code.

## Broader context

This slice 9 work is one thread of the larger PR #4 landing + Delta interop-hardening effort tracked in
`doc/upstream-landing-notes.md`. That doc also lists the unrelated remaining items (the ORC/Avro/Iceberg
sections of `doc/known-issues.md` were never verified against code; small interop divergences).
