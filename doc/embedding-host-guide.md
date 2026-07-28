# Embedding engineered-wood as a table-format engine

Most callers want the whole library: hand it Arrow batches, get Delta semantics. This document is for the
other case — a host that already **owns its data plane** (its own parquet codec, its own execution engine)
and wants engineered-wood only for the *log*: snapshots, protocol conformance, transactions, and conflict
resolution. DuckDB-style extensions are the motivating case.

The pieces below are designed to compose. Used together they let a host read, write, and mutate a Delta table
without engineered-wood ever touching the bytes — while still getting spec-conformant commits.

## 1. Pin a snapshot

Everything else keys off one pinned version. Start a transaction and use its snapshot for every step:

```csharp
var txn = table.StartTransaction();
var snapshot = txn.Snapshot;   // NOT table.CurrentSnapshot
```

`DeltaTable.CurrentSnapshot` advances whenever another writer commits. `DeltaTransaction.Snapshot` does not,
which is what makes file ordinals (below) mean the same thing at plan time and at commit time.

## 2. Plan the scan yourself

`DeltaTable.PlanFiles` gives the same superset-safe prune verdict the built-in read paths use, without
reading anything:

```csharp
IReadOnlyList<PlannedFile> planned = table.PlanFiles(predicate, snapshot: txn.Snapshot);
```

Each `PlannedFile` carries the `AddFile` (path, partition values, stats, deletion vector, row-tracking
fields) and a `FileOrdinal`. Two properties matter:

- **Ordinals are assigned before pruning**, so the sequence is ascending but *gapped*. A pruned file still
  consumes its position, because the addressing domain is the full active set.
- **Ordinals are snapshot-scoped.** They are the file's index in the path-sorted active set, so a concurrent
  append can renumber everything after it. Plan against the transaction's snapshot and they stay valid.

Files are returned with their deletion vectors unresolved. Read the deleted positions with the public
`DeletionVectorReader` and exclude them however your engine prefers — pushing them down as a
`file_row_number NOT IN (…)` predicate is usually cheaper than filtering rows in C#.

## 3. Address rows

`FileOrdinal` plus an absolute in-file position is the coordinate the row-level APIs speak. The read paths
also emit it packed into a single value:

```
transient row id = (fileOrdinal << 40) | absolutePositionInFile
```

`ReadAllWithRowIdsAsync` surfaces it as the `_metadata.row_id` column, so a host can correlate rows it read
with the coordinates it later mutates. Note this is a *transient* address, valid only against the snapshot
it came from — distinct from row **tracking**, whose `baseRowId` is a stable identity that survives rewrites.

> **The name is shared, the number is not.** Spark's `_metadata.row_id` is the STABLE row-tracking id;
> engineered-wood's `ReadAllWithRowIdsAsync` column of the same name is the transient address above. Do not
> persist it or compare it across snapshots.

`ReadRowsByRowIdsAsync` reads exactly the rows for a set of those addresses, and reports both coordinates
back through out-parameters:

- `rowIdsOut` — each returned row's transient rowid. Batching and deletion-vector filtering both break any
  positional correspondence between what you asked for and what came back, so this is what you pair them by.
- `sourceRowTrackingOut` — each row's STABLE id and commit version: the materialized value where the file has
  one, otherwise the spec derivation `baseRowId + position` / `defaultRowCommitVersion`. Null only for a
  source that predates row tracking. This is the identity to carry through a rewrite.

### Preserving identity across your own rewrite

A host-side UPDATE moves rows to a new file, so their ids can no longer be derived from position. Read the
stable ids, then hand them back when writing the post-image:

```csharp
var tracking = new List<(long?[] Ids, long?[] Versions)>();
var postImages = new List<RecordBatch>();
await foreach (var batch in table.ReadRowsByRowIdsAsync(targets, sourceRowTrackingOut: tracking))
    postImages.Add(YourEngine.Apply(batch));

var files = await table.WriteDataFilesAsync(
    postImages, materializedRowIds: tracking.SelectMany(t => t.Ids).ToList());
```

The ids are written into the table's declared materialized row-id column, which a spec reader honors over the
add's `baseRowId`. They ride the partition split with their rows and stay out of the physical rename and the
statistics. The commit *version* is deliberately not materialized — it should advance to the rewriting
commit, which the add's `defaultRowCommitVersion` already says. Requires the table to declare
`delta.rowTracking.materializedRowIdColumnName`.

## 4. Swap in your own codec

`IDataFileReader` and `IDataFileWriter` replace the built-in parquet path, wired through `DeltaTableOptions`.
The library still handles deletion-vector filtering, schema-evolution backfill, partition-column
materialization, type widening, and row-tracking materialization around your codec — those are log
semantics, not file-format concerns.

## 5. Stage work on the transaction

This is the part that most repays reading. A host arrives with work **already done**, so it stages results
rather than handing over batches and predicates:

| Method | Stages |
|---|---|
| `StageDataFiles(files)` | Data files you already wrote (append-shaped) |
| `StageRowDeletesAsync(positionsByOrdinal)` | A deletion-vector DELETE of rows you identified |
| `StageSchemaChange(change)` | An ALTER computed by `ComputeAddColumn` / `ComputeRenameColumn` / … |
| `StageChangeDataAsync(rows, changeType)` | Change Data Feed rows for the statement you just ran |
| `StageActions(actions)` | Anything else — `txn` ids, your own domain metadata |

```csharp
var txn = table.StartTransaction();

var planned = table.PlanFiles(predicate, snapshot: txn.Snapshot);
// ... your engine scans those files and decides what changes ...

var files = await table.WriteDataFilesAsync(newRows);   // or your own writer
txn.StageDataFiles(files);
await txn.StageRowDeletesAsync(positionsByOrdinal);
await txn.StageChangeDataAsync(deletedRows, "delete");

long version = await txn.CommitAsync();   // ONE atomic version
```

**Do not reimplement the commit loop.** `CommitAsync` runs the same optimistic-concurrency loop the
built-in operations use: it checks conflicts against every version that landed since the transaction
started, rebases when nothing it read was invalidated, and retries. A staged delete composes with a
concurrent delete of *different* rows in the same file, and relocates its rows by stable row id if a
concurrent compaction rewrote the file away. None of that needs driving from outside.

Row ids stay correct across several staged operations: each reserves a contiguous range continuing from the
last, and a rebase re-derives the whole range against the advanced high-water mark.

### When to drop to the lower layer

`ComputeDeletionVectorActionsAsync`, `RebaseDvDmlActionsAsync`, `CheckLogicalRebaseAsync`, and
`CommitDataFilesAsync(expectedVersion:)` are the primitives `DeltaTransaction` is built from. They remain
public for a host that genuinely needs to own the retry loop — one driving its own distributed commit
protocol, say. If you are using them to do what the table above describes, use the transaction instead: the
ordering, the read-set bookkeeping, and the row-id arithmetic are invariants the loop already enforces.

## CTAS: writing files before the table exists

A host streaming a `CREATE TABLE AS SELECT` wants its data files on storage before commit 0 is written. Under
column mapping that is a trap: physical names are random GUIDs, so if `CreateAsync` assigns them the files
already written are orphaned. Assign the mapping yourself, write against it, then create with the same schema:

```csharp
var (assigned, _) = ColumnMapping.AssignColumnMapping(SchemaConverter.FromArrowSchema(arrowSchema));
// ... your writer emits files using assigned's physical names ...

await using var table = await DeltaTable.CreateAsync(
    fs, arrowSchema, columnMappingMode: ColumnMappingMode.Name, preAssignedSchema: assigned);
```

`preAssignedSchema` is used verbatim — the max-column-id in the metadata is derived from it rather than
reassigned, and a schema with no field ids under a mapping mode is rejected rather than producing an
unreadable table. `OpenOrCreateAsync` takes it too, so a CTAS retried after a crash reopens what its earlier
attempt created.

## Limits

- **Overwrite modes are not stageable.** They remove the whole active set, which is exactly what a rebase
  cannot re-derive. Use the auto-committing `WriteAsync(mode: Overwrite)` / `DynamicOverwriteAsync`.
- **Identity columns and IcebergCompat reject externally-written files** — both need write-time per-row
  processing an outside writer did not do. Check `DeltaTable.SupportsExternalDataFileCommit`.
- **A transaction is single-use and not thread-safe.** Many transactions may race across threads; drive each
  from one.

## See also

- `doc/pr4-to-master-migration.md` — porting notes for the downstream patch set this seam grew out of
- `doc/slice9-concurrency-resume.md` — the concurrency work: OCC, row-level DML, the stable-id remap
- `doc/codec-seam-investigation.md` — why `IDataFile*` exists
