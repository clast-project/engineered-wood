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

## 3. Read, with the metadata you need

`DeltaTable.ReadAsync` is the one read entry point; `DeltaReadOptions` carries everything that varies.

```csharp
await foreach (var batch in table.ReadAsync(new DeltaReadOptions
{
    Columns  = ["id", "payload"],
    Filter   = predicate,                       // same superset-safe prune as PlanFiles
    AtVersion = pinnedVersion,                  // omit for the current snapshot
    Metadata = DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
}))
```

`DeltaRowMetadata` is `[Flags]`, and that is the point: the three kinds resolve from the *same* per-file
read, so asking for two costs one pass rather than two reads of the table — and the values agree row for
row, which two reads across a concurrent commit could not promise.

| Flag | Appends | Use it for |
|---|---|---|
| `RowAddress` | `_ew_row_address` (Int64, non-null) | a host whose rowid must be one `BIGINT` |
| `Locator` | `{prefix}file_path`, `{prefix}row_index` | feeding the DML directly (§4) |
| `RowTracking` | `{prefix}row_id`, `{prefix}row_commit_version` | durable identity across rewrites |

`MetadataPrefix` (default `_metadata.`, matching Spark's struct) renames the `Locator` and `RowTracking`
columns. `RowAddress` is not prefixed — it has no Spark counterpart to borrow a name from. A metadata name
that collides with one of the table's own columns is refused rather than shadowing it, which is what the
prefix is there to resolve.

**`GetReadSchema(options)` returns the schema `ReadAsync` will emit, without reading anything** — the same
projection, the same metadata columns, in the same order. A scan that has to advertise its schema at bind
time gets it here instead of paying for a metadata open.

```csharp
Schema advertised = table.GetReadSchema(options);
```

`ReadAllAsync` and `ReadAtVersionAsync` remain as convenience wrappers for the ordinary caller. They expose
no metadata; that is `ReadAsync`'s job.

The change feed takes the same treatment through `DeltaChangeReadOptions` — `StartVersion` / `EndVersion` /
`Columns` / `Metadata` / `MetadataPrefix`. Only `RowTracking` is valid there: a `_change_data` file is not in
the snapshot's active set, so neither address would name a row anything else could resolve.

## 4. Address rows

**`RowSelection` is the row-level DML boundary key**, and the only one. It is per data file, keyed by the
file's `add.path` exactly as the snapshot records it, holding the ABSOLUTE in-file positions selected —
absolute meaning the parquet row index, counting rows a deletion vector hides. `DeleteRowsAsync`,
`UpdateRowsAsync`, `ReadRowsAsync` and `DeltaTransaction.StageRowDeletesAsync` all take one.

There are three ways to build it:

```csharp
// Straight from your own scan output.
RowSelection.ByPath(positionsByPath);

// From batches read with DeltaRowMetadata.Locator — the _metadata.file_path / _metadata.row_index pair.
RowSelection.FromLocatorColumns(batches);

// From packed single-BIGINT addresses, resolved against THE SNAPSHOT THEY WERE MINTED AGAINST.
RowSelection.FromRowAddresses(addresses, txn.Snapshot);
```

### Why the key is a path

A `FileOrdinal` is the file's index in the *path-sorted active set*, so it means something only in the
snapshot it came from: a concurrent append inserts a path into the sort order and renumbers everything after
it. An ordinal that has gone stale but is still *in range* silently addresses a **different file**; one that
falls out of range silently selects **nothing**. Neither is detectable at the point of use.

`FromRowAddresses` resolves the ordinal to a path *at construction*, against a snapshot you pass explicitly.
That is where a stale address is caught, while you still have the context to explain it:

```csharp
// Default: throw, naming the ordinal and the size of the active set it fell outside.
RowSelection.FromRowAddresses(addresses, txn.Snapshot);
// Opt back into the old silent skip if you genuinely tolerate it.
RowSelection.FromRowAddresses(addresses, txn.Snapshot, StaleAddressPolicy.Skip);
```

A stale *path* is detectable, so the DML reports it rather than skipping: if a concurrent commit removed or
rewrote a file the selection names, `DeleteRowsAsync` / `UpdateRowsAsync` / `ReadRowsAsync` throw naming it.
Stage the delete on a `DeltaTransaction` instead and the commit loop reconciles that case for you (§6).

### The packing codec

A host whose own rowid must be one `BIGINT` — DuckDB's, say — packs the pair with `TransientRowAddress`:

```csharp
long address = TransientRowAddress.Pack(fileOrdinal, positionInFile);
int  ordinal  = TransientRowAddress.FileOrdinal(address);
long position = TransientRowAddress.Position(address);
```

Use the helpers rather than open-coding the shift — the split is `TransientRowAddress.PositionBits` and is
not part of the format. This is a *codec*, not the DML key: unpack it into a `RowSelection` before mutating.

> **An address, not an identity.** `_ew_row_address` says WHERE a row sits, and only in the snapshot it came
> from: a concurrent append renumbers ordinals, and any rewrite moves positions. Never persist one, never
> compare two from different snapshots.
>
> Delta's own stable id — Spark's `_metadata.row_id`, backed by row tracking's `baseRowId` — is a different
> number, reported by `sourceRowTrackingOut` below. The two columns had the same name until recently, which
> read as a promise of durability the address cannot keep.

`ReadRowsAsync(selection, sourceRowTrackingOut:)` reads exactly the selected rows.
`sourceRowTrackingOut` reports, per yielded batch and row-aligned with it, each row's STABLE id and commit
version: the materialized value where the file has one, otherwise the spec derivation `baseRowId + position`
/ `defaultRowCommitVersion`. Null only for a source that predates row tracking. This is the identity to carry
through a rewrite.

To pair returned rows with what you asked for, read with `DeltaRowMetadata.Locator` — batching and
deletion-vector filtering both break any positional correspondence, and the locator pair is the same key the
selection is built on.

### Preserving identity across your own rewrite

A host-side UPDATE moves rows to a new file, so their ids can no longer be derived from position. Read the
stable ids, then hand them back when writing the post-image:

```csharp
var tracking = new List<(long?[] Ids, long?[] Versions)>();
var postImages = new List<RecordBatch>();
await foreach (var batch in table.ReadRowsAsync(selection, sourceRowTrackingOut: tracking))
    postImages.Add(YourEngine.Apply(batch));

var files = await table.WriteDataFilesAsync(
    postImages, materializedRowIds: tracking.SelectMany(t => t.Ids).ToList());
```

The ids are written into the table's declared materialized row-id column, which a spec reader honors over the
add's `baseRowId`. They ride the partition split with their rows and stay out of the physical rename and the
statistics. The commit *version* is deliberately not materialized — it should advance to the rewriting
commit, which the add's `defaultRowCommitVersion` already says. Requires the table to declare
`delta.rowTracking.materializedRowIdColumnName`.

## 5. Swap in your own codec

`IDataFileReader` and `IDataFileWriter` replace the built-in parquet path, wired through `DeltaTableOptions`.
The library still handles deletion-vector filtering, schema-evolution backfill, partition-column
materialization, type widening, and row-tracking materialization around your codec — those are log
semantics, not file-format concerns.

### The write path is value-blind

Between your batch and your `IDataFileWriter`, the library moves and renames columns but never inspects what
is *in* them. So you may present **your own physical representation** for a column whose Delta type you have
declared — the partition split, the physical rename, and the statistics collector all pass it through.

This is what lets a host handle a representation the library has no mode for. The motivating case is
`variant`: if your Arrow boundary cannot carry the canonical struct storage, exchange each value as one
self-delimiting blob, convert on your own side, and declare the column `variant` via `preAssignedSchema` or
the Delta-typed `AddColumnAsync`/`ComputeAddColumn` overloads:

```csharp
await table.AddColumnAsync(new StructField
{
    Name = "payload", Type = new PrimitiveType { TypeName = "variant" }, Nullable = true,
});
```

Those overloads exist because the Arrow ones infer the Delta type from the Arrow field — a marker-tagged
binary column would be added as Delta `binary`, permanently, in a metadata commit.

**The read path is deliberately not symmetric.** A variant-declared column must arrive as the physical
struct-of-binary (or an already-wrapped `VariantArray`); anything else throws rather than emit a column that
contradicts the declared type. A host reader splits its blob into `(metadata, value)` — in-process work that
never crosses a foreign ABI — and gets a canonical `VariantArray` back, which it converts on its own side.

Both properties are pinned by `CodecSeamValueBlindnessTests`.

## 6. Stage work on the transaction

This is the part that most repays reading. A host arrives with work **already done**, so it stages results
rather than handing over batches and predicates:

| Method | Stages |
|---|---|
| `StageDataFiles(files)` | Data files you already wrote (append-shaped) |
| `StageRowDeletesAsync(selection)` | A deletion-vector DELETE of rows you identified (§4) |
| `StageSchemaChange(change)` | An ALTER computed by `ComputeAddColumn` / `ComputeRenameColumn` / … |
| `StageChangeDataAsync(rows, changeType)` | Change Data Feed rows for the statement you just ran |
| `StageActions(actions)` | Anything else — `txn` ids, your own domain metadata |

```csharp
var txn = table.StartTransaction();

var planned = table.PlanFiles(predicate, snapshot: txn.Snapshot);
// ... your engine scans those files and decides what changes ...

var selection = RowSelection.FromRowAddresses(doomedAddresses, txn.Snapshot);

var files = await table.WriteDataFilesAsync(newRows);   // or your own writer
txn.StageDataFiles(files);
await txn.StageRowDeletesAsync(selection);
await txn.StageChangeDataAsync(deletedRows, "delete");

long version = await txn.CommitAsync();   // ONE atomic version
```

Build the selection against `txn.Snapshot`, not `table.CurrentSnapshot` — that is what makes the rows the
delete names agree with what the commit validates.

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
