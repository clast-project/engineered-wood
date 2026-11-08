# Embedding engineered-wood as a table-format engine

This guide is for a host that **owns its data plane** (its own Parquet codec, its own execution engine) and
wants engineered-wood only for the Delta *log*: snapshots, protocol conformance, transactions and conflict
resolution. DuckDB-style extensions are the motivating case. The pieces below compose, so a host can read,
write and mutate a Delta table without engineered-wood touching the bytes, and still get spec-conformant
commits.

## 1. Pin a snapshot

Plan, read, address and commit against **one** pinned version, and pass it explicitly to every step. A step
left to `CurrentSnapshot` can silently disagree with the others.

```csharp
await using var txn = table.StartTransaction();
var snapshot = txn.Snapshot;   // NOT table.CurrentSnapshot
```

Use `await using`: staging writes files (see [Abandoning a transaction](#abandoning-a-transaction)).

`DeltaTable.CurrentSnapshot` advances whenever another writer commits. `DeltaTransaction.Snapshot` does not,
so file ordinals mean the same thing at plan time and commit time.

### Opening a handle per transaction

A `DeltaTable` is one mutable handle: a refresh or commit on it moves `CurrentSnapshot` for everyone holding
it. To give each transaction its own handle without replaying the log each time, keep the last `Snapshot`
(it is immutable) and open from it:

```csharp
// One listing of _delta_log/, plus the commits since `cached`.
await using var table = await DeltaTable.OpenAsync(fs, cached, options);
cached = table.CurrentSnapshot;
```

The handle is at the table's LATEST version, as a fresh `OpenAsync(fs)` would be. It falls back to a full
open when continuing from `cached` cannot be shown correct: log cleanup removed commits after it, the table
was dropped and re-created at the path (detected by the log files' ETag, or size/modification time), or
`cached` cannot be matched to the log. Writers can use it too; a metadata change committed while `cached`
was held is in the new snapshot, not a conflict.

**Key the cache by table location.** A snapshot does not know which table it came from. One from another
table is usually detected and rebuilt, but not when the commit files are byte-identical (a copied log, or two
identical commits on S3, whose ETag is the content MD5).

This is not a pin. To commit against the version the work was planned on, see the next section.

### When the transaction starts later than the pin

A host whose transaction spans several statements pins a version at the first statement but may only open
the transaction at the flush. `StartTransaction()` bases on `CurrentSnapshot`, so validation would check
nothing. Base it on the planned version instead:

```csharp
// A version number, for a host that cannot keep the table open between statements.
var txn = await table.StartTransactionAsync(pinnedVersion);

// Or a snapshot you already hold (another transaction's, say). No I/O.
var txn = table.StartTransaction(pinnedSnapshot);
```

Both take an optional `IsolationLevel` and refuse a version ahead of the table. The snapshot form also
refuses a snapshot of a different table.

The difference is observable: a pinned transaction sees a concurrent delete of the same row and throws
`DeltaConflictException`; the same work on a current-based transaction finds the row already hidden,
reports zero rows deleted, and commits nothing without telling the host.

## 2. Plan the scan yourself

`DeltaTable.PlanFiles` returns the same superset-safe prune verdict the built-in reads use, without reading
data:

```csharp
IReadOnlyList<PlannedFile> planned = table.PlanFiles(predicate, snapshot: txn.Snapshot);
```

Each `PlannedFile` carries the `AddFile` (path, partition values, stats, deletion vector, row-tracking
fields) and a `FileOrdinal`:

- **Ordinals are assigned before pruning**, so they ascend with gaps; a pruned file keeps its position.
- **Ordinals are snapshot-scoped.** They index the path-sorted active set, so a concurrent append can
  renumber them. Plan against the transaction's snapshot.

Deletion vectors are returned unresolved. Read them with the public `DeletionVectorReader` and exclude the
rows however your engine prefers; a `file_row_number NOT IN (…)` pushdown is usually cheaper than filtering
in C#.

## 3. Read, with the metadata you need

`DeltaTable.ReadAsync` is the read entry point; `DeltaReadOptions` carries everything that varies.

```csharp
await foreach (var batch in table.ReadAsync(new DeltaReadOptions
{
    Columns  = ["id", "payload"],
    Filter   = predicate,                       // same prune as PlanFiles
    Snapshot = txn.Snapshot,                    // the version you pinned (§1)
    Metadata = DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
}))
```

**Inside a transaction, always set `Snapshot`.** Unset, the read follows `CurrentSnapshot`. `AtVersion` is
the time-travel form for a caller with no snapshot; setting both throws.

`DeltaRowMetadata` is `[Flags]`. All kinds resolve from the same per-file read, so combining them costs one
pass and the values agree row for row.

| Flag | Appends | Use it for |
|---|---|---|
| `RowAddress` | `_ew_row_address` (Int64, non-null) | a host whose rowid must be one `BIGINT` |
| `Locator` | `{prefix}file_path`, `{prefix}row_index` | feeding the DML directly (§4) |
| `RowTracking` | `{prefix}row_id`, `{prefix}row_commit_version` | durable identity across rewrites |

`MetadataPrefix` (default `_metadata.`, matching Spark) renames the `Locator` and `RowTracking` columns;
`RowAddress` is not prefixed. A metadata name that collides with a table column is refused.

`GetReadSchema(options)` returns the schema `ReadAsync` will emit (projection plus metadata columns, in
order) without reading anything, for a scan that must advertise its schema at bind time. It honours
`Snapshot` but not `AtVersion`.

```csharp
Schema advertised = table.GetReadSchema(options);
```

`ReadAllAsync` and `ReadAtVersionAsync` are convenience wrappers and expose no metadata.

The change feed, `ReadChangesAsync(DeltaChangeReadOptions)`, takes `StartVersion` / `EndVersion` / `Columns` /
`Metadata` / `MetadataPrefix`. Only `RowTracking` is valid there; a `_change_data` file is not in the active
set, so the other two would address nothing.

## 4. Address rows

**`RowSelection` is the row-level DML key.** It is per data file, keyed by `add.path` exactly as the
snapshot records it, holding ABSOLUTE in-file positions (the Parquet row index, counting rows a deletion
vector hides). `DeleteRowsAsync`, `UpdateRowsAsync`, `ReadRowsAsync` and
`DeltaTransaction.StageRowDeletesAsync` take one. Three ways to build it:

```csharp
// From your own scan output: path -> positions.
RowSelection.ByPath(positionsByPath);

// From batches read with DeltaRowMetadata.Locator.
RowSelection.FromLocatorColumns(batches);

// From packed BIGINT addresses, resolved against THE SNAPSHOT THEY WERE MINTED AGAINST.
RowSelection.FromRowAddresses(addresses, txn.Snapshot);
```

### Why the key is a path

A `FileOrdinal` means something only in its own snapshot. A stale ordinal that is still in range silently
addresses a **different file**; one out of range silently selects nothing. So `FromRowAddresses` resolves
ordinals to paths at construction, against the snapshot you pass, and by default throws on an ordinal
outside the active set:

```csharp
RowSelection.FromRowAddresses(addresses, txn.Snapshot);                          // throws on stale
RowSelection.FromRowAddresses(addresses, txn.Snapshot, StaleAddressPolicy.Skip); // skips instead
```

A stale *path* is reported too: if a concurrent commit removed or rewrote a file the selection names,
`DeleteRowsAsync` / `UpdateRowsAsync` / `ReadRowsAsync` throw naming it. Staged on a `DeltaTransaction`
instead, the commit loop reconciles that case (§6).

### The packing codec

A host whose rowid must be one `BIGINT` packs the pair with `TransientRowAddress`:

```csharp
long address = TransientRowAddress.Pack(fileOrdinal, positionInFile);
int  ordinal  = TransientRowAddress.FileOrdinal(address);
long position = TransientRowAddress.Position(address);
```

Use the helpers; the split (`TransientRowAddress.PositionBits`) is not part of any format. Convert to a
`RowSelection` before mutating.

> **An address, not an identity.** `_ew_row_address` says where a row sits in one snapshot. A concurrent
> append renumbers ordinals and any rewrite moves positions. Never persist one or compare two from different
> snapshots. Delta's stable id is the `RowTracking` column `_metadata.row_id`, a different number.

### Reading selected rows

`ReadRowsAsync(selection, options)` reads exactly the selected rows. `DeltaRowReadOptions` has `Metadata`,
`MetadataPrefix` and `ResolveAgainst`. Set `ResolveAgainst` inside a transaction; without it the read follows
`CurrentSnapshot`, where a concurrent rewrite makes the selection's paths look stale.

Batching and deletion-vector filtering break positional correspondence, so ask for metadata columns and match
on a key, not on order:

```csharp
await foreach (var batch in table.ReadRowsAsync(
    selection,
    new DeltaRowReadOptions
    {
        Metadata = DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
        ResolveAgainst = txn.Snapshot,
    }))
```

`Locator` returns the `(add.path, absolute position)` pair; `RowAddress` returns it packed. `RowTracking`
returns each row's stable id and commit version: the materialized value where the file has one, otherwise
`baseRowId + position` / `defaultRowCommitVersion`. It is null only for a file that predates row tracking on
the table. On a table without row tracking, asking for it throws.

### Preserving identity across your own rewrite

A host-side UPDATE moves rows to a new file, so their ids can no longer be derived from position. Read the
stable ids and pass them back when writing the post-image:

```csharp
var originalIds = new List<long?>();
var postImages = new List<RecordBatch>();
await foreach (var batch in table.ReadRowsAsync(
    selection,
    new DeltaRowReadOptions { Metadata = DeltaRowMetadata.RowTracking, ResolveAgainst = txn.Snapshot }))
{
    var ids = (Int64Array)batch.Column(RowTrackingConfig.RowIdColumnName);
    for (int i = 0; i < batch.Length; i++)
        originalIds.Add(ids.IsNull(i) ? null : ids.GetValue(i));

    // Your engine's output, built to the TABLE's schema.
    postImages.Add(YourEngine.Apply(batch));
}

var files = await table.WriteDataFilesAsync(postImages, materializedRowIds: originalIds);
```

The ids go into the table's materialized row-id column (requires
`delta.rowTracking.materializedRowIdColumnName`), which spec readers honour over `baseRowId`. The commit
version is not materialized; it advances to the rewriting commit.

**Build the post-image to the table's schema.** A batch carrying a column the table does not declare (such
as a forwarded metadata column) is refused, naming the column. A batch with *fewer* columns is legal; absent
columns read as null.

## 5. Swap in your own codec

`IDataFileReader` and `IDataFileWriter` replace the built-in Parquet path, set through
`DeltaTableOptions.DataFileReader` / `DataFileWriter`. The library still applies deletion vectors,
schema-evolution backfill, partition-column materialization, type widening and row-tracking materialization
around your codec.

A `DataFileReader` is called for one file at a time unless you set `DeltaTableOptions.MaxConcurrentReads`
explicitly; setting it above 1 declares your reader safe to call concurrently. Batches it returns keep
whatever Arrow types it produces (the built-in reader returns every decimal as `Decimal128`). To bound memory
across tables and readers, share one `DeltaTableOptions.MemoryBudget`.

### The write path is value-blind

Between your batch and your `IDataFileWriter`, the library moves and renames columns but never inspects
their values. So you may present **your own physical representation** for a column whose Delta type you
declared. For example, if your Arrow boundary cannot carry variant's struct storage, exchange each value as
a blob and declare the column `variant` via `preAssignedSchema` or the Delta-typed overloads (the Arrow ones
would infer `binary`, permanently):

```csharp
await table.AddColumnAsync(new StructField
{
    Name = "payload", Type = new PrimitiveType { TypeName = "variant" }, Nullable = true,
});
```

**The read path is not symmetric.** A variant-declared column must arrive from your reader as the physical
struct-of-binary (or a `VariantArray`); anything else throws. Split your blob into `(metadata, value)` on
the way in and you get a canonical `VariantArray` back.

## 6. Stage work on the transaction

A host arrives with work already done, so it stages results rather than handing over batches and
predicates. (A `DeltaTransaction` also has computed operations, `WriteAsync`, `DeleteAsync`, `UpdateAsync`,
`MergeAsync` and `OverwriteWhereAsync`, for work engineered-wood should do itself.)

**Three prefixes, three retry contracts:**

| Prefix | Is | On a concurrent commit |
|---|---|---|
| `Stage*` | an **effect** (staged output) | rebased and retried |
| `Require*` | a **precondition** on the base version | re-checked every attempt; violation throws `InvalidOperationException`, not retried |
| `Declare*` | a **declaration** of what your scan read | widens the read set; what conflicts depends on the isolation level |

| Method | Stages |
|---|---|
| `StageDataFiles(files)` | Data files you already wrote (append) |
| `StageDataFilesAsync(files, bornDeleted:, identityValuesPreGenerated:)` | The same, with full parity to `CommitDataFilesAsync` |
| `StageRowDeletesAsync(selection)` | A deletion-vector DELETE of rows you identified (§4) |
| `StageRowDeletesAsync(selection, RowDeleteMode.CopyOnWrite)` | The same DELETE as a file rewrite, for a table without deletion vectors |
| `StageSchemaChange(change)` | An ALTER computed by `ComputeAddColumn` / `ComputeRenameColumn` / … |
| `StageChangeDataAsync(rows, changeType)` | Change Data Feed rows for the statement you ran |
| `StageActions(actions)` | Anything else, e.g. your own domain metadata |
| `RequireAppTransaction(appId, version, precondition)` | Idempotent-producer compare-and-set |
| `DeclareRead(predicate)` | What your scan's predicate depended on |
| `DeclareFilesRead(paths)` | The files it read (what `PlanFiles` returned) |
| `DeclareWholeTableRead()` | The same, when the scan had no bound |

Use `StageDataFiles` unless you need one of the async form's arguments:

- **`bornDeleted`**: rows this transaction inserted and then deleted, so they appear in no committed
  version. The add carries an inline deletion vector and `tightBounds=false` stats. Keyed by
  `WrittenDataFile.RelativePath` (which becomes `add.path`), and may name only files in the same call.
  Requires deletion vectors on the table.
- **`identityValuesPreGenerated`**: you called `GenerateIdentityValues` yourself. Without it an identity
  table's appends cannot be staged at all.

The async form also adds variant bounds missing from the files' stats, read from each footer.

Both staging calls take **`constraintsEnforcedByCaller`**. A table declaring CHECK constraints, invariants
or generated columns refuses staged files unless you pass `true`, asserting your engine already enforced
them. Nothing verifies the assertion. `DeltaTable.SupportsExternalDataFileCommit` reports `false` for such a
table.

```csharp
await using var txn = table.StartTransaction();

var planned = table.PlanFiles(predicate, snapshot: txn.Snapshot);
// ... your engine scans those files and decides what changes ...

var selection = RowSelection.FromRowAddresses(doomedAddresses, txn.Snapshot);

var files = await table.WriteDataFilesAsync(newRows);   // or your own writer
txn.StageDataFiles(files);
await txn.StageRowDeletesAsync(selection);

long version = await txn.CommitAsync();   // ONE atomic version
```

Build the selection against `txn.Snapshot`, so the rows the delete names agree with what the commit
validates. Each file may be deleted from or rewritten by only one staged operation per transaction; a
second one naming the same file throws, so pass all of a file's positions in one selection.

A staged delete defaults to deletion vectors and throws on a table without them. `RowDeleteMode.CopyOnWrite`
instead rewrites each touched file at staging time (an abort deletes the rewrite) and commits
`remove`(old) + `add`(rewritten).

**Change Data Feed.** A deletion-vector delete writes no change rows (the feed infers them); a copy-on-write
delete writes its own `delete` change file. Because a reader takes a commit's change files as its *whole*
feed when it has any, a commit that carries any change file (a copy-on-write delete, a computed
`DeleteAsync`/`UpdateAsync`, `StageChangeDataAsync`, a raw `cdc` action) also gets change rows written for
every staged append (`insert`) and deletion-vector delete (`delete`). If you stage an operation's change
rows yourself with `StageChangeDataAsync`, pass `changeDataStagedByCaller: true` to the call that staged the
operation (`StageDataFiles`, `StageDataFilesAsync`, or `StageRowDeletesAsync(selection, mode, ct,
changeDataStagedByCaller: true)`), or the rows appear twice. Actions staged with `StageActions` are yours to
cover.

### Abandoning a transaction

Staging writes files (appended Parquet, rewrite post-images, deletion vectors, change files) before
`CommitAsync` publishes anything. `DeltaTransaction` is `IAsyncDisposable`; disposing an uncommitted
transaction deletes what it wrote:

```csharp
await using var txn = table.StartTransaction();
```

`AbortAsync(ct)` is the same cleanup, called explicitly. Both are no-ops after a successful commit, and both
are best-effort: a failed delete is swallowed rather than masking the original error.

An abort does **not** delete:

- the data file a deletion-vector DELETE re-adds (it is live table data; only the `.bin` was written here);
- files you passed to `StageDataFiles` / `StageDataFilesAsync` (you wrote them, and may stage them again).

Without an abort or dispose, abandoned files stay until VACUUM's retention horizon passes.

### Abandoning a buffered write

`WriteDataFilesAsync` returns a plain list and keeps no handle, so the library cannot tell an abandoned write
from one not yet committed. Discard it yourself:

```csharp
var files = await table.WriteDataFilesAsync(batches);
try
{
    // ... more statements, your own validation ...
    await table.CommitDataFilesAsync(files, extraActions: dml);
}
catch
{
    await table.DiscardDataFilesAsync(files);   // reclaims bytes; not committing is the rollback
    throw;
}
```

- **Not committing is the rollback.** An unreferenced file is invisible to readers. `DiscardDataFilesAsync`
  only reclaims storage that would otherwise wait out `delta.deletedFileRetentionDuration`.
- **It refuses to delete a file the table references**, checking the log (not the handle's cached
  snapshot). Nothing is deleted when it refuses.

### Exactly-once producers

`RequireAppTransaction` commits the `txn` action recording your progress atomically with the data:

```csharp
// Optional pre-check, to skip writing a batch the table already holds.
if (txn.IsAppTransactionApplied("my-producer", batchId))
    return;

txn.RequireAppTransaction("my-producer", batchId, AppTransactionPrecondition.NotApplied);
```

The precondition is re-checked against every concurrent commit before each attempt; the pre-check is an
optimisation, not the guard. A violation throws `AppTransactionPreconditionException` (an
`InvalidOperationException`, deliberately not a retried `DeltaConflictException`) with `AppId`,
`RequiredVersion`, `Precondition` and `ActualPrevious`.

| Precondition | The table must record | Use it when |
|---|---|---|
| `None` (default) | anything | you implement your own policy |
| `Absent` | nothing | this is the producer's **first** batch |
| `Exactly(n)` | precisely `n` | batch boundaries can **move** across a restart |
| `NotApplied` | nothing, or `< version` | Delta-Spark's rule; batch boundaries are fixed |

`NotApplied` deduplicates a replay of the same batch but is **blind to a moved boundary**: with 1000
recorded, a producer restarting from a stale checkpoint at 800 and resubmitting 801–1300 passes it and
writes 801–1000 twice. `Exactly(800)` refuses that. Delta-Spark can rely on `NotApplied` because a streaming
`batchId` is bound to a fixed offset range; your own counter may not be.

Each appId is checked independently, and the first failure aborts the whole commit. One transaction may
require at most one version per appId.

### Declaring what you read

The commit loop sees what you *wrote*, never what your engine *read*, so declare it. The shapes answer
different questions; declare both when you have both:

| Declaration | Says | Catches a concurrent… |
|---|---|---|
| `DeclareRead(predicate)` | the rows my scan *would* have matched | **add** matching the predicate (`concurrentAppend`) |
| `DeclareFilesRead(paths)` | the files my scan *did* read | **remove** of one of those files (`concurrentDeleteRead`) |
| `DeclareWholeTableRead()` | everything | every add **and** every remove |

```csharp
var planned = table.PlanFiles(predicate, snapshot: txn.Snapshot);
txn.DeclareFilesRead(planned.Select(p => p.File.Path).ToList());   // what I read
txn.DeclareRead(predicate);                                        // what I would have read
```

`DeclareFilesRead` is keyed by `add.path`. Declaring three files of three hundred means a concurrent delete
in the other 297 does not abort you. It does not protect against concurrent adds (that is the predicate's
job), and if you really read the racer's files the abort is correct. A path not active at `txn.ReadVersion`
throws `ArgumentException`, and nothing is declared. Calling `DeclareWholeTableRead` as well simply keeps
whole-table.

Under `WriteSerializable` (the default) a concurrent *blind append* is exempt from `DeclareRead`; under
`Serializable` it conflicts. `DeclareFilesRead` behaves the same at both levels.

### What the commit records

```csharp
txn.Operation = "MERGE";          // null (default): the staged kind, or "WRITE" when mixed
txn.IsBlindAppend = false;        // null (default): recorded only if this transaction saw a read
txn.CommitProperties = props;     // commitInfo.userMetadata and other keys
```

**Set `IsBlindAppend = false` if your engine read the table** before staging an append. Left null, a staged
append records no `commitInfo.isBlindAppend`, and engineered-wood's own conflict checker infers an adds-only
commit as blind, exempting it from later writers' concurrent-append checks. Set `true` only when nothing was
read.

**The staged surface can express whatever `CommitDataFilesAsync` can**, except the overwrite/rewrite family
(see [Limits](#limits)). `StagedCommitParityTests` enforces this.

**Do not reimplement the commit loop.** `CommitAsync` checks conflicts against every version since the
transaction's base, rebases when nothing it read was invalidated, and retries. A staged delete composes with
a concurrent delete of *different* rows in the same file, and follows its rows by stable row id through a
concurrent compaction. Row ids stay correct across several staged operations and across rebases.

### When to drop to the lower layer

`ComputeDeletionVectorActionsAsync`, `RebaseDvDmlActionsAsync`, `CheckLogicalRebaseAsync` and
`CommitDataFilesAsync(expectedVersion:)` are the primitives `DeltaTransaction` is built from. They stay
public for a host that must own the retry loop (e.g. its own distributed commit protocol). Otherwise use the
transaction. `CommitDataFilesAsync` with `expectedVersion` makes one attempt and throws
`DeltaConflictException` with `DELTA_STALE_TRANSACTION_SNAPSHOT` if the table moved.

## Going lower still: committing without the table layer

A host that reads and writes its own Parquet, interprets its own schemas and prunes on its own does not need
`EngineeredWood.DeltaLake.Table`. `EngineeredWood.DeltaLake` alone can snapshot, commit and checkpoint:

```csharp
using Ex = EngineeredWood.Expressions.Expressions;

var log = new TransactionLog(fs);
var snapshot = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs), atVersion: null);

var committer = new LogCommitter(log, new LogCommitOptions { CheckpointInterval = 10 });

var result = await committer.CommitAsync(new LogCommitRequest
{
    BaseSnapshot = snapshot,
    Actions = [new AddFile { Path = "part-0.parquet", /* your writer's numbers */ }],
    // What you read. Blind is the default: it conflicts only with a metadata or protocol change,
    // a delete/delete, or a write to a domainMetadata domain it also writes.
    Reads = new ReadSet { Predicates = [Ex.LessThan("id", LiteralValue.Of(50L))] },
    Operation = "WRITE",
});

// result.Version is where it LANDED (a rebase can land later than snapshot.Version + 1);
// result.Snapshot is the state there.
```

On a collision the committer reads the intervening commits, asks `ConflictChecker` whether any invalidated
what you read or removed, and either aborts (first committer wins) or rebases and retries, up to
`LogCommitRequest.MaxAttempts` (default 100). It also refuses, before writing, a table whose writer features
this library does not implement (`LogCommitOptions.ValidateWriteProtocol`, default on); at this layer
nothing else checks that.

Two things to get right:

- **`Reads` is yours to declare.** An empty read set means "depended on nothing" and is taken at its word.
  `Predicates` are tested against a concurrent add's stats; `Files` names what you read; `WholeTable` is the
  safe blunt answer.
- **Actions tied to their landing version cannot be replayed.** If an `add` carries a `baseRowId`, or a
  deletion vector was computed against a specific file state, set `RebaseSafe = false` (a collision then
  aborts with `DELTA_REBASE_UNSAFE`) or supply an `ICommitRebaseHandler` to re-derive them.
  `RecomputeRebaseHandler` rebuilds the whole action list from the newest snapshot, which is also what an
  overwrite needs.

`DeltaFilePruner` is public at this layer, for partition and statistics skipping in a scan you plan
yourself.

### Handling a conflict

`DeltaConflictException` is classified:

```csharp
catch (DeltaConflictException e) when (e.Recovery == ConflictRecovery.Replay)
{
    // The actions are still valid; only the version they aimed at was taken. Re-attempt them.
}
catch (DeltaConflictException e)
{
    // Replan: re-read, recompute, commit that.
    // e.ErrorCode says what moved; e.ConflictingVersion says which commit did it.
}
```

`Replay` is rare: `LogCommitter` replays internally, so it escapes only when `MaxAttempts` runs out or when
you call `TransactionLog.WriteCommitAsync` yourself. Everything else is `Replan`; never replay work whose
premise is gone.

`ErrorCode` is a `DELTA_*` constant from `DeltaErrorCodes` or `DeltaTableErrorCodes`, one flat namespace
shared with format errors, so one `switch` covers both. Codes with a delta-spark counterpart
(`DELTA_CONCURRENT_WRITE`, `DELTA_METADATA_CHANGED`, `DELTA_PROTOCOL_CHANGED`,
`DELTA_CONCURRENT_DELETE_READ`, `DELTA_CONCURRENT_DELETE_DELETE`, `DELTA_CONCURRENT_APPEND`) use its name.
Engineered-wood's own include `DELTA_DOMAIN_METADATA_CONFLICT`, `DELTA_REBASE_UNSAFE`,
`DELTA_ROW_LEVEL_CONFLICT` (row-level reconciliation failed, e.g. the same row was concurrently changed) and
`DELTA_STALE_TRANSACTION_SNAPSHOT`. Match on the code, never the message.

Without the table layer you give up the Arrow read/write path, deletion-vector DML, compaction, vacuum, CDF
writing, identity columns, and row-level conflict reconciliation. `DeltaTable` remains the recommended
surface unless you specifically need to avoid it.

## CTAS: writing files before the table exists

A host streaming `CREATE TABLE AS SELECT` writes data files before commit 0. Under column mapping, physical
names are random, so if `CreateAsync` assigns them the files already written are orphaned. Assign the mapping
yourself, write against it, then create with the same schema:

```csharp
var (assigned, _) = ColumnMapping.AssignColumnMapping(SchemaConverter.FromArrowSchema(arrowSchema));
// ... your writer emits files using assigned's physical names ...

await using var table = await DeltaTable.CreateAsync(
    fs, arrowSchema, columnMappingMode: ColumnMappingMode.Name, preAssignedSchema: assigned);
```

`preAssignedSchema` is used verbatim; the max column id is derived from it, and a schema with no field ids
under a mapping mode is rejected. `OpenOrCreateAsync` takes it too, so a CTAS retried after a crash reopens
what the earlier attempt created.

## Limits

- **Full, partition and dynamic overwrites are not stageable.** They remove the whole active set (or
  partition), which a rebase cannot re-derive. Use `WriteAsync(batches, DeltaWriteMode.Overwrite)` /
  `DynamicOverwriteAsync`. A predicate overwrite (Spark's `replaceWhere`) is stageable:
  `DeltaTransaction.OverwriteWhereAsync` stages a predicate delete plus an append.
- **The rewrite family is not stageable.** A `dataChange=false` compaction or clustering OPTIMIZE embeds the
  attempted version's row-id high-water mark. Use `CompactAsync`.
- **Externally written files are refused on some tables.** IcebergCompat always; identity columns unless you
  pass `identityValuesPreGenerated`; CHECK constraints, invariants and generated columns unless you pass
  `constraintsEnforcedByCaller`. Check `DeltaTable.SupportsExternalDataFileCommit` before writing files.
- **A transaction is single-use and not thread-safe.** Many may race across threads; drive each from one. A
  commit that throws ends it; dispose it and start a new one.
- **A failed auto-committing operation leaves nothing behind.** `WriteAsync`, `UpdateAsync`,
  `DeleteRowsAsync`, `UpdateRowsAsync`, `CompactAsync` and the rest delete their own output when their
  commit does not land. Like an abort, they never delete a file a deletion vector masks, files a rewrite
  would have replaced, or anything the host wrote.

## See also

- `doc/delta-concurrency.md`: OCC, row-level DML, the stable-id remap
- `doc/codec-seam-investigation.md`: why `IDataFile*` exists, and its open contract obligations
- `doc/known-issues.md`: gaps a host is most likely to hit
