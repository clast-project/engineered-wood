# Embedding engineered-wood as a table-format engine

Most callers want the whole library: hand it Arrow batches, get Delta semantics. This document is for the
other case — a host that already **owns its data plane** (its own parquet codec, its own execution engine)
and wants engineered-wood only for the *log*: snapshots, protocol conformance, transactions, and conflict
resolution. DuckDB-style extensions are the motivating case.

The pieces below are designed to compose. Used together they let a host read, write, and mutate a Delta table
without engineered-wood ever touching the bytes — while still getting spec-conformant commits.

## 1. Pin a snapshot

Everything else keys off one pinned version, and **all four steps below take it explicitly** — plan, read,
address, commit. Any step left to `CurrentSnapshot` is a step that can silently disagree with the other
three.

```csharp
await using var txn = table.StartTransaction();
var snapshot = txn.Snapshot;   // NOT table.CurrentSnapshot
```

`await using`, because staging **writes files**: see [Abandoning a transaction](#abandoning-a-transaction).

`DeltaTable.CurrentSnapshot` advances whenever another writer commits. `DeltaTransaction.Snapshot` does not,
which is what makes file ordinals (below) mean the same thing at plan time and at commit time.

### When the transaction starts later than the pin

A host whose transaction spans several of its own statements pins a version at the *first* statement, but may
only open the transaction at the flush. `StartTransaction()` bases on `CurrentSnapshot`, which makes the
commit loop's validation **vacuous**: it asks what landed since the latest version, and the answer is
nothing. Base it on the version the work was actually planned against instead:

```csharp
// A version number is what a host that cannot keep the table open between statements can carry.
var txn = await table.StartTransactionAsync(pinnedVersion);

// Or, when you still hold the snapshot itself — another transaction's, say. No I/O.
var txn = table.StartTransaction(pinnedSnapshot);
```

Both refuse a version ahead of the table, and the snapshot form refuses a snapshot belonging to a *different*
Delta table — its active set is a different one, so every path, ordinal and row-id range derived from it would
address the wrong file with nothing looking wrong.

The difference is observable. A transaction pinned to the version its rows were addressed against sees a
concurrent delete of the same row and raises `DeltaConflictException`; the same work on a current-based
transaction finds the row already hidden, reports **zero rows deleted**, and commits nothing — the host is
never told another writer got there first. (Both behaviours are pinned by `PinnedVersionTests`.)

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
    Snapshot = txn.Snapshot,                    // read the version you pinned (§1)
    Metadata = DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
}))
```

**Inside a transaction, always set `Snapshot`.** Left unset the read follows `CurrentSnapshot`, so its rows —
and any address minted from them — can come from a version the transaction is not validating against.
`AtVersion` is the time-travel form for a caller with no snapshot in hand; setting both throws rather than
picking one, since two ways to name a version that can disagree is the hazard this removes.

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
time gets it here instead of paying for a metadata open. It honours `Snapshot` (resolving one costs no I/O,
so you get the pinned version's schema) but not `AtVersion`, which would need a log read.

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
> number, read as `DeltaRowMetadata.RowTracking` below. The two columns had the same name until recently,
> which read as a promise of durability the address cannot keep.

`ReadRowsAsync(selection, options:)` reads exactly the selected rows. Its options are `DeltaRowReadOptions` —
`Metadata` / `MetadataPrefix` / `ResolveAgainst`, the §3 vocabulary applied to this read. Set `ResolveAgainst`
inside a transaction: without it the read follows `CurrentSnapshot`, where a concurrent rewrite makes the
selection's paths look stale when they are exactly the ones the transaction is still validating against.

To pair returned rows with what you asked for, ask for metadata columns — batching and deletion-vector
filtering both break any positional correspondence, so match on a KEY rather than on order:

```csharp
await foreach (var batch in table.ReadRowsAsync(
    selection,
    new DeltaRowReadOptions
    {
        Metadata = DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,   // combinable, one pass
        ResolveAgainst = txn.Snapshot,
    }))
```

`Locator` gives back the same `(add.path, absolute position)` pair the selection is built on; `RowAddress`
gives it packed, for a host whose own rowid is one `BIGINT`. `RowTracking` gives each row's STABLE id and
commit version — the materialized value where the file has one, otherwise the spec derivation
`baseRowId + position` / `defaultRowCommitVersion`. Null only for a file that predates row tracking on the
table; on a table with no row tracking at all the ASK is refused, naming `Locator` / `RowAddress` instead,
rather than handing back a column of nulls that cannot be told apart from "not assigned yet".

### Preserving identity across your own rewrite

A host-side UPDATE moves rows to a new file, so their ids can no longer be derived from position. Read the
stable ids, then hand them back when writing the post-image:

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

The ids are written into the table's declared materialized row-id column, which a spec reader honors over the
add's `baseRowId`. They ride the partition split with their rows and stay out of the physical rename and the
statistics. The commit *version* is deliberately not materialized — it should advance to the rewriting
commit, which the add's `defaultRowCommitVersion` already says. Requires the table to declare
`delta.rowTracking.materializedRowIdColumnName`.

**The post-image must not carry the metadata columns.** They are an input to your engine, not part of its
output — so build the post-image to the table's schema, as your engine would anyway. Forwarding the read's
batch verbatim is **refused**, naming the column: both write paths reject a batch carrying a column the table
does not declare, because the parquet file would carry it while every Delta read projected it away, costing
bytes in every file with nothing reporting it. A batch with *fewer* columns than the table stays legal — an
absent column reads as null, which is a choice rather than a mistake.

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
rather than handing over batches and predicates.

**Three prefixes, three retry contracts.** The distinction is not cosmetic — it is what the commit loop does
when each one fails:

| Prefix | Is | On a concurrent commit |
|---|---|---|
| `Stage*` | an **effect** — staged output | rebased and retried |
| `Require*` | a **precondition** — a fact about the base version | re-checked every attempt; violation throws `InvalidOperationException` and is **not** retried |
| `Declare*` | a **declaration** — what your scan read | widens the read set; what conflicts with it depends on the isolation level |

A precondition cannot become true by retrying, which is why it does not raise `DeltaConflictException`: the
loop would retry that, and no amount of retrying makes an already-committed batch un-commit. A declaration is
the opposite — it is subject to *policy*, so the same declaration and the same racer can produce different
verdicts at different isolation levels.

| Method | Stages |
|---|---|
| `StageDataFiles(files)` | Data files you already wrote (append-shaped) |
| `StageDataFilesAsync(files, bornDeleted:, identityValuesPreGenerated:)` | The same, with full parity to `CommitDataFilesAsync` |
| `StageRowDeletesAsync(selection)` | A deletion-vector DELETE of rows you identified (§4) |
| `StageSchemaChange(change)` | An ALTER computed by `ComputeAddColumn` / `ComputeRenameColumn` / … |
| `StageChangeDataAsync(rows, changeType)` | Change Data Feed rows for the statement you just ran |
| `StageActions(actions)` | Anything else — your own domain metadata |
| `RequireAppTransaction(appId, version, precondition)` | Idempotent-producer compare-and-set |
| `DeclareRead(predicate)` | What your own scan depended on |
| `DeclareFilesRead(paths)` | The files it actually read — what `PlanFiles` just handed you |
| `DeclareWholeTableRead()` | The same, when the scan had no bound at all |

Use the plain `StageDataFiles` unless you need one of the async form's two arguments:

- **`bornDeleted`** — rows this transaction inserted and then deleted, so they never appear in *any*
  committed version. The add is born with an inline deletion vector (and `tightBounds=false` stats, which
  the spec requires once a vector hides rows the bounds were computed over) rather than the commit carrying
  an insert that a later one undoes. Keyed by `WrittenDataFile.RelativePath` — which is what `add.path`
  becomes — so it is a `RowSelection` like every other DML key. It can only name a file in the same call;
  these files are in no snapshot yet.
- **`identityValuesPreGenerated`** — you called `GenerateIdentityValues` yourself, so the per-row write-time
  work an outside writer skipped has already happened. Without it an identity table's appends **cannot be
  staged at all**, which meant a host on such a table had no transaction to put anything else into either.

```csharp
await using var txn = table.StartTransaction();

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

### Abandoning a transaction

Staging **writes files** — a staged append's parquet, a rewrite's post-image, deletion vectors, change
files — before `CommitAsync` publishes anything. So a transaction that is refused, conflicts, or is simply
dropped on an exception path has already put bytes on storage. `DeltaTransaction` is `IAsyncDisposable` for
that reason:

```csharp
await using var txn = table.StartTransaction();   // deletes what it wrote, if it never commits
```

`AbortAsync(ct)` is the same cleanup under a name, for a host that decides mid-transaction not to proceed.
Both are **no-ops after a successful commit** (those files are the table's data now) and both are
best-effort: a delete that fails is swallowed rather than allowed to mask the error that caused the abort.

Two things an abort deliberately does **not** delete, because they are not the transaction's to collect:

- the data file a deletion-vector DELETE re-adds — that parquet is live table data, and only the `.bin`
  beside it was written here;
- files you handed to `StageDataFiles` — you wrote them, before the transaction saw them, and you may well
  mean to stage them onto the next one.

Without an abort or a dispose, an abandoned transaction's files sit on storage until VACUUM's retention
horizon passes — for a crash-looping producer, a whole batch per restart.

### Exactly-once producers

`RequireAppTransaction` commits the `txn` action recording your progress **atomically with the data it
describes**, so there is no window in which one exists without the other:

```csharp
// Skip a batch the table already holds WITHOUT writing it: WriteAsync writes its parquet immediately, so a
// batch staged and then refused is parquet written for nothing (the abort deletes it again, but the write
// still happened).
if (txn.IsAppTransactionApplied("my-producer", batchId))
    return;

txn.RequireAppTransaction("my-producer", version: batchId, AppTransactionPrecondition.NotApplied);
```

The precondition is re-checked against every concurrent commit before each attempt — the pre-check above is
an optimisation, not the guard, and cannot close the race against a twin producer. A violation throws
`AppTransactionPreconditionException` (an `InvalidOperationException`, deliberately **not** a
`DeltaConflictException`, which the commit loop would retry) carrying `AppId`, `RequiredVersion`,
`Precondition` and `ActualPrevious`, so a host need not parse the message.

Four preconditions, and which one you want depends on where your version numbers come from:

| Precondition | The table must record | Use it when |
|---|---|---|
| `None` (default) | anything — no check | you implement your own policy and just want the `txn` record |
| `Absent` | nothing at all | this is the producer's **first** batch |
| `Exactly(n)` | precisely `n` | your batch boundaries can **move** across a restart — see below |
| `NotApplied` | nothing, or `< version` | Delta-Spark's rule; batch boundaries are fixed |

`NotApplied` deduplicates a replay of the *same* batch and tolerates gaps, which is what you want when the
version is a dense counter. It is **blind to a replay whose boundary moved**: with 1000 recorded, a producer
that restarts from a stale checkpoint at 800 and resubmits 801–1300 passes `NotApplied` and writes rows
801–1000 a second time. `Exactly(800)` refuses it, because the producer's belief about where it left off is
precisely what the comparison tests. Delta-Spark can rely on `NotApplied` alone because its version is a
structured-streaming `batchId` bound by the checkpoint to a fixed offset range; if you pick your own counter
you have no such guarantee.

Requirements for different appIds in one transaction are judged independently and the first failure aborts
the whole commit — a commit is atomic, so the ones that hold cannot be applied on their own.

### Declaring what you read

The commit loop can see what you *wrote*, but never what your engine *read* — so a scan's dependencies have
to be declared. Three shapes, and they are not alternatives to pick one of: they answer different questions,
and a scan that has answers to two should give both.

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

`DeclareFilesRead` is the middle ground, and the form you already hold — planning handed you exactly this
list. Keyed by `add.path`, the same key `RowSelection` speaks. Declaring three files of three hundred means a
concurrent delete in the other 297 does not abort you, which is the entire difference from
`DeclareWholeTableRead`.

Two things it deliberately does **not** do:

- **No protection against concurrent adds.** A file that did not exist when you scanned was not in your read
  set, so a file list cannot speak to phantom rows. That is what the predicate is for — hence declaring both
  above. They compose; they do not compete.
- **No rescue when you really did read everything.** If the declared set covers the racer's files, the abort
  is correct, and no declaration shape avoids it. (Spark behaves the same: `filterFiles()` over all files
  still aborts against a concurrent delete.)

A path that is not active at `txn.ReadVersion` throws `ArgumentException`, and nothing is declared when it
does. A stale path is *detectable*, unlike a stale ordinal, and one that matches nothing would silently
protect nothing. `DeclareWholeTableRead` is strictly stronger, so calling it alongside `DeclareFilesRead`
simply keeps whole-table rather than erroring.

Under `WriteSerializable` — the default — a concurrent *blind append* is exempt from `DeclareRead`; under
`Serializable` it conflicts. That difference is the levels' whole distinction, and it is why these are
`Declare*` and not `Require*`. `DeclareFilesRead` reads the same at both levels: a `dataChange=true` remove of
a file you read invalidates what you decided whichever level you are at.

### What the commit records

`Operation` is a property, not a per-call argument, because Delta's operation field is one string per commit:

```csharp
txn.Operation = "MERGE";   // null (the default) keeps the inference
```

Left null, a transaction that staged one kind of work reports that kind and a mixed one reports `"WRITE"` —
which is exactly when you want to say something better.

**Whatever the auto-committing surface can express, the staged surface can express.** That is an invariant,
not an aspiration: `StagedCommitParityTests` walks `CommitDataFilesAsync`' parameters by reflection and
requires each to be either mapped to a real member of `DeltaTransaction` or allow-listed with a reason. The
next capability added without a staged counterpart fails a build rather than waiting for a host to report
it. The allow-list holds only the overwrite/rewrite family — see **Limits**.

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
- **The rewrite family is not stageable either** — a `dataChange=false` compaction or a clustering
  OPTIMIZE removes the files it replaces, and a rewrite's fresh add embeds the attempted version's row-id
  high-water mark, so it cannot be replayed verbatim onto a newer one. Use `CompactAsync`.
- **IcebergCompat rejects externally-written files** — it needs write-time per-row processing an outside
  writer did not do. Check `DeltaTable.SupportsExternalDataFileCommit`. Identity columns are the same, with
  one escape: generate the values yourself and pass `identityValuesPreGenerated`.
- **A transaction is single-use and not thread-safe.** Many transactions may race across threads; drive each
  from one. A commit that throws ends it too — abort or dispose it and start a new one.
- **The auto-committing paths other than `DeleteAsync` still leak on a failed commit.** `WriteAsync`,
  `UpdateAsync`, `DeleteRowsAsync` and `CompactAsync` write their files and commit them without a
  transaction to hang the cleanup on, so a conflict there leaves vacuum-able orphans as before. Drive the
  work through a transaction if that matters to you. Tracked as issue #47.

## See also

- `doc/delta-concurrency.md` — the concurrency machinery: OCC, row-level DML, the stable-id remap
- `doc/codec-seam-investigation.md` — why `IDataFile*` exists, and its open contract obligations
- `doc/known-issues.md` — the gaps a host is most likely to hit
