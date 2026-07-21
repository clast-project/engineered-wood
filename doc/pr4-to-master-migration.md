# PR #4 → master: what diverged, why, and how to reconcile

**Audience.** The PR #4 author (and the downstream `fabricator-extension` that consumes engineered-wood). This
note summarizes how the PR #4 branch's Delta implementation differs from what actually landed on `master`, why
`master` chose differently, and — concretely — what to change to align.

**How this was produced.** A file-level and public-member diff of `pr-4` vs `master` for
`src/EngineeredWood.DeltaLake.Table`, plus a read of the diverging types. Companion docs:
[`codec-seam-investigation.md`](codec-seam-investigation.md) (the seam decision in depth) and
[`upstream-landing-notes.md`](upstream-landing-notes.md).

**Headline.** The two branches are at **full public-API-name parity** — every public member name on `pr-4`
also exists on `master`; `master` only *adds* (`StartTransaction`, `ComputeRenameField`, extra
`UpdateByRowIdsAsync` overloads). So there is almost nothing to rename. The real divergence is **three
architectural substitutions**, all where `master` picked a smaller or more general mechanism:

| Area | PR #4 construct | master construct | Kind |
|---|---|---|---|
| Copy-on-write rewrite | `IDataFileRewriter` + `DeltaTableOptions.DataFileRewriter` (execution seam) | `IDataFileReader` + `IDataFileWriter` (codec seam) **+** row-id DML | **removed / replaced** |
| Merge-on-read update | `UpdateViaVectorsAsync` (internal) | none — `UpdateByRowIdsAsync` (copy-on-write) or DV-delete + append | **dropped, substitute** |
| DV DML rebase | `CommitDvDmlWithRebaseAsync` (internal) | `CommitOccAsync(rowLevelDeletes:)` on the shared OCC loop | **refactored** |
| Variant plumbing | `VariantTransport` (internal) | `VariantColumnCoercion` + Arrow extension registry + `EmitVariantLogicalType` option | **refactored (internal)** |
| Concurrency | inline OCC | `ConflictChecker` / `DeltaTransaction` / `IsolationLevel` / `StartTransaction` | **extracted (additive)** |
| Nested-field ALTER, CDF write, buffered seam, gaps 1–4 | present | present (at parity) | **no change needed** |

Everything below expands the rows that require action.

---

## 1. The codec / rewrite seam — the one that actually matters

This is where the fabricator extension touches EW, and it is the substitution to understand first.

### What PR #4 did

`IDataFileRewriter.ReadRewriteAsync(fileOrdinal, sourceRelativePath, excludePositions, ct, rowTracking?)` is an
**execution** seam. For a copy-on-write DELETE/UPDATE, the *host* (DuckDB) reads the source parquet itself
(`read_parquet(… file_row_number)`), drops the excluded positions, applies the UPDATE `SET` substitution in SQL,
and returns the transformed rows as logical Arrow batches (optionally with the trailing row-tracking columns).
The Delta layer keeps column-mapping rename, stats, row-tracking materialization, the `remove`/`add` actions, and
the commit. It declined column-mapped and schema-evolved files (fell back to EW's own reader).

### What master does instead

master splits the same job into a **codec** seam plus the **row-id DML** surface:

- `IDataFileReader.ReadAsync(relativePath, physicalColumns, ct)` — host owns the raw parquet **decode** only
  (physical names, file order, DV rows *included*). Everything above the decode — DV filtering, logical rename,
  schema-evolution backfill, partition re-add, transient rowids, row-tracking — stays in EW.
- `IDataFileWriter.WriteAsync(batches, relativePath, ct)` — host owns the parquet **encode**. EW hands it the
  fully-prepared physical batch (partition split done, logical→physical rename done, row-tracking columns already
  materialized) and computes `add.stats` itself.
- The **row-level transform** (which `IDataFileRewriter` put in DuckDB SQL) stays as an EW-side delegate on the
  row-id DML methods.

Both seams are `[Experimental("EWDELTA0001")]` and set on `DeltaTableOptions.{DataFileReader,DataFileWriter}`.

### Why

From [`codec-seam-investigation.md`](codec-seam-investigation.md) §6–7: `IDataFileReader`/`IDataFileWriter` draw
the boundary at **parquet serialization** (Arrow batch ⇄ bytes), which is what "DuckDB writes the data,
engineered-wood writes the log" actually means once "data" is shrunk to "bytes". `IDataFileRewriter` draws it at
**query execution** — it delegates *semantics* (the predicate, the join, the substitution, schema-evolution
backfill, row-id computation). That is a categorically larger commitment, it required row-tracking-through-rewrite,
it declined column-mapped/evolved files anyway, and it *still* crossed the transformed batches back into C# for
stats + the writer — so it did not deliver the "bytes never leave DuckDB" win it looked like. The genuine
never-crosses path (a whole-query `COPY … SELECT read_parquet(…) …`) lives *outside* any batch-in/batch-out
interface, which is exactly what fabricator's `RunCopySql` already is.

### How to migrate

Your primary stated use case — **UPDATE a Delta table from a DuckDB join against it** — is now a first-class,
codec-only path. The shape:

1. Read with rowids: `ReadAllWithRowIdsAsync(...)` appends `_metadata.row_id = (fileOrdinal<<40)|absPos` (a
   transient rowid, valid within one snapshot). Hand those rows to DuckDB.
2. Run the join in DuckDB, producing `rowid → new SET values`.
3. Apply with **no substitution code** via the new convenience overload:

   ```csharp
   // `updates`: one row per changed rowid — a `_metadata.row_id` column + one column per SET column
   // (logical table-column names, matching Arrow types). Straight from the DuckDB join result.
   await table.UpdateByRowIdsAsync(updates /*, rowIdColumn: "_metadata.row_id"*/);
   ```

   EW reads the affected files (through your `IDataFileReader` if set), substitutes the SET columns keyed by rowid
   (type-agnostic, no per-type code), and rewrites them (through your `IDataFileWriter` if set) with stats,
   row-tracking, `remove`/`add`, and an OCC commit.

   If you want to own the row-building, use the delegate overload that hands you the per-row rowids:

   ```csharp
   await table.UpdateByRowIdsAsync(rowIds,
       (fileOrdinal, sourceBatches, rowIdsPerBatch) => /* return modified batches, keyed by rowid */);
   ```

Net: **delete `DataFileRewriter` from your options and your `IDataFileRewriter` implementation.** Keep (or add)
your `IDataFileReader`/`IDataFileWriter` for the codec inversion; move the DELETE/UPDATE transform out of DuckDB
SQL and into the row-id DML call (the join itself stays in DuckDB). The one thing you lose is DuckDB *executing*
the row transform; you keep DuckDB executing the *join* (the expensive, relational part), which is the actual
value. See the codec-seam doc §7 "Framing 1" for the full argument, and §7's MERGE note for composing
matched-update (this) + not-matched-insert (`WriteDataFilesAsync`/`CommitDataFilesAsync`) + matched-delete (the DV
path) into one commit via `extraActions`.

---

## 2. Merge-on-read UPDATE (`UpdateViaVectorsAsync`) — dropped; substitute

**PR #4** had `UpdateViaVectorsAsync`: on a DV table, UPDATE was merge-on-read — DV-delete the old rows and append
a small post-image file — so no full rewrite. **master does not have this.** master's row-id UPDATE
(`UpdateByRowIdsAsync`) is copy-on-write only; its DV path is delete-only (`DeleteByRowIdsViaVectorsAsync`).

**Migrate by** choosing per case:

- If a full rewrite of the touched files is acceptable, call `UpdateByRowIdsAsync` (copy-on-write).
- If you specifically want the merge-on-read shape (DV-delete + small append, DV as the default DML mode), build
  it from the primitives master *does* expose: `DeleteByRowIdsViaVectorsAsync(rowIds, …)` for the delete half, and
  `WriteDataFilesAsync` + `CommitDataFilesAsync` (fusing the DV remove/add and the post-image add into one commit
  via `extraActions`) for the append half. This is more explicit than PR #4's single method but uses only landed,
  tested surface.
- If merge-on-read UPDATE-by-rowid as a single call is worth having, it is a reasonable **additive** proposal for
  master (a `UpdateByRowIdsViaVectorsAsync` companion to the delete one) — flag it rather than reintroducing the
  rewriter to get it.

---

## 3. DV DML rebase (`CommitDvDmlWithRebaseAsync`) — refactored, no caller change

**PR #4** rebased concurrent DV DML through a dedicated internal `CommitDvDmlWithRebaseAsync`. **master** folded
the same behavior into the shared OCC commit loop: `CommitOccAsync(…, rowLevelDeletes: …)` resolves a row-level
collision by re-unioning each edited file's DV against the latest snapshot (`ResolveRowLevelDeletesAsync`), and a
losing DELETE across a concurrent rewrite is remapped by stable row id. This is **internal** — if you were calling
`CommitDvDmlWithRebaseAsync` from within a fork of `DeltaTable`, route through the row-id DML public methods
instead (they already invoke the OCC loop with the right read-set); no public caller change otherwise.

---

## 4. Variant (`VariantTransport` → `VariantColumnCoercion`) — internal, consumer-transparent

Both `VariantTransport` (PR #4) and `VariantColumnCoercion` (master) are **internal**, so a consumer never named
either. On master, Delta-layer variant support is: `SchemaConverter` maps the `"variant"` type both directions,
`variantType` is a supported reader+writer feature with a protocol upgrade, the Delta read path always registers
the Arrow variant extension (`WithVariantExtension`), and `DeltaTableOptions.EmitVariantLogicalType` (default
`true`) controls whether the parquet VARIANT annotation is emitted on write (turn it off for a
Spark-4.0.x-compatible unannotated layout). Preservation through compaction/CoW is covered by
`Compaction_OfVariantTable_PreservesValues` / `Delete_OnVariantTable_PreservesTheExtensionType` and cross-checked
in `VariantInteropTests`. **Migrate by** dropping any `VariantTransport`-specific wiring and relying on the
extension + `EmitVariantLogicalType`; variant "just works" on a Delta table now. (See the dated UPDATE in
`codec-seam-investigation.md` §5.1 — that section's earlier "variant is rejected at the schema layer" claim is
itself superseded.)

---

## 5. Concurrency (OCC) — extracted into named types; adopt or ignore

**PR #4** did optimistic concurrency inline. **master** extracted it: `Concurrency/ConflictChecker.cs` (a pure
verdict function), `DeltaTransaction.cs` + `StartTransaction()` (the public multi-statement transaction),
`IsolationLevel.cs`, and the `CommitOccAsync` loop that every auto-committer (`WriteAsync` blind-append,
`DeleteAsync`, `UpdateAsync`, the row-id DML) already routes through with rebase-retry. This is **purely additive**
— nothing you called was removed. If your fork open-coded conflict checks, delete them and lean on
`ConflictChecker`/`CommitOccAsync`; if you want explicit multi-statement transactions, use `StartTransaction()`.

---

## 6. Already at parity — no action

These landed on master with the same public names/shapes, so a consumer needs no change (recheck signatures if you
forked internals):

- **Nested-field ALTER** — `AddFieldAsync` / `RenameFieldAsync` / `DropFieldAsync` and the buffered
  `ComputeAddField` / `ComputeRenameField` / `ComputeDropField` (master adds `ComputeRenameField`).
- **CDF write** — `WriteChangeDataFileAsync`, and CDF is now spec-conformant on column-mapping + partitioned tables
  (physical-named `_change_data`, partition re-add on read), Spark-verified.
- **Buffered-transaction seam** — `WriteDataFilesAsync` / `CommitDataFilesAsync` / `DeferredSchemaChange` /
  `ComputeAddColumn`/`RenameColumn`/`DropColumn` / `SetSchemaAsync` / identity + DV compute methods / `ReadRowsByRowIdsAsync`.
- **Row-id read/delete** — `ReadAllWithRowIdsAsync`, `OrderedActiveBaseRowIdsAsync`,
  `DeleteByRowIdsViaVectorsAsync`, `DeleteByRowIdsAsync`.

---

## 7. Migration checklist

1. **Remove** `DeltaTableOptions.DataFileRewriter` and your `IDataFileRewriter` implementation.
2. **Keep / add** `IDataFileReader` + `IDataFileWriter` for the DuckDB codec inversion (unchanged contract; see the
   §5.3–6 obligations in `codec-seam-investigation.md` — decoded/on-disk `relativePath`, directory creation,
   `PARQUET:field_id` / `ARROW:extension:*` on handed-off batches).
3. **Replace** DuckDB-executed CoW DELETE/UPDATE with: `ReadAllWithRowIdsAsync` → DuckDB join → `UpdateByRowIdsAsync(updates)`
   (or the rowid-exposed delegate) for update; `DeleteByRowIdsViaVectorsAsync` / `DeleteByRowIdsAsync` for delete.
4. **Replace** `UpdateViaVectorsAsync` with `UpdateByRowIdsAsync` (CoW) or `DeleteByRowIdsViaVectorsAsync` + a
   post-image append via `CommitDataFilesAsync` (merge-on-read), or propose the additive
   `UpdateByRowIdsViaVectorsAsync`.
5. **Drop** any `VariantTransport` wiring; rely on the variant extension + `EmitVariantLogicalType`.
6. **Delete** any open-coded OCC; use `ConflictChecker`/`CommitOccAsync` (auto) or `StartTransaction()` (explicit).
7. **Leave** nested ALTER, CDF, the buffered seam, and row-id read/delete as-is — they are at parity.
