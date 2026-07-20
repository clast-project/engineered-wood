# Landing cmettler PR #4 — progress & remaining work

Working notes for the effort to land Christoph Mettler's PR #4
(`https://github.com/CurtHagenlocher/engineered-wood/pull/4`, branch `pr-4`, 55 commits,
~9.7k additions) onto `master` **slice by slice**. The PR's own review decomposition is in
`doc/upstream-candidates.md` on the `pr-4` branch (9 slices). Overall the PR is high quality —
mostly spec-compliance / interop bug fixes validated against Spark 4.x, delta-kernel-rs and DuckDB;
the full `pr-4` branch is green (DeltaLake 168, Table 182, Parquet 585/585).

**Slices are a review lens, not commit boundaries.** The 55 commits interleave themes, so most
slices are landed by reconstructing the net diff of the relevant files (or hand-porting a
cross-cutting thread) and committing as one focused change — not a clean cherry-pick. Commits are
authored as Christoph Mettler with a `Co-Authored-By: Claude` trailer and a body note where the
change diverges from the original PR.

---

## Remaining gaps at a glance (consolidated — as of 2026-07-20)

The bulk of PR #4 has landed. Slices 1–8, 10, 11 are in (table below), and **slice 9** (the strategic
optimistic-concurrency / row-level-concurrency thread) has since landed through **Layer 3 (A)** — see
`doc/slice9-concurrency-resume.md` for that whole arc (ConflictChecker, `DeltaTransaction`, auto-committer
OCC, analyzable-predicate DELETE/UPDATE, row-level DV union, and the opt-in deletion-vectors declaration).
What follows is the forward view; everything below this section is the chronological landing record.

**Open product decision**

1. **Codec seam — keep-and-fix vs revert `9302723`.** `IDataFileWriter`/`IDataFileReader` shipped on
   `DeltaTableOptions` with **no in-tree implementations** (only `CodecSeamTests`). Whether "engineered-wood
   as a Delta metadata engine with a bring-your-own Parquet codec" is a story to support is unresolved. If
   KEPT: fix the seam audit findings (`doc/codec-seam-investigation.md` §5.3–6 — `relativePath` encoding
   contract, partition-directory-creation obligation, the load-bearing `PARQUET:field_id`/`ARROW:extension:*`
   field-metadata contract, and partitioned `CodecSeamTests` coverage). **`IDataFileRewriter` (the *execution*
   seam) was deliberately NOT landed** — it delegates DML semantics, not encoding, and cannot be honoured
   without row-tracking-through-rewrite (now deferred, see #5/#8) and declines column-mapped/schema-evolved
   files. The `ProcessFileBatchesAsync` extraction underneath is behaviour-preserving and stays either way.
   Full framing: `doc/codec-seam-investigation.md` §6.

**Deferred features (parked as skipped tests in `PendingCoverageTests.cs` — 17 total)**

2. **Buffered multi-statement transaction seam** (10 × `BufferedTxn`). `WriteDataFilesAsync` /
   `CommitDataFilesAsync` / the `Compute*` family / `ReadRowsByRowIdsAsync` / `ReconcileBatchToFields`, and
   identity-value chaining across statements (a fused atomic ALTER+INSERT+DELETE at one version). Explicitly
   **not needed for OCC correctness**; deferred.
3. **`CommitDataFilesAsync(dataChange:, clusteringProvider:)` + the clustering rewrite-commit shape**
   (1 × `CommitDataFiles`). `WrittenDataFile.Tags` → `add.tags`. Slice-10 leftover, **gated on #2**.
4. **`SetSchemaAsync`** (2 × `SetSchema`). Adopt a whole incoming schema as a metadata-only commit (compute
   drops+adds; no-op when logically identical). Slice-8 leftover, standalone.
5. **Layer 3 (B) — row-level concurrency across rewrites** (4 × `RowLevelConcurrency`:
   `ConcurrentUpdateAndDelete_DisjointRows_BothLand`, `DeleteThroughConcurrentCompaction_Remapped`,
   `DeleteThroughCompaction_RowConcurrentlyDeleted_RowLevelConflict`, `BufferedFlow_…`). Deferred behind the
   row-tracking write fail-fast; needs a spec-conformant row-tracking writer first
   (`doc/row-tracking-conformance-brief.md`). `BufferedFlow_…` additionally needs #2.

**Standalone bugs / interop gaps (still live, not parked)**

6. **CoW UPDATE writes to the wrong directory on partitioned tables.** `ComputeUpdateActionsAsync`
   (`DeltaTable.cs:1683`) builds the rewrite filename as a bare `{Guid:N}.parquet` with **no partition
   subdirectory**, while its `add` carries `PartitionValues` and the append path uses
   `PartitionUtils.BuildPartitionPath`. Pre-existing, present in the built-in branch too, **no test**.
   Concrete and fixable now, independent of every decision above.
7. **Non-ASCII characters left literal in `add.path`** (Deferred follow-up B). Research DONE — the reference
   encoding is two-layer (Hive-escape then percent-encode), pinned by
   `DeltaRs_NonAsciiPartition_PathEncodingGroundTruth`; the fix to `DeltaPath.Encode` is **not applied**. Low
   urgency — delta-rs reads EW's literal form fine; it bites strict readers / byte comparisons and Spark
   parity.
8. **Row tracking is read-only** (2026-07-20). A spec-conformant writer (materialized-column naming, id
   preservation through rewrites, tier-3 Spark validation) is deferred and is the prerequisite for #5. Full
   brief: `doc/row-tracking-conformance-brief.md`.

**Resolved as "no action" (recorded so they are not re-litigated)**

- **Column-mapping protocol shape** (EW's legacy v2/v5 vs Spark's v2/writer-7 hybrid) — measured; recommend
  **leave it alone** (Spark reads EW's form; v3/v7 is strictly worse; delta-rs can't read column mapping
  either way). See "Column-mapping protocol shape" below.
- **Variant registration "bug"** — investigated and RETRACTED; EW fails closed. Variant is now a shipped
  feature (`VariantTests` / `VariantInteropTests`).
- **pr-4's `CheckpointReader` / `CompactionExecutor`** — deliberately NOT taken; they regress landed fixes
  (slice-3 `remove.deletionVector`, slice-4 path encoding). Never take wholesale.

---

## Landed on master (as of 2026-07-18)

| Slice | Commit(s) | Notes |
|---|---|---|
| 1 — Parquet writer correctness | `ced8d34`, `a742796` | Verbatim cherry-picks (null-struct alignment, all-null page → PLAIN, ExpandArray widths, VariantType.specification_version, ns-timestamp, deprecated min/max). |
| 2 — Spec DV serialization | `c4a5a07` | 64-bit RoaringBitmapArray + on-disk `.bin` framing. **Both legacy fallbacks removed** (library never shipped → no legacy EW DVs; fallbacks were fragile/OOM-prone). |
| 3 — Spec checkpoint content + tombstone DVs | `b41f5ad` | Preserve add.deletionVector/baseRowId/features/config, NULLABLE action structs, retained tombstones. **Added `remove.deletionVector` round-trip** beyond the PR. Row-tracking HWM reconciliation deliberately excluded (deferred to slice 5). |
| 4 — Spec path encoding (`DeltaPath`) | `74bc1aa` | Hive-escaped partition dirs + URL-encoded `add.path`, decoded at every read site + vacuum. Hand-ported cross-cutting wiring. **See non-ASCII follow-up below.** |
| 5 — writer features + protocol declaration | `cc8d6fe`, `c1b1474`, `70d2384`, `8e3fa8d` | `cc8d6fe`: ToArrowField preserves field metadata (PR #21, prerequisite). `c1b1474`: allowlist appendOnly/invariants/checkConstraints/generatedColumns + `HonorWriterFeatures` on Write/Delete/Update. `70d2384`: declare schema-driven features (timestampNtz/identityColumns/columnMapping-in-v7) at `CreateAsync` + protocol upgrade on `AddColumnAsync`. `8e3fa8d`: `delta.rowTracking` HWM emission + `SnapshotBuilder` reconciliation. **Did NOT allowlist variantType/rowTracking** as supported features (need later slices). |
| 6 — column mapping | `aa3f0e2`, `7a327f0`, `4754a72` | `aa3f0e2`: physical names in BOTH modes + new `ColumnMappingRecursive.ToPhysical/ToLogical` (nested struct children) + numeric `delta.columnMapping.id`. `7a327f0`: compaction re-stamps field ids & widens against the physical-named target schema. `4754a72`: `AddColumnAsync`/`RenameColumnAsync`/`DropColumnAsync` metadata-only + `SchemaEvolution.BackfillMissingColumns` read-path reconcile. **See slice-6 leftovers below.** |
| 7 (PARTIAL) — pluggable codec seam | `9302723` | `IDataFileWriter` + `IDataFileReader` on `DeltaTableOptions`, both default-null. Wired into the write path, the CoW UPDATE rewrite, compaction, and `ReadFileAsync` (via the extracted `ProcessFileBatchesAsync`). `CleanField` preserves `ARROW:extension:*`. **`IDataFileRewriter` NOT landed — see below.** |
| 8 (PARTIAL) — misc reader/DML correctness | `adc3b44`, `8cbf8d2`, `e025880`, `ebeb841`, `e83a232`, `d79abc7`, `1883f51`, `7bf3f6c`, `e0567cb`, `4cd1f06` | Thrift wire-type guards (+ALP test gating; Parquet 585/585, was 573). Empty-page snappy stream. ListVersions ascending. S3 conditional rename. Row-filter type coverage. Nested stats end-to-end. TIME→Time64. DV-qualified removes + compaction DV exclusion. `DecimalOutputKind` read option. Always-on commitInfo + `GetHistoryAsync`. **See slice-8 leftovers below.** |

| 10 — clustered (liquid) table interop | `093185f` | Allowlist the `clustering` writer feature (every write to a Databricks/Fabric CLUSTER BY table failed before), `CreateAsync(clusteringColumns)`, `SetClusteringColumnsAsync`, clustering/partitioning mutual exclusion, `UpgradeProtocolForWriterFeatures`. **Domain stores PHYSICAL names** (OSS Delta `None.get`-crashes otherwise). Interop only — writing clustered layouts is not implemented. |
| 11 — partitioned compaction (bugfix) | `e26a88f` | Candidates group by partition; each group compacts into its own Hive dir; target schema excludes partition columns. **Pre-existing data-corruption bug** (reproduced at `8d7ef32`): merged across partitions, stamped every row with `candidates[0]`, and threw IndexOutOfRange in the common shape. |

Verification standard for each: builds on net10.0/net8.0/netstandard2.0, Delta suites green on both
TFMs. GOTCHA: `parquet-testing` is a git submodule — worktrees don't auto-populate it, and without it
~98 Parquet reader tests spuriously fail (`git submodule update --init parquet-testing`, or test in
the main checkout).

## The inflection point (slice 5 onward)

Slices 1–4 were cleanly separable from the PR's central `DeltaTable.cs` write-path refactor
(`WriteCoreAsync` + the commit seams). Slice 5 onward weave *into* that refactor and into each other —
but slices 5 and 6 were nonetheless landed piecemeal without taking the refactor, by hand-porting each
thread onto master's existing write path. Slice 5 is now **complete**; the three pieces that were
deferred as refactor-/slice-6-coupled all landed once slice 6 supplied `AddColumnAsync`:

- ~~Create-time feature declaration~~ — `70d2384`. `variantType` omitted (no variant support on master;
  the `RequiredSchemaFeatures` hook is in place for when there is).
- ~~Row-tracking HWM emission + `SnapshotBuilder` reconciliation~~ — `8e3fa8d`.
- ~~Protocol-upgrade-on-ALTER~~ — `70d2384` (on `AddColumnAsync`; `SetSchemaAsync` still doesn't exist —
  see slice-6 leftovers).

### Write-path refactor taken (`808b944`)

`WriteCoreAsync` is now the single write path (append / overwrite / static partition overwrite / dynamic
partition overwrite / repartition-on-overwrite). Taken **refactor-only**: the codec seam and the
buffered-transaction + logical-rebase machinery were stripped out of the ported body, so slices 7 and 9 are
still open decisions. A leakage check (`DataFileWriter|VariantTransport|CheckLogicalRebase|
CommitDataFilesAsync`) over the Table project returns nothing.

What this means for 7 and 9: the structural prerequisite is now in place, so both can be ported onto
master's own `WriteCoreAsync` rather than requiring pr-4's whole `DeltaTable.cs`. Note pr-4's file is a
superset of master's `DeltaTable.cs` work but its `CheckpointReader` (drops slice 3's
`remove.deletionVector`) and `CompactionExecutor` (regresses slice 4's path encoding) are NOT — never take
those wholesale.

### The seam question — RESOLVED as to *why*, decision still open (2026-07-19)

**Superseded twice.** The original slice-7 rationale was wrong; so was its first correction. Full
write-up with sources and quotes: **`doc/codec-seam-investigation.md`**. Summary of the corrected
position:

- The seam is **not about VARIANT**. It exists so a host can swap out engineered-wood's Parquet codec
  entirely. The consumer — published 2026-07-19 as
  `https://github.com/cmettler/fabricator-extension` — plugs in DuckDB's `COPY … (FORMAT parquet)`.
  Its `docs/native-delta-write.md` is the design doc, and its motivation is explicit: *"Every
  engineered-wood defect this project hit lived in its parquet layer, never its `_delta_log` layer
  [...] this de-risks the part of engineered-wood whose future is uncertain (its parquet codec)."*
- **Chronology settles it**: `IDataFileWriter` landed downstream 2026-07-04; VARIANT landed 2026-07-06
  and merely *reused* the existing seam (`docs/variant-support.md`: *"UPDATE / copy-on-write DELETE /
  OPTIMIZE — LIFTED via the `IDataFileReader` codec seam"*). cmettler in the PR thread: *"This is a
  simple abstraction to plugin a different parquet codec."*
- The registration gap described in the previous version of this section **is real**, and is a
  standalone data-integrity bug (see Remaining work #1) — but closing it does **not** remove the seam's
  reason to exist. That inference was the error.
- What DuckDB's writer actually buys downstream is narrow: bloom filters (we write none), its
  encoding/footer maturity, a codec-name mapping, `ROW_GROUP_SIZE`. Notably **not**: encryption, custom
  footer metadata, DV bytes, or stats — downstream routes stats back through our `StatsCollector`
  because DuckDB's `RETURN_STATS` gives decoded VARCHAR min/max that can round.
- The seam still ships with **no in-tree implementations**; `CodecSeamTests` on master remain its only
  tests.

**No working assumption recorded — this is now a product decision**, not a technical one: whether
"engineered-wood as a Delta metadata engine with bring-your-own codec" is a story to support. `9302723`
is a clean single commit to revert; the `ProcessFileBatchesAsync` extraction underneath is
behaviour-preserving and should be KEPT either way. `doc/codec-seam-investigation.md` §6 frames both
branches.

Audit findings from the same investigation (fix if the seam stays, delete with it otherwise) are
enumerated in that document §5: `relativePath` encoding unspecified with the two write call sites
disagreeing, partition-directory creation an unstated obligation, `PARQUET:field_id` /
`ARROW:extension:*` field metadata load-bearing but undocumented, and `CodecSeamTests` covering no
partitioned case. One finding there is **independent of the seam**: the copy-on-write UPDATE rewrite
writes to the table root with no partition subdirectory while its `add` carries `PartitionValues`
(`DeltaTable.cs:1189`) — pre-existing, present in the built-in branch too, untested.

### Remaining work

1. ~~**Variant registration**~~ — **investigated and RETRACTED (2026-07-19); there is no bug.** It is
   true that the Delta layer passes `ParquetReadOptions.Default` (null `ExtensionRegistry`), but the
   path is unreachable: the Delta *schema* layer rejects a variant column in both directions before any
   parquet reader exists — `"variant"` throws `Unknown Delta primitive type` on read, and `VariantType`
   (an `ExtensionType`, **not** a `StructType`) throws `Cannot convert Arrow type` on write. We already
   fail closed; `pr-4`'s `ThrowIfVariantRewrite` has nothing to guard here. Now pinned by two
   `SchemaConverterTests` cases (DeltaLake 199 -> 201), which also assert the not-StructType-derived
   property — if that ever changes upstream, `FromArrowType`'s `ArrowStructType` arm *would* silently
   map variant to a Delta struct. Variant at the Delta layer is therefore a **feature** (see 2), not a
   defect to fix.
1a. ~~**Shredded VARIANT reads are silently empty (Parquet layer)**~~ — **FIXED (2026-07-19).** A real
   bug, found while checking (1). The reader wrapped a shredded column as `VariantArray` but never
   reassembled: `value` is empty, the data is in `typed_value`, so `GetValueBytes` returned 0 bytes
   while `IsNull` was false — a valid row holding an empty variant. 61 of 131 corpus cases failed. The
   pre-existing sweep test passed throughout because it only checks annotation-vs-wrapping agreement
   and never reads a value.

   Fix: `Parquet/Data/VariantShredding.cs` reassembles via `Apache.Arrow.Operations` 23.0.0 (new
   `PackageReference`; ships net462/netstandard2.0/net8.0, so every TFM is covered, and the net10.0
   AOT/trim gate stays warning-free). `NestedAssembler` calls it after wrapping. The result is an
   ordinary unshredded `VariantArray` — correct and uniform, but the shredded layout is not preserved,
   so a caller cannot inspect `typed_value` afterwards. If that is ever needed (predicate pushdown into
   `typed_value`), it should become an explicit opt-in on `ParquetReadOptions` rather than the default.

   Verified by `VariantCorpus_MatchesReferenceVariants`, driven by the corpus's `cases.json`: **135
   rows match semantically, 36 byte-exact, all 6 declared error cases now throw** (malformed shredding
   was previously accepted and turned into garbage). Comparison is semantic (both sides rendered to
   JSON) because reassembly legitimately re-canonicalizes metadata — offset-size bits, dictionary
   pruning; byte-exactness is additionally required only for unshredded columns, which pass through
   untouched. The three cases the corpus marks *"not valid according to the spec and implementations
   can choose to error, or read the shredded value"* are treated as implementation-defined: Arrow reads
   the shredded value where Iceberg prefers the residual, and both are conformant.

2. ~~**Variant at the Delta layer**~~ — **DONE (2026-07-19).** Delta `variant` columns now work end to
   end, without the codec seam and without PR #4's `VariantTransport`.

   - **Schema**: `SchemaConverter` maps `"variant"` ⇄ Arrow `VariantType` (the
     `arrow.parquet.variant` extension over `struct<metadata, value>`). The `VariantType` arm precedes
     an `ExtensionType` catch-all that THROWS for any other extension — an unknown extension must not
     degrade to its storage type, which would silently drop its meaning.
   - **Protocol**: `variantType` allowlisted as a reader+writer feature; declared at CREATE and on
     ALTER. **`CreateAsync` now shares `RequiredSchemaFeatures` with the ALTER path** instead of
     hand-rolling a `timestampNtz` check — the divergence was why variant was declared on ALTER but not
     at CREATE, and the same trap would catch the next schema-driven feature.
   - **Read**: `DeltaTable` routes every data-file read through options carrying a registry that knows
     the variant extension (`_dataFileReadOptions`). Without it a VARIANT group decodes as a bare
     struct — silently contradicting the table's declared schema. A caller-supplied registry is
     CLONED, never mutated, and its other extensions are preserved.
   - **DML / maintenance**: extension arms added to both `TakeRows` copies
     (`DeletionVectorFilter`, `PartitionUtils`), which filter through the storage and re-wrap. Without
     these, DELETE/UPDATE threw, and so did any partitioned write of a table containing a variant
     column *even when variant was not the partition column* (`BuildFilteredBatch` takes rows from
     every non-partition column).
   - **Silent-corruption fixes found by audit, not by tests**: `DeletionVectorFilter.CreateEmptyArray`
     returned a `StringArray` for any unrecognised type (wrong-typed column vs its own schema when a
     batch is fully deleted); `ValueWidener.BuildNullArray` did the same for a missing column;
     `ValueWidener.TypesMatch` returned true for ANY two extension types, since all report
     `TypeId.Extension`. All three now handle extensions explicitly.
   - **Stats**: min/max correctly omitted (no dispatch arm ⇒ skipped), `nullCount`/`numRecords` still
     emitted — pinned by test rather than left to luck.
   - **`ParquetReadOptions` is now a `record`** (matching its `ParquetWriteOptions` sibling) so a layer
     deriving options gets a compiler-generated copy of every member. A hand-written copy would
     silently drop any option added later.

   Covered by `VariantTests` (13 tests: round-trip, feature declaration, schema JSON, stats, DELETE,
   partitioned write, compaction, ADD COLUMN backfill, variant-partition-column rejection, both
   annotation modes, the by-name coercion, and nested variant inside a struct) plus 4
   `SchemaConverterTests`. **Nested variant** (a variant inside a struct/list/map) now reads correctly:
   `ArrowSchemaConverter` already produced a `VariantType` field at every depth, and
   `Parquet/Data/VariantNestedWrapper` reconciles the assembled arrays to match it (top-level stays
   `NestedAssembler`'s job; the wrapper composes with it). Covered by four parquet tests
   (struct/list/map + the no-registry symmetry) and the Delta `NestedVariant_InsideStruct_RoundTrips`.
   Two caveats: nested wrapping needs the annotation (it keys off the parquet reader's
   variant-awareness), so an *unannotated* nested variant — Spark 4.0.x, or EW's own
   `EmitVariantLogicalType=false` output — is not wrapped (the Delta-layer coercion is top-level only);
   and there is still **no shredding on write** — EW emits the storage struct as-is, spec-legal but an
   interop asymmetry against Spark/DuckDB. Neither caveat affects the common (annotated) path.

   **Externally validated (2026-07-19)** against delta-rs 1.6.2, Spark 4.0.1 and Spark 4.1.1, both
   directions, via `VariantInteropTests`. This is where round-trip-through-EW was proven insufficient —
   it surfaced two real defects, both since fixed:

   - **EW's reader keyed off the parquet annotation, not the Delta schema.** The spec (Reader
     Requirements for Variant) says the schema is authoritative. So an *unannotated* variant table
     (Spark 4.0.x, or any spec-minimal writer) silently read back as a bare `struct<value, metadata>`.
     Fixed by `VariantColumnCoercion`, which wraps a schema-declared variant column whether or not the
     file is annotated — and reorders the storage children BY NAME first, because Arrow's positional
     `VariantType` factory would otherwise swap Spark's `(value, metadata)` order.
   - **Spark 4.0.x cannot read EW's default (annotated) output** — its parquet-mr predates the VARIANT
     logical type and throws an NPE. The Delta spec does not require the annotation (it defines only the
     struct-of-binary), so `DeltaTableOptions.EmitVariantLogicalType = false` writes the bare struct for
     4.0.x compatibility. delta-rs and Spark 4.1 read both forms; Spark 4.0.1 reads only the unannotated
     one; the annotation is what Spark 4.1+/DuckDB expect. Validated matrix:

     | | delta-rs 1.6.2 | Spark 4.0.1 | Spark 4.1.1 |
     |---|---|---|---|
     | annotated (default) | ✅ | ❌ NPE | ✅ |
     | unannotated (compat) | ✅ | ✅ | ✅ |

   A footgun this exercise caught independently: the first `VariantTests` used INVALID variant value
   bytes (a truncated int8 as "true"). They passed because EW and delta-rs treat the value as opaque;
   Spark decodes and rejected them with `MALFORMED_VARIANT`. Fixed to spec-valid encodings.
3. **Slice 9** (row-level concurrency — **STRATEGIC**) — absorbs slice 8's OCC/conflict-checker material.
4. **The seam decision** — keep-and-fix vs revert `9302723`; see the seam question above and
   `doc/codec-seam-investigation.md`. `IDataFileRewriter` + row-tracking-through-rewrite only if it
   survives, and it is a categorically larger commitment than the other two (it delegates DML
   *semantics*, not encoding).
5. **Clustering bits coupled to `CommitDataFilesAsync`** — `dataChange`/`clusteringProvider` params and
   `WrittenDataFile.Tags` -> `add.tags`. Need the buffered-transaction API from slice 9.
6. ~~**Stale `doc/known-issues.md`**~~ — DONE (2026-07-19). Re-verified claim by claim against the
   code, not against these notes. Roughly a dozen entries described gaps that had since been closed:
   Parquet VARIANT + UUID emission + nanosecond TIME; Delta's writer-feature table (appendOnly /
   invariants / checkConstraints / generatedColumns / clustering are all supported-as-listed now,
   with `HonorWriterFeatures` failing closed when ACTIVE), rowTracking domain emission, nested stats
   + string truncation, checkpoint tags/DV/tombstones, `ToArrowField` metadata, the ALTER APIs, and
   the create-time/ALTER protocol upgrades. Added the non-ASCII path encoding and the
   column-mapping protocol-shape divergence as interop entries, and a "last verified" stamp.

## External validation — tier 1 (delta-rs) landed 2026-07-19

Until now all Delta validation was round-trip only, which proves reader and writer agree on a dialect,
not that the dialect is Delta. `test/EngineeredWood.DeltaLake.Table.Tests/Interop/` now drives
delta-rs (pip `deltalake`, validated against 1.6.2) as an independent oracle: `delta_rs_driver.py`
(read / describe / raw_log / checkpoint_only_read / write), `DeltaRs.cs` (locator + invoker), and
`DeltaRsInteropTests.cs` (6 tests). Absent `deltalake` the tests no-op; set
`EW_REQUIRE_DELTA_INTEROP=1` in CI so a missing toolchain fails loudly instead of silently reverting
the suite to round-trip-only.

**Its first run failed 5 of 6, all real bugs, all invisible to round-tripping:**

1. **`ParquetWriteOptions.OmitPathInSchema` defaulted to `true`** — so EW omitted `path_in_schema`,
   which the current Parquet spec requires. *Every parquet file EW wrote — data files and Delta
   checkpoints alike — was rejected by pyarrow, ParquetSharp and delta-kernel-rs as a corrupt thrift
   footer.* EW's own reader tolerates the omission, so nothing caught it. Tellingly, every existing
   cross-validation site set `OmitPathInSchema = false` by hand ("ParquetSharp requires
   path_in_schema") — the external tests were opted out of the broken default, so the default itself
   was never externally validated. Now defaults to `false` and is marked `[Experimental("EWPARQUET0002")]`
   so enabling it requires a deliberate pragma.
2. **`metaData.format.options` omitted when empty** — delta-kernel-rs decodes it as a non-nullable map
   and fails the entire log read. Every EW-written table was unreadable by delta-rs and DuckDB.
3. **`metaData.configuration` omitted when null** — same failure mode, same fix.
4. **`ActionSerializer` threw on explicit JSON nulls** — delta-rs writes `"baseRowId": null,
   "tags": null, …` where EW omits the fields; `DeserializeAdd` called `GetInt64()` on the Null token.
   *EW could not open any delta-rs-written table.* Fixed centrally in `ReadObject`: an explicit null
   means "absent" for every Delta action field.

Suites after the fixes: DeltaLake 180, Delta Table 243 (+6 interop, 27 skipped), Parquet 590,
Iceberg 241, Lance.Table 92 — all green on net10.0/net8.0, all TFMs build.

**Known tier-1 blind spot**: EW declares column mapping with the legacy `minReader=2`/`minWriter=5`
numbering, and delta-rs 1.6.2 declines to open it, so it cannot validate physical-name resolution —
that needs tier 3 (PySpark), which now covers it.
`EwWritten_ColumnMapping_CommitShapeIsSpecCorrect_ReadBackNeedsTier3` pins the commit shape off-disk
and asserts the rejection reason, so it will fail if delta-rs ever gains support.

**CORRECTION (2026-07-19)**: an earlier revision of this section claimed that emitting v3/v7 with a
`columnMapping` reader feature "would make these tables readable by delta-rs and DuckDB too". **That is
false.** delta-rs 1.6.2 cannot read column mapping in ANY declaration form — measured, both rejected:

| Declaration | delta-rs |
|---|---|
| reader 2 (legacy) | `minimum reader version is 2 but deltalake only supports 1 or 3 with {timestampNtz, variantType, variantType-preview}` |
| reader 3 + `columnMapping` readerFeature | `these reader features … are not yet supported by the deltalake reader` |

It is an unimplemented-feature gap, not a declaration gap. (delta-rs also rejects `deletionVectors`
as a reader feature through this API — tier 1's reach over feature-gated tables is narrow, and such
tables mostly belong to tier 3.) The v2/v5-vs-v3/v7 question is therefore **not** an interop
blocker; see the protocol-shape note below for what actually remains of it.

### Column-mapping protocol shape — what Spark really writes

Measured against Spark 4.0.1 / delta-spark 4.0.0. For plain column mapping Spark emits a **hybrid**,
not the legacy pair:

```json
{"minReaderVersion": 2, "minWriterVersion": 7,
 "writerFeatures": ["columnMapping", "invariants", "appendOnly"]}
```

Legacy on the reader side, table-features on the writer side, no `readerFeatures` at all. Add another
feature (e.g. deletionVectors) and it becomes reader 3 / writer 7 with both lists populated.

So the comment in `DeltaTable.cs` claiming "reader v2 / writer v5, no lists is what Spark itself
writes" was **wrong** and has been corrected in place.

What remains is cosmetic, and the case for changing is weak:

- EW's `minWriterVersion=5` implies `checkConstraints`/`generatedColumns` among others, but the
  obligation those impose is CONDITIONAL on the table actually declaring a constraint or a generated
  column. A writer lacking them writes correct data to a table that uses neither.
- EW already fails closed when they ARE active: `HonorWriterFeatures` rejects the write on
  `delta.constraints.*`, `delta.invariants` and `delta.generationExpression`. Both key assumptions
  are now pinned against real Spark output by `SparkWritten_CheckConstraint_EwRefusesToWrite` and
  `SparkWritten_GeneratedColumn_EwRefusesToWrite` — Spark does use exactly those keys.
- Spark reads EW's v2/v5 form fine (`EwWritten_ColumnMapping_SparkResolvesPhysicalNamesToLogical`).

**Recommendation: leave it alone.** Matching Spark's hybrid would buy byte-compatibility with the
reference implementation and nothing measurable. Full v3/v7 is strictly worse — it raises the reader
bar 2→3 for no gain, since delta-rs cannot read it either way. Read-side needs no work regardless: EW
detects column mapping from `delta.columnMapping.mode` in the configuration, not from the protocol
version, so it already reads Spark's hybrid form.

## External validation — tier 3 (PySpark) landed 2026-07-19

`Interop/spark_driver.py` + `Spark.cs` + `SparkInteropTests.cs` (6 tests), validated against
pyspark 4.0.1 / delta-spark 4.0.0 — the pair PR #4 itself was validated against. Gated by
`EW_REQUIRE_SPARK_INTEROP=1`. The shared process/JSON plumbing was extracted to `InteropDriver.cs`,
which tier 1 now uses too.

Tests deliberately do NOT duplicate tier 1 — each covers something delta-rs structurally cannot:
column-mapping read-back (the tier-1 blind spot), `DESCRIBE DETAIL`, clustering-column resolution,
Spark `OPTIMIZE` over an EW table, and a Spark-written deletion vector read by EW.

**Finding: every clustered table EW wrote was rejected by Spark.**

```
DELTA_FEATURES_PROTOCOL_METADATA_MISMATCH: Unable to operate on this table because the following
table features are enabled in metadata but not listed in protocol: invariants
```

Table-features mode is all-or-nothing: at writer 7 there are no implicit capabilities left, so every
feature the legacy version implied must be listed explicitly. `CreateAsync` escalates to writer 7 when
clustering (or identityColumns, or timestampNtz) is present, but built its feature list from empty —
dropping the `appendOnly`/`invariants` that writer 2 implied. `UpgradeProtocolForFeatures` already did
this correctly for ALTER; creation did not. Fixed by capturing the legacy baseline versions before
feature escalation and merging `LegacyWriterFeatures`/`LegacyReaderFeatures` in before the
`ProtocolAction` is built. Slice 10's clustering interop never actually worked against OSS Delta.

**Performance — one JVM for the whole run.** `spark_driver.py serve` keeps a single SparkSession
alive and takes commands over stdin; `InteropDriver(persistent: true)` drives it. Per-command process
launch meant ~15s of JVM startup around ~1s of real work. The 12-test interop suite went from **87s to
22s**, and the cost is now per-run rather than per-test, so adding tier-3 tests is roughly free.
Protocol: request and result both travel as files (`os.replace` after write, so the file never exists
half-written) with a `__EW_DONE__<name>` line on stdout purely as the wakeup — stdout carries no
payload because Spark writes to it freely. Requests are serialized on a lock since one process means
one stdin. delta-rs stays one-shot; it has no startup cost to amortize.

**Harness note — availability probing must check what the tier actually needs.** `import pyspark`
succeeds on a machine with no JDK, so the first version of this tier went RED rather than no-op when
run without `JAVA_HOME` — the exact failure the availability mechanism exists to prevent, in reverse.
`Spark.Preflight()` now checks for a real JDK and (on Windows) `winutils.exe` before the Python probe,
so a half-configured machine gets a precise reason instead of a JVM stack trace. Note also that
`HADOOP_HOME` alone is insufficient on Windows: `hadoop.dll` must be on `PATH` or Hadoop throws
`UnsatisfiedLinkError: NativeIO$Windows.access0`; `Spark.BuildEnvironment()` handles that for callers.

## Tier 2 (DuckDB) — DROPPED, and why

The original three-tier plan had DuckDB's delta extension as tier 2. It is not worth building:

- **delta-rs already IS delta-kernel-rs.** The installed `deltalake` 1.6.2 binary embeds the
  `delta-kernel-rs` crate, and every log-replay bug found in this effort surfaced as
  `DeltaError: Kernel error: ...`. DuckDB's delta extension is the same kernel, so its log-replay
  coverage is entirely subsumed by tier 1.
- Genuinely DuckDB-only would be (a) a third Parquet reader and (b) predicate pushdown through a
  foreign planner. (a) is already covered at the right layer by
  `test/EngineeredWood.Parquet.Compatibility/` (92-file cross-tool validation). (b) is real and
  valuable — but reachable from the tiers that already exist, which is what the stats/pruning tests
  below do.

**Statistics and pruning coverage (added 2026-07-19, in place of tier 2).** This is the failure mode
that motivated the axis: wrong `minValues`/`maxValues` raise nothing anywhere — a pruning engine just
skips a file it should have read and the query silently returns FEWER ROWS. Every earlier interop
test reads whole tables, which never consults statistics at all.

- Tier 1 — `EwWritten_PerFileStats_DescribeTheFilesTheyBelongTo` compares per-file
  min/max/nullCount/numRecords (via `DeltaTable.get_add_actions(flatten=True)`) against what EW
  actually wrote; plus filtered-read and partition-filtered-read correctness.
- Tier 3 — `EwWritten_MinMaxStats_SparkSkipsFilesWithoutLosingRows` and
  `EwWritten_NestedStats_SparkSkipsOnNestedFieldWithoutLosingRows` assert BOTH the right rows and
  that `files_scanned < files_total`. The file count is what keeps them honest: row correctness alone
  would also pass on an engine that never pruned. Nested stats (slice 8) had never been read by
  anything outside EW; they are correct.

## Deferred follow-ups (do after the PR-landing work)

### A. VACUUM spec alignment — DONE (2026-07-19)

Landed. **The plan recorded below was wrong on its central point**, and measuring beat reading again:

- The plan said the keep-set should include "unexpired `RemoveFile` tombstones". **It must not.**
  Measured against Spark 4.0.1 / delta-spark 4.0.0: `VACUUM ... RETAIN 0 HOURS` deletes a file
  orphaned seconds earlier, with the tombstone still fresh in the log. That matches the documented
  contract — vacuum removes files not referenced by the CURRENT version, which is precisely what ends
  time travel past the retention window. Implementing tombstone protection made two existing tests
  fail, which is how the error surfaced.
- `delta.deletedFileRetentionDuration` is the **default retention** for a RETAIN-less vacuum, not an
  independent protection window. Measured: with the property at `interval 0 seconds`, a RETAIN-less
  `VACUUM` collects a just-orphaned file immediately. It is now honored in `DeltaTable.VacuumAsync`
  (explicit argument > table property > `DeltaTableOptions.VacuumRetention`), parsed by the new
  `IntervalParser` (months/years deliberately rejected as calendar-relative).

What actually shipped: keep-set = the current version's `add` paths **plus their deletion-vector
paths**; sweep everything under the table root with **no extension filter**; exclude `_delta_log/`
and `_change_data/`.

Two findings beyond the original scope:

- **A latent data-loss bug in the OLD implementation.** CDF files live in `_change_data/`, are
  referenced by `cdc` actions, and so never appear in `ActiveFiles` — but they ARE `.parquet`, so the
  old vacuum deleted readable change-data-feed history once past retention. `_change_data/` is now
  excluded outright. That under-deletes (expired CDF is never collected); a proper keep-set needs the
  snapshot to track `cdc` actions, which it does not.
- **Absolute-path (`p`) deletion vectors now make vacuum refuse.** Their targets cannot be resolved
  against the table root from the action alone, so vacuum cannot prove they lie outside the swept
  directory — and deleting a live DV silently resurrects every row it masked. EW never writes `p`
  vectors; such a table came from another engine.

The DV path derivation moved to a shared `DeletionVectorPath` used by BOTH the reader and vacuum. If
those two ever disagreed, vacuum would delete a live vector — there must be exactly one implementation.

Validated end to end by `SparkWrittenDeletionVector_SurvivesEwVacuum` (Spark writes a DV, EW vacuums
at zero retention, Spark re-reads and the masked row is still masked) and `EwVacuumed_SparkStillReadsTable`.

<details>
<summary>Original plan (kept for the record — note the tombstone error above)</summary>

### A. VACUUM spec alignment (`VacuumExecutor.cs`)

EW's `VacuumExecutor` diverges from official Delta (under-deletes): it only considers `.parquet` files
(so orphaned `deletion_vector_*.bin` leak forever), protects only active-add data paths, has no
tombstone model, and is file-mtime-based (ignores `delta.deletedFileRetentionDuration`).

Official algorithm (verified against delta-io/delta `VacuumCommand.scala` + PROTOCOL.md): mark-and-sweep
driven by snapshot state — build the keep-set over every FileAction (active `AddFile`s AND unexpired
`RemoveFile` tombstones), adding each one's data path AND its deletion-vector path
(`getDeletionVectorRelativePathAndSize`); list the whole table dir recursively with NO extension filter;
delete anything not in the keep-set past the cutoff. Retention defaults to
`delta.deletedFileRetentionDuration` = 1 week; tombstone protection keyed on `delTimestamp`.

Rewrite plan: (1) drop the `.parquet`-only filter; (2) keep-set = active adds' data+DV paths PLUS
unexpired tombstones' data+DV paths (use `Snapshot.Tombstones`, added in slice 3 / `b41f5ad`); (3) honor
`delta.deletedFileRetentionDuration` (default 1 week), gate tombstoned files on `deletionTimestamp`.
Behavioral change to flag to Curt: EW VACUUM will then actually delete orphaned DV `.bin` and removed
data files past retention (it doesn't today). Correct/spec-matching; default 7-day retention protects
recent files. Ship with tests. Curt's preference: match the official implementation, not EW's.

</details>

### B. Percent-encode non-ASCII in `DeltaPath.Encode` — RESEARCH DONE (2026-07-19)

**The research pass this section asked for has been run**, against delta-rs 1.6.2 rather than by
reading Spark source. Ground truth is now pinned as an assertion in
`DeltaRsInteropTests.DeltaRs_NonAsciiPartition_PathEncodingGroundTruth`. The encoding is **two
layers**, which is the part a from-first-principles fix would most likely get wrong:

| value | on-disk directory (Hive escape) | `add.path` (re-encoded) |
|---|---|---|
| `café` | `region=caf%C3%A9` | `region=caf%25C3%25A9/…` |
| `日本` | `region=%E6%97%A5%E6%9C%AC` | `region=%25E6%2597%25A5%25E6%259C%25AC/…` |
| `a b#c?d` | `region=a%20b%23c%3Fd` | `region=a%2520b%2523c%253Fd/…` |

So `add.path` = percent-encode(hive-escape(value)) — every `%` produced by layer 1 becomes `%25` in
layer 2. Non-ASCII is encoded as UTF-8 bytes, confirming this section's hypothesis.

**Severity is lower than assumed**: `EwWritten_NonAsciiPartition_DeltaRsReadsSameRows` PASSES today —
delta-rs reads EW's literal-non-ASCII form fine. The divergence is in what EW *produces*, so it bites
strict readers and byte-level comparisons rather than delta-rs. Still worth fixing for Spark parity;
no longer urgent.

### B (original notes)

`DeltaPath.Encode` (`src/EngineeredWood.DeltaLake/DeltaPath.cs`) currently escapes only
`% space # ?` + control chars and leaves **non-ASCII characters literal**. Spark/delta-rs percent-encode
non-ASCII (as UTF-8 bytes) in `add.path`, so an EW-written table whose partition values / paths contain
non-ASCII may not be readable by strict foreign readers — an interop gap. EW's own reader round-trips
fine (`Uri.UnescapeDataString` is a no-op on literals), so this only matters for cross-engine reads.

Fix approach (needs a short research pass first, like the VACUUM one, to confirm Spark's exact behavior):
confirm Spark UTF-8 percent-encodes non-ASCII in `add.path`, then update `DeltaPath.Encode` to
percent-encode each non-ASCII char's UTF-8 bytes as `%XX`, and verify `Decode` (`Uri.UnescapeDataString`)
round-trips. Don't guess the encoding — a wrong "fix" is worse than the current faithful port.
