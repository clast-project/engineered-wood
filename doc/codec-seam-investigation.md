# The codec seam (`IDataFileWriter` / `IDataFileReader` / `IDataFileRewriter`) — investigation

**Status: research complete, decision open (2026-07-19).** Written to record the findings behind the
"seam question" in `doc/upstream-landing-notes.md`, whose recorded rationale turned out to be wrong.
Nothing here has been acted on; `doc/upstream-landing-notes.md` still carries the superseded version.

## Sources

- cmettler's PR #4 thread (`https://github.com/clast-project/engineered-wood/pull/4`), in particular
  the exchange of 2026-07-19.
- The downstream consumer, published 2026-07-19: `https://github.com/cmettler/fabricator-extension`
  — a DuckDB extension hosting a C# bridge over CoreCLR. Its `docs/native-delta-write.md` (the design
  doc for the whole effort) and `docs/variant-support.md` are the primary evidence.
- Our own `9302723` (the seam as landed), `CodecSeamTests.cs`, and the `pr-4` branch for
  `IDataFileRewriter`.

## 1. The headline: the seam is not about VARIANT

The rationale recorded in the landing notes — *"working assumption: the seam comes out once the variant
extension is registered"* — is falsified by the downstream source.

- **Chronology.** `IDataFileWriter` landed **2026-07-04** as phase P1 of a planned architectural
  inversion. VARIANT landed **2026-07-06** and explicitly *reused* the already-existing seam:
  `docs/variant-support.md` says *"UPDATE / copy-on-write DELETE / OPTIMIZE — LIFTED via the
  `IDataFileReader` codec seam (second pass)."* Variant is a passenger, not the driver.
- **cmettler's own words** in the PR thread: *"This is a simple abstraction to plugin a different
  parquet codec. I simply use the duckdb COPY with format parquet in `IDataFileWriter`."*
- **The stated motivation** (`docs/native-delta-write.md` §1) is about our Parquet layer:

  > Every engineered-wood defect this project hit lived in its **parquet layer**, never its
  > `_delta_log` layer: decimal read corruption, the RoaringBitmap DV byte format,
  > DataPage-V2-by-default, signed min/max without `column_orders`, missing `path_in_schema`, the
  > copy-on-write footer bugs. [...] This **de-risks the part of engineered-wood whose future is
  > uncertain** (its parquet codec) while keeping the part that is genuinely good.

Registering the variant extension on the Delta layer's internal Parquet interop therefore closes the
*variant* gap but does not remove the seam's reason to exist.

## 2. What the slogan gets right and wrong

"DuckDB writes the data, engineered-wood writes the log" is the downstream **aspiration**, close to
verbatim (`§12`: *"engineered-wood shrinks to the log/protocol/DV-bitmap layer [...] every parquet byte
— read and write — is DuckDB's"*). As a description of the seam **as landed** it oversimplifies three
ways:

1. **EW still writes data files.** Deletion-vector bitmaps are data, and EW writes those bytes; the
   seam is not involved in a DV delete at all. Same for CDF `_change_data/` parquet (deferred
   downstream — "CDF tables keep the EW-read path").
2. **Everything data-shaped above the byte layer stays in EW.** By the time `WriteAsync` is called, EW
   has already done the partition split, the logical→physical column-mapping rename, and row-tracking
   materialization (`__delta_row_id` is in `physicalBatch` *before* the writer sees it — which is why
   row tracking on append works for free downstream). EW computes `add.stats` itself afterward via
   `StatsCollector`. The read side is symmetric: `IDataFileReader` returns raw batches and DV
   filtering / logical rename / type widening / partition re-add / schema-evolution backfill all stay
   above the decode in `ProcessFileBatchesAsync`.
3. **The real line is parquet serialization vs. everything else** — a much smaller job than "the data."

## 3. Taxonomy of the gaps

### Category 1 — EW feature/quality gaps (closeable; mostly closed)

- **Bloom filters.** EW writes none. DuckDB writes them on dict-encoded columns; downstream uses their
  presence as a native-write fingerprint.
- **The correctness bugs**: decimal read corruption, DV byte format, DataPage-V2-by-default, signed
  min/max without `column_orders`, missing `path_in_schema`, null-struct definition/value
  misalignment, all-null pages declaring a delta encoding with a 0-byte payload, thrift field dispatch
  desync. All closeable, and most already landed on master via the PR slices.
- **VARIANT annotation on write + extension registration on the Delta layer's reader.** Open — see §5.

Category 1 is not a standing argument for the seam. It is a maturity delta at a point in time; every
item is a bug or missing feature with a known fix.

### Category 2 — genuine boundary limits (not Arrow's type system; the seam's *shape*)

Arrow-the-type-system held up. VARIANT initially failed to cross with `Unsupported Arrow type VARIANT`,
but that was fixed by registering an Arrow extension type; the blob-instead-of-struct transport
(`fabricator.variant`) works around an upstream **DuckDB appender bug** (`ArrowAppender::FinalizeChild`
walks the logical type's 4 children against an appender built for the internal type), not an Arrow
limitation.

What genuinely does not fit is the `IReadOnlyList<RecordBatch>` contract:

- **Materialization.** ~~`WriteAsync` takes a realized batch list — the whole file in memory.~~ **Half
  fixed (2026-07-20):** `WriteAsync` now takes `IAsyncEnumerable<RecordBatch>` (symmetric with the
  streaming `IDataFileReader.ReadAsync`), so a host may consume a file's batches incrementally instead of
  the interface forcing the whole file into memory; EW passes a lazy enumerable (a rewrite still collects
  survivors internally for `StatsCollector`, but that is EW's business, not the contract's). What remains
  is the **one-call-per-file** shape: downstream's `RunCopyPartitioned` (streaming `COPY … PARTITION_BY …
  APPEND true`, bounded memory, *many files from one pass*) is still **not** expressible on the interface.
- **The C-ABI crossing itself.** `RunCopySql` (clustered OPTIMIZE) runs the per-file read, the global
  `ORDER BY` (DuckDB's spilling sort) and the write inside a *single* host query so data never crosses
  into C#. `IDataFileRewriter` is the same instinct. Any batch-in/batch-out contract forecloses
  whole-query optimization by construction.

The tell: the three most interesting DuckDB entry points downstream are all *outside* the interface.

### Category 3 — where the seam is *worse* (DuckDB deficiencies, borne by the host)

- DuckDB's `COPY` **ignores Arrow field metadata**, so the host must lift `PARQUET:field_id` into an
  explicit `FIELD_IDS` clause or an id-mode column-mapping table reads all-NULL. Downstream handles
  top-level columns only; nested is unimplemented.
- `RETURN_STATS` yields **decoded VARCHAR** min/max whose float rounding could produce a too-narrow min
  and skip live rows — so downstream routes stats back through EW's `StatsCollector` anyway.
- Single-file `COPY` **does not create parent directories**.
- A SQL-NULL variant written via DuckDB reads back in Spark as a variant JSON-null.

### Category 4 — risk posture (not a gap; no feature work closes it)

Downstream's own framing is a bet on a battle-tested implementation plus an explicit hedge against
EW's direction being unsettled. If category 1 closes and their trust follows, the seam becomes optional
for them; if it does not, they keep it regardless of parity. This is the category that actually decides
whether the seam is permanent.

## 4. Division of labour on the DML path (as built downstream)

DuckDB never generates DV bitmaps, and it also does not rewrite everything.

- **EW writes every DV bitmap.** A sweep of the bridge found no DV writing; the rewriter only
  *consumes* DV positions (passed as `excludePositions` so a CoW UPDATE on a DV table touches only
  live rows). `§12` assigns "deletion-vector bitmap bytes" to EW outright.
- **DuckDB supplies row positions, not bitmaps.** The DML mechanism is the transient rowid
  `(fileOrdinal << 40) | file_row_number` from DuckDB's scan; C# decodes matched rowids to
  `(file, positions)` and EW encodes the DV. DuckDB answers "which rows," EW answers "what bytes."
- **Under DV mode a delete rewrites nothing** — *"DV-mode DELETE is unchanged (no data rewrite →
  native writer N/A)"*. DV became the **default** DML mode downstream on 2026-07-04. UPDATE on a DV
  table is merge-on-read (DV-delete the old rows, append a small post-image file), so the writer emits
  only that small append.

Remaining true rewrites through the seam: **copy-on-write UPDATE on non-DV tables**, and
**compaction / OPTIMIZE**. On the default path, the DELETE path — whose correctness bugs were the most
painful — stays entirely EW's. This is also why DV composes cleanly with row tracking: no rewrite means
positions, row ids and commit versions are preserved for free, with no materialized columns.

## 5. Defects and gaps found while auditing (independent of the decision)

1. ~~**Variant registration gap — a data-integrity bug.**~~ **RETRACTED (2026-07-19) — not a bug.**
   The first version of this document claimed a compaction or copy-on-write rewrite would silently
   strip the VARIANT annotation, on the grounds that `DeltaTableOptions.ParquetReadOptions` defaults to
   `ParquetReadOptions.Default` (null `ExtensionRegistry`) while the Parquet layer's VARIANT decode is
   gated on that registry. The premise is true; **the conclusion does not follow, because the code path
   is unreachable.** The Delta *schema* layer rejects a variant column in both directions, before any
   parquet reader is constructed:
   - **Read**: a Spark/Delta 4.x `"variant"` schema type hits the default arm of
     `SchemaConverter.ParsePrimitive` → `DeltaFormatException("Unknown Delta primitive type: variant")`
     (`SchemaConverter.cs:100-101`).
   - **Write**: `VariantType` derives from `ExtensionType`, **not** `StructType` (verified against
     Apache.Arrow 23.0.0 by reflection), so it matches no arm of `FromArrowType` and falls through to
     `DeltaFormatException("Cannot convert Arrow type ... to Delta type.")` (`SchemaConverter.cs:170-171`).

   So engineered-wood already fails closed, and `pr-4`'s `ThrowIfVariantRewrite` guard has nothing to
   guard on master. Pinned by `SchemaConverterTests.VariantDeltaType_IsRejected_NotSilentlyMappedToStruct`
   and `...VariantArrowType_IsRejected_NotSilentlyMappedToStruct`, which also assert the
   not-StructType-derived property directly — if `VariantType` ever became struct-derived upstream, the
   `ArrowStructType` arm *would* silently convert it to a Delta `struct<metadata,value>`, and that is the
   real latent hazard worth watching.

   What remains at the Delta layer is a **feature gap, not a defect**: a `"variant"` Delta type in
   `SchemaConverter`, the `variantType` table feature in `ProtocolVersions` and `RequiredSchemaFeatures`,
   and shredded read/write — i.e. PR #4's `VariantTransport` slice. Only *then* does the registry default
   on the Delta read path become a live question.

   **But the investigation it prompted did find a real one, at the Parquet layer — see 1a.**

1a. **Shredded VARIANT columns silently materialised as EMPTY values (Parquet layer) — FIXED
   (2026-07-19).** Was reachable by anyone who set `ParquetReadOptions.ExtensionRegistry` and read a
   Spark- or DuckDB-written variant column — both shred by default.

   **The defect.** The reader wrapped a shredded column as a `VariantArray` but performed **no
   reassembly**. For a partially-shredded row the `value` child is empty and the data lives in
   `typed_value`, so `GetValueBytes` returned **zero bytes while `IsNull` reported false** — a valid row
   holding an empty variant. No exception, no warning. Only the fully-shredded layout (no `value` child
   at all) threw, and Apache.Arrow's own message there named the fix: *"Use the shredding-aware readers
   in `Apache.Arrow.Operations.Shredding`."* Measured against `parquet-testing/shredded_variant`:
   **61 of 131 readable cases failed value comparison.**

   **Why it went unnoticed.** `SweepTest_ShreddedVariantCorpus_AllReadAsVariantArray` asserts only that
   schema annotation and array wrapping *agree*, plus that some column reports `IsShredded`. It never
   reads a value. It passed for months on a corpus it could not decode — and the corpus ships
   `*_row-N.variant.bin` reference bytes that no test compared against. Worth remembering as a pattern:
   a green sweep test is not evidence for the thing its name implies.

   **The fix.** `Parquet/Data/VariantShredding.cs` reassembles each row via `Apache.Arrow.Operations`
   23.0.0 (new `PackageReference`; ships net462/netstandard2.0/net8.0, covering every TFM, and the
   net10.0 AOT/trim gate stays warning-free). `NestedAssembler` calls it immediately after wrapping.
   The output is an ordinary unshredded `VariantArray`: values correct and uniform, but the shredded
   layout is not preserved, so a caller cannot inspect `typed_value` afterwards. That trade is
   deliberate and documented on the type — the reader's contract is to materialise values the caller can
   read without taking a second dependency. If preserving the layout is ever needed (predicate pushdown
   into `typed_value`), it should be an explicit opt-in on `ParquetReadOptions`, not the default.

   **Verification** — `VariantCorpus_MatchesReferenceVariants`, driven by the corpus's own `cases.json`:
   135 rows match semantically, 36 byte-exact, and all 6 declared error cases now **throw** (malformed
   shredding — conflicting `value`/`typed_value`, non-object values with shredded fields, unsupported
   shredded types — was previously accepted and turned into garbage, so this is a second correctness
   gain). Two assertion strengths on purpose: semantic comparison everywhere, because reassembly
   legitimately re-canonicalizes metadata (offset-size bits, dictionary pruning); byte-exactness
   additionally for unshredded columns, which pass through untouched. The three cases the corpus marks
   *"not valid according to the spec and implementations can choose to error, or read the shredded
   value"* are treated as implementation-defined — Arrow reads the shredded value where Iceberg prefers
   the residual, and both are conformant.

   **Sequencing consequence (now unblocked).** Delta variant support could not have been built on the
   old foundation: a Delta reader that merely registered the extension would have inherited silent
   empty-value reads for every Spark-written column.
2. ~~**CoW UPDATE writes to the wrong directory on partitioned tables.**~~ **FIXED (2026-07-20)**, and
   independent of the seam decision. `ComputeUpdateActionsAsync` built the rewrite filename as a bare
   `{Guid:N}.parquet` at the table root while the add copied `addFile.PartitionValues`. Now it joins the
   source's partition directory (reuse the source path's encoded prefix verbatim; decode for the physical
   write), mirroring the compaction rewrite — so a host codec receives the correct partitioned `fileName`
   too. Measured: delta-rs read the root-dropped file fine (partition values are authoritative from the log),
   so it was a spec-layout bug, not data loss. See `doc/upstream-landing-notes.md` gap #6 for the tests.
3. **`relativePath` encoding is unspecified and the call sites disagree.** The reader's XML doc says
   "URL-decoded"; the writer's says nothing. `DeltaTable.cs` (write path) passes the **raw** name;
   `CompactionExecutor` passes an explicitly `DeltaPath.Decode`d one. They agree only because both
   happen to be the on-disk form. Downstream consumes the path verbatim (`Replace('\\','/')
   .TrimStart('/')`, no unescape anywhere in the bridge), so the de facto contract is
   **decoded / on-disk**. State it in both docs and make the write path consistent.
4. **Directory creation is an unstated obligation.** EW never creates partition directories; the
   built-in path works only because `LocalTableFileSystem.CreateAsync` calls `Directory.CreateDirectory`
   implicitly. Downstream discovered this and mkdir -p's itself, best-effort and non-fatal (object
   stores have implicit directories). Note their own `RunCopySql` skips the mkdir their `RunCopy` does
   — an illustration of how easy the obligation is to miss.
5. **Field metadata on the handed-off batch is load-bearing and undocumented.** `PARQUET:field_id`
   (id-mode column mapping) and `ARROW:extension:*` (host type discrimination — the reason
   `CleanField` preserves them) both matter to the host, and the contract says nothing.
6. **`CodecSeamTests` does not cover partitioned tables through the seam at all**, which is why 2–4
   went unnoticed.

## 6. Framing for the decision

The two seams are not the same kind of commitment:

- `IDataFileWriter` / `IDataFileReader` are a **codec** seam — narrow, honest, roughly what the slogan
  describes once "data" is shrunk to "bytes."
- `IDataFileRewriter` is an **execution** seam. It delegates *semantics*, not encoding: the DELETE
  predicate, the UPDATE join and value substitution, schema-evolution NULL backfill, and
  row-id/commit-version computation all move into DuckDB SQL. It is also the interface that cannot be
  honoured without row-tracking-through-rewrite (absent on master), and it declines column-mapped and
  schema-evolved files anyway. `9302723` was right to leave it out.

If the seam **stays**: fix 3–6 above, and be explicit that it is a per-file, materialized, batch-in
contract. Hosts wanting streaming partitioned writes or in-engine sort/filter will keep going around it,
as fabricator already does with three non-interface entry points. Widening it toward "hand me a query
plan" is a categorically larger API and probably not the right shape for EW.

If the seam **goes**: category 2 is what is being declined — a defensible line, provided it is drawn
deliberately rather than on the assumption that variant was the whole story. `9302723` is a clean single
commit to revert; the `ProcessFileBatchesAsync` extraction underneath is behaviour-preserving and should
be **kept either way**.

Either way, the standalone items that survive the decision are the codec-seam contract obligations in
§5.3–6 (both framings keep reader+writer); §5.1 was retracted (no bug), §5.1a and §5.2 are fixed.

## 7. A more consistent split — two framings, and where the rewriter earns its keep

*A recommendation to weigh, **not** the recommendation. The abstraction-level mismatch below is real, but
which resolution is right turns on a question this document cannot settle: **who owns query execution.***

**The observation.** `IDataFileReader`/`IDataFileWriter` sit at the CODEC level (Arrow batch ⇄ parquet
bytes). `IDataFileRewriter` sits at the EXECUTION level — it runs the DELETE filter / UPDATE substitution in
DuckDB SQL (`read_parquet(… file_row_number) WHERE file_row_number NOT IN (…)`, a `LEFT JOIN` for UPDATE).
Two levels in one seam. Whether that is an *inconsistency* depends entirely on which boundary you think the
seam draws.

**Framing 1 — the seam is "parquet serialization" (codec).** Then the rewriter is the odd one out, and the
consistent split is: keep reader+writer, **drop the rewriter**, and model the remaining true rewrites
(copy-on-write UPDATE on non-DV tables; per-file compaction) as **read → transform-in-EW → write**. Every
seam becomes bytes↔batches; EW owns all DML semantics, which also deletes the rewriter's column-mapping /
schema-evolution / row-tracking-through-rewrite coupling. Grounded: DV is the DEFAULT DML mode downstream, so
the rewriter is already off the default path (a DV DELETE writes only a bitmap; UPDATE is merge-on-read), and
the highest-value native rewrite — clustered OPTIMIZE with a whole-query `ORDER BY` — already runs OUTSIDE
the interface. What you give up is DuckDB executing the DML transform in SQL — which is exactly what made the
seam an execution seam. **This framing assumes EW is the DML front-end** (a C# caller supplies the
predicate/updater as delegates).

**Framing 2 — the seam is "compute + bulk IO" (DuckDB is the query engine).** Then reader (DuckDB scans),
writer (DuckDB encodes) and rewriter (DuckDB transforms) are ALL "DuckDB does compute; EW owns `_delta_log` +
DV bitmaps + stats + OCC" — consistent along the OWNERSHIP axis, differing only in data shape
(batch / batch / operation). Here the rewriter is not an anomaly; it is the DML member of a compute seam, and
you would expect MORE execution entry points, not fewer. **This framing assumes DuckDB is the DML front-end**
(users write SQL against Delta tables; the extension is DuckDB-native) — which is what fabricator actually is.

**Where the execution seam solves something genuinely hard in EW — the deciding case: joins and MERGE.**
This is the test that separates the framings, because it is where "transform-in-EW" stops being a pure
per-row function and becomes a *relational* computation against data EW does not have:

- **Read-side joins** (`SELECT … FROM delta JOIN duck_table …`) are clean under either framing: DuckDB scans
  the Delta table (file list + DV positions from EW) and joins in DuckDB. No rewrite; the codec reader suffices.
- **Write-side joins / MERGE** are the hard case. `UPDATE delta SET x = d.y FROM duck_table d WHERE
  delta.k = d.k`, or a full `MERGE … WHEN MATCHED UPDATE/DELETE WHEN NOT MATCHED INSERT`, derives the new
  values (and the matched set) from a JOIN against another table. Under **Framing 1**, EW would have to pull
  the source table into Arrow and run the join **in C#** — a spilling hash-join executor EW does not have and
  should not grow; it would be re-implementing DuckDB. Under **Framing 2** the rewriter is the natural
  vehicle: DuckDB evaluates the join / `SET` expression (against any DuckDB table, subquery, function or UDF),
  keys the new values by transient rowid, and the rewrite applies them — exactly the `LEFT JOIN against
  (position → new SET values)` shape fabricator already uses. MERGE composes from the three seams the host
  orchestrates: matched-update via the rewriter, not-matched-insert via the writer, matched-delete via a DV.

  The general principle: **the execution seam earns its keep exactly when the DML transform is a query, not a
  pure function of the source row** — joins, MERGE, correlated subqueries, SQL/UDF-rich `SET`/`WHERE`,
  aggregation-derived updates. A codec-only seam can only carry transforms EW can express as a delegate; once
  the front-end is SQL, re-hosting those semantics in EW is a non-goal.

**So the decision is really: who owns query execution for DML?** If EW is (or should stay) the DML
front-end, Framing 1 (codec-only) is cleaner and smaller, and joins/MERGE are simply out of scope. If a
DuckDB-fronted Delta table that participates in joins and MERGE against DuckDB tables is a capability worth
having, Framing 2 is the architecture, the rewriter is consistent within it, and "a more consistent split"
means *naming the compute-vs-metadata boundary and expecting execution entry points* — not collapsing to
codec. The mismatch this section opened with is a mismatch only under Framing 1; under Framing 2 it dissolves.

Either framing keeps the §5.3–6 codec obligations, and either can ship without the rewriter *today* — Framing
1 permanently, Framing 2 until join/MERGE DML is actually wanted. What should NOT happen is keeping the
rewriter as-is while *assuming* Framing 1: that is the abstraction-level mismatch with no owning story, which
is the state this section exists to flag.
