# Lance — future work

Snapshot of remaining gaps in `EngineeredWood.Lance` and
`EngineeredWood.Lance.Table` as of the v2.0/v2.1/v2.2 reader-and-writer
landing. Sorted within each section by likely user impact (highest
first).

## Type coverage

| Type | Reader | Writer | Notes |
|---|---|---|---|
| HalfFloat (Float16) | not yet | not yet | Common in ML embeddings; pylance writes it as `Flat(16)` with `logical_type="halffloat"`. The existing `LanceSchemaConverter` already understands the string. |
| LargeString, LargeBinary | not yet | not yet | Pylance falls back to these when offsets exceed 2 GiB. Schema converter and writer dispatch both need extension. |
| DenseUnion, SparseUnion | not yet | not yet | Rare in practice; Lance uses an "as-struct" representation under the hood. |
| FSL with `has_validity=true` (inner-element nulls) | reader supports | writer rejects | The reader's nested-leaf path handles the `validityBufSize` per-chunk encoding; the writer dispatch refuses non-zero `inner.NullCount`. |
| FSL with non-primitive inner (struct-of-FSL, FSL-of-FSL) | not yet | not yet | v2.2+ feature; pylance only writes FSL-of-primitive in v2.1. |
| List of List (writer) | reader supports | not yet | Reader's recursive cascade walker handles arbitrary nested lists; writer's `WriteListColumnCommonAsync` only accepts primitive / string / binary inner types. |

## Encoding coverage

| Encoding / Layout | Reader | Writer | Notes |
|---|---|---|---|
| `MiniBlockLayout` `General(ZSTD)` on FSL / Variable / Bool / nested cascades | only fixed-width primitive | only fixed-width primitive | Writer wraps Flat values when `LanceCompressionScheme.Zstd` is set; reader only unwraps in `DecodeFixedWidthMiniBlock`. Extending to other shapes unlocks compression for strings, lists, and bool. |
| `MiniBlockLayout` `Fsst` (FSST symbol-table strings) | yes | not yet | Writer always emits `Variable` for strings. FSST gives ~3-5× over Variable for repetitive text. |
| `MiniBlockLayout` `InlineBitpacking` / `OutOfLineBitpacking` (Fastlanes) | yes | not yet | Writer always emits `Flat`. Bitpacking gives ~3× over Flat for low-entropy integer columns. |
| `MiniBlockLayout` `Dictionary` (layout-level dictionary) | yes | not yet | Useful for low-cardinality string columns. |
| `FullZipLayout` (any shape) | yes | not yet | Used for "large value" columns where transposing costs more than zipping; writer always picks MiniBlock. |
| `ConstantLayout` (all-null pages) | yes | not yet | Writer would need to detect all-null pages and switch encoding. |
| `BlobLayout` (two-level external blob storage) | not yet | not yet | Used by pylance for very large binary values. |
| v2.0 / v2.1 `Rle`, `ByteStreamSplit`, `PackedStruct` | not yet | not yet | Niche; pylance rarely emits them. |

## Dataset / table layer

| Feature | Status | Notes |
|---|---|---|
| Multi-page nested types in the writer | not yet | `WriteColumnAsync(name, IReadOnlyList<IArrowArray>)` rejects struct / list / FSL / map. The reader handles multi-page nested without issue. |
| `UpdateAsync` with nested-typed columns | not yet | `TakeRows` only handles leaf shapes. Re-using the recursive read walker for take would close this. |
| `CompactAsync` with nested-typed columns | not yet | Same `TakeRows` limitation. |
| `CompactAsync` size targets / target-fragments | not yet | Always packs every compactable source fragment into a single output. Production deployments typically want target-row-count or target-byte-size knobs and parallel rewrite. |
| Fragment-level concurrency control (CAS commit) | not yet | Manifest filename uses `{u64::MAX - version}.manifest`; concurrent appenders can race. Need atomic-create or rename-based CAS, plus retry-with-rebase logic. |
| `DeleteAsync` / `UpdateAsync` with predicates over indexed columns | not yet | We don't use the secondary indices for fragment pruning during write-side operations. Reader does. |
| Roaring-bitmap deletion files (writer) | not yet | Reader handles both `ARROW_ARRAY` and `BITMAP`. Writer always emits `ARROW_ARRAY`; switching to `BITMAP` for dense deletes would save space (`>50%` deleted is the typical threshold). |
| Schema evolution (alter / rename / drop column) | not yet | No API. Lance supports adding columns to existing fragments by appending column files. |
| Adding columns via "data file augmentation" | not yet | Lance's schema model lets a fragment span multiple data files (one per column group). Our writer always uses a single file per fragment. |
| Index writing (B-tree, bitmap, vector) | not yet | Reader uses indices for fragment pruning. Writing indices for newly-appended fragments is unimplemented. |
| Deletion-vector compaction (re-rewrite the deletion file) | not yet | After many small deletes the deletion file grows; the same offsets accumulate into one Arrow file. |
| Row IDs (`row_id_sequence`) | reader exposes | not yet | The writer doesn't emit stable row IDs; this matters for joins and merge operations. |
| Field IDs that don't match column index | not yet | Our writer always assigns field IDs sequentially starting at 0. Lance's spec allows non-contiguous IDs (useful after schema evolution). |

## File-level features

| Feature | Status | Notes |
|---|---|---|
| Multipart-upload sequential file abstraction | not yet | The writer needs one contiguous local file or a stream-shaped target. Without multipart upload we can't write directly to S3 / Azure Blob without buffering. |
| Priority-aware range scheduling | not yet | The reader fetches columns in declaration order. Cloud workloads benefit from prioritising the smallest column first or interleaving by user-supplied priority. |
| HTTP multi-range requests | not yet | The reader coalesces nearby ranges on the client; HTTP/2 multi-range requests would consolidate further. |
| 64 KiB chunks (`has_large_chunk = true`) | reader accepts | writer never emits | A v2.2 feature for >32 KiB chunk sizes; useful for large-row datasets. |

## Validation gaps

- **Compatibility sweep**: every committed pylance fixture must read
  back identically. The repo currently ships ~80 pylance-produced
  `.lance` files; this set should grow as new encodings are exercised.
- **Writer round-trips via pylance**: most writer surfaces have a
  `*_CrossValidatedAgainstPylance` test. List-of-list, FSL-with-
  inner-nulls, and the deeper Map shapes (map-of-struct, map-of-list)
  don't yet — partly because the writer doesn't emit them.
- **ZSTD on non-fixed-width**: writer doesn't apply, reader doesn't
  unwrap; both move together when extending the General(ZSTD) scope.

## Where to start

If you want to extend this codebase, the highest-leverage bites in
priority order:

1. **HalfFloat (Float16)** — small, finite scope; closes the "every
   numeric Arrow type works" expectation.
2. **General(ZSTD) on Variable strings/binary** — biggest user-visible
   compression gap. Requires reader unwrap in `DecodeVariableMiniBlock`
   and writer wrap in `BuildVariableLeafSingleChunk` /
   `BuildVariablePageProtoAsync`.
3. **Roaring-bitmap deletion files (writer)** — once the deletion set
   is dense (say >50%), the bitmap is much smaller than the IPC
   `Int32Array`.
4. **`UpdateAsync` / `CompactAsync` with nested types** — extend
   `TakeRows` (or replace it with a recursive walker) to handle
   StructArray / ListArray / FixedSizeListArray / MapArray.
5. **Fragment-level CAS** — rename-based commit + retry-with-rebase
   on conflict. Required for any production multi-writer workflow.
