# Documentation

## Guides and reference

These describe current behaviour.

| Document | Contents |
|---|---|
| [known-issues.md](known-issues.md) | Known gaps and interop caveats, per package |
| [embedding-host-guide.md](embedding-host-guide.md) | Using the Delta log layer from a host engine that brings its own data plane |
| [delta-concurrency.md](delta-concurrency.md) | Delta optimistic concurrency: conflicts, retries, rebase |
| [running-tests.md](running-tests.md) | Running the test suites and the interop tiers (PySpark, delta-rs, DuckDB, cloud emulators) |
| [releasing.md](releasing.md) | Cutting a release: the API compatibility check, tagging, trusted publishing |
| [lance.md](lance.md) | Lance format, type and encoding coverage |
| [lance-future-work.md](lance-future-work.md) | Remaining Lance gaps, by user impact |
| [vortex.md](vortex.md) | Vortex format, type and encoding coverage |
| [row-tracking-conformance-brief.md](row-tracking-conformance-brief.md) | Delta row tracking: the spec, EngineeredWood's behaviour, measured Spark behaviour |
| [vacuum-hidden-directories.md](vacuum-hidden-directories.md) | Which paths VACUUM may delete, and where EngineeredWood differs from Spark |
| [arrow-span-lifetime.md](arrow-span-lifetime.md) | Why spans over native Arrow buffers can outlive their memory, and the keep-alive rule |

## Design notes

Point-in-time records of designs, experiments and measurements. They explain why the code is the way it is,
but are not kept up to date as the code changes. Check the code before relying on one.

| Document | Kind | Contents |
|---|---|---|
| [predicate-pushdown-design.md](predicate-pushdown-design.md) | design | The shared expression library and pushdown into Parquet, Delta, Iceberg and Lance |
| [row-level-filtering.md](row-level-filtering.md) | design | Exact row-level filtering and late materialization in the Parquet reader and Delta scans (#502): design, per-phase record and measurements (implemented through phase 5; follow-ups #804 and the all-false prune in §31-§32) |
| [parquet-page-index.md](parquet-page-index.md) | design | ColumnIndex / OffsetIndex plan and per-phase record (implemented) |
| [parquet-pfor.md](parquet-pfor.md) | design | Experimental PFOR encoding: layout, divergences from the proposal, testing |
| [parquet-fsst.md](parquet-fsst.md) | design | Experimental FSST / FSST_16: the spec versus the proofs of concept, validation |
| [parquet-extended-precision-timestamps.md](parquet-extended-precision-timestamps.md) | design | Experimental `TIMESTAMP` on `FIXED_LEN_BYTE_ARRAY(12)`: decisions, risks, limits |
| [encryption-design.md](encryption-design.md) | design | Plan for Parquet Modular Encryption (not started) |
| [ree-constant-column-encoding.md](ree-constant-column-encoding.md) | design | Run-end encoding for constant columns: what landed and what was measured |
| [constant-array-and-widening-resume.md](constant-array-and-widening-resume.md) | design | Outcome of the Arrow-builder audit (`ArrowCompute` Repeat / MakeNullArray / Widen) |
| [codec-seam-investigation.md](codec-seam-investigation.md) | investigation | Why `IDataFileReader` / `IDataFileWriter` stay a codec seam, and its open contract gaps |
| [parquet_encoding_research.md](parquet_encoding_research.md) | investigation | How Java, C++, Rust, Go and DuckDB choose encodings and sizes when writing |
| [dictionary-encoding-cost.md](dictionary-encoding-cost.md) | investigation | The cost of a declined dictionary attempt, and the fixes made |
| [batched-bitpacked-runs.md](batched-bitpacked-runs.md) | investigation | Prototype: batching RLE literal runs (off by default) |
| [fixed-list-fast-path.md](fixed-list-fast-path.md) | investigation | Prototype: a fast path for fixed-length lists |
| [plank-comparison.md](plank-comparison.md) | investigation | Comparison with Plank; a native buffer pool prototype (not merged) |
| [lance-reader-design.md](lance-reader-design.md) | historical | The original Lance reader design |
