# Iceberg corpora

Tables written by other engines, committed with what each engine reads back from them. The Iceberg tests
are checked against these files (`Corpus/*Tests.cs`).

| Corpus | Writer | Tables |
|---|---|---|
| `spark/` | Spark 4.1.2 + iceberg-spark-runtime-4.1_2.13 1.12.0 | 13: every partition transform (identity, bucket, truncate, year/month/day/hour, void) over int, long, decimal, string, binary, date, timestamp, timestamptz, uuid and fixed; DVs, with several in one Puffin file; v2 position deletes; v2 equality deletes scoped by partition and sequence number; a v2 → v3 upgrade; defaults of 13 types; unshredded and shredded variant; branches and tags; a compaction carrying lineage |
| `iceberg-go/` | iceberg-go v0.8.0-rc0 | 5: the driver's scenarios (`v3_dv`, `v3_part`, `v2_upgrade`, `v3_cow`, `v3_eqdel`), with timestamptz_ns columns |

## Layout

- `corpus.json`: writer versions, the table list, and `warehouse_uri`, the neutral location everything was
  written under. Every path inside the tables is absolute under it, so tests map that prefix onto `wh/`.
- `wh/`: the tables, exactly as written. Names are short (warehouse `wh`, namespace `db`, partition
  fields like `i_b` and `dt_m`) so the deepest path stays well inside Windows' 260-character limit. `.gitattributes` keeps them byte for byte.
- `expected/<table>.json`: what the writer reads back.
  - For Spark: rows per ref, the lineage columns, and the `files`, `all_delete_files`, `manifests`,
    `snapshots` and `refs` metadata tables. `files` includes the raw bounds and `readable_metrics`.
  - For iceberg-go: its `scan` and `describe` output.

## Regenerating

Regenerating replaces the corpus, and every snapshot id, UUID and file name changes with it. Diff the
`expected/` files before committing.

- **Spark:** `Interop/spark/generate_corpus.py` (see its docstring for the environment). It takes about 30 s.
  Spark's `owner` table property is pinned to `ew-corpus`, so no user name lands in the metadata.
- **iceberg-go:** build the driver in `Interop/iceberg-go`, then run `Interop/iceberg-go/generate_corpus.py`.

## Gaps found while generating

Spark's own gaps are recorded in `expected/` as they are; the corpus does not work around them.

- **uuid bounds:** Iceberg 1.12's `files.readable_metrics` cannot render a uuid column (a
  `ClassCastException` in `MetricsUtil$ReadableColMetricsStruct`). For `t_javatypes` and
  `defaults`, `files` is captured without `readable_metrics`, and the error is kept as `files_error`.
- **Fixed partition values:** Iceberg Java (1.8.0 through main) writes a plain `fixed[N]` in a manifest as
  length-prefixed `bytes` and reads it back the same way (apache/iceberg#17501), so `t_javatypes`' manifest,
  with its identity(fixed[4]) partition, does not match its own Avro schema. fastavro refuses it as well.
- **Shredded-variant lineage:** Spark 4.1 + Iceberg 1.12 cannot project a shredded-variant table's lineage
  columns without the variant column (`Unsupported variant: shredded typed_value primitive`). The lineage
  is captured with the variant column included.
- **Lineage bounds in go:** iceberg-go's copy-on-write rewrite writes bounds for the reserved lineage
  columns (`_row_id`, `_last_updated_sequence_number`).
