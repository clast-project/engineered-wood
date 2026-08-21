# Which directories VACUUM may sweep, and why `_delta_index` is not protected

Delta's vacuum has a rule about which paths under the table root are eligible for
collection. It is not in `PROTOCOL.md` — it lives in the implementations, identically
enough in two of them to be treated as the specification. This note records it verbatim,
because the rule is unobvious in both directions and we got it backwards once already
(see [#54](https://github.com/clast-project/engineered-wood/issues/54)).

## The rule

`DeltaTableUtils.isHiddenDirectory`, delta-io/delta,
`spark/src/main/scala/org/apache/spark/sql/delta/DeltaTable.scala`:

```scala
/** Whether a path should be hidden for delta-related file operations, such as Vacuum and Fsck. */
def isHiddenDirectory(
    partitionColumnNames: Seq[String],
    pathName: String,
    shouldIcebergMetadataDirBeHidden: Boolean = true): Boolean = {
  // Names of the form partitionCol=[value] are partition directories, and should be
  // GCed even if they'd normally be hidden. The _db_index directory contains (bloom filter)
  // indexes and these must be GCed when the data they are tied to is GCed.
  // metadata name is reserved for converted iceberg metadata with delta universal format
  (shouldIcebergMetadataDirBeHidden && pathName.equals("metadata")) ||
  (pathName.startsWith(".") || pathName.startsWith("_")) &&
    !pathName.startsWith("_delta_index") && !pathName.startsWith("_change_data") &&
    !partitionColumnNames.exists(c => pathName.startsWith(c ++ "="))
}
```

delta-rs, `crates/core/src/operations/vacuum.rs`, reimplements it almost character for
character (without the Iceberg clause):

```rust
fn is_hidden_directory(partition_columns: &[String], path: &Path) -> Result<bool, DeltaTableError> {
    let path_name = path.to_string();
    Ok((path_name.starts_with('.') || path_name.starts_with('_'))
        && !path_name.starts_with("_delta_index")
        && !path_name.starts_with("_change_data")
        && !partition_columns
            .iter()
            .any(|partition_column| path_name.starts_with(partition_column)))
}
```

**Hidden means protected from the sweep.** So:

| Path | Hidden? | Vacuum |
|---|---|---|
| `_delta_log/` | yes | protected |
| any other `.`- or `_`-prefixed directory | yes | **protected** |
| `metadata/` (UniForm Iceberg output) | yes by default, flag-controlled | protected |
| `_delta_index/` | **no** — explicit carve-out | **swept** |
| `_change_data/` | **no** — explicit carve-out | **swept** |
| `<partitionCol>=<value>/` | **no**, even when the column name starts with `_` | swept |

The default posture is the opposite of the intuitive one, and the opposite of ours: the
spec **protects the whole hidden class and sweeps three named exceptions**, where
`VacuumExecutor.IsExcludedDirectory` protects two named directories and sweeps everything
else.

## Why `_delta_index` is swept

This is the part that reads as a bug and is not. A bloom filter index is *tied to* a
specific data file. When the data is collected the index must be collected with it, or the
table accumulates indexes pointing at files that no longer exist. Databricks documents
VACUUM as the intended cleanup path after `DROP BLOOMFILTER INDEX`.

A foreign engine sweeping the directory will also remove indexes for *live* data, because
its keep-set has no way to know which index belongs to which live file. delta-rs and OSS
Delta have exactly this behaviour, so it is ecosystem-normal rather than a defect unique to
us. Databricks has since deprecated bloom filter indexes and advises removing them, which
makes the whole case low-stakes.

## Our divergences

As of #92 there is **one**, and it is deliberate.

**`_change_data/` — deliberate, safe direction.** The spec sweeps it and relies on the
keep-set containing live CDF files. We cannot build that set: the snapshot does not track
`cdc` actions. So we protect the directory instead, which under-deletes (expired CDF is
never collected) but cannot destroy readable history. Revisit when the snapshot learns
about `cdc`. It now falls out of the general hidden rule rather than being a named
exclusion, so matching the spec exactly is one `continue` away — the same shape
`_delta_index` already uses.

## Closed

**Unknown hidden directories.** Was the substance of #54: we swept any `.foo/` or `_foo/`
the spec protects. Now protected, **per path component** rather than on the leading one
(see the ambiguity note below), and applied to files as well as directories, which is what
`DeltaFileOperations.recursiveListDirs` does — a recursive listing would otherwise reach a
hidden directory's contents by their full path and collect them one level down.

**`metadata/`.** Was a real gap: we swept UniForm's converted Iceberg metadata, which
nothing in the log references and no keep-set can protect. Now hidden by name, matching
upstream's literal `pathName.equals("metadata")` rather than any naming rule — no prefix
convention reaches it. The toggle is `DeltaTableOptions.HideIcebergMetadataDirectory`,
defaulting to hidden, which is upstream's default for `shouldIcebergMetadataDirBeHidden`.
Scoped to a directory component, since upstream's predicate is `isHiddenDirectory` and a
data file named `metadata` is not what is being protected.

**`_delta_index/` — still swept, now on purpose.** We used to sweep it because we swept
everything unnamed; it is now an explicit carve-out from the hidden rule, which is what
both references do. This is the trap worth remembering: the instinct when generalising the
rule is to protect it, and that would introduce a divergence rather than close one.

## One ambiguity between the two references

Whether the predicate applies to each path *segment* or only the leading one. Spark applies
it to directory names during recursive descent, so it holds at every level. delta-rs calls
`starts_with` on the whole relative path, so in practice only the first segment is tested —
a nested `data/_foo/x.parquet` is unprotected there.

**We follow Spark and apply it per segment.** The cost of the extra protection is
under-deletion, which is the direction vacuum already prefers everywhere else — and the
alternative is not stable under a recursive listing, which `LocalTableFileSystem` performs
(`SearchOption.AllDirectories`).

## Reading upstream

The comment in the Scala says `_db_index` while the code tests `_delta_index`. The comment
is stale; the code is authoritative.

## Sources

- [DeltaTable.scala](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/DeltaTable.scala)
- [VacuumCommand.scala](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/commands/VacuumCommand.scala)
- [delta-rs vacuum.rs](https://github.com/delta-io/delta-rs/blob/main/crates/core/src/operations/vacuum.rs)
- [Databricks bloom filter indexes (deprecated)](https://learn.microsoft.com/en-us/azure/databricks/optimizations/bloom-filters)

Read against the implementations on 2026-08-02.
