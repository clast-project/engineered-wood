# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
#
# Generates Lance dataset fixtures for EngineeredWood.Lance.Table.Tests
# cross-validation. Each fixture is a Lance dataset *directory* (not a
# single .lance file), with `_versions/`, `data/`, and `_transactions/`.
#
# Usage:
#   pip install pylance pyarrow
#   python generate_test_data.py              # every fixture
#   python generate_test_data.py base_paths   # only base_paths_v21/
#   python generate_test_data.py schema_evolution   # only the #995 fixtures
#
# The generated directories are committed to source control so tests can
# run without invoking pylance.

import os
import shutil

import lance
import pyarrow as pa

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def write_dataset(name: str, *, mode_seq, version: str = "2.1") -> None:
    """Create a fresh dataset directory and apply a sequence of write
    operations. `mode_seq` is a list of (table, mode) pairs."""
    target = os.path.join(THIS_DIR, name)
    if os.path.exists(target):
        shutil.rmtree(target)
    for table, mode in mode_seq:
        lance.write_dataset(table, target, data_storage_version=version, mode=mode)
    sz = sum(os.path.getsize(os.path.join(r, f))
             for r, _, fs in os.walk(target) for f in fs)
    print(f"  {name}: {sz} bytes")


def main() -> None:
    # Single-fragment dataset, primitive types.
    write_dataset("simple_v21", mode_seq=[
        (pa.table({
            "id": pa.array(list(range(10)), type=pa.int32()),
            "name": pa.array([f"item-{i}" for i in range(10)], type=pa.string()),
        }), "create"),
    ])

    # Multi-fragment dataset (3 appends → 3 separate data files).
    write_dataset("three_fragments_v21", mode_seq=[
        (pa.table({"x": pa.array(list(range(0, 5)), type=pa.int32())}), "create"),
        (pa.table({"x": pa.array(list(range(5, 10)), type=pa.int32())}), "append"),
        (pa.table({"x": pa.array(list(range(10, 15)), type=pa.int32())}), "append"),
    ])

    # Dataset with multiple manifest versions (time travel input).
    write_dataset("multi_version_v21", mode_seq=[
        (pa.table({"v": pa.array(["v0a", "v0b"], type=pa.string())}), "create"),
        (pa.table({"v": pa.array(["v1c"], type=pa.string())}), "append"),
        (pa.table({"v": pa.array(["v2d", "v2e"], type=pa.string())}), "append"),
    ])

    # Sparse-deletion fixture: 100 rows, delete 3 → pylance picks
    # ARROW_ARRAY encoding (tiny .arrow file with one UInt32Array column
    # of deleted row offsets, ZSTD-compressed Arrow IPC stream).
    _write_with_delete("deletes_sparse_v21",
        pa.table({"id": pa.array(list(range(100)), type=pa.int32()),
                  "name": pa.array([f"r{i}" for i in range(100)], type=pa.string())}),
        delete_predicate="id IN (5, 17, 42)")

    # Dense-deletion fixture: 10000 rows, delete every other → pylance
    # picks the BITMAP encoding (.bin Roaring bitmap, CRoaring portable
    # variant where the no-run cookie is followed by a u32 container
    # count and a mandatory offset header).
    _write_with_delete("deletes_dense_v21",
        pa.table({"id": pa.array(list(range(10000)), type=pa.int32())}),
        delete_predicate="id % 2 = 0")

    # BTREE scalar index on a 3-fragment dataset. Each fragment has 5
    # int32 values in disjoint, increasing ranges so a predicate like
    # `x > 100` cleanly cuts to a subset of fragments via the index.
    _write_with_index("btree_index_v21",
        mode_seq=[
            (pa.table({"x": pa.array([1, 2, 3, 4, 5], type=pa.int32())}), "create"),
            (pa.table({"x": pa.array([10, 20, 30, 40, 50], type=pa.int32())}), "append"),
            (pa.table({"x": pa.array([100, 200, 300, 400, 500], type=pa.int32())}), "append"),
        ],
        indexed_field="x",
        index_type="BTREE")

    # BITMAP scalar index on a low-cardinality int32 column. Across 3
    # fragments, the unique values 1/2/3 are distributed so each value
    # appears in some-but-not-all fragments — letting a query like
    # `x = 3` skip fragments that don't contain it. Wire format
    # decoded: bitmap_page_lookup.lance has schema (keys, bitmaps); the
    # bitmaps blob is [u32 count][per entry: u32 frag_id, u32 size,
    # CRoaring portable Roaring32Map of row offsets].
    _write_with_index("bitmap_index_v21",
        mode_seq=[
            (pa.table({"x": pa.array([1, 2, 3], type=pa.int32())}), "create"),
            (pa.table({"x": pa.array([1, 2, 1], type=pa.int32())}), "append"),
            (pa.table({"x": pa.array([3, 1, 2], type=pa.int32())}), "append"),
        ],
        indexed_field="x",
        index_type="BITMAP")

    # A BTREE index on struct field s.x beside an unindexed top-level x
    # (#771): the index must not answer for x. Two fragments: x 0..99 and
    # 1000..1099, s.x 100..199 and 1100..1199.
    _write_with_index("nested_index_v21", mode_seq=_nested_index_tables(),
        indexed_field="s.x",
        index_type="BTREE")


def _nested_index_tables():
    tables = []
    for base, mode in [(0, "create"), (1000, "append")]:
        s = pa.StructArray.from_arrays(
            [pa.array(range(base + 100, base + 200), type=pa.int32())], names=["x"])
        tables.append((pa.table({
            "x": pa.array(range(base, base + 100), type=pa.int32()),
            "s": s,
        }), mode))
    return tables


def _write_with_index(name, mode_seq, indexed_field, index_type, version="2.1"):
    """Create a dataset and add a scalar index to a field."""
    target = os.path.join(THIS_DIR, name)
    if os.path.exists(target):
        shutil.rmtree(target)
    for table, mode in mode_seq:
        lance.write_dataset(table, target, data_storage_version=version, mode=mode)
    lance.LanceDataset(target).create_scalar_index(indexed_field, index_type)
    sz = sum(os.path.getsize(os.path.join(r, f))
             for r, _, fs in os.walk(target) for f in fs)
    print(f"  {name}: {sz} bytes")


def _write_with_delete(name, table, *, delete_predicate, version="2.1"):
    """Create a dataset, then delete rows matching the predicate."""
    target = os.path.join(THIS_DIR, name)
    if os.path.exists(target):
        shutil.rmtree(target)
    lance.write_dataset(table, target, data_storage_version=version, mode="create")
    lance.LanceDataset(target).delete(delete_predicate)
    sz = sum(os.path.getsize(os.path.join(r, f))
             for r, _, fs in os.walk(target) for f in fs)
    print(f"  {name}: {sz} bytes")


def write_schema_evolution_fixtures():
    """Datasets whose data files no longer line up with the manifest schema
    (#995). drop_columns and add_columns change only the manifest: the data
    file keeps its fields/column_indices, so a reader must map each manifest
    field to its column through them rather than by position."""
    struct_type = pa.struct([("x", pa.int64()), ("y", pa.string())])

    def flat():
        return pa.table({
            "a": pa.array([1, 2, 3], type=pa.int64()),
            "b": pa.array([10, 20, 30], type=pa.int64()),
            "c": pa.array([100, 200, 300], type=pa.int64()),
        })

    def nested():
        return pa.table({
            "a": pa.array([1, 2, 3], type=pa.int64()),
            "s": pa.array([{"x": 1, "y": "p"}, {"x": 2, "y": "q"}, {"x": 3, "y": "r"}],
                          type=struct_type),
            "l": pa.array([[1], [2, 3], []], type=pa.list_(pa.int64())),
            "c": pa.array([100, 200, 300], type=pa.int64()),
        })

    def evolve(name, table, version, change):
        target = os.path.join(THIS_DIR, name)
        if os.path.exists(target):
            shutil.rmtree(target)
        lance.write_dataset(table, target, data_storage_version=version)
        change(lance.dataset(target))
        ds = lance.dataset(target)
        files = [(df.fields, df.column_indices)
                 for f in ds.get_fragments() for df in f.metadata.files]
        print(f"  {name}: schema {ds.schema.names}, files {files}, "
              f"rows {ds.to_table().to_pylist()}")

    # Schema [b, c]; the file still holds a, b, c as fields [0, 1, 2].
    for version in ("2.1", "2.0"):
        evolve(f"dropped_column_v{version.replace('.', '')}", flat(), version,
               lambda ds: ds.drop_columns(["a"]))
    # Schema [s{y}, l, c]. In 2.1 the file lists leaves only (s.y alone is
    # still one contiguous column); in 2.0 it lists s, s.x, s.y as three
    # columns, so s{y} is no longer contiguous.
    for version in ("2.1", "2.0"):
        evolve(f"dropped_nested_v{version.replace('.', '')}", nested(), version,
               lambda ds: ds.drop_columns(["a", "s.x"]))
    # Schema [a, b, c, n]: n is in no data file (pylance reads it as nulls).
    evolve("added_null_column_v21", flat(), "2.1",
           lambda ds: ds.add_columns(pa.schema([pa.field("n", pa.int64())])))


def write_base_paths_fixtures(version="2.1"):
    """Datasets whose manifests have base_paths (#963). Each base path
    pylance stores is the ABSOLUTE local path of this machine's copy, so
    the tests copy base_paths_v21/ and point every base at the copy (the
    last segment of each stored path names the sibling directory).

    clone_source + clone: a shallow clone. The source has two fragments,
    one deleted row in each and a BTREE index; the clone then appends a
    fragment and deletes a row of fragment 0, so it mixes data and deletion
    files of base 0 (the source) with its own.

    multi: written with initial_bases/target_bases: fragment 0 under
    rootbase/data/ (is_dataset_root), fragment 1 directly in filebase/,
    fragment 2 under multi/data/, then a delete across all three (whose
    deletion files lance writes under multi/_deletions/)."""
    root = os.path.join(THIS_DIR, "base_paths_v21")
    if os.path.exists(root):
        shutil.rmtree(root)
    os.makedirs(root)

    def ids(a, b):
        return pa.table({"id": pa.array(range(a, b), type=pa.int32()),
                         "v": pa.array([f"v{i}" for i in range(a, b)])})

    source = os.path.join(root, "clone_source")
    lance.write_dataset(ids(0, 10), source, data_storage_version=version)
    ds = lance.write_dataset(ids(10, 20), source, mode="append")
    ds.delete("id = 3 or id = 13")
    ds.create_scalar_index("id", "BTREE")
    clone = ds.shallow_clone(os.path.join(root, "clone"), ds.version)
    clone.insert(ids(100, 102))
    clone.delete("id = 5")

    main = os.path.join(root, "multi")
    lance.write_dataset(ids(0, 10), main, data_storage_version=version,
        initial_bases=[
            lance.DatasetBasePath(os.path.join(root, "rootbase"), name="rb", is_dataset_root=True),
            lance.DatasetBasePath(os.path.join(root, "filebase"), name="fb", is_dataset_root=False),
        ],
        target_bases=["rb"])
    lance.write_dataset(ids(10, 20), main, mode="append", target_bases=["fb"])
    ds = lance.write_dataset(ids(20, 30), main, mode="append")
    ds.delete("id % 10 = 0")

    for name in ("clone", "multi"):
        d = lance.dataset(os.path.join(root, name))
        print(f"  base_paths_v21/{name}: {d.count_rows()} rows, bases "
              f"{ {k: (v.path, v.is_dataset_root) for k, v in d.base_paths().items()} }")


if __name__ == "__main__":
    import sys
    if sys.argv[1:] == ["base_paths"]:
        write_base_paths_fixtures()
    elif sys.argv[1:] == ["schema_evolution"]:
        write_schema_evolution_fixtures()
    else:
        main()
        write_base_paths_fixtures()
        write_schema_evolution_fixtures()
