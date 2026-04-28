# Copyright (c) Curt Hagenlocher. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
#
# Generates Lance dataset fixtures for EngineeredWood.Lance.Table.Tests
# cross-validation. Each fixture is a Lance dataset *directory* (not a
# single .lance file), with `_versions/`, `data/`, and `_transactions/`.
#
# Usage:
#   pip install pylance pyarrow
#   python generate_test_data.py
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


if __name__ == "__main__":
    main()
