# Copyright (c) Curt Hagenlocher. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
#
# Generates the Phase 2 cross-validation .lance files using pylance.
#
# Usage:
#   pip install pylance pyarrow
#   python generate_test_data.py
#
# Writes .lance files next to this script. pylance produces datasets (a
# directory with a data/ subdir holding a randomly-named .lance file); this
# script extracts that file to a clean name.

import os
import shutil
import tempfile

import lance
import pyarrow as pa

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def write_one(name: str, table: pa.Table, version: str = "2.0") -> None:
    with tempfile.TemporaryDirectory() as td:
        ds_path = os.path.join(td, name)
        lance.write_dataset(
            table, ds_path,
            data_storage_version=version,
            mode="create",
        )
        # pylance creates exactly one .lance data file for small tables.
        lance_files = [
            os.path.join(root, f)
            for root, _, files in os.walk(ds_path)
            for f in files
            if f.endswith(".lance")
        ]
        if len(lance_files) != 1:
            raise RuntimeError(
                f"Expected 1 .lance file for {name}, got {len(lance_files)}: {lance_files}")
        out = os.path.join(THIS_DIR, f"{name}.lance")
        shutil.copyfile(lance_files[0], out)
        print(f"  {os.path.getsize(out):6d}  {out}")


def main() -> None:
    write_one("int32_nonull",
              pa.table({"x": pa.array([1, 2, 3, 4, 5], type=pa.int32())}))
    write_one("int32_nulls",
              pa.table({"x": pa.array([1, None, 3, None, 5], type=pa.int32())}))
    write_one("int32_allnull",
              pa.table({"x": pa.array([None, None, None], type=pa.int32())}))
    write_one("int64_nonull",
              pa.table({"x": pa.array([10, 20, 30, 40, 50], type=pa.int64())}))
    write_one("double_nonull",
              pa.table({"d": pa.array([1.5, 2.5, 3.5], type=pa.float64())}))
    write_one("float_nonull",
              pa.table({"f": pa.array([1.5, 2.5, 3.5], type=pa.float32())}))
    write_one("bool_nonull",
              pa.table({"b": pa.array([True, False, True, True], type=pa.bool_())}))
    write_one("string_nonull",
              pa.table({"s": pa.array(["foo", "bar", "baz"], type=pa.string())}))
    write_one("string_nulls",
              pa.table({"s": pa.array(["foo", None, "baz"], type=pa.string())}))
    write_one("fsb_nonull",
              pa.table({"fb": pa.array([b"abcd", b"efgh", b"ijkl"], type=pa.binary(4))}))
    # Repetitive strings (2 unique values × 50 rows each) trigger pylance's
    # Dictionary encoding in v2.0 — exercises DictionaryDecoder + BinaryDecoder
    # for the items sub-encoding.
    write_one("repetitive_strings",
              pa.table({"s": pa.array(["hello world"] * 50 + ["goodbye world"] * 50,
                                      type=pa.string())}))

    # Phase 5: nested types.
    write_one("list_int",
              pa.table({"xs": pa.array([[1, 2, 3], [4], [], [5, 6]],
                                        type=pa.list_(pa.int32()))}))
    write_one("list_nulls",
              pa.table({"xs": pa.array([[1, 2], None, [3, 4]],
                                        type=pa.list_(pa.int32()))}))
    write_one("fsl_int",
              pa.table({"fs": pa.array([[1, 2, 3], [4, 5, 6]],
                                        type=pa.list_(pa.int32(), list_size=3))}))
    write_one("struct_2i32",
              pa.table({"s": pa.array([{"a": 1, "b": 10}, {"a": 2, "b": 20}],
                                       type=pa.struct([("a", pa.int32()),
                                                       ("b", pa.int32())]))}))
    # Reminder: v2.0 SimpleStruct has no validity bitmap, so pylance materializes
    # "null struct" rows as structs of default-valued children ({a:0, b:0} here).
    write_one("struct_nulls",
              pa.table({"s": pa.array([{"a": 1, "b": 10}, None, {"a": 3, "b": 30}],
                                       type=pa.struct([("a", pa.int32()),
                                                       ("b", pa.int32())]))}))
    write_one("list_struct",
              pa.table({"xs": pa.array([[{"a": 1}, {"a": 2}], [{"a": 3}]],
                                        type=pa.list_(pa.struct([("a", pa.int32())])))}))

    # Phase 6: v2.1 structural layouts. Only primitive leaves are exercised
    # (nested types + strings move to Phase 7 because v2.1 encodes them via
    # rep/def and Variable, neither of which Phase 6 handles).
    write_one("int32_v21",
              pa.table({"x": pa.array([1, 2, 3, 4, 5], type=pa.int32())}),
              version="2.1")
    write_one("int32_nulls_v21",
              pa.table({"x": pa.array([1, None, 3, None, 5], type=pa.int32())}),
              version="2.1")
    write_one("int64_v21",
              pa.table({"x": pa.array([10, 20, 30, 40, 50], type=pa.int64())}),
              version="2.1")
    write_one("double_v21",
              pa.table({"d": pa.array([1.5, 2.5, 3.5], type=pa.float64())}),
              version="2.1")
    # Big enough to produce more than one mini-block chunk. 2000 × 4B = 8KB
    # which crosses the default MAX_MINIBLOCK_BYTES threshold. Use a PRNG-like
    # sequence with full 32-bit entropy so pylance picks Flat over
    # InlineBitpacking (which is a Phase 9 / Fastlanes dependency).
    random_like = [((i * 1103515245 + 12345) & 0xFFFFFFFF) for i in range(2000)]
    random_like = [v - 0x100000000 if v >= 0x80000000 else v for v in random_like]
    write_one("large_int32_v21",
              pa.table({"x": pa.array(random_like, type=pa.int32())}),
              version="2.1")

    # Phase 7: v2.1 rep/def — strings and lists of primitives.
    write_one("string_v21",
              pa.table({"s": pa.array(["foo", "bar", "baz"], type=pa.string())}),
              version="2.1")
    write_one("string_nulls_v21",
              pa.table({"s": pa.array(["foo", None, "baz"], type=pa.string())}),
              version="2.1")
    write_one("list_int_v21",
              pa.table({"xs": pa.array([[1, 2, 3], [4], [], [5, 6]],
                                        type=pa.list_(pa.int32()))}),
              version="2.1")
    write_one("list_nulls_v21",
              pa.table({"xs": pa.array([[1, 2], None, [3, 4]],
                                        type=pa.list_(pa.int32()))}),
              version="2.1")
    # list<FixedSizeList<float32, 1024>> — pylance picks FullZipLayout
    # with bits_rep=1, bits_def=2 (NULL_AND_EMPTY_LIST). Each row in
    # buffer 0 starts with a 1-byte ctrl word (def in low 2 bits, rep in
    # bit 2); visible rows are followed by 4096-byte FSL payloads. The
    # nested-leaf path now decodes this via DecodeNestedLeafPage.
    _list_big_fsl_t = pa.list_(pa.list_(pa.float32(), list_size=1024))
    write_one("list_big_embeddings_v21",
              pa.table({"x": pa.array([
                  [[float(j) for j in range(1024)], [float(j + 1) for j in range(1024)]],
                  [],
                  None,
                  [[float(j + 2) for j in range(1024)]],
              ], type=_list_big_fsl_t)}),
              version="2.1")

    # FixedSizeList<float32, 1024> nullable — large embeddings with row
    # nulls trigger pylance's FullZipLayout with bits_def=1 + has_validity.
    _embed_dim = 1024
    write_one("nullable_embeddings_v21",
              pa.table({"emb": pa.array(
                  [[float(j) for j in range(_embed_dim)] if i % 2 == 0 else None
                   for i in range(5)],
                  type=pa.list_(pa.float32(), list_size=_embed_dim))}),
              version="2.1")

    # list<FixedSizeList<float32, 3>> with inner nulls — exercises the
    # has_validity=true + num_buffers=2 path on the FSL nested-leaf
    # decoder. Inner None positions become per-item validity bits.
    write_one("list_fsl_inner_nulls_v21",
              pa.table({"lf": pa.array(
                  [[[1.0, None, 3.0], [4.0, 5.0, None]],
                   None,
                   [[7.0, 8.0, 9.0]]],
                  type=pa.list_(pa.list_(pa.float32(), list_size=3)))}),
              version="2.1")

    # struct<int32, FixedSizeList<float32, 3>> — FSL inside a struct.
    _struct_fsl_t = pa.struct([
        ("id", pa.int32()),
        ("emb", pa.list_(pa.float32(), list_size=3)),
    ])
    write_one("struct_fsl_v21",
              pa.table({"r": pa.array([
                  {"id": 1, "emb": [1.0, 2.0, 3.0]},
                  {"id": 2, "emb": [4.0, 5.0, 6.0]},
                  {"id": 3, "emb": [7.0, 8.0, 9.0]},
              ], type=_struct_fsl_t)}),
              version="2.1")

    # list<FixedSizeList<float32, 3>> — exercises the recursive walker's
    # FSL dispatch. Mixed shapes: empty inner list, null outer row,
    # and 1-2 FSL rows per outer list. has_validity stays false (no
    # per-float nulls).
    _list_fsl_t = pa.list_(pa.list_(pa.float32(), list_size=3))
    write_one("list_fsl_float_v21",
              pa.table({"lf": pa.array(
                  [[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
                   [],
                   None,
                   [[7.0, 8.0, 9.0]]],
                  type=_list_fsl_t)}),
              version="2.1")

    # Large list<int32> → triggers a multi-chunk page on the leaf column.
    # Use full-int32-range values via a PRNG-style formula so pylance
    # picks Flat (not InlineBitpacking) for the leaf encoding. 100K rows
    # of 1-5 ints each emits hundreds of mini-block chunks.
    _big_list_int_data = []
    _list_seed = 0x9e3779b9
    _val_seed = 0x12345678
    for _r in range(100_000):
        _list_seed = (_list_seed * 1103515245 + 12345) & 0xFFFFFFFF
        _list_len = 1 + (_list_seed >> 16) % 5
        _row = []
        for _ in range(_list_len):
            _val_seed = (_val_seed * 1103515245 + 12345) & 0xFFFFFFFF
            _row.append((_val_seed - 0x80000000) & 0xFFFFFFFF)
            # Convert to signed int32
            if _row[-1] >= 0x80000000:
                _row[-1] -= 0x100000000
        _big_list_int_data.append(_row)
    write_one("big_list_int_v21",
              pa.table({"xs": pa.array(_big_list_int_data,
                                        type=pa.list_(pa.int32()))}),
              version="2.1")

    # Phase 8: v2.1 FullZipLayout. Large fixed-size-list values trigger
    # pylance's FullZip encoding (one I/O per value, used for embeddings
    # and similar big-per-value columns). Use a deterministic pattern so
    # tests can verify exact values without replicating a PRNG.
    #   value[row, j] = row * dim + j
    embeddings_dim = 1024
    embeddings_rows = 10
    emb_rows = [[float(r * embeddings_dim + j) for j in range(embeddings_dim)]
                for r in range(embeddings_rows)]
    write_one("embeddings_v21",
              pa.table({"e": pa.array(emb_rows, type=pa.list_(pa.float32(), embeddings_dim))}),
              version="2.1")

    big_dim = 4096
    big_rows = 5
    big_data = [[float(r * big_dim + j) for j in range(big_dim)]
                for r in range(big_rows)]
    write_one("big_fsl_v21",
              pa.table({"fs": pa.array(big_data, type=pa.list_(pa.float32(), big_dim))}),
              version="2.1")

    # Phase 9: Fastlanes InlineBitpacking. Sequential 0..1999 has small bit
    # widths per chunk (10 bits for the first 1024 values, 11 bits for the
    # next 976) and pylance picks InlineBitpacking. Multi-chunk to exercise
    # the per-chunk varying bit widths.
    write_one("inline_bp_int32_v21",
              pa.table({"x": pa.array(list(range(2000)), type=pa.int32())}),
              version="2.1")
    # Smaller / bigger ranges to exercise different bit widths in a single
    # chunk.
    write_one("inline_bp_int32_small_v21",
              pa.table({"x": pa.array(list(range(0, 1000, 5)), type=pa.int32())}),
              version="2.1")

    # Phase 10: broaden the v2.0 type matrix. Extra primitives we hadn't
    # exercised (int8, int16, uint16, uint32, uint64, halffloat, date32,
    # timestamp_us, decimal128) plus zero-row and single-row edge cases.
    import pyarrow.compute as _pc  # noqa: F401  (silences "unused import" if pyarrow strips)
    write_one("int8_nonull",
              pa.table({"x": pa.array([-128, -1, 0, 1, 127], type=pa.int8())}))
    write_one("int16_nonull",
              pa.table({"x": pa.array([-32768, -1, 0, 1, 32767], type=pa.int16())}))
    write_one("uint16_nonull",
              pa.table({"x": pa.array([0, 1, 65535], type=pa.uint16())}))
    write_one("uint32_nonull",
              pa.table({"x": pa.array([0, 4294967295], type=pa.uint32())}))
    write_one("uint64_nonull",
              pa.table({"x": pa.array([0, 18446744073709551615], type=pa.uint64())}))
    write_one("date32_nonull",
              pa.table({"d": pa.array([0, 19000, -1], type=pa.date32())}))
    write_one("timestamp_us_nonull",
              pa.table({"t": pa.array([0, 1_700_000_000_000_000, -1], type=pa.timestamp("us"))}))

    # Edge case: a single-row file. (pylance refuses to write a 0-row
    # dataset, so an "empty" file isn't a producible round-trip case.)
    write_one("int32_one_row",
              pa.table({"x": pa.array([42], type=pa.int32())}))
    write_one("int32_one_row_v21",
              pa.table({"x": pa.array([42], type=pa.int32())}),
              version="2.1")

    # Phase 7b: v2.1 multi-leaf struct. Each leaf is a separate physical
    # column; outer-layer rep/def levels are shared across siblings and
    # encode struct-level nullability (def=2 with [NULLABLE_ITEM,
    # NULLABLE_ITEM] → struct row is null, cascades to all children).
    write_one("struct_2i32_v21",
              pa.table({"s": pa.array(
                  [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 3, "b": 30}],
                  type=pa.struct([("a", pa.int32()), ("b", pa.int32())]))}),
              version="2.1")
    write_one("struct_2i32_nullable_v21",
              pa.table({"s": pa.array(
                  [{"a": 1, "b": 10}, None, {"a": 3, "b": 30}],
                  type=pa.struct([("a", pa.int32()), ("b", pa.int32())]))}),
              version="2.1")

    # List-of-struct, v2.1. Three rep/def layers:
    #   [<leaf>, <struct>, <list>]
    # Each struct leaf is a separate physical column; rep+def levels are
    # shared across leaves and across the list boundary. Three rows of
    # varying list lengths exercise the rep encoding too.
    list_struct_type = pa.list_(pa.struct([("a", pa.int32()), ("b", pa.int32())]))
    write_one("list_struct_v21",
              pa.table({"xs": pa.array(
                  [[{"a": 1, "b": 10}, {"a": 2, "b": 20}],
                   [{"a": 3, "b": 30}],
                   [{"a": 4, "b": 40}, {"a": 5, "b": 50}, {"a": 6, "b": 60}]],
                  type=list_struct_type)}),
              version="2.1")

    # Same but with nullable list, nullable struct, and a couple of edge
    # rows: a null list, an empty list, and a null struct inside a list.
    write_one("list_struct_nullable_v21",
              pa.table({"xs": pa.array(
                  [[{"a": 1, "b": 10}, {"a": 2, "b": 20}],
                   None,                                 # null list
                   [],                                   # empty list
                   [{"a": 3, "b": 30}, None, {"a": 5, "b": 50}],  # null struct mid-list
                   [{"a": 6, "b": 60}]],
                  type=list_struct_type)}),
              version="2.1")

    # Deeper composition: struct<m: list<struct<a: int, b: int>>>. Each
    # leaf has 4 layers [item, inner_struct, list, outer_struct]. Tests the
    # walker against the deepest shape pylance currently reaches in our
    # test set: a list of structs sitting inside an outer struct.
    deep_lis_type = pa.struct([
        ("m", pa.list_(pa.struct([("a", pa.int32()), ("b", pa.int32())]))),
    ])
    deep_lis_rows = []
    for i in range(8):
        if i == 3: deep_lis_rows.append({"m": None})
        elif i == 5: deep_lis_rows.append(None)
        elif i == 6: deep_lis_rows.append({"m": []})
        else:
            deep_lis_rows.append({"m": [
                {"a": 17 + i * 11 + 0, "b": 113 + i * 19 + 0},
                {"a": 17 + i * 11 + 1, "b": 113 + i * 19 + 1},
            ]})
    write_one("struct_list_struct_v21",
              pa.table({"s": pa.array(deep_lis_rows, type=deep_lis_type)}),
              version="2.1")

    # list<struct<list<int32>>>. Layers [item, inner_list, struct, outer_list]:
    # struct *between* two list layers. Tests that struct-between-lists
    # cascade behaves differently from list-cascade.
    write_one("list_struct_list_int_v21",
              pa.table({"xs": pa.array(
                  [[{"m": [1, 2]}, {"m": [3]}],
                   [{"m": []}, {"m": [4, 5]}],
                   None,
                   [{"m": [6]}]],
                  type=pa.list_(pa.struct([("m", pa.list_(pa.int32()))])))}),
              version="2.1")

    # list<list<int32>>. Two list layers, so rep ∈ {0, 1, 2}.
    # 5 outer rows: [[1,2],[3]], [], None, [[4],[],[5,6,7]], [[8,9]].
    write_one("list_list_int_v21",
              pa.table({"xs": pa.array(
                  [[[1, 2], [3]],
                   [],
                   None,
                   [[4], [], [5, 6, 7]],
                   [[8, 9]]],
                  type=pa.list_(pa.list_(pa.int32())))}),
              version="2.1")

    # Three-deep struct nesting: struct<l1: struct<l2: struct<a, b>>>.
    # Each leaf has 4 layers [item, l2, l1, top]. Nullability at every
    # level — exercises the recursive walker's cascade.
    deep_struct_type = pa.struct([
        ("l1", pa.struct([
            ("l2", pa.struct([("a", pa.int32()), ("b", pa.int32())])),
        ])),
    ])
    # Use a longer row sequence with prime-spaced values so pylance picks
    # Flat (not Rle) for the value compression.
    deep_rows = []
    for i in range(12):
        if i == 4: deep_rows.append({"l1": {"l2": None}})            # l2 null
        elif i == 7: deep_rows.append({"l1": None})                  # l1 null
        elif i == 9: deep_rows.append(None)                          # top null
        else:
            deep_rows.append({"l1": {"l2": {"a": 17 + i * 11, "b": 113 + i * 19}}})
    write_one("struct_depth3_v21",
              pa.table({"s": pa.array(deep_rows, type=deep_struct_type)}),
              version="2.1")

    # Mixed-shape outer struct with a struct grandchild. Layer shapes per
    # leaf:
    #   x:       [item, outer_struct]                 (2 layers, no rep)
    #   ab.a:    [item, inner_struct, outer_struct]   (3 layers, no rep)
    #   ab.b:    [item, inner_struct, outer_struct]   (3 layers, no rep)
    mixed_struct_child_type = pa.struct([
        ("x", pa.int32()),
        ("ab", pa.struct([("a", pa.int32()), ("b", pa.int32())])),
    ])
    write_one("struct_mixed_with_struct_child_v21",
              pa.table({"s": pa.array(
                  [{"x": 11, "ab": {"a": 17, "b": 113}},
                   {"x": 22, "ab": None},                   # inner null
                   None,                                     # outer null
                   {"x": 44, "ab": {"a": 41, "b": 313}},
                   {"x": 55, "ab": {"a": 53, "b": 419}},
                   {"x": 66, "ab": {"a": 67, "b": 521}}],
                  type=mixed_struct_child_type)}),
              version="2.1")

    # Mixed-shape outer struct: outer has children of different "shapes"
    # (one primitive, one list). The two leaf columns will have different
    # layer counts: x's layers = [item, outer_struct]; xs's layers =
    # [item, list, outer_struct]. They still share the outer-struct layer.
    mixed_outer_type = pa.struct([
        ("x", pa.int32()),
        ("xs", pa.list_(pa.int32())),
    ])
    write_one("struct_mixed_shapes_v21",
              pa.table({"s": pa.array(
                  [{"x": 1, "xs": [10, 11]},
                   {"x": 2, "xs": [20]},
                   None,                                 # outer null
                   {"x": 3, "xs": []},                   # empty list
                   {"x": 4, "xs": None},                 # null list, primitive valid
                   {"x": 5, "xs": [50, 51, 52]}],
                  type=mixed_outer_type)}),
              version="2.1")

    # large_list: same data shape as list but Arrow LargeListType uses
    # i64 offsets. On disk in Lance v2.1 the encoding should be identical
    # to a regular list — only the Arrow schema metadata differs.
    write_one("large_list_int_v21",
              pa.table({"xs": pa.array(
                  [[1, 2, 3], [4, 5], [], None, [6]],
                  type=pa.large_list(pa.int32()))}),
              version="2.1")

    # FixedSizeList<int32, dim=3> with rows that are themselves null. Only
    # whole-row null, not leaf null inside the FSL — pyarrow rejects the
    # latter for fixed_size_list.
    write_one("fsl_int_nullable_v21",
              pa.table({"xs": pa.array(
                  [[1, 2, 3], [4, 5, 6], None, [7, 8, 9]],
                  type=pa.list_(pa.int32(), list_size=3))}),
              version="2.1")

    # struct-of-struct, v2.1. Three layers per leaf: [item, inner_struct,
    # outer_struct]. Both outer and inner struct can be null (cascades).
    nested_struct_type = pa.struct([
        ("inner", pa.struct([("a", pa.int32()), ("b", pa.int32())])),
    ])
    write_one("struct_of_struct_v21",
              pa.table({"s": pa.array(
                  [{"inner": {"a": 17, "b": 113}},
                   {"inner": {"a": 29, "b": 227}},
                   {"inner": {"a": 41, "b": 313}}],
                  type=nested_struct_type)}),
              version="2.1")
    # Use varied / prime-spaced values so pylance picks Flat instead of Rle
    # for the value compression. Keep the structure: row 1 has inner null,
    # row 3 has outer struct null, plus a few all-valid rows.
    write_one("struct_of_struct_nullable_v21",
              pa.table({"s": pa.array(
                  [{"inner": {"a": 17, "b": 113}},
                   {"inner": None},                           # inner null
                   None,                                       # outer null
                   {"inner": {"a": 41, "b": 313}},
                   {"inner": {"a": 53, "b": 419}},
                   {"inner": {"a": 67, "b": 521}}],
                  type=nested_struct_type)}),
              version="2.1")

    # struct-of-list, v2.1. Three layers per leaf: [item, list, outer_struct].
    struct_of_list_type = pa.struct([("xs", pa.list_(pa.int32()))])
    write_one("struct_of_list_v21",
              pa.table({"s": pa.array(
                  [{"xs": [1, 2, 3]},
                   {"xs": []},
                   {"xs": [4, 5]},
                   None,                  # outer struct null
                   {"xs": None}],         # outer struct valid, inner list null
                  type=struct_of_list_type)}),
              version="2.1")

    # Inner-only nullable: outer struct non-nullable in the Arrow schema,
    # one child non-nullable, the other nullable. Forces pylance to pick
    # different RepDefLayer combinations for siblings sharing the same
    # outer (struct) layer:
    #   s.a: [ALL_VALID_ITEM, ALL_VALID_ITEM]  (the existing all-valid case)
    #   s.b: [NULLABLE_ITEM,  ALL_VALID_ITEM]  (only leaf can be null)
    inner_struct_type = pa.struct([
        pa.field("a", pa.int32(), nullable=False),
        pa.field("b", pa.int32(), nullable=True),
    ])
    write_one("struct_inner_nullable_v21",
              pa.table({"s": pa.array(
                  [{"a": 1, "b": 10}, {"a": 2, "b": None}, {"a": 3, "b": 30}],
                  type=inner_struct_type)},
                  schema=pa.schema([pa.field("s", inner_struct_type, nullable=False)])),
              version="2.1")


    # Phase 14: multi-chunk Variable in MiniBlock. Even modest string columns
    # get sliced into multiple mini-block chunks (any column past ~64 rows
    # of varied 200B strings hits 6+ chunks for example). Use a deterministic
    # PRNG-style filler so tests can verify exact bytes without rerunning
    # NumPy.
    long_strings = [
        f"row-{i:06d}-{'x' * (50 + (i * 17) % 30)}-end" for i in range(200)
    ]
    write_one("multichunk_strings_v21",
              pa.table({"s": pa.array(long_strings, type=pa.string())}),
              version="2.1")
    # With nulls scattered through the multi-chunk page.
    long_with_nulls = [
        s if i % 7 != 3 else None
        for i, s in enumerate(long_strings)
    ]
    write_one("multichunk_strings_nulls_v21",
              pa.table({"s": pa.array(long_with_nulls, type=pa.string())}),
              version="2.1")

    # MiniBlockLayout-level dictionary (low-cardinality strings). pylance
    # picks this layout when string values heavily repeat — it stores a
    # dictionary on the MiniBlockLayout itself (not as a CompressiveEncoding
    # wrapper) and per-row indices into it.
    dupes = ["cat", "dog", "fish", "bird", "snake"] * 20
    write_one("dict_strings_v21",
              pa.table({"s": pa.array(dupes, type=pa.string())}),
              version="2.1")
    dupes_with_nulls = [v if i % 5 != 0 else None for i, v in enumerate(dupes)]
    write_one("dict_strings_nulls_v21",
              pa.table({"s": pa.array(dupes_with_nulls, type=pa.string())}),
              version="2.1")

    # FullZipLayout(bits_per_offset) + General(ZSTD) + Variable for very
    # large strings. pylance picks this layout when individual values are
    # large enough that per-value compression beats mini-block packing
    # (~64KB per value here). Each row's payload is a 12-byte framing
    # header (u32 LE compressed_remainder + u64 LE uncompressed_size)
    # followed by a standard ZSTD frame; nullable variant prepends a
    # 1-byte def marker (0 = valid, 1 = null).
    import hashlib
    def _make_big_string(seed: int, size: int) -> str:
        out = []
        h = hashlib.sha256(str(seed).encode()).digest()
        while len(out) * len(h) < size:
            out.append(h.hex())
            h = hashlib.sha256(h).digest()
        return "".join(out)[:size]

    big_rows = [_make_big_string(i, 65536) for i in range(5)]
    write_one("big_strings_v21",
              pa.table({"s": pa.array(big_rows, type=pa.string())}),
              version="2.1")
    big_rows_nulls = [_make_big_string(i, 65536) if i % 3 != 1 else None
                       for i in range(6)]
    write_one("big_strings_nulls_v21",
              pa.table({"s": pa.array(big_rows_nulls, type=pa.string())}),
              version="2.1")

    # FSST-compressed strings. Pylance picks Fsst value_compression for
    # medium-cardinality string columns whose values share enough common
    # substrings that a learned 256-symbol table beats raw Variable encoding
    # but doesn't repeat enough for layout-level Dictionary. Natural-text
    # patterns hit this reliably. The nullable variant uses
    # OutOfLineBitpacking(bits_per_value=1) for def levels, fastlanes-packed.
    fsst_rows = [
        f"the quick brown fox jumps over lazy dog #{i:04d} variant {i*7 % 100:02d}"
        for i in range(2000)
    ]
    write_one("fsst_strings_v21",
              pa.table({"s": pa.array(fsst_rows, type=pa.string())}),
              version="2.1")
    fsst_rows_nulls = [r if i % 9 != 4 else None for i, r in enumerate(fsst_rows)]
    write_one("fsst_strings_nulls_v21",
              pa.table({"s": pa.array(fsst_rows_nulls, type=pa.string())}),
              version="2.1")


if __name__ == "__main__":
    main()
