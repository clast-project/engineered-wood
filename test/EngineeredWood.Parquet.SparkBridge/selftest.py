#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""Checks that the Spark bridge answers, and that it still catches what it exists to catch.

A reader bridge that always says OK is worse than no bridge: it turns an unchecked risk into a
green tick. So this asserts BOTH directions.

    1. `info` answers without starting a JVM.
    2. A plain Parquet file reads back, byte for byte equal to what was written.
    3. A BYTE_STREAM_SPLIT file is REPORTED AS A FAILURE -- in BOTH page versions, since
       EngineeredWood's own default was V2 when #269 shipped -- naming the encoding and the
       vectorized reader, which is the shape of the defect Parquity's matrix could not see.

(3) is the one that matters. #269 shipped because every engine in the matrix could read what we
wrote; the bridge is only worth having while it still disagrees with them here.

THE TRAP THIS SCRIPT WALKS AROUND. pyarrow will quietly DICTIONARY-encode a low-cardinality float
column and ignore `use_byte_stream_split` entirely, so a naive "write a BSS file" produces a file
with no BSS in it and the check passes for the wrong reason. The encodings are read back out of
the footer and asserted before the file is used.

    python selftest.py
"""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import tempfile
from pathlib import Path

BRIDGE = Path(__file__).resolve().parent / "spark_parquet_bridge.py"


def main() -> int:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        print(f"SKIP: pyarrow is needed to build the fixtures: {error}")
        return 0

    failures: list[str] = []

    # 1 -- info, which must not pay for a JVM.
    info = run("info")
    if info.returncode != 0:
        print(f"SKIP: the bridge cannot report info, so Spark is not set up here: {info.stdout}")
        return 0

    reported = json.loads(info.stdout)
    print(f"info: spark {reported.get('version')} directions={reported.get('directions')}")
    if reported.get("directions") != ["read"]:
        failures.append(f"directions should be exactly ['read']: {reported.get('directions')}")

    with tempfile.TemporaryDirectory(prefix="spark-bridge-selftest-") as workspace:
        root = Path(workspace)
        random.seed(269)

        # Enough distinct values that pyarrow will not choose a dictionary instead.
        values = [random.random() for _ in range(2000)]
        table = pa.table(
            {
                "f32": pa.array(values, pa.float32()),
                "f64": pa.array(values, pa.float64()),
                "id": pa.array(range(2000), pa.int32()),
            }
        )

        plain = root / "plain.parquet"
        pq.write_table(table, plain, use_dictionary=False)

        # BOTH PAGE VERSIONS. EngineeredWood's own default was V2 when #269 shipped, so a fixture
        # that only built V1 would be testing the neighbouring case; and the two were measured to
        # fail for the same reason rather than as a V1/V2 split, which is worth keeping asserted.
        splits = {}
        for version in ("1.0", "2.0"):
            path = root / f"byte-stream-split-v{version[0]}.parquet"
            pq.write_table(
                table,
                path,
                use_byte_stream_split=["f32", "f64"],
                use_dictionary=False,
                data_page_version=version,
            )
            splits[version] = path

            # The fixture has to BE what it claims, or (3) proves nothing.
            encodings = pq.ParquetFile(path).metadata.row_group(0).column(0).encodings
            print(f"fixture v{version[0]} encodings: {encodings}")
            if "BYTE_STREAM_SPLIT" not in encodings:
                failures.append(f"the v{version[0]} fixture carries no BYTE_STREAM_SPLIT: {encodings}")

        # 2 -- a plain file reads, and reads correctly.
        arrow = root / "plain.arrow"
        outcome = run("read", "--parquet", str(plain), "--arrow", str(arrow))
        if outcome.returncode != 0:
            failures.append(f"a plain file did not read: {outcome.stdout.strip()}")
        elif not arrow.is_file():
            failures.append("the bridge reported success without writing an Arrow file")
        else:
            read_back = pa.ipc.open_file(arrow).read_all()
            if not read_back.equals(table):
                failures.append("the Arrow output does not equal the table that was written")
            else:
                print(f"plain: OK, {read_back.num_rows} rows round-tripped through Spark")

        # 3 -- the one that matters, in both page versions.
        for version, path in splits.items():
            outcome = run("read", "--parquet", str(path), "--arrow", str(root / f"split{version[0]}.arrow"))
            if outcome.returncode != 1:
                failures.append(
                    f"a v{version[0]} BYTE_STREAM_SPLIT file was NOT reported as a failure "
                    f"(exit {outcome.returncode}) -- the bridge has stopped catching #269's class"
                )
                continue

            evidence = json.loads(outcome.stdout)
            detail = str(evidence.get("detail", ""))
            print(f"byte-stream-split v{version[0]}: reported {evidence.get('kind')}: {detail}")
            if "BYTE_STREAM_SPLIT" not in detail:
                failures.append(f"the v{version[0]} evidence does not name the encoding: {detail}")
            if "Vectorized" not in detail:
                failures.append(
                    "the evidence does not name the vectorized reader, which is what separates "
                    f"'Spark cannot read this' from 'Spark's DEFAULT reader cannot': {detail}"
                )

    run("stop")

    for failure in failures:
        print(f"FAIL: {failure}")

    print("FAILED" if failures else "OK: the bridge answers, and still catches BYTE_STREAM_SPLIT")
    return 1 if failures else 0


def run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - our own interpreter and script.
        [sys.executable, str(BRIDGE), *arguments],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ},
    )


if __name__ == "__main__":
    sys.exit(main())
