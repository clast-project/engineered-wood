# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

"""Generates the committed iceberg-go corpus: TestData/corpus/iceberg-go.

A second, smaller corpus beside the Spark one, from a second writer, so a Java quirk is not mistaken for
the spec. It runs the driver's scenarios (main.go: v3_dv, v3_part, v2_upgrade, v3_cow, v3_eqdel) into a
neutral warehouse, C:/tmp/ew-iceberg-corpus/iceberg-go/warehouse, and records what iceberg-go itself reads
back (scan, with the lineage columns on v3 tables, and describe) in expected/<scenario>.json.

Unlike the Spark corpus this one carries timestamptz_ns columns: iceberg-go writes and reads them.

    python generate_corpus.py --driver <iceberg-go-driver.exe> --out <TestData/corpus/iceberg-go>

Build the driver first, from this directory: go build -o <dir>/iceberg-go-driver.exe .
The output directory is replaced. The sqlite catalog is not copied: tests open tables by metadata file.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile

WAREHOUSE = os.path.abspath("/tmp/ew-iceberg-corpus/iceberg-go/warehouse")
SCENARIOS = ["v3_dv", "v3_part", "v2_upgrade", "v3_cow", "v3_eqdel"]


def call(driver, command, args):
    with tempfile.TemporaryDirectory() as tmp:
        args_file = os.path.join(tmp, "args.json")
        result_file = os.path.join(tmp, "result.json")
        with open(args_file, "w", encoding="utf-8") as fh:
            json.dump(args, fh)
        subprocess.run([driver, command, args_file, result_file], check=True)
        with open(result_file, encoding="utf-8") as fh:
            result = json.load(fh)
    if not result.get("ok"):
        raise RuntimeError(f"{command} {args}: {result.get('error')}")
    del result["ok"]
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--driver", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    if os.path.exists(WAREHOUSE):
        shutil.rmtree(WAREHOUSE)
    os.makedirs(WAREHOUSE)

    version = json.loads(subprocess.run([args.driver, "version"], check=True, capture_output=True, text=True).stdout)
    expected = {}
    for name in SCENARIOS:
        print(f"== {name}", flush=True)
        made = call(args.driver, "scenario", {"warehouse": WAREHOUSE, "scenario": name})
        location = made["metadata_location"]
        described = call(args.driver, "describe", {"metadata_location": location})
        scanned = call(args.driver, "scan", {
            "metadata_location": location,
            "row_lineage": described["format_version"] >= 3,
        })
        expected[name] = {"scenario": made, "describe": described, "scan": scanned}

    out = os.path.abspath(args.out)
    if os.path.exists(out):
        shutil.rmtree(out)
    shutil.copytree(WAREHOUSE, os.path.join(out, "wh"),
                    ignore=shutil.ignore_patterns("catalog.db*", "*.crc"))
    os.makedirs(os.path.join(out, "expected"))
    for name, data in expected.items():
        with open(os.path.join(out, "expected", f"{name}.json"), "w", encoding="utf-8", newline="\n") as fh:
            json.dump(data, fh, indent=1, ensure_ascii=False, sort_keys=True)
            fh.write("\n")
    with open(os.path.join(out, "corpus.json"), "w", encoding="utf-8", newline="\n") as fh:
        json.dump({
            "generator": "test/EngineeredWood.Iceberg.Tests/Interop/iceberg-go/generate_corpus.py",
            "iceberg_go_version": version.get("v"),
            "go_version": version.get("go"),
            "warehouse_uri": "file:///" + WAREHOUSE.replace("\\", "/").lstrip("/"),
            "tables": SCENARIOS,
        }, fh, indent=1)
        fh.write("\n")
    print(f"wrote {len(expected)} tables to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
