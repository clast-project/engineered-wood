#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""Narrow a CI build target to its test projects.

Takes the target the "Select affected projects" step chose (the solution, or a
solution filter) and writes a solution filter holding only its TEST projects:
those whose project file references Microsoft.NET.Test.Sdk.

Building that filter builds each library only for the frameworks its tests
reference, instead of every framework it ships, and skips the benchmark and
tool projects, which no test job runs. CI's secondary test legs use it; the
primary leg still builds the full target, so everything is still compiled
somewhere and the packages are built from it.

Usage:
  tests_only_slnf.py TARGET OUTPUT.slnf
"""

from __future__ import annotations

import json
import re
import sys

SOLUTION = "engineered-wood.slnx"


def projects_of(target: str) -> list[str]:
    if target.endswith(".slnf"):
        with open(target, encoding="utf-8") as f:
            return [p.replace("\\", "/") for p in json.load(f)["solution"]["projects"]]
    with open(target, encoding="utf-8") as f:
        return re.findall(r'<Project\s+Path="([^"]+)"', f.read())


def is_test_project(path: str) -> bool:
    with open(path, encoding="utf-8") as f:
        return "Microsoft.NET.Test.Sdk" in f.read()


def main() -> int:
    target, output = sys.argv[1], sys.argv[2]
    tests = [p for p in projects_of(target) if is_test_project(p)]
    with open(output, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"solution": {"path": SOLUTION, "projects": [p.replace("/", "\\") for p in tests]}}, f, indent=2)
        f.write("\n")
    print(f"{len(tests)} test projects from {target}:")
    for p in tests:
        print(f"  {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
