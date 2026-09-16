#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""Choose which projects CI builds and tests for a change.

A change to one library only needs the tests of that library and of the
projects that reference it, directly or transitively. This script maps each
changed file to the project that owns it, closes that set over ProjectReference
in the reverse direction, and writes a solution filter (.slnf) naming the
result. `dotnet restore/build/test/pack` then take the filter in place of
engineered-wood.slnx: the build still compiles every dependency, but only the
listed test projects run.

FAILS SAFE. Any changed file this cannot attribute to a project selects the full
solution: repository-wide build inputs (Directory.Build.props, the solution,
the signing key, .editorconfig, CI itself), a deleted project, a path nothing
claims. So does an empty diff. Running too much is slow; running too little lets a break merge.

What owns a file:
  * the project whose directory contains it;
  * any project whose .csproj pulls it in from outside its own directory
    (`<Compile Include="..\\Shared\\...">`, `<Import Project="..\\..\\build\\...">`,
    `AdditionalImportDirs="..\\..."`), found by reading the project files;
  * RUNTIME_INPUTS below, for files a test opens at run time by walking up from
    its output directory. No project file mentions those, so they are listed by
    hand. Keep it complete: a runtime input missing here is attributed to
    nobody, which runs everything, so an omission costs time, never coverage.

Usage:
  select_projects.py --base SHA [--head SHA] [--full] [--slnf PATH] [--github-output PATH]
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import re
import subprocess
import sys

SOLUTION = "engineered-wood.slnx"

# Files a test reads at run time from outside its project directory.
# Path prefix -> projects that read it.
RUNTIME_INPUTS = {
    # TestData.cs and VariantArrayRoundTripTests.cs walk up to the submodule.
    "parquet-testing": ["test/EngineeredWood.Parquet.Tests/EngineeredWood.Parquet.Tests.csproj"],
}

# Inputs to every project's build. A change here selects the full solution.
GLOBAL_INPUTS = (
    SOLUTION,
    "Clast.snk",
    ".editorconfig",
    ".gitattributes",
    ".gitmodules",
    "global.json",
    "NuGet.config",
    "Directory.Packages.props",
    ".github/",
    "assets/",  # packed into every library
)

# Files that feed no build, test or package. On their own they select nothing.
# (Documentation is already filtered out before this script runs.)
INERT = (
    ".vscode/",
    ".gitignore",
    "LICENSE",
)

_ATTR = re.compile(r'\b(Include|Project|AdditionalImportDirs)\s*=\s*"([^"]*)"')
_PROJECT_REF = re.compile(r'<ProjectReference\s+Include\s*=\s*"([^"]+)"')


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True, encoding="utf-8"
    ).stdout


def norm(path: str) -> str:
    return posixpath.normpath(path.replace("\\", "/"))


def solution_projects() -> list[str]:
    with open(SOLUTION, encoding="utf-8") as f:
        return [norm(p) for p in re.findall(r'<Project\s+Path="([^"]+)"', f.read())]


def read_project(csproj: str) -> tuple[list[str], list[str]]:
    """Returns (referenced projects, paths outside the project directory it pulls in)."""
    project_dir = posixpath.dirname(csproj)
    with open(csproj, encoding="utf-8") as f:
        text = f.read()

    refs = [norm(posixpath.join(project_dir, r)) for r in _PROJECT_REF.findall(text)]

    external = []
    for element in re.findall(r"<[A-Za-z][^<>]*>", text):
        if element.startswith(("<ProjectReference", "<PackageReference")):
            continue
        for _, value in _ATTR.findall(element):
            for item in value.split(";"):
                # In a .csproj both properties name the project's own directory,
                # which is what the path is joined onto below anyway.
                item = (item.replace("$(MSBuildThisFileDirectory)", "")
                            .replace("$(MSBuildProjectDirectory)", ""))
                if "$(" in item or "@(" in item or not item.strip():
                    continue
                item = re.split(r"[*?]", item)[0]  # a glob's fixed prefix
                resolved = norm(posixpath.join(project_dir, item.strip()))
                if not is_under(resolved, project_dir):
                    external.append(resolved)
    return refs, external


def is_under(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix.rstrip("/") + "/")


def changed_files(base: str, head: str) -> list[str]:
    # --no-renames lists a moved file under both names, so a file moved out of
    # a project still counts against it.
    out = git("diff", "--name-only", "--no-renames", base, head)
    return [line for line in out.splitlines() if line]


def is_doc(path: str) -> bool:
    return path.startswith("doc/") or ("/" not in path and path.endswith(".md"))


def select(changed: list[str]) -> tuple[str, list[str], list[str]]:
    """Returns (scope, selected projects, explanation lines)."""
    projects = solution_projects()
    if not changed:
        # Nothing to attribute says nothing about what is safe to skip; the
        # documentation check in ci.yml treats an empty diff as code, too.
        return "full", projects, ["the diff is empty"]
    for p in projects:
        if not os.path.isfile(p):
            return "full", projects, [f"{p}: listed in {SOLUTION} but missing"]

    references = {}
    consumers: dict[str, set[str]] = {}
    for p in projects:
        refs, external = read_project(p)
        references[p] = refs
        for path in external:
            consumers.setdefault(path, set()).add(p)
    for prefix, readers in RUNTIME_INPUTS.items():
        consumers.setdefault(prefix, set()).update(readers)

    # Deepest directory first, so a nested project wins over its parent.
    by_dir = sorted(((posixpath.dirname(p), p) for p in projects),
                    key=lambda d: -len(d[0]))

    why: list[str] = []
    seeds: set[str] = set()
    files_by_owner: dict[str, list[str]] = {}
    for path in changed:
        if is_doc(path) or any(is_under(path, i.rstrip("/")) for i in INERT):
            continue
        if any(is_under(path, g.rstrip("/")) for g in GLOBAL_INPUTS) \
                or posixpath.basename(path) in ("Directory.Build.props", "Directory.Build.targets"):
            return "full", projects, [f"{path}: repository-wide build input"]

        owners = set()
        for d, p in by_dir:
            if is_under(path, d):
                owners.add(p)
                break
        for prefix, readers in consumers.items():
            if is_under(path, prefix):
                owners |= readers
        if not owners:
            return "full", projects, [f"{path}: not attributable to any project"]
        for p in owners:
            files_by_owner.setdefault(p, []).append(path)
        seeds |= owners

    for p in sorted(files_by_owner):
        files = files_by_owner[p]
        more = f" and {len(files) - 1} more" if len(files) > 1 else ""
        why.append(f"changed: {p} ({files[0]}{more})")

    if not seeds:
        return "none", [], why

    dependents: dict[str, set[str]] = {p: set() for p in projects}
    for p, refs in references.items():
        for r in refs:
            if r not in dependents:
                return "full", projects, [f"{p} references {r}, which is not in {SOLUTION}"]
            dependents[r].add(p)

    selected = set(seeds)
    stack = list(seeds)
    while stack:
        for d in dependents[stack.pop()]:
            if d not in selected:
                selected.add(d)
                stack.append(d)
                why.append(f"dependent: {d}")

    if selected == set(projects):
        return "full", projects, why
    return "partial", sorted(selected, key=projects.index), why


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", help="commit to diff against; omit or unresolvable -> full")
    ap.add_argument("--head", default="HEAD")
    ap.add_argument("--full", action="store_true", help="select the whole solution")
    ap.add_argument("--slnf", default="ci-affected.slnf")
    ap.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT"))
    args = ap.parse_args()

    base_ok = bool(args.base) and subprocess.run(
        ["git", "cat-file", "-e", f"{args.base}^{{commit}}"], capture_output=True).returncode == 0
    if args.full:
        scope, selected, why = "full", solution_projects(), ["full run requested"]
    elif not base_ok:
        scope, selected, why = "full", solution_projects(), ["base commit unavailable"]
    else:
        scope, selected, why = select(changed_files(args.base, args.head))

    if scope == "partial":
        with open(args.slnf, "w", encoding="utf-8", newline="\n") as f:
            json.dump({"solution": {"path": SOLUTION, "projects": [p.replace("/", "\\") for p in selected]}},
                      f, indent=2)
            f.write("\n")
        target = args.slnf
    else:
        target = SOLUTION

    print(f"scope: {scope}")
    print(f"target: {target}")
    for line in why:
        print(line)
    if scope != "none":
        print("selected projects:")
        for p in selected:
            print(f"  {p}")

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(f"### Project selection: {scope}\n\n")
            if scope == "partial":
                f.write("Only these projects (and what they reference) are built; "
                        "only the test projects among them run.\n\n")
                f.writelines(f"- `{p}`\n" for p in selected)
            elif scope == "none":
                f.write("No change affects a build, test or package.\n")
            else:
                f.write("The whole solution is built and tested.\n")
            if why:
                f.write("\n<details><summary>Why</summary>\n\n")
                f.writelines(f"- {line}\n" for line in why)
                f.write("\n</details>\n")

    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as f:
            f.write(f"scope={scope}\n")
            f.write(f"target={target}\n")
            f.write(f"tests={' '.join(p for p in selected if p.endswith('.Tests.csproj'))}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
