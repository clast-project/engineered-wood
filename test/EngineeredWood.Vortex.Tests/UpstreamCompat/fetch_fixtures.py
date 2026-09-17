#!/usr/bin/env python3
"""Fetch upstream vortex's published backward-compatibility fixtures.

Upstream (vortex-test/compat-gen) publishes one set of small .vortex files per
release to a public S3 bucket. VortexUpstreamCompatTests reads every one of them
with EW and compares the values against vortex's own reader (vortex-oracle).

fixtures.lock.json pins exactly which releases and files are tested, with a
SHA-256 for each. The tests enumerate their cases from it, so it is committed;
the files themselves (~120 MB across all releases) are not, and land in
fixtures/ next to this script (git-ignored) unless --dest says otherwise.

    python fetch_fixtures.py              # download what the lock names; verify hashes
    python fetch_fixtures.py --update     # re-pin the lock to everything the store has now

--update is how a new upstream release is taken on: it is a deliberate change to a
committed file, never something a test run does on its own. Standard library only.
"""

import argparse
import hashlib
import json
import os
import sys
import tempfile
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
LOCK = os.path.join(HERE, "fixtures.lock.json")
DEFAULT_STORE = "https://vortex-compat-fixtures.s3.amazonaws.com"


def get(url):
    with urllib.request.urlopen(url, timeout=60) as r:
        return r.read()


def update(store):
    versions = json.loads(get(f"{store}/versions.json"))
    pinned = {}
    for v in versions:
        manifest = json.loads(get(f"{store}/v{v}/arrays/manifest.json"))
        files = {}
        for f in manifest["fixtures"]:
            if "sha256" not in f:
                sys.exit(f"v{v}/{f['name']} has no sha256 in its manifest; refusing to pin it unverified")
            files[f["name"]] = f["sha256"]
        pinned[v] = dict(sorted(files.items()))
        print(f"{v}: {len(files)} fixtures")
    with open(LOCK, "w", encoding="utf-8", newline="\n") as out:
        json.dump({"store": store, "versions": pinned}, out, indent=2)
        out.write("\n")
    print(f"pinned {len(pinned)} versions in {LOCK}")


def fetch(dest):
    with open(LOCK, encoding="utf-8") as f:
        lock = json.load(f)
    store = lock["store"]
    fetched = present = 0
    for v, files in lock["versions"].items():
        vdir = os.path.join(dest, v)
        os.makedirs(vdir, exist_ok=True)
        for name, sha in files.items():
            path = os.path.join(vdir, name)
            if os.path.exists(path):
                with open(path, "rb") as f:
                    if hashlib.sha256(f.read()).hexdigest() == sha:
                        present += 1
                        continue
            data = get(f"{store}/v{v}/arrays/{name}")
            actual = hashlib.sha256(data).hexdigest()
            if actual != sha:
                sys.exit(f"v{v}/{name}: sha256 {actual} does not match the pinned {sha}")
            # Write-then-rename, so an interrupted run never leaves a truncated fixture behind.
            fd, tmp = tempfile.mkstemp(dir=vdir)
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, path)
            fetched += 1
    print(f"{fetched} fetched, {present} already present, in {dest}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dest", default=os.path.join(HERE, "fixtures"))
    p.add_argument("--update", action="store_true", help="re-pin fixtures.lock.json from the store")
    p.add_argument("--store", default=DEFAULT_STORE)
    args = p.parse_args()
    if args.update:
        update(args.store)
    else:
        fetch(args.dest)


if __name__ == "__main__":
    main()
