"""In each run of consecutive `GC.KeepAlive(x);` lines, keeps only the last occurrence of each name."""
import re
import sys
from pathlib import Path

KEEP = re.compile(r"^\s*GC\.KeepAlive\((\w+)\);\s*$")
for root in sys.argv[1:]:
    for path in sorted(Path(root).rglob("*.cs")):
        if "obj" in path.parts or "bin" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        nl = "\r\n" if "\r\n" in text else "\n"
        lines = text.split(nl)
        out, removed, i = [], 0, 0
        while i < len(lines):
            if not KEEP.match(lines[i]):
                out.append(lines[i])
                i += 1
                continue
            j = i
            while j < len(lines) and KEEP.match(lines[j]):
                j += 1
            run = lines[i:j]
            names = [KEEP.match(l).group(1) for l in run]
            kept = [l for k, l in enumerate(run) if names[k] not in names[k + 1:]]
            removed += len(run) - len(kept)
            out.extend(kept)
            i = j
        if removed:
            path.write_bytes(nl.join(out).encode("utf-8"))
            print(f"{path}: {removed} duplicates removed")
