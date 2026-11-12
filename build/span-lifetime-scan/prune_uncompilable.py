"""Removes inserted GC.KeepAlive(x); lines that the build reports as CS0103 (x not in scope).

Reads build output on stdin; prints each removed line as file(line): name, for manual handling.
"""
import re
import sys

# Any path up to the (line,column) suffix: C:\... on Windows, /home/... elsewhere (review round 2).
pattern = re.compile(r"^(?P<file>.+?)\((?P<line>\d+),\d+\): error CS0103: The name '(?P<name>\w+)' does not exist")
hits = {}
for raw in sys.stdin:
    m = pattern.match(raw.strip())
    if m:
        hits.setdefault(m.group("file"), set()).add((int(m.group("line")), m.group("name")))

for path, entries in sorted(hits.items()):
    with open(path, encoding="utf-8", newline="") as f:
        lines = f.readlines()
    keep = []
    for number, text in enumerate(lines, start=1):
        names = {name for line, name in entries if line == number}
        if names and re.fullmatch(r"\s*GC\.KeepAlive\((\w+)\);\s*", text) and re.fullmatch(r"\s*GC\.KeepAlive\((\w+)\);\s*", text).group(1) in names:
            print(f"{path}({number}): {text.strip()}")
            continue
        keep.append(text)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.writelines(keep)
