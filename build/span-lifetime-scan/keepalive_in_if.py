"""Lists GC.KeepAlive lines that sit inside an #if/#elif/#else branch, so only some targets compile them."""
import re
import sys
from pathlib import Path

for root in sys.argv[1:]:
    for path in sorted(Path(root).rglob("*.cs")):
        if "obj" in path.parts or "bin" in path.parts:
            continue
        depth = 0
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            s = line.strip()
            if s.startswith("#if"):
                depth += 1
            elif s.startswith("#endif"):
                depth -= 1
            elif depth > 0 and re.match(r"GC\.KeepAlive\(", s):
                print(f"{path}({number}): {s}")
