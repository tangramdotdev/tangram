"""Embed the client and pure Python dependencies from the locked workspace."""

import importlib.util
import json
import sys
from pathlib import Path

if f"{sys.version_info.major}.{sys.version_info.minor}" != sys.argv[3]:
    raise SystemExit("the workspace Python must match the interpreter selected by PyO3")

modules = {}
roots = [Path(sys.argv[1]) / "tangram"]
for name in ["h2", "hpack", "hyperframe", "tomli_w", "yaml"]:
    spec = importlib.util.find_spec(name)
    if spec is None or spec.origin is None:
        raise SystemExit(f"missing {name}: run uv sync --locked --all-packages")
    roots.append(Path(spec.origin).parent)
for root in roots:
    for path in sorted(root.rglob("*.py")):
        parts = path.relative_to(root.parent).with_suffix("").parts
        package = parts[-1] == "__init__"
        name = ".".join(parts[:-1] if package else parts)
        modules[name] = [path.read_text(), package]
        for index in range(1, len(name.split("."))):
            parent = ".".join(name.split(".")[:index])
            modules.setdefault(parent, ["", True])
Path(sys.argv[2]).write_text(json.dumps(modules))
