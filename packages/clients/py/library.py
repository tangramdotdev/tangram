"""Copy the client and its Python dependencies into an embedded library."""

import importlib.util
import shutil
import sys
from pathlib import Path


def copy(destination: Path) -> None:
    client = Path(__file__).parent / "src" / "tangram"
    roots = [client]
    for name in ["h2", "hpack", "hyperframe", "tomli_w", "yaml"]:
        spec = importlib.util.find_spec(name)
        if spec is None or spec.origin is None:
            raise SystemExit(f"missing locked python package: {name}")
        roots.append(Path(spec.origin).parent)
    for root in roots:
        for path in sorted(root.rglob("*")):
            if not path.is_file() or "__pycache__" in path.parts:
                continue
            if path.suffix in {".so", ".pyc"}:
                continue
            target = destination / path.relative_to(root.parent)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)


if __name__ == "__main__":
    sys.path.insert(0, sys.argv[2])
    copy(Path(sys.argv[1]))
