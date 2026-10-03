"""Freeze the initialization modules with the pinned host CPython interpreter."""

import json
import marshal
import sys
from pathlib import Path

stdlib, output, version = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
if sys.version.split()[0] != version:
    raise SystemExit("the freezing interpreter must match the pinned CPython version")
modules = {
    "_frozen_importlib": stdlib / "importlib/_bootstrap.py",
    "_frozen_importlib_external": stdlib / "importlib/_bootstrap_external.py",
    "abc": stdlib / "abc.py",
    "codecs": stdlib / "codecs.py",
    "encodings": stdlib / "encodings/__init__.py",
    "encodings.aliases": stdlib / "encodings/aliases.py",
    "encodings.ascii": stdlib / "encodings/ascii.py",
    "encodings.latin_1": stdlib / "encodings/latin_1.py",
    "encodings.utf_8": stdlib / "encodings/utf_8.py",
    "io": stdlib / "io.py",
    "zipimport": stdlib / "zipimport.py",
}
entries = []
for index, (name, path) in enumerate(sorted(modules.items())):
    filename = "/tangram/python/" + path.relative_to(stdlib).as_posix()
    code = marshal.dumps(
        compile(path.read_bytes(), filename, "exec", dont_inherit=True)
    )
    (output / f"frozen_{index}.h").write_text(
        f"static const unsigned char frozen_{index}[] = {{"
        + ",".join(map(str, code))
        + "};\n"
    )
    entries.append(
        [name, index, path.name == "__init__.py", path.relative_to(stdlib).as_posix()]
    )
(output / "frozen.json").write_text(json.dumps(entries))
