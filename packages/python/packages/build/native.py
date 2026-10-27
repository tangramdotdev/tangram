"""Namespace allocator symbols in native ELF objects before linking CPython."""

import hashlib
import os
import shutil
import struct
import subprocess
from pathlib import Path


def namespace(source: Path, output: Path, target: str, sysroot: Path) -> Path:
    data = source.read_bytes()
    if data[:4] != b"\x7fELF" or b"mi_" not in data:
        return source
    if data[4:6] != b"\x02\x01":
        raise RuntimeError("expected a little-endian ELF64 object")
    offset = struct.unpack_from("<Q", data, 40)[0]
    size, count = struct.unpack_from("<HH", data, 58)
    sections = [
        struct.unpack_from("<IIQQQQIIQQ", data, offset + index * size)
        for index in range(count)
    ]
    names = set()
    for section in sections:
        if section[1] != 2:
            continue
        strings = sections[section[6]]
        for position in range(section[4], section[4] + section[5], section[9]):
            name_offset = strings[4] + struct.unpack_from("<I", data, position)[0]
            name = data[name_offset : data.index(b"\0", name_offset)]
            if name.startswith((b"mi_", b"_mi_")):
                names.add(name.decode("ascii"))
    if not names:
        return source
    digest = hashlib.sha256(data + Path(__file__).read_bytes()).hexdigest()
    stamp = output.with_suffix(".sha256")
    if output.is_file() and stamp.is_file() and stamp.read_text() == digest:
        return output
    tool = os.environ.get("OBJCOPY_" + target.replace("-", "_")) or os.environ.get(
        "OBJCOPY"
    )
    if tool is None:
        candidates = [
            str(
                sysroot
                / "lib/rustlib"
                / os.environ.get("HOST", target)
                / "bin/llvm-objcopy"
            ),
            target + "-objcopy",
            "llvm-objcopy",
            "objcopy",
        ]
        tool = next((path for path in candidates if shutil.which(path)), None)
    if tool is None:
        raise RuntimeError("set OBJCOPY to a tool supporting the target ELF objects")
    output.parent.mkdir(parents=True, exist_ok=True)
    renames = output.with_suffix(".symbols")
    renames.write_text(
        "".join(f"{name} tangram_python_{name}\n" for name in sorted(names))
    )
    temporary = output.with_suffix(".tmp")
    subprocess.run(
        [tool, "--redefine-syms=" + str(renames), str(source), str(temporary)],
        check=True,
    )
    temporary.replace(output)
    stamp.write_text(digest)
    return output
