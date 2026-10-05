"""Install the workspace's locked wheels into an explicit build directory."""

import fcntl
import hashlib
import html
import os
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path


def requirements(lock: dict) -> tuple[str, str]:
    packages = {package["name"]: package for package in lock["package"]}
    if len(packages) != len(lock["package"]):
        raise ValueError("multiple locked versions require environment-aware selection")
    pending = ["tangram"]
    selected = set()
    while pending:
        name = pending.pop()
        if name in selected:
            continue
        selected.add(name)
        for dependency in packages[name].get("dependencies", []):
            if set(dependency) != {"name"}:
                raise ValueError(
                    "conditional dependencies require environment-aware selection"
                )
            pending.append(dependency["name"])
    selected.remove("tangram")
    lines, links = [], []
    for name in sorted(selected):
        package = packages[name]
        wheels = package.get("wheels", [])
        if not wheels:
            raise ValueError(f"no locked wheels for {name}")
        hashes = " ".join(f"--hash={wheel['hash']}" for wheel in wheels)
        lines.append(f"{name}=={package['version']} {hashes}")
        links.extend(
            f'<a href="{html.escape(wheel["url"], quote=True)}">wheel</a>'
            for wheel in wheels
        )
    return "\n".join(lines) + "\n", "\n".join(links)


def prepare(workspace: Path, output: Path, python: str = sys.executable) -> Path:
    locked = (workspace / "uv.lock").read_bytes()
    key = hashlib.sha256(
        locked
        + Path(__file__).read_bytes()
        + str(sys.implementation.cache_tag).encode()
        + python.encode()
    ).hexdigest()
    output.mkdir(parents=True, exist_ok=True)
    destination = output / f"packages-{key}"
    with (output / "packages.lock").open("w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        if destination.is_dir():
            return destination
        requirements_text, links = requirements(tomllib.loads(locked.decode()))
        with tempfile.TemporaryDirectory(dir=output) as temporary:
            temporary = Path(temporary)
            requirements_path = temporary / "requirements.txt"
            requirements_path.write_text(requirements_text)
            index = temporary / "wheels.html"
            index.write_text(links)
            target = temporary / "packages"
            subprocess.run(
                [
                    python,
                    "-I",
                    "-m",
                    "pip",
                    "--isolated",
                    "--disable-pip-version-check",
                    "install",
                    "--no-deps",
                    "--no-compile",
                    "--no-index",
                    "--find-links",
                    str(index),
                    "--only-binary=:all:",
                    "--require-hashes",
                    "--target",
                    str(target),
                    "--requirement",
                    str(requirements_path),
                ],
                env={**os.environ, "PIP_CONFIG_FILE": os.devnull},
                stdout=sys.stderr,
                check=True,
            )
            target.rename(destination)
    return destination


if __name__ == "__main__":
    print(
        prepare(
            Path(sys.argv[1]).resolve(),
            Path(sys.argv[2]).resolve(),
            sys.argv[3] if len(sys.argv) > 3 else sys.executable,
        )
    )
