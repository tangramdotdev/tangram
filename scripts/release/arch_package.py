"""Prepare the AUR source package from staged GNU tarballs."""

import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LINUX = ROOT / "packages" / "linux"
RELEASE = ROOT / "release"
TARBALL_BASE = "https://github.com/tangramdotdev/tangram/releases/download"
ARCHITECTURES = ("aarch64", "x86_64")


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(version, targets):
    if not targets or any(
        target not in {f"{arch}-linux-gnu" for arch in ARCHITECTURES}
        for target in targets
    ):
        raise ValueError("expected GNU Linux targets for the AUR package")
    if not re.fullmatch(r"[0-9][0-9A-Za-z.+~-]*", version):
        raise ValueError("expected a release version for the AUR package")
    archives = {
        target.removesuffix("-linux-gnu"): checksum(
            RELEASE / f"tangram_{target}.tar.gz"
        )
        for target in targets
    }
    service = (LINUX / "systemd" / "tangram.service").read_bytes()
    service = service.replace(
        b"ExecStart=/usr/bin/tangram ", b"ExecStart=/usr/lib/tangram/tangram "
    )
    files = {
        "config.json": (LINUX / "config.json").read_bytes(),
        "tangram.service": service,
        "tangram.sysusers": (LINUX / "arch" / "tangram.sysusers").read_bytes(),
        "LICENSE": (ROOT / "LICENSE").read_bytes(),
    }
    with tempfile.TemporaryDirectory(prefix=".aur-", dir=RELEASE) as directory:
        temporary = Path(directory)
        for name, data in files.items():
            (temporary / name).write_bytes(data)
        write_recipe(temporary, version, archives)
        output = RELEASE / "aur"
        output.mkdir(exist_ok=True)
        for path in temporary.iterdir():
            path.replace(output / path.name)
    print(f"prepared {output.relative_to(ROOT)}")


def write_recipe(directory, version, archives):
    values = {
        "PKGVER": version.replace("-", ".").replace("~", "_"),
        "ARCHITECTURES": " ".join(f"'{arch}'" for arch in sorted(archives)),
        "CONFIG_SHA256": checksum(directory / "config.json"),
        "SERVICE_SHA256": checksum(directory / "tangram.service"),
        "SYSUSERS_SHA256": checksum(directory / "tangram.sysusers"),
        "LICENSE_SHA256": checksum(directory / "LICENSE"),
    }
    sources = []
    for arch, digest in sorted(archives.items()):
        name = f"tangram_{arch}-linux-gnu.tar.gz"
        url = f"{TARBALL_BASE}/v{version}/{name}"
        sources.append(f"source_{arch}=('{url}')")
        sources.append(f"sha256sums_{arch}=('{digest}')")
    values["ARCHIVE_SOURCES"] = "\n".join(sources)
    recipe = (LINUX / "arch" / "PKGBUILD.in").read_text()
    for key, value in values.items():
        recipe = recipe.replace(f"@{key}@", value)
    recipe_path = directory / "PKGBUILD"
    recipe_path.write_text(recipe)
    if os.getuid() != 0 and shutil.which("makepkg"):
        command = ["makepkg", "--printsrcinfo"]
    else:
        directory.chmod(0o755)
        recipe_path.chmod(0o644)
        user = f"{os.getuid() or 65534}:{os.getgid() or 65534}"
        command = [
            "docker",
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "--network=none",
            "--user",
            user,
            "--env",
            "BUILDDIR=/tmp",
            "--env",
            "PKGDEST=/tmp",
            "--env",
            "SRCDEST=/tmp",
            "--volume",
            f"{directory}:/package:ro",
            "--workdir",
            "/package",
            "archlinux:base-devel",
            "makepkg",
            "--printsrcinfo",
        ]
    metadata = subprocess.check_output(command, cwd=directory)
    (directory / ".SRCINFO").write_bytes(metadata)


if __name__ == "__main__":
    try:
        main(sys.argv[1], sys.argv[2:])
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
