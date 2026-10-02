"""Build the standalone client with the installed Python interpreter."""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="release")
    args = parser.parse_args()
    if sys.version_info < (3, 12):  # noqa: UP036
        parser.error("the Python client requires Python 3.12 or later")
    directory = Path(__file__).resolve().parent
    root = directory.parents[2]
    environment = root / ".venv"
    python = environment / "bin/python"
    uv = shutil.which("uv")
    if uv is None:
        parser.error("the Python workspace requires uv")
    subprocess.run(
        [
            uv,
            "sync",
            "--locked",
            "--all-packages",
            "--no-install-package",
            "tangram",
            "--python",
            sys.executable,
            "--no-managed-python",
        ],
        cwd=root,
        check=True,
    )
    subprocess.run(
        [str(python), "-m", "maturin", "develop", "--profile", args.profile],
        cwd=directory,
        env={**os.environ, "VIRTUAL_ENV": str(environment)},
        check=True,
    )


if __name__ == "__main__":
    main()
