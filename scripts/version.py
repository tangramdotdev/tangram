"""Identify a clean checkout by its commit, or edited contents by a Git tree hash."""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path


def version(directory: Path) -> str:
    def git(*args: str, env: dict[str, str] | None = None) -> str:
        return subprocess.check_output(
            ["git", *args], cwd=directory, env=env, text=True
        ).strip()

    commit = git("rev-parse", "HEAD")
    committed_tree = git("rev-parse", f"{commit}^{{tree}}")
    with tempfile.TemporaryDirectory(prefix="tangram-version-") as temporary:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(temporary) / "index")}
        git("read-tree", commit, env=env)
        git("add", "--all", env=env)
        tree = git("write-tree", env=env)
    return commit if tree == committed_tree else tree


if __name__ == "__main__":
    print(version(Path(__file__).resolve().parent.parent))
