"""Tests for selecting the embedded client's locked dependencies."""

import runpy
import unittest
from pathlib import Path

requirements = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "build/packages.py")
)["requirements"]


def package(name, dependencies=()):
    return {
        "name": name,
        "version": "1.0",
        "dependencies": [{"name": name} for name in dependencies],
        "wheels": [{"url": f"https://example.com/{name}.whl", "hash": "sha256:abc"}],
    }


class BuildTests(unittest.TestCase):
    def test_selects_transitive_runtime_dependencies_without_tools(self):
        lock = {
            "package": [
                package("tangram", ["h2", "yaml"]),
                package("h2", ["hpack"]),
                package("hpack"),
                package("yaml"),
                package("ruff"),
            ]
        }
        text, links = requirements(lock)
        self.assertEqual(
            text.splitlines(),
            [
                "h2==1.0 --hash=sha256:abc",
                "hpack==1.0 --hash=sha256:abc",
                "yaml==1.0 --hash=sha256:abc",
            ],
        )
        self.assertNotIn("ruff", links)
        self.assertNotIn("tangram", links)

    def test_rejects_unhandled_conditional_dependencies(self):
        root = package("tangram", ["h2"])
        h2 = package("h2")
        h2["dependencies"] = [{"name": "hpack", "marker": "sys_platform == 'linux'"}]
        with self.assertRaisesRegex(ValueError, "conditional dependencies"):
            requirements({"package": [root, h2, package("hpack")]})

    def test_rejects_missing_locked_wheels(self):
        dependency = package("h2")
        dependency["wheels"] = []
        with self.assertRaisesRegex(ValueError, "no locked wheels"):
            requirements({"package": [package("tangram", ["h2"]), dependency]})
