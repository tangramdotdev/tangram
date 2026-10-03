"""Native client builds retain stable interpreter configuration files."""

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "tangram_test_backend", Path(__file__).parents[1] / "backend.py"
)
assert spec is not None and spec.loader is not None
backend = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backend)


class BackendTests(unittest.TestCase):
    def test_configuration_reuse_and_restoration(self):
        with tempfile.TemporaryDirectory() as target:
            with patch.dict(
                os.environ, {"CARGO_TARGET_DIR": target, "PYO3_CONFIG_FILE": "previous"}
            ):
                with backend.environment():
                    first = Path(os.environ["PYO3_CONFIG_FILE"])
                    contents = first.read_bytes()
                    timestamp = first.stat().st_mtime_ns
                    self.assertEqual(first.parent, Path(target).resolve() / "pyo3")
                self.assertEqual(os.environ["PYO3_CONFIG_FILE"], "previous")
                with self.assertRaisesRegex(RuntimeError, "build failed"):
                    with backend.environment():
                        second = Path(os.environ["PYO3_CONFIG_FILE"])
                        self.assertEqual(second, first)
                        self.assertEqual(second.read_bytes(), contents)
                        self.assertEqual(second.stat().st_mtime_ns, timestamp)
                        raise RuntimeError("build failed")
                self.assertEqual(os.environ["PYO3_CONFIG_FILE"], "previous")
                self.assertTrue(first.is_file())

    def test_configuration_changes_select_a_new_file(self):
        with tempfile.TemporaryDirectory() as target:
            with patch.dict(os.environ, {"CARGO_TARGET_DIR": target}):
                os.environ.pop("PYO3_CONFIG_FILE", None)
                with backend.environment():
                    first = Path(os.environ["PYO3_CONFIG_FILE"])
                get_config_var = backend.sysconfig.get_config_var
                with patch.object(
                    backend.sysconfig,
                    "get_config_var",
                    side_effect=lambda name: (
                        not get_config_var(name)
                        if name == "Py_DEBUG"
                        else get_config_var(name)
                    ),
                ):
                    with backend.environment():
                        second = Path(os.environ["PYO3_CONFIG_FILE"])
                        self.assertNotEqual(second, first)
                        self.assertNotEqual(second.read_bytes(), first.read_bytes())
                self.assertNotIn("PYO3_CONFIG_FILE", os.environ)
                self.assertTrue(first.is_file())
                self.assertTrue(second.is_file())
