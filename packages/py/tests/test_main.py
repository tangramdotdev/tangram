"""Runtime initialization restores Python's import hooks after failures."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

spec = importlib.util.spec_from_file_location(
    "tangram_test_main", Path(__file__).parents[1] / "src/main.py"
)
assert spec is not None and spec.loader is not None
main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(main)


class StartupTests(unittest.TestCase):
    def setUp(self):
        self.hooks = list(sys.meta_path)
        self.module = {"kind": "py", "referent": {"node": "/test/main.tg.py"}}
        self.context = json.dumps(
            {
                "args": [],
                "cwd": "/test",
                "env": {},
                "export": None,
                "module": self.module,
                "token": None,
                "url": "http://localhost",
            }
        )
        self.tg = ModuleType("tangram")
        self.tg.__dict__.update(
            process=SimpleNamespace(set_process=Mock()),
            Module=SimpleNamespace(from_data=lambda value: value),
            Value=SimpleNamespace(from_data=lambda value: value),
            client=SimpleNamespace(close=AsyncMock()),
        )
        self.host = Mock()
        self.host.inventory.return_value = "null"
        self.host.module.return_value = (json.dumps(self.module), "value = 1")

    def tearDown(self):
        sys.meta_path[:] = self.hooks

    def test_client_import_failure_removes_the_embedded_importer(self):
        with patch.object(
            main.importlib, "import_module", side_effect=ImportError("client")
        ):
            with self.assertRaisesRegex(ImportError, "client"):
                main.run(self.context, "{}", self.host)
        self.assertEqual(sys.meta_path, self.hooks)

    def test_process_setup_failure_removes_the_embedded_importer(self):
        self.tg.process.set_process.side_effect = ValueError("context")
        with patch.dict(sys.modules, {"tangram": self.tg}):
            with self.assertRaisesRegex(ValueError, "context"):
                main.run(self.context, "{}", self.host)
        self.assertEqual(sys.meta_path, self.hooks)

    def test_inventory_failure_does_not_affect_the_next_invocation(self):
        self.host.inventory.side_effect = RuntimeError("inventory")
        with patch.dict(sys.modules, {"tangram": self.tg}):
            with self.assertRaisesRegex(RuntimeError, "inventory"):
                main.run(self.context, "{}", self.host)
            self.assertEqual(sys.meta_path, self.hooks)
            self.host.inventory.side_effect = None
            self.assertEqual(main.run(self.context, "{}", self.host), (0, None, None))
        self.assertEqual(sys.meta_path, self.hooks)
        self.assertFalse(any(name.startswith("_tangram_entry") for name in sys.modules))
        self.tg.client.close.assert_awaited_once()
