"""Runtime initialization restores Python's import hooks after failures."""

import importlib.util
import json
import linecache
import subprocess
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

    def test_client_import_failure_preserves_the_import_hooks(self):
        with patch.object(
            main.importlib, "import_module", side_effect=ImportError("client")
        ):
            with self.assertRaisesRegex(ImportError, "client"):
                main.run(self.context, self.host)
        self.assertEqual(sys.meta_path, self.hooks)

    def test_process_setup_failure_preserves_the_import_hooks(self):
        self.tg.process.set_process.side_effect = ValueError("context")
        with patch.dict(sys.modules, {"tangram": self.tg}):
            with self.assertRaisesRegex(ValueError, "context"):
                main.run(self.context, self.host)
        self.assertEqual(sys.meta_path, self.hooks)

    def test_inventory_failure_does_not_affect_the_next_invocation(self):
        self.host.inventory.side_effect = RuntimeError("inventory")
        with patch.dict(sys.modules, {"tangram": self.tg}):
            with self.assertRaisesRegex(RuntimeError, "inventory"):
                main.run(self.context, self.host)
            self.assertEqual(sys.meta_path, self.hooks)
            self.host.inventory.side_effect = None
            self.assertEqual(main.run(self.context, self.host), (0, None, None))
        self.assertEqual(sys.meta_path, self.hooks)
        self.assertFalse(any(name.startswith("_tangram_entry") for name in sys.modules))
        self.tg.client.close.assert_awaited_once()

    def test_system_exit_matches_cpython(self):
        for code in [
            None,
            True,
            7,
            -2,
            256,
            sys.maxsize,
            -sys.maxsize - 1,
            sys.maxsize + 1,
            -sys.maxsize - 2,
            2**100,
            -(2**100),
        ]:
            with self.subTest(code=code):
                source = f"raise SystemExit({code!r})"
                expected = subprocess.run([sys.executable, "-c", source]).returncode
                self.host.module.return_value = (json.dumps(self.module), source)
                with patch.dict(sys.modules, {"tangram": self.tg}):
                    self.assertEqual(
                        main.run(self.context, self.host), (expected, None, None)
                    )


class SourceTests(unittest.TestCase):
    def test_user_modules_choose_their_annotation_semantics(self):
        tg = ModuleType("tangram")
        tg.__dict__["Module"] = SimpleNamespace(from_data=lambda value: value)
        data = {"kind": "py", "referent": {"node": "/test/main.tg.py"}}
        for future in [False, True]:
            with self.subTest(future=future):
                source = "def function(value: int) -> str:\n    return str(value)\n"
                if future:
                    source = "from __future__ import annotations\n" + source
                host = Mock()
                host.inventory.return_value = "null"
                host.module.return_value = (json.dumps(data), source)
                finder = main.Finder(host, tg, data)
                try:
                    module = finder.load_entry()
                    expected = (
                        {"value": "int", "return": "str"}
                        if future
                        else {"value": int, "return": str}
                    )
                    self.assertEqual(module.function.__annotations__, expected)
                finally:
                    finder.close()

    def test_invocations_restore_the_source_cache(self):
        tg = ModuleType("tangram")
        tg.__dict__["Module"] = SimpleNamespace(from_data=lambda value: value)
        existing = (10, None, ["previous\n"], "/test/previous.tg.py")
        original = linecache.cache.copy()
        try:
            linecache.cache[existing[3]] = existing
            for filename, source in [
                ("/test/previous.tg.py", "value = 1"),
                ("/test/new.tg.py", "value = 2"),
                ("/test/broken.tg.py", "raise RuntimeError('failed')"),
                ("/test/syntax.tg.py", "invalid ="),
            ]:
                with self.subTest(filename=filename):
                    data = {"kind": "py", "referent": {"node": filename}}
                    host = Mock()
                    host.inventory.return_value = "null"
                    host.module.return_value = (json.dumps(data), source)
                    finder = main.Finder(host, tg, data)
                    try:
                        if filename.endswith("broken.tg.py"):
                            with self.assertRaises(RuntimeError):
                                finder.load_entry()
                        elif filename.endswith("syntax.tg.py"):
                            with self.assertRaises(SyntaxError):
                                finder.load_entry()
                        else:
                            finder.load_entry()
                        self.assertEqual(linecache.getlines(filename), [source])
                    finally:
                        finder.close()
                    if filename == existing[3]:
                        self.assertIs(linecache.cache[filename], existing)
                    else:
                        self.assertNotIn(filename, linecache.cache)
                    finder.close()
            self.assertFalse(
                any(name.startswith("_tangram_entry") for name in sys.modules)
            )
        finally:
            linecache.cache.clear()
            linecache.cache.update(original)
