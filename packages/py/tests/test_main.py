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


def describe(data, filename=None):
    data = json.loads(json.dumps(data))
    key = json.loads(json.dumps(data))
    options = key["referent"].get("options", {})
    options.pop("tokens", None)
    if not options:
        key["referent"].pop("options", None)
    return json.dumps(
        {
            "data": data,
            "filename": filename or data["referent"]["node"],
            "key": json.dumps(key, sort_keys=True),
            "package": data["kind"] == "py"
            and Path(filename or data["referent"]["node"]).name == "tangram.py",
        }
    )


def source_host(data, source):
    host = Mock()
    host.describe.side_effect = lambda value: describe(json.loads(value))
    host.resolve.return_value = '{"kind": "fallback"}'
    host.metadata.return_value = '{"imports": {}}'
    host.load.return_value = source
    return host


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
        self.host = source_host(self.module, "value = 1")

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

    def test_descriptor_failure_does_not_affect_the_next_invocation(self):
        self.host.describe.side_effect = RuntimeError("descriptor")
        with patch.dict(sys.modules, {"tangram": self.tg}):
            with self.assertRaisesRegex(RuntimeError, "descriptor"):
                main.run(self.context, self.host)
            self.assertEqual(sys.meta_path, self.hooks)
            self.host.describe.side_effect = lambda value: describe(json.loads(value))
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
                self.host.load.return_value = source
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
                host = source_host(data, source)
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

    def test_resolution_is_lazy_and_refreshes_cached_authorization(self):
        entry = {"kind": "py", "referent": {"node": "/test/main.tg.py"}}
        child = {"kind": "py", "referent": {"node": "/test/child.tg.py"}}
        source = """
import importlib.util
spec = importlib.util.find_spec('child')
assert tg.calls == 0
import child, same
assert child is same and child.value == 42 and tg.calls == 1
assert importlib.util.find_spec('child') is child.__spec__
"""
        host = source_host(entry, source)
        host.load.side_effect = lambda serialized: (
            source
            if json.loads(serialized)["referent"]["node"] == "/test/main.tg.py"
            else "tg.calls += 1\nvalue = 42\ndef fail():\n    raise ValueError('fail')"
        )
        host.metadata.return_value = json.dumps(
            {
                "imports": {
                    name: {"kind": "py", "reference": "child"}
                    for name in ("child", "same", "unused")
                }
            }
        )

        def resolve(request):
            request = json.loads(request)
            if request["kind"] == "package" or request.get("name") not in (
                "child",
                "same",
            ):
                return '{"kind": "fallback"}'
            self.assertEqual(request["imports"][request["name"]]["reference"], "child")
            data = json.loads(json.dumps(child))
            data["referent"]["options"] = {
                "tokens": {"local": [str(host.resolve.call_count)]}
            }
            return json.dumps(
                {
                    "kind": "resolved",
                    "value": {
                        "target": {
                            "kind": "module",
                            "value": json.loads(describe(data)),
                        },
                        "context": None,
                        "steps": [],
                        "root": None,
                    },
                }
            )

        host.resolve.side_effect = resolve
        tg = ModuleType("tangram")
        tg.__dict__.update(
            Error=RuntimeError,
            Module=SimpleNamespace(from_data=lambda value: value),
            calls=0,
        )
        finder = main.Finder(host, tg, entry)
        setattr(main.importlib, "import_module", finder.import_module)
        setattr(main.importlib.util, "find_spec", finder.find_module_spec)
        try:
            namespace = finder.load_entry()
            try:
                namespace.child.fail()
            except ValueError as exception:
                error = finder.error(exception)
                self.assertEqual(
                    error["location"]["file"]["value"]["referent"]["options"]["tokens"][
                        "local"
                    ],
                    [str(host.resolve.call_count)],
                )
            else:
                self.fail("expected the child to raise")
            self.assertEqual(host.load.call_count, 2)
            self.assertEqual(
                namespace.child.__tangram_module__["referent"]["options"]["tokens"][
                    "local"
                ],
                [str(host.resolve.call_count)],
            )
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
                    host = source_host(data, source)
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
