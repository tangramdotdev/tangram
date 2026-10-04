"""Runtime initialization restores Python's import hooks after failures."""

import importlib.util
import json
import linecache
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
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
        }
    )


def source_host(data, source):
    host = Mock()
    host.describe.side_effect = lambda value: describe(json.loads(value))
    host.resolve_path.return_value = None
    host.namespace_exists.return_value = False
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

        def resolve(referrer, import_):
            self.assertEqual(json.loads(import_)["reference"], "child")
            data = json.loads(json.dumps(child))
            data["referent"]["options"] = {
                "tokens": {"local": [str(host.resolve.call_count)]}
            }
            return describe(data)

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


class ImportTests(unittest.TestCase):
    def test_artifact_member_context_cannot_escape_its_referent_root(self):
        self.load(
            {
                "main": {
                    "filename": "tangram.py",
                    "path": "tangram.py",
                    "id": "dir_root",
                    "imports": {"../tangram.py": "outside"},
                    "text": """
try:
    from .. import value
except ImportError as error:
    assert str(error) == 'attempted relative import beyond top-level package'
else:
    assert False
""",
                },
                "outside": {
                    "filename": "tangram.py",
                    "text": "raise AssertionError('outside the artifact root')",
                },
            }
        )

    def test_member_referents_determine_package_context_before_execution(self):
        for imports in (
            "import leaf\nimport pkg.namespace.leaf",
            "import pkg.namespace.leaf\nimport leaf",
        ):
            with self.subTest(imports=imports):
                self.load(
                    {
                        "main": {
                            "filename": "main.tg.py",
                            "declarations": {"leaf": "leaf", "pkg": "pkg"},
                            "text": imports
                            + "\nassert leaf is pkg.namespace.leaf\n"
                            + "assert leaf.parent is pkg and leaf.read() == 42",
                        },
                        "pkg": {
                            "filename": "tangram.py",
                            "path": "tangram.py",
                            "imports": {"namespace/leaf.tg.py": "leaf"},
                            "text": "value = 42\nfrom .namespace import leaf",
                        },
                        "leaf": {
                            "filename": "leaf.tg.py",
                            "path": "namespace/leaf.tg.py",
                            "imports": {"../tangram.py": "pkg"},
                            "text": """
import importlib
from .. import value
parent = importlib.import_module('..', __package__)
calls = globals().get('calls', 0) + 1
assert calls == 1 and value == 42
def read():
    from .. import value
    return value
""",
                        },
                    }
                )

    def test_standard_loader_sequence_reuses_the_module_and_executes_once(self):
        self.load(
            {
                "main": {
                    "filename": "main.tg.py",
                    "declarations": {"child": "child", "same": "child"},
                    "text": """
import importlib.util
import sys
spec = importlib.util.find_spec('child')
module = importlib.util.module_from_spec(spec)
assert importlib.util.module_from_spec(spec) is module
assert tg.attempts == 0
assert 'value = 42' in spec.loader.get_source(spec.name)
assert tg.attempts == 0
sys.modules[spec.name] = module
spec.loader.exec_module(module)
assert module.value == 42 and tg.attempts == 1
spec.loader.exec_module(module)
import child, same
assert child is same is module
assert importlib.util.module_from_spec(spec) is module
assert tg.attempts == 1
""",
                },
                "child": {
                    "filename": "child.tg.py",
                    "text": "tg.attempts += 1\nvalue = 42",
                },
            }
        )

    def test_standalone_modules_reject_relative_imports(self):
        self.load(
            {
                "main": {
                    "filename": "main.tg.py",
                    "imports": {"helper.tg.py": "helper"},
                    "text": """
assert __package__ == ''
try:
    from .helper import value
except ImportError as error:
    assert str(error) == 'attempted relative import with no known parent package'
else:
    assert False
""",
                },
                "helper": {"filename": "helper.tg.py", "text": "value = 42"},
            }
        )

    def test_namespace_ancestry_stops_at_the_top_level_package(self):
        self.load(
            {
                "main": {
                    "filename": "tangram.py",
                    "imports": {"namespace/leaf.tg.py": "leaf"},
                    "text": """
import importlib
value = 42
from .namespace import leaf
assert leaf.parent is importlib.import_module('.', __package__)
try:
    from .. import missing
except ImportError as error:
    assert str(error) == 'attempted relative import beyond top-level package'
else:
    assert False
""",
                },
                "leaf": {
                    "filename": "leaf.tg.py",
                    "path": "namespace/leaf.tg.py",
                    "imports": {"../tangram.py": "main"},
                    "text": """
import importlib
from .. import value
assert value == 42
parent = importlib.import_module('..', __package__)
try:
    from ... import missing
except ImportError as error:
    assert str(error) == 'attempted relative import beyond top-level package'
else:
    assert False
""",
                },
            }
        )

    def test_declarations_are_scoped_and_shared_identities_execute_once(self):
        records = {
            "main": {
                "filename": "main.tg.py",
                "declarations": {"left": "left", "same": "left", "right": "right"},
                "text": """
import importlib
import importlib.util
import left
import same as alias
from right import read
assert left is alias
assert left.calls == 1
assert read() == 43
assert left.read() == 42
assert importlib.import_module('left') is left
assert importlib.util.find_spec('same') is left.__spec__
assert __import__('left') is left
""",
            },
            "left": {
                "filename": "tangram.py",
                "declarations": {"common": "debug"},
                "text": """
import importlib
calls = globals().get('calls', 0) + 1
def read():
    return importlib.import_module('common').value
""",
            },
            "right": {
                "filename": "tangram.py",
                "declarations": {"common": "release"},
                "text": "def read():\n    from common import value\n    return value\n",
            },
            "debug": {
                "filename": "tangram.py",
                "imports": {"helper.tg.py": "debug_helper"},
                "text": "from .helper import value",
            },
            "release": {
                "filename": "tangram.py",
                "imports": {"helper.tg.py": "release_helper"},
                "text": "from .helper import value",
            },
            "debug_helper": {"filename": "helper.tg.py", "text": "value = 42"},
            "release_helper": {"filename": "helper.tg.py", "text": "value = 43"},
        }
        self.load(records)

    def test_relative_imports_preserve_package_exports_and_identity(self):
        records = {
            "main": {
                "filename": "main.tg.py",
                "declarations": {"pkg": "pkg"},
                "text": (
                    "import pkg; assert pkg.child.parent is pkg; assert "
                    "pkg.child.result == 42; assert pkg.sub.child.parent is pkg"
                ),
            },
            "pkg": {
                "filename": "tangram.py",
                "imports": {"child.tg.py": "child", "sub/tangram.py": "sub"},
                "text": "value = 42\nfrom . import child, sub",
            },
            "child": {
                "filename": "child.tg.py",
                "text": (
                    "import importlib\n"
                    "from . import value\n"
                    "result = value\n"
                    "parent = importlib.import_module('.', __package__)\n"
                    "assert importlib.util.find_spec('.', __package__) is "
                    "parent.__spec__\n"
                    "assert __import__('', globals(), fromlist=('value',), "
                    "level=1) is parent"
                ),
            },
            "sub": {
                "filename": "tangram.py",
                "imports": {"child.tg.py": "sub_child"},
                "text": "from . import child",
            },
            "sub_child": {
                "filename": "child.tg.py",
                "text": (
                    "import importlib\n"
                    "from .. import value\n"
                    "assert value == 42\n"
                    "parent = importlib.import_module('..', __package__)"
                ),
            },
        }
        self.load(records)

    def test_package_fromlists_keep_referrer_specific_module_edges(self):
        records = {
            "main": {
                "filename": "tangram.py",
                "imports": {"left.tg.py": "left", "right.tg.py": "right"},
                "text": (
                    "from . import left, right\n"
                    "assert left.helper.value == 42 and right.helper.value == 43"
                ),
            },
            "left": {
                "filename": "left.tg.py",
                "imports": {"helper.tg.py": "debug"},
                "text": "from . import helper",
            },
            "right": {
                "filename": "right.tg.py",
                "imports": {"helper.tg.py": "release"},
                "text": "from . import helper",
            },
            "debug": {"filename": "helper.tg.py", "text": "value = 42"},
            "release": {"filename": "helper.tg.py", "text": "value = 43"},
        }
        self.load(records)

    def test_child_entries_initialize_their_package(self):
        records = {
            "pkg": {
                "filename": "tangram.py",
                "imports": {"child.tg.py": "child"},
                "text": (
                    "value = 42\n"
                    "from . import child\n"
                    "assert child.parent is __import__('', globals(), "
                    "fromlist=('value',), level=1)"
                ),
            },
            "child": {
                "filename": "child.tg.py",
                "text": (
                    "import importlib\n"
                    "from . import value\n"
                    "assert value == 42\n"
                    "parent = importlib.import_module('.', __package__)"
                ),
            },
        }
        self.load(records, entry="child", initializer="pkg")

    def test_cycles_and_failed_initialization_retries(self):
        records = {
            "main": {
                "filename": "main.tg.py",
                "declarations": {"cycle": "cycle", "broken": "broken"},
                "text": """
import cycle
assert cycle.value == 7
for attempt in range(2):
    try:
        import broken
    except RuntimeError:
        assert attempt == 0
    else:
        assert broken.value == 8
""",
            },
            "cycle": {
                "filename": "tangram.py",
                "declarations": {"cycle": "cycle"},
                "text": "value = 7\nimport cycle\nassert cycle.value == 7",
            },
            "broken": {
                "filename": "broken.tg.py",
                "text": """
tg.attempts += 1
if tg.attempts == 1:
    raise RuntimeError('retry')
value = 8
""",
            },
        }
        self.load(records)

    def test_declared_children_initialize_and_share_their_package(self):
        records = {
            "main": {
                "filename": "main.tg.py",
                "declarations": {"child": "child", "pkg": "pkg"},
                "text": (
                    "import child, pkg; assert child.result == 42; "
                    "assert child.parent is pkg; assert pkg.child is child; "
                    "assert child.calls == 1"
                ),
            },
            "pkg": {
                "filename": "tangram.py",
                "imports": {"child.tg.py": "child"},
                "text": "value = 42\nfrom . import child",
            },
            "child": {
                "filename": "child.tg.py",
                "imports": {"tangram.py": "pkg"},
                "text": (
                    "import importlib\nfrom . import value\nresult = value\n"
                    "parent = importlib.import_module('.', __package__)\n"
                    "calls = globals().get('calls', 0) + 1"
                ),
            },
        }
        self.load(records)

    def test_failed_package_initialization_can_be_retried(self):
        records = {
            "main": {
                "filename": "main.tg.py",
                "declarations": {"child": "child"},
                "text": (
                    "try:\n    import child\nexcept RuntimeError:\n    pass\n"
                    "import child\nassert child.value == 42\nassert tg.attempts == 2"
                ),
            },
            "pkg": {
                "filename": "tangram.py",
                "text": (
                    "tg.attempts += 1\nif tg.attempts == 1:\n"
                    "    raise RuntimeError('retry')\nvalue = 42"
                ),
            },
            "child": {
                "filename": "child.tg.py",
                "text": "from . import value",
            },
        }
        self.load(records, initializers={"child": "pkg"})

    def test_cached_children_use_the_retried_parent_package(self):
        self.load(
            {
                "main": {
                    "filename": "main.tg.py",
                    "declarations": {"pkg": "pkg"},
                    "text": """
try:
    import pkg
except RuntimeError:
    pass
import pkg
assert pkg.sub.read() == 2
""",
                },
                "pkg": {
                    "filename": "tangram.py",
                    "imports": {"sub/tangram.py": "sub"},
                    "text": """
tg.attempts += 1
value = tg.attempts
from . import sub
if tg.attempts == 1:
    raise RuntimeError('retry')
""",
                },
                "sub": {
                    "filename": "sub/tangram.py",
                    "text": "def read():\n    from .. import value\n    return value",
                },
            }
        )

    def test_filesystem_child_entries_initialize_their_package(self):
        with TemporaryDirectory() as directory:
            path = Path(directory).resolve()
            (path / "tangram.py").write_text("value = 42\nfrom . import child")
            (path / "child.tg.py").write_text(
                "from . import value\nresult = value\n"
                "calls = globals().get('calls', 0) + 1"
            )
            data = {"kind": "py", "referent": {"node": str(path / "child.tg.py")}}
            host = source_host(data, "")

            def resolve_path(serialized, reference, kind):
                module = json.loads(serialized)
                path = (Path(module["referent"]["node"]).parent / reference).resolve()
                exists = path.is_dir() if kind == "directory" else path.is_file()
                if not exists:
                    return None
                module = {"kind": kind, "referent": {"node": str(path)}}
                return describe(module)

            host.resolve_path.side_effect = resolve_path
            host.load.side_effect = lambda serialized: Path(
                json.loads(serialized)["referent"]["node"]
            ).read_text()
            tg = ModuleType("tangram")
            tg.__dict__["Module"] = SimpleNamespace(from_data=lambda value: value)
            finder = main.Finder(host, tg, data)
            sys.meta_path.insert(0, finder)
            try:
                module = finder.load_entry()
                self.assertEqual(module.result, 42)
                self.assertEqual(module.calls, 1)
                self.assertIs(sys.modules[module.__package__].child, module)
            finally:
                finder.close()
                sys.meta_path.remove(finder)

    def load(self, records, *, entry="main", initializer=None, initializers=None):
        for name, record in records.items():
            record["data"] = {"kind": "py", "referent": {"node": name}}
            if "path" in record:
                record["data"]["referent"]["options"] = {"path": record["path"]}
            if "id" in record:
                record["data"]["referent"].setdefault("options", {})["id"] = record[
                    "id"
                ]
            record.setdefault("imports", {})
            record.setdefault("declarations", {})
        host = Mock()
        host.namespace_exists.side_effect = lambda serialized, path: any(
            main.posixpath.normpath(reference).startswith(
                main.posixpath.normpath(path) + "/"
            )
            for reference in records[json.loads(serialized)["referent"]["node"]][
                "imports"
            ]
        )
        initializers = initializers or ({entry: initializer} if initializer else {})
        parents = {}
        for source, record in records.items():
            if Path(record["filename"]).name != "tangram.py":
                continue
            for path, target in record["imports"].items():
                if path.startswith(".."):
                    continue
                components = Path(path).parts
                if len(components) == 1:
                    parents[target] = source
                elif len(components) == 2 and components[-1] == "tangram.py":
                    parents[target] = source

        def descriptor(data, filename=None):
            name = data["referent"]["node"]
            return describe(data, filename or records[name]["filename"])

        def resolve_path(serialized, path, kind):
            data = json.loads(serialized)
            name = data["referent"]["node"]
            record = records[name]
            path = main.posixpath.normpath(path)
            if path in record["imports"]:
                target = record["imports"][path]
                return descriptor(records[target]["data"])
            if path == "tangram.py":
                target = (
                    name
                    if Path(record["filename"]).name == "tangram.py"
                    else initializers.get(name, parents.get(name))
                )
                if target is not None:
                    return descriptor(records[target]["data"])
            if path.endswith("/tangram.py") and path.startswith("../"):
                level = len(Path(path).parts) - 1
                target = name
                if Path(record["filename"]).name != "tangram.py":
                    target = initializers.get(name, parents.get(name))
                for _ in range(level):
                    target = parents.get(target)
                if target is not None:
                    return descriptor(records[target]["data"])
            return None

        host.describe.side_effect = lambda serialized: descriptor(
            json.loads(serialized)
        )
        host.resolve_path.side_effect = resolve_path
        host.resolve.side_effect = lambda serialized, import_: descriptor(
            records[json.loads(import_)["reference"]]["data"]
        )
        host.load.side_effect = lambda serialized: records[
            json.loads(serialized)["referent"]["node"]
        ]["text"]
        host.metadata.side_effect = lambda filename, text: json.dumps(
            {
                "imports": {
                    alias: {"kind": "py", "reference": target}
                    for record in records.values()
                    if record["text"] == text
                    for alias, target in record["declarations"].items()
                }
            }
        )
        tg = ModuleType("tangram")
        tg.__dict__.update(
            Module=SimpleNamespace(from_data=lambda value: value), attempts=0
        )
        original_import = main.importlib.import_module
        original_spec = main.importlib.util.find_spec
        finder = main.Finder(host, tg, records[entry]["data"])
        sys.meta_path.insert(0, finder)
        setattr(main.importlib, "import_module", finder.import_module)
        setattr(main.importlib.util, "find_spec", finder.find_module_spec)
        try:
            finder.load_entry()
            self.assertNotIn("left", sys.modules)
        finally:
            finder.close()
            sys.meta_path.remove(finder)
        self.assertIs(main.importlib.import_module, original_import)
        self.assertIs(main.importlib.util.find_spec, original_spec)
        self.assertFalse(
            any(name.startswith("_tangram_modules") for name in sys.modules)
        )
