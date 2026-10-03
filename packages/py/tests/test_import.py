"""Embedded library imports preserve Python's module and resource contracts."""

import importlib
import importlib.machinery
import importlib.metadata
import importlib.resources
import importlib.util
import inspect
import pkgutil
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location(
    "tangram_test_import", Path(__file__).parents[1] / "src/import.py"
)
assert spec is not None and spec.loader is not None
embedded = importlib.util.module_from_spec(spec)
spec.loader.exec_module(embedded)


class ImportTests(unittest.TestCase):
    def setUp(self):
        self.files = {
            "_tangram_test_package/__init__.py": b"from .child import value\n",
            "_tangram_test_package/child.py": b"def value():\n    return 42\n",
            "_tangram_test_package/data/message.txt": b"hello\n",
            "_tangram_test_namespace/child.py": b"value = 7\n",
            "_tangram_test_namespace/data/message.txt": b"namespace resource\n",
            "_tangram_test_package/sub/__init__.py": b"",
            "_tangram_test_package/sub/leaf.py": b"value = 9\n",
            "tangram_test_dist-1.2.3.dist-info/METADATA": (
                b"Metadata-Version: 2.1\nName: Tangram-Test.Dist\nVersion: 1.2.3\n"
            ),
            "tangram_test_dist-1.2.3.dist-info/entry_points.txt": (
                b"[test_plugins]\nexample = _tangram_test_package.child:value\n"
            ),
            "tangram_test_dist-1.2.3.dist-info/RECORD": (
                b"_tangram_test_package/data/message.txt,,\n"
            ),
            "_tangram_test_encoding.py": b"# coding: latin-1\nvalue = 'caf\xe9'\n",
        }
        self.assets = SimpleNamespace(paths=frozenset(self.files), get=self.files.get)
        self.finder = embedded.Finder(self.assets)
        sys.meta_path.insert(0, self.finder)
        self.path = list(sys.path)
        sys.path.append(embedded.PREFIX.rstrip("/"))
        self.hooks = list(sys.path_hooks)
        sys.path_hooks.insert(0, self.finder.path_hook)

    def tearDown(self):
        sys.meta_path.remove(self.finder)
        sys.path_hooks[:] = self.hooks
        sys.path[:] = self.path
        for path in list(sys.path_importer_cache):
            if path == embedded.PREFIX.rstrip("/") or path.startswith(embedded.PREFIX):
                del sys.path_importer_cache[path]
        for name in list(sys.modules):
            if name.startswith("_tangram_test_"):
                del sys.modules[name]

    def test_relative_imports_and_source_inspection(self):
        package = importlib.import_module("_tangram_test_package")
        self.assertEqual(package.value(), 42)
        self.assertEqual(
            inspect.getsource(package.value), "def value():\n    return 42\n"
        )
        assert package.__file__ is not None
        self.assertTrue(package.__file__.startswith(embedded.PREFIX))

    def test_namespace_package(self):
        child = importlib.import_module("_tangram_test_namespace.child")
        self.assertEqual(child.value, 7)
        self.assertIsNone(sys.modules["_tangram_test_namespace"].__file__)

    def test_namespace_resources(self):
        package = importlib.import_module("_tangram_test_namespace")
        root = importlib.resources.files(package)
        self.assertEqual([entry.name for entry in root.iterdir()], ["child.py", "data"])
        self.assertTrue((root / "data").is_dir())
        resource = root / "data" / "message.txt"
        self.assertTrue(resource.is_file())
        self.assertEqual(resource.read_text(encoding="utf-8"), "namespace resource\n")
        self.assertIsNone(package.__file__)
        assert package.__spec__ is not None
        self.assertIsNone(package.__spec__.origin)
        self.assertFalse(package.__spec__.has_location)
        self.assertEqual(list(package.__path__), [embedded.PREFIX + package.__name__])
        child = importlib.import_module(package.__name__ + ".child")
        self.assertEqual(child.value, 7)

    def test_package_resources(self):
        package = importlib.import_module("_tangram_test_package")
        resource = importlib.resources.files(package) / "data" / "message.txt"
        self.assertEqual(resource.read_text(encoding="utf-8"), "hello\n")
        self.assertEqual(
            pkgutil.get_data(package.__name__, "data/message.txt"), b"hello\n"
        )
        directory = importlib.resources.files(package) / "data"
        self.assertEqual([entry.name for entry in directory.iterdir()], ["message.txt"])

    def test_module_resource_anchors(self):
        child = importlib.import_module("_tangram_test_package.child")
        root = importlib.resources.files(child)
        self.assertEqual((root / "data" / "message.txt").read_bytes(), b"hello\n")
        module = importlib.import_module("_tangram_test_encoding")
        root = importlib.resources.files(module)
        self.assertEqual(
            (root / "_tangram_test_package" / "data" / "message.txt").read_bytes(),
            b"hello\n",
        )

    def test_module_discovery(self):
        package = importlib.import_module("_tangram_test_package")
        modules = list(pkgutil.iter_modules(package.__path__, package.__name__ + "."))
        self.assertEqual(
            [(module.name, module.ispkg) for module in modules],
            [(package.__name__ + ".child", False), (package.__name__ + ".sub", True)],
        )
        names = [
            module.name
            for module in pkgutil.walk_packages(
                package.__path__, package.__name__ + "."
            )
        ]
        self.assertIn(package.__name__ + ".sub.leaf", names)
        self.assertIn(
            "_tangram_test_package", [info.name for info in pkgutil.iter_modules()]
        )
        with self.assertRaises(ImportError):
            self.finder.path_hook("/unrelated/library")

    def test_imports_honor_package_search_paths(self):
        package = importlib.import_module("_tangram_test_package")
        original = package.__path__
        for path in [[], ["/unrelated/package"]]:
            package.__path__ = path
            with self.assertRaises(ModuleNotFoundError):
                importlib.import_module(package.__name__ + ".sub")
        package.__path__ = original
        child = importlib.import_module(package.__name__ + ".sub.leaf")
        self.assertEqual(child.value, 9)
        finder = self.finder.path_hook(original[0])
        self.assertIsNone(finder.find_spec(package.__name__))
        alias = finder.find_spec("_tangram_test_namespace.child")
        assert alias is not None
        self.assertEqual(
            alias.origin, embedded.PREFIX + "_tangram_test_package/child.py"
        )
        self.assertIsNotNone(finder.find_spec(package.__name__ + ".child"))

    def test_resolution_matches_python(self):
        files = {
            "first/module.py": b"value = 1\n",
            "second/module.py": b"value = 2\n",
            "first/package.py": b"",
            "first/package/__init__.py": b"",
            "first/namespace/one.py": b"",
            "second/namespace/two.py": b"",
            "first/overridden/one.py": b"",
            "second/overridden.py": b"",
            "first/replaced/one.py": b"",
            "second/replaced/__init__.py": b"",
            "first/shadow.py": b"",
            "second/shadow/__init__.py": b"",
        }
        finder = embedded.Finder(SimpleNamespace(paths=frozenset(files), get=files.get))
        cases = [
            ("module", []),
            ("missing", ["first", "second"]),
            ("module", ["first", "second"]),
            ("module", ["second", "first"]),
            ("package", ["first", "second"]),
            ("namespace", ["first", "second"]),
            ("overridden", ["first", "second"]),
            ("replaced", ["first", "second"]),
            ("shadow", ["first", "second"]),
            ("alias.module", ["second", "first"]),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, contents in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(contents)
            for fullname, directories in cases:
                with self.subTest(fullname=fullname, directories=directories):
                    expected = importlib.machinery.PathFinder.find_spec(
                        fullname, [str(root / path) for path in directories]
                    )
                    actual = finder.search(
                        fullname, [embedded.PREFIX + path for path in directories]
                    )
                    if expected is None:
                        self.assertIsNone(actual)
                        continue
                    assert actual is not None
                    self.assertEqual(actual.name, expected.name)
                    self.assertEqual(actual.loader is None, expected.loader is None)
                    if expected.origin is not None:
                        self.assertEqual(
                            actual.origin,
                            embedded.PREFIX
                            + Path(expected.origin).relative_to(root).as_posix(),
                        )
                    expected_paths = expected.submodule_search_locations
                    actual_paths = actual.submodule_search_locations
                    if expected_paths is None:
                        self.assertIsNone(actual_paths)
                    else:
                        assert actual_paths is not None
                        self.assertEqual(
                            list(actual_paths),
                            [
                                embedded.PREFIX
                                + Path(path).relative_to(root).as_posix()
                                for path in expected_paths
                            ],
                        )

    def test_redirected_imports_and_namespace_resources(self):
        package = importlib.import_module("_tangram_test_package")
        for filename, contents in {
            "first/selected.py": b"value = 1\n",
            "second/selected.py": b"value = 2\n",
            "first/shared/one.py": b"value = 3\n",
            "first/shared/data/one.txt": b"one",
            "second/shared/two.py": b"value = 4\n",
            "second/shared/data/two.txt": b"two",
            "second/shared/data/one.txt": b"shadowed",
            "third/shared/three.py": b"value = 5\n",
        }.items():
            self.files[filename] = contents
        self.assets.paths = frozenset(self.files)
        finder = embedded.Finder(self.assets)
        sys.meta_path.insert(0, finder)
        try:
            package.__path__ = [embedded.PREFIX + path for path in ["second", "first"]]
            selected = importlib.import_module(package.__name__ + ".selected")
            self.assertEqual(selected.value, 2)
            self.assertEqual(selected.__package__, package.__name__)
            assert selected.__file__ is not None
            self.assertEqual(selected.__file__, embedded.PREFIX + "second/selected.py")
            package.__path__ = [embedded.PREFIX + path for path in ["first", "second"]]
            shared = importlib.import_module(package.__name__ + ".shared")
            self.assertEqual(importlib.import_module(shared.__name__ + ".one").value, 3)
            self.assertEqual(importlib.import_module(shared.__name__ + ".two").value, 4)
            resources = importlib.resources.files(shared) / "data"
            self.assertEqual(
                [entry.name for entry in resources.iterdir()], ["one.txt", "two.txt"]
            )
            self.assertEqual((resources / "one.txt").read_bytes(), b"one")
            self.assertEqual((resources / "two.txt").read_bytes(), b"two")
            package.__path__.append(embedded.PREFIX + "third")
            self.assertEqual(
                importlib.import_module(shared.__name__ + ".three").value, 5
            )
        finally:
            sys.meta_path.remove(finder)

    def test_relative_and_circular_imports_after_redirection(self):
        package = importlib.import_module("_tangram_test_package")
        self.files.update(
            {
                "redirect/sub/__init__.py": b"from .child import value\n",
                "redirect/sub/child.py": b"from ..cycle_a import value\n",
                "redirect/cycle_a.py": (
                    b"name = 'a'\nfrom . import cycle_b\nvalue = name + cycle_b.name\n"
                ),
                "redirect/cycle_b.py": (
                    b"from . import cycle_a\nassert cycle_a.name == 'a'\nname = 'b'\n"
                ),
            }
        )
        self.assets.paths = frozenset(self.files)
        finder = embedded.Finder(self.assets)
        sys.meta_path.insert(0, finder)
        try:
            package.__path__ = [embedded.PREFIX + "redirect"]
            sub = importlib.import_module(package.__name__ + ".sub")
            self.assertEqual(sub.value, "ab")
            first = importlib.import_module(package.__name__ + ".cycle_a")
            second = importlib.import_module(package.__name__ + ".cycle_b")
            self.assertIs(second.cycle_a, first)
            self.assertIs(first.cycle_b, second)
        finally:
            sys.meta_path.remove(finder)

    def test_path_normalization_and_failed_imports(self):
        spec = self.finder.search(
            "alias.child", [embedded.PREFIX + "other/../_tangram_test_package/./"]
        )
        assert spec is not None
        self.assertEqual(
            spec.origin, embedded.PREFIX + "_tangram_test_package/child.py"
        )
        self.assertIsNone(self.finder.search("child", [embedded.PREFIX + "../outside"]))
        package = importlib.import_module("_tangram_test_package")
        filename = "_tangram_test_package/broken.py"
        self.files[filename] = b"raise RuntimeError('failed')\n"
        self.assets.paths = frozenset(self.files)
        finder = embedded.Finder(self.assets)
        sys.meta_path.insert(0, finder)
        try:
            fullname = package.__name__ + ".broken"
            with self.assertRaisesRegex(RuntimeError, "failed"):
                importlib.import_module(fullname)
            self.assertNotIn(fullname, sys.modules)
            self.files[filename] = b"value = 6\n"
            self.assertEqual(importlib.import_module(fullname).value, 6)
        finally:
            sys.meta_path.remove(finder)

    def test_distribution_discovery(self):
        self.assertEqual(importlib.metadata.version("tangram-test_dist"), "1.2.3")
        distribution = importlib.metadata.distribution("TANGRAM.TEST.DIST")
        entry = next(iter(distribution.entry_points))
        self.assertEqual(entry.group, "test_plugins")
        self.assertEqual(entry.load()(), 42)
        assert distribution.files is not None
        resource = distribution.files[0].locate()
        assert isinstance(resource, embedded.Resource)
        self.assertEqual(resource.read_bytes(), b"hello\n")
        self.assertEqual(
            list(
                self.finder.find_distributions(
                    importlib.metadata.DistributionFinder.Context(path=["/unrelated"])
                )
            ),
            [],
        )

    def test_distribution_search_paths(self):
        self.assertEqual(list(importlib.metadata.distributions(path=[])), [])
        self.assertEqual(
            list(importlib.metadata.distributions(path=["/unrelated"])), []
        )
        for path in [
            embedded.PREFIX.rstrip("/"),
            embedded.PREFIX + ".",
            embedded.PREFIX + "_tangram_test_package/..",
        ]:
            with self.subTest(path=path):
                distributions = list(importlib.metadata.distributions(path=[path]))
                self.assertEqual([entry.version for entry in distributions], ["1.2.3"])
        for path in [embedded.PREFIX + "..", embedded.PREFIX + "_tangram_test_package"]:
            with self.subTest(path=path):
                self.assertEqual(
                    list(importlib.metadata.distributions(path=[path])), []
                )
        original = sys.path
        try:
            sys.path = []
            self.assertEqual(importlib.metadata.version("tangram-test.dist"), "1.2.3")
            self.assertEqual(list(importlib.metadata.distributions(path=[])), [])
        finally:
            sys.path = original

    def test_source_encoding(self):
        module = importlib.import_module("_tangram_test_encoding")
        self.assertEqual(module.value, "caf\u00e9")

    def test_resource_paths_and_open_arguments(self):
        root = embedded.Resource(self.assets, "_tangram_test_package")
        for path in ["", ".", "./"]:
            self.assertEqual(root.joinpath(path).path, root.path)
        resource = root.joinpath("data//./message.txt/")
        self.assertEqual(resource.read_bytes(), b"hello\n")
        with resource.open("rt", "utf-8", "strict", "") as stream:
            self.assertEqual(stream.read(), "hello\n")
        for args, kwargs in [
            (("utf-8",), {}),
            ((), {"encoding": "utf-8"}),
            ((), {"errors": "strict"}),
        ]:
            with self.subTest(args=args, kwargs=kwargs):
                with self.assertRaises(ValueError):
                    resource.open("rb", *args, **kwargs)
        with self.assertRaises(IsADirectoryError):
            root.open("rb")
        with self.assertRaises(FileNotFoundError):
            root.joinpath("missing.txt").open("rb")
        with self.assertRaises(ValueError):
            root.joinpath("/outside")

    def test_resources_are_read_only(self):
        resource = embedded.Resource(
            self.assets, "_tangram_test_package/data/message.txt"
        )
        with self.assertRaises(ValueError):
            resource.open("w")
        with self.assertRaises(ValueError):
            resource.joinpath("../other")
