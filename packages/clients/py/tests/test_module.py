"""Module descriptors preserve source spelling, locations, and proofs."""

import unittest

from tangram.file import File
from tangram.graph import Graph, Pointer
from tangram.location import Location
from tangram.module import Module
from tangram.referent import Referent


class ModuleTests(unittest.TestCase):
    def test_constructor_and_source_namespace(self):
        module = Module({"kind": "py", "referent": {"node": "main.py"}})
        self.assertEqual(module.kind, "py")
        self.assertEqual(
            Module.Source.to_data_string(module.referent.node), "./main.py"
        )
        self.assertEqual(Module.Source.to_data_string("../main.py"), "../main.py")
        self.assertEqual(Module.Source.to_data_string("/main.py"), "/main.py")
        self.assertEqual(module.to_data()["referent"]["node"], "main.py")
        self.assertEqual(module.to_data_string(), "./main.py?kind=py")

    def test_data_and_string_round_trip(self):
        options = {
            "artifact": "dir_example",
            "id": "fil_example",
            "location": Location.from_data_string("local"),
            "name": "hello world!",
            "path": "some/path",
            "tag": "my/tag",
            "tokens": {"local": ["a", "b"]},
        }
        module = Module("ts", Referent("./main.ts", options))
        for round_trip in (
            Module.from_data(module.to_data()),
            Module.from_data_string(module.to_data_string()),
        ):
            self.assertEqual(round_trip, module)
        self.assertTrue(module.to_data_string().endswith("&kind=ts"))
        self.assertIn("name=hello%20world!", module.to_data_string())
        self.assertNotIn("tokens", module.without_token().referent.options)
        self.assertEqual(module.referent.options["tokens"], {"local": ["a", "b"]})

    def test_children_inherit_referent_options(self):
        child = File.with_id("fil_example")
        module = Module("file", Referent(child, {"location": "local"}))
        self.assertEqual(module.children(), [child])
        self.assertEqual(child.state.location, "local")
        self.assertEqual(module.to_referent().options["location"], "local")
        pointer = Pointer(Graph.with_id("gph_example"), 1, "file")
        module = Module("file", Referent(pointer))
        self.assertEqual(module.children(), [pointer.graph])
        self.assertEqual(Module.from_data(module.to_data()).referent.node, pointer)
        self.assertEqual(Module.Data.children(module.to_data()), ["gph_example"])

    def test_data_namespaces_and_location(self):
        module = Module(
            "ts",
            Referent("./main.ts", {"location": Location.from_data_string("local")}),
        )
        span = {
            "start": {"line": 0, "character": 1},
            "end": {"line": 0, "character": 2},
        }
        location = Module.Location(module=module, range=span)
        data = Module.Location.to_data(location)
        self.assertEqual(Module.Location.to_data(Module.Location.from_data(data)), data)
        self.assertEqual(Module.Location.children(location), [])
        self.assertEqual(Module.Location.Data.children(data), [])
        stripped = Module.Location.Data.without_location_and_tokens(data)
        self.assertEqual(stripped["range"], span)
        self.assertNotIn("location", stripped["module"]["referent"]["options"])
        compact = {
            "kind": "ts",
            "referent": "fil_example?name=x&location=local&tokens[local][0]=x",
        }
        self.assertEqual(
            Module.Data.without_location_and_tokens(compact),
            {"kind": "ts", "referent": "fil_example?name=x"},
        )
        for source, expected in (
            ("./main.ts", []),
            ("fil_example", ["fil_example"]),
            ("12", []),
            (12, []),
        ):
            self.assertEqual(
                Module.Data.children({"kind": "ts", "referent": {"node": source}}),
                expected,
            )

    def test_string_parser_errors_match_source(self):
        for source in (
            "./main.ts",
            "./main.ts?kind=ts&unknown=x",
            "./main.ts?kind=ts&tokens[local][1]=x",
            "./main.ts?kind",
        ):
            with self.subTest(source=source), self.assertRaises(ValueError):
                Module.from_data_string(source)
        module = Module.from_data_string("./main.ts?kind=ts=ignored?ignored")
        self.assertEqual(module.kind, "ts")
