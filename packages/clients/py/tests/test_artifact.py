"""Check artifact dispatch and the JS namespace predicates."""

import unittest

from helpers import ObjectTestCase

from tangram.artifact import Artifact
from tangram.blob import Blob
from tangram.directory import Directory
from tangram.file import File
from tangram.graph import Graph, Pointer
from tangram.referent import Referent
from tangram.symlink import Symlink


class ArtifactTests(ObjectTestCase):
    def test_id_dispatch_and_type_guard(self):
        for prefix, type_ in (("dir", Directory), ("fil", File), ("sym", Symlink)):
            with self.subTest(prefix=prefix):
                artifact = Artifact.with_id(prefix + "_example")
                self.assertIsInstance(artifact, type_)
                self.assertIsInstance(artifact, Artifact)
                self.assertTrue(artifact.state.stored)
                self.assertTrue(Artifact.Id.is_(artifact.id))
                self.assertIs(Artifact.expect(artifact), artifact)
        self.assertFalse(Artifact.is_(Blob("data")))
        self.assertFalse(Artifact.Id.is_(None))
        self.assertFalse(Artifact.Id.is_("blb_example"))
        with self.assertRaises(TypeError):
            Artifact.assert_(Blob("data"))
        with self.assertRaises(ValueError):
            Artifact.with_id("blb_example")
        with self.assertRaises(TypeError):
            Artifact.with_id(None)

    def test_referent_preserves_state(self):
        options = {"location": "remote:example", "tokens": {"local": ["proof"]}}
        artifact = Artifact.with_referent(Referent("fil_example", options))
        self.assertEqual(artifact.state.location, options["location"])
        self.assertEqual(artifact.state.tokens, options["tokens"])

    def test_referent_without_options_and_constructor_state_match_js(self):
        artifact = Artifact.with_referent(Referent("fil_example", None))
        self.assertIsNone(artifact.state.location)
        self.assertEqual(artifact.state.tokens, {})
        blob = Blob({"object": {"bytes": b"contents"}, "stored": False})
        self.assertFalse(blob.state.stored)
        file = File(
            {
                "object": {
                    "contents": blob,
                    "dependencies": {},
                    "executable": False,
                    "module": None,
                },
                "stored": False,
            }
        )
        self.assertFalse(file.state.stored)
        self.assertEqual(file._value["contents"], blob)

    async def test_graph_pointer_dispatch(self):
        graph = Graph(
            [
                {"kind": "directory", "entries": {"hello": 1}},
                {"kind": "file", "contents": Blob("hello")},
                {"kind": "symlink", "artifact": 0, "path": "hello"},
            ]
        )
        for index, kind in enumerate(("directory", "file", "symlink")):
            artifact = Artifact.with_pointer(Pointer(graph, index, kind))
            self.assertEqual(artifact.kind, kind)
            self.assertFalse(artifact.state.stored)
        directory = Artifact.with_pointer(Pointer(graph, 0, "directory"))
        self.assertEqual(await (await directory.get("hello")).text(), "hello")
        with self.assertRaises(ValueError):
            Artifact.with_pointer(Pointer(graph, 0, "blob"))

    def test_data_guard_matches_js_order_and_nulls(self):
        for data in (
            {},
            "graph=example",
            {"index": 0},
            {"children": []},
            {"entries": {}},
            {"contents": None},
            {"dependencies": None},
            {"artifact": 0},
            {"artifact": {"index": 1}},
            {"path": None},
            {"executable": False},
            {"module": None},
        ):
            with self.subTest(data=data):
                self.assertTrue(Artifact.Data.is_(data))
        for data in (
            None,
            [],
            {"entries": None},
            {"executable": None},
            {"artifact": False},
            {"index": False},
            {"other": True},
            {"children": None, "contents": "blb_example"},
        ):
            with self.subTest(data=data):
                self.assertFalse(Artifact.Data.is_(data))


if __name__ == "__main__":
    unittest.main()
