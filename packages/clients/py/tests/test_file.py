import asyncio
import unittest

import tangram as tg
from tangram.file import File


class FileTests(unittest.IsolatedAsyncioTestCase):
    async def test_builder_and_argument_resolution(self):
        async def text():
            return "hello"

        child = tg.File("child")
        builder = tg.file(text()).contents(" world").dependency("child", child)
        builder.executable().module("js")
        file = await builder
        self.assertEqual(await (await file.contents).length, 11)
        self.assertTrue(await file.executable)
        self.assertEqual(await file.module, "js")
        self.assertIs((await file.dependencies)["child"].node, child)
        self.assertEqual((await builder).id, file.id)
        copied = await tg.file(file, "!")
        self.assertEqual(await copied.length, 12)
        self.assertFalse(await copied.executable)
        self.assertIsNone(await copied.module)
        cleared = await tg.file(file, {"contents": None, "dependencies": None})
        self.assertEqual(await cleared.length, 0)
        self.assertEqual(await cleared.dependencies, {})
        future = asyncio.get_running_loop().create_future()
        future.set_result(file)
        self.assertIs(await File.new(future), file)

    async def test_contents_and_dependencies_inherit_proofs(self):
        contents = tg.Blob("contents")
        child = tg.File("child")
        overridden = tg.File("overridden")
        local = tg.File("local")
        options = {
            "location": {"name": "dependency"},
            "tokens": {"dependency": ["proof"]},
        }
        file = await tg.file(contents).dependencies(
            {
                "child": child,
                "local": {"node": local, "options": {"location": {}}},
                "override": {"node": overridden, "options": options},
                "missing": {"node": None, "options": {"name": "missing"}},
                "null": None,
            }
        )
        file.location = "parent"
        file.tokens = {"parent": ["parent proof"]}
        self.assertIs(await file.contents, contents)
        self.assertEqual(contents.location, "parent")
        self.assertEqual(contents.tokens, file.tokens)
        dependencies = await file.dependencies
        self.assertEqual(child.location, "parent")
        self.assertEqual(local.location, {})
        self.assertEqual(overridden.location, {"name": "dependency"})
        self.assertEqual(
            overridden.tokens, {"dependency": ["proof"], "parent": ["parent proof"]}
        )
        self.assertEqual(dependencies["missing"].options, {"name": "missing"})
        self.assertIsNone(dependencies["null"])
        self.assertEqual(await file.dependency_objects, [child, local, overridden])

    async def test_graph_and_cross_graph_dependencies(self):
        external = tg.Graph([{"kind": "file", "contents": tg.Blob("external")}])
        graph = tg.Graph(
            [
                {
                    "kind": "file",
                    "contents": tg.Blob("source"),
                    "dependencies": {
                        "local": tg.Referent(1),
                        "external": tg.Referent(external.pointer(0, "file")),
                    },
                },
                {"kind": "file", "contents": tg.Blob("child")},
            ]
        )
        file = File.with_pointer(graph.pointer(0, "file"))
        file.location = "remote"
        dependencies = await file.dependencies
        self.assertIsInstance(dependencies["local"].node, File)
        self.assertEqual(dependencies["local"].node.location, "remote")
        self.assertEqual(await dependencies["external"].node.length, 8)
        self.assertEqual(await file.length, 6)

    async def test_object_and_data_namespaces_strip_proofs(self):
        child = tg.File("child")
        file = await tg.file("hello").dependency(
            "child",
            {
                "node": child,
                "options": {
                    "location": {"name": "remote"},
                    "tokens": {"remote": ["proof"]},
                    "name": "child",
                },
            },
        )
        object_ = await file.object()
        data = File.Object.to_data(object_)
        self.assertEqual(File.Object.children(object_), [await file.contents, child])
        self.assertEqual(
            File.Data.children(data),
            [
                (await file.contents).id,
                child.id,
            ],
        )
        decoded = File.Object.from_data(data)
        self.assertEqual(decoded["dependencies"]["child"].node.id, child.id)
        stripped = File.Data.without_location_and_tokens(data)
        referent = tg.Referent.from_data_string(stripped["dependencies"]["child"])
        self.assertEqual(referent.options, {"name": "child"})
        self.assertNotIn(
            "tokens",
            tg.Referent.from_data_string(data["dependencies"]["child"]).options,
        )
        pointer = tg.Graph([{"kind": "file", "contents": tg.Blob("x")}]).pointer(
            0, "file"
        )
        pointer_data = File.Object.to_data(pointer)
        self.assertEqual(
            File.Data.without_location_and_tokens(pointer_data), pointer_data
        )
        self.assertEqual(File.Data.children(pointer_data), [pointer.graph.id])


class FileCodecParityTests(unittest.TestCase):
    def test_data_children_handles_missing_contents_like_js(self):
        self.assertEqual(File.Data.children({}), [])

    def test_object_children_orders_contents_before_dependencies(self):
        from tangram.blob import Blob
        from tangram.referent import Referent

        blob = Blob("contents")
        dependency = File("dependency")
        value = {"dependencies": {"dep": Referent(dependency)}, "contents": blob}
        self.assertEqual(File.Object.children(value), [blob, dependency])

    def test_decode_preserves_graph_node_zero_dependency(self):
        from tangram.blob import Blob

        value = File.from_data(
            {"contents": Blob("contents").id, "dependencies": {"dep": {"node": 0}}}
        )
        self.assertEqual(value._value["dependencies"]["dep"].node, 0)
