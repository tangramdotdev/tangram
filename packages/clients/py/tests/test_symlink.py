import asyncio

from helpers import ObjectTestCase

import tangram as tg


class SymlinkTests(ObjectTestCase):
    async def test_args_and_builders_resolve_nested_futures(self):
        file = await tg.file("contents")
        link = (
            await tg.symlink(asyncio.sleep(0, result="old"))
            .artifact(asyncio.sleep(0, result=file))
            .path(asyncio.sleep(0, result=None))
        )
        self.assertIs(await link.artifact, file)
        self.assertIsNone(await link.path)
        self.assertIs(await tg.Symlink.new(link), link)
        self.assertEqual(
            await tg.Symlink.arg("first", {"path": "second"}), {"path": "second"}
        )
        self.assertIs((await tg.Symlink.arg_resolved(link))["artifact"], link)

    async def test_templates_and_invalid_templates(self):
        file = await tg.file("contents")
        self.assertEqual(await (await tg.symlink(tg.Template(["hello"]))).path, "hello")
        link = await tg.symlink(tg.Template([file, "/hello"]))
        self.assertIs(await link.artifact, file)
        self.assertEqual(await link.path, "hello")
        with self.assertRaises(AssertionError):
            await tg.symlink(tg.Template([file, "hello"]))
        with self.assertRaises(AssertionError):
            await tg.symlink(tg.Template([file, "/a", "b"]))
        with self.assertRaisesRegex(ValueError, "invalid template"):
            await tg.symlink(tg.Template([]))

    async def test_graph_pointer_and_child_proofs(self):
        file = await tg.file("contents")
        graph = await tg.graph(
            {
                "nodes": [
                    {"kind": "file", "contents": "contents"},
                    {"kind": "symlink", "artifact": 0, "path": None},
                ]
            }
        )
        link = await tg.symlink({"graph": graph, "index": 1, "kind": "symlink"})
        link.state.location = tg.Location.from_data_string("local")
        target = await link.artifact
        self.assertIsInstance(target, tg.File)
        self.assertEqual(target.state.location, link.state.location)
        self.assertIsNone(await link.path)
        self.assertEqual(await link.children, [graph])
        with self.assertRaisesRegex(ValueError, "cannot merge a graph pointer"):
            await tg.Symlink.arg_resolved(graph.pointer(1, "symlink"), {"path": "x"})
        inline = tg.Symlink.with_object({"artifact": file, "path": None})
        self.assertEqual(await inline.children, [file])

    async def test_object_and_data_conversion(self):
        file = await tg.file("contents")
        object_ = {"artifact": file, "path": "child"}
        data = tg.Symlink.Object.to_data(object_)
        self.assertEqual(data, {"artifact": file.id, "path": "child"})
        decoded = tg.Symlink.Object.from_data(data)
        self.assertEqual(decoded["artifact"].id, file.id)
        self.assertEqual(tg.Symlink.Data.children(data), [file.id])
        link = tg.Symlink.from_data(data)
        self.assertEqual(await link.path, "child")
        self.assertEqual(tg.Symlink.Object.to_data(await link.object()), data)

    async def test_resolve_matches_source_cases(self):
        file = await tg.file("contents")
        directory = await tg.directory({"child": file})
        direct = await tg.symlink(file)
        self.assertIs(await direct.resolve(), file)
        nested = await tg.symlink(direct)
        self.assertIs(await nested.resolve(), file)
        link = await tg.symlink({"artifact": directory, "path": "child"})
        self.assertIs(await link.resolve(), file)
        missing = await tg.symlink({"artifact": directory, "path": "missing"})
        self.assertIsNone(await missing.resolve())
        with self.assertRaisesRegex(
            ValueError, "cannot resolve a symlink with no artifact"
        ):
            await (await tg.symlink("relative")).resolve()
        with self.assertRaisesRegex(ValueError, "invalid symlink"):
            await (await tg.symlink()).resolve()
        with self.assertRaisesRegex(ValueError, "invalid symlink"):
            await (await tg.symlink({"artifact": file, "path": "child"})).resolve()

    async def test_constructor_and_assertions(self):
        link = tg.Symlink(
            {"object": {"artifact": None, "path": "hello"}, "stored": False}
        )
        self.assertEqual(await link.path, "hello")
        self.assertIs(tg.Symlink.expect(link), link)
        tg.Symlink.assert_(link)
        with self.assertRaises(AssertionError):
            tg.Symlink.expect(None)
