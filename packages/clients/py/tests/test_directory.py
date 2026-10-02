"""Exercise directory construction and traversal against the JS source contract."""

import asyncio
import unittest

import tangram as tg
from tangram.directory import Directory


class DirectoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_builder_resolves_entries_and_merges_directories(self):
        async def entries():
            return {"nested/a": "a", "remove": "remove"}

        async def child():
            return await tg.file("b")

        builder = tg.directory(entries()).entry("nested//./b/", child())
        builder.entries({"remove": None})
        directory = await builder
        self.assertEqual(
            (await (await (await directory.get("nested/a")).contents).object())[
                "bytes"
            ],
            b"a",
        )
        self.assertEqual(
            (await (await (await directory.get("nested/b")).contents).object())[
                "bytes"
            ],
            b"b",
        )
        self.assertIsNone(await directory.try_get("remove"))
        repeated = await builder
        self.assertEqual(repeated.id, directory.id)
        merged = await tg.directory(directory, {"nested": {"c": "c"}})
        self.assertEqual(
            list(await (await merged.get("nested")).entries), ["a", "b", "c"]
        )

    async def test_directory_arguments_merge_entries_in_insertion_order(self):
        first = await tg.directory({"z": "z", "nested": {"a": "a"}})
        second = await tg.directory({"a": "a", "nested": {"b": "b"}})
        directory = await tg.directory(first, second)
        self.assertEqual([name async for name, _ in directory], ["z", "nested", "a"])
        self.assertEqual(
            [name async for name, _ in directory.walk()],
            ["z", "nested", "nested/a", "nested/b", "a"],
        )
        with self.assertRaisesRegex(ValueError, "normal"):
            await tg.directory({"./a": "a"})
        with self.assertRaisesRegex(ValueError, "normal"):
            await tg.directory({"a/../b": "b"})
        with self.assertRaisesRegex(ValueError, "without kind"):
            await tg.directory({"a": 1})

    async def test_symlinks_follow_js_terminal_artifact_behavior(self):
        target = await tg.directory({"file": "hello"})
        directory = await tg.directory(
            {
                "target": target,
                "relative": tg.Symlink("target/file"),
                "artifact": tg.Symlink(artifact=target),
                "artifact_path": tg.Symlink("file", artifact=target),
            }
        )
        self.assertEqual(
            (await (await (await directory.get("relative")).contents).object())[
                "bytes"
            ],
            b"hello",
        )
        # The JS implementation returns an artifact symlink's target immediately.
        self.assertIs(await directory.get("artifact/ignored"), target)
        self.assertEqual(
            (
                await (
                    await (await directory.get("artifact_path/ignored")).contents
                ).object()
            )["bytes"],
            b"hello",
        )
        self.assertIs(await directory.get("target/../target"), target)
        self.assertIsNone(await directory.try_get("target/file/child"))
        with self.assertRaisesRegex(ValueError, "external"):
            await directory.get("../target")
        with self.assertRaisesRegex(ValueError, "invalid path"):
            await directory.get("/target")
        self.assertIs(await directory.get("."), directory)

    async def test_branch_inherits_proofs_before_iterating(self):
        leaf = await tg.directory({"file": "hello"})
        directory = await tg.directory(
            {"children": [{"directory": leaf, "count": 1, "last": "file"}]}
        )
        directory.location = "remote"
        directory.tokens = {"remote": ["proof"]}
        artifact = await directory.get("file")
        self.assertEqual(leaf.location, "remote")
        self.assertEqual(leaf.tokens, {"remote": ["proof"]})
        self.assertEqual(artifact.location, "remote")
        self.assertEqual(artifact.tokens, {"remote": ["proof"]})

    async def test_graph_branch_and_cross_graph_pointer_resolution(self):
        external = tg.Graph([{"kind": "directory", "entries": {"a": tg.File("a")}}])
        graph = tg.Graph(
            [
                {
                    "kind": "directory",
                    "children": [
                        {"directory": 1, "count": 1, "last": "z"},
                        {
                            "directory": external.pointer(0, "directory"),
                            "count": 1,
                            "last": "a",
                        },
                    ],
                },
                {"kind": "directory", "entries": {"z": 2}},
                {"kind": "file", "contents": tg.Blob("z")},
            ]
        )
        directory = graph.pointer(0, "directory").artifact()
        directory.location = "remote"
        directory.tokens = {"remote": ["proof"]}
        self.assertEqual(list(await directory.entries), ["z", "a"])
        child = await directory.get("z")
        self.assertEqual((await (await child.contents).object())["bytes"], b"z")
        self.assertEqual(child.location, "remote")
        self.assertEqual(child.tokens, {"remote": ["proof"]})
        self.assertIsInstance(
            await Directory.resolve_edge_in_graph(1, graph), Directory
        )
        with self.assertRaisesRegex(TypeError, "missing graph"):
            await Directory.resolve_edge(1)
        with self.assertRaisesRegex(TypeError, "expected a directory"):
            await Directory.resolve_edge_in_graph(2, graph)

    async def test_data_and_object_namespaces(self):
        file = tg.File("hello")
        directory = await tg.directory({"hello": file})
        object_ = await directory.object()
        data = Directory.Object.to_data(object_)
        self.assertEqual(Directory.Object.children(object_), [file])
        self.assertEqual(Directory.Data.children(data), [file.id])
        self.assertEqual(
            Directory.Object.from_data(data)["entries"]["hello"].id, file.id
        )
        future = asyncio.get_running_loop().create_future()
        future.set_result(directory)
        self.assertIs(await Directory.new(future), directory)


if __name__ == "__main__":
    unittest.main()
