"""Check shared object loading and graph pointer resolution."""

import asyncio
import unittest

from helpers import ObjectTestCase

import tangram as tg


class ObjectTests(ObjectTestCase):
    async def test_canceling_one_waiter_does_not_cancel_a_shared_load(self):
        blob = tg.Blob("hello")
        handle = tg.Blob.with_id(blob.id)
        ready, release = asyncio.Event(), asyncio.Event()

        class Client:
            calls = 0

            async def get_object(self, *_args, **_kwargs):
                self.calls += 1
                ready.set()
                await release.wait()
                return {"data": blob.to_data(), "tokens": {}, "children": {}}

        client = Client()
        canceled = asyncio.create_task(handle.load(client))
        await ready.wait()
        retained = asyncio.create_task(handle.load(client))
        canceled.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await canceled
        release.set()
        self.assertEqual(await retained, {"bytes": b"hello"})
        self.assertEqual(client.calls, 1)

    async def test_load_inherits_parent_and_child_response_tokens(self):
        contents = tg.Blob("hello")
        file = tg.File(contents)
        handle = tg.File.with_id(file.id)

        class Client:
            calls = 0

            async def get_object(self, *_args, **_kwargs):
                self.calls += 1
                return {
                    "data": file.to_data(),
                    "tokens": {"local": ["parent"]},
                    "children": {contents.id: {"tokens": {"local": ["child"]}}},
                }

        client = Client()
        object = await handle.object(client)
        self.assertEqual(object["contents"].tokens, {"local": ["child", "parent"]})
        handle.state.inherit_tokens({"remote": ["updated"]})
        await handle.load(client)
        self.assertEqual(object["contents"].tokens["remote"], ["updated"])
        self.assertEqual(client.calls, 1)

    async def test_in_memory_load_inherits_tokens(self):
        child = tg.File("hello")
        parent = tg.Directory({"hello": child})
        parent.state.tokens = {"local": ["proof"]}
        await parent.object()
        self.assertEqual(child.tokens, parent.tokens)

    async def test_pointer_load_inherits_graph_location(self):
        for kind in ("directory", "file", "symlink"):
            graph = tg.Graph.with_id("gph_test")
            artifact = graph.pointer(0, kind).artifact()
            artifact.state.location = {"remote": "cloud", "region": "east"}
            await artifact.object()
            self.assertEqual(graph.state.location, artifact.state.location)
            artifact.state.location = {"remote": "other"}
            await artifact.load()
            self.assertEqual(graph.state.location["remote"], "cloud")

    async def test_cyclic_graph(self):
        graph = tg.Graph(
            [
                {"kind": "directory", "entries": {"self": 0, "hello": 1}},
                {"kind": "file", "contents": tg.Blob("hello")},
            ]
        )
        directory = graph.pointer(0, "directory").artifact()
        self.assertEqual(await (await directory.get("self/self/hello")).text(), "hello")


if __name__ == "__main__":
    unittest.main()
