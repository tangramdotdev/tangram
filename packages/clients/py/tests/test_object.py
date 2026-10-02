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
