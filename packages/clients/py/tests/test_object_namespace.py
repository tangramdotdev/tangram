import asyncio
import unittest

from helpers import ObjectTestCase
from test_authorization import token

import tangram as tg


class ObjectNamespaceTests(ObjectTestCase):
    async def test_object_and_data_round_trip(self):
        blob = await tg.Blob.new("hello")
        object_ = blob.state.object
        self.assertEqual(tg.Object.Object.to_data(object_), blob.to_data())
        self.assertEqual(tg.Object.Object.from_data(blob.to_data()), object_)
        file = await tg.File.new(blob)
        self.assertEqual(tg.Object.Data.children(file.to_data()), [blob.id])
        self.assertEqual(tg.Object.Id.kind(blob.id), "blob")

    async def test_store_batches_and_reuses_shared_objects(self):
        blob = await tg.Blob.new("hello")
        file = await tg.File.new(blob)
        identifiers = await asyncio.gather(file.store(), file.store())
        self.assertEqual(identifiers, [file.id, file.id])
        self.assertEqual(set(self.object_client.objects), {file.id, blob.id})
        self.assertTrue(file.state.stored)
        self.assertTrue(blob.state.stored)
        file.unload()
        self.assertIsNone(file.state.object)
        self.assertEqual(await file.text(), "hello")

    async def test_state_copies_location_and_tokens(self):
        blob = await tg.Blob.new("hello")
        location = {"remote": "example"}
        blob.state.location = location
        location["remote"] = "changed"
        self.assertEqual(blob.state.location, {"remote": "example"})
        returned = blob.state.location
        returned["remote"] = "changed again"
        self.assertEqual(blob.state.location, {"remote": "example"})
        tokens = {"local": ["authorization"]}
        blob.state.tokens = tokens
        tokens["local"].clear()
        self.assertEqual(blob.state.tokens, {"local": ["authorization"]})
        blob.state.tokens["local"].clear()
        self.assertEqual(blob.state.tokens, {"local": ["authorization"]})

    async def test_state_store_promise_and_finish_validation(self):
        blob = await tg.Blob.new("hello")
        task = asyncio.create_task(asyncio.sleep(0))
        blob.state.start_store_promise(task)
        with self.assertRaisesRegex(ValueError, "cannot start"):
            blob.state.start_store_promise(task)
        with self.assertRaisesRegex(ValueError, "invalid object batch output"):
            blob.state.finish_store(tg.Referent("blb_invalid"))
        blob.state.clear_store_promise(task)
        self.assertIsNone(blob.state.store_promise)
        await task
        blob.state.finish_store(tg.Referent(blob.id))
        self.assertTrue(blob.state.stored)

    async def test_unstored_unload_is_noop_and_state_uses_loaded_object(self):
        blob = await tg.Blob.new("hello")
        blob.unload()
        self.assertIsNotNone(blob.state.object)
        state = tg.Object.State({"object": blob.state.object, "stored": False})
        self.assertEqual(state.id, blob.id)
        self.assertEqual(state.kind, "blob")

    async def test_collect_tokens_skips_covered_children_but_keeps_sync(self):
        blob = await tg.Blob.new("hello")
        file = await tg.File.new(blob)
        root_proof = token(file.id, ["object_subtree"])
        child_proof = token(blob.id, ["object_node"])
        sync_proof = token("syn_one", ["sync_read"])
        file.state.tokens = {"local": [root_proof]}
        blob.state.tokens = {"local": [child_proof, sync_proof]}
        self.assertEqual(
            file.state.collect_tokens(), {"local": sorted([root_proof, sync_proof])}
        )
        self.assertEqual(tg.Object.kind(file), "file")

    async def test_file_proofs_are_removed_before_hashing(self):
        blob = await tg.Blob.new("hello")
        file = await tg.File.new(
            {
                "contents": blob,
                "dependencies": {
                    "dep": tg.Referent(blob, {"tokens": {"local": ["fake proof"]}})
                },
            }
        )
        cleaned = tg.Object.Data.without_location_and_tokens(file.to_data())
        self.assertNotIn("tokens", cleaned["value"]["dependencies"]["dep"])
        self.assertEqual(file.id, tg.host.object_id(cleaned))


if __name__ == "__main__":
    unittest.main()
