import asyncio
import unittest

import tangram as tg
from tangram.referent import Referent


class BatchClient:
    def __init__(self):
        self.batches = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.invalid = False

    async def post_object_batch(self, objects):
        self.batches.append(objects)
        self.started.set()
        await self.release.wait()
        nodes = [Referent(object_["id"]) for object_ in objects]
        if self.invalid:
            nodes[-1] = Referent(nodes[0].node)
        return {"objects": nodes}


class ValueNamespaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_store_batches_children_first_and_deduplicates_equal_ids(self):
        first = await tg.blob("same")
        second = await tg.blob("same")
        file = await tg.file(first)
        client = BatchClient()
        self.assertIsNone(await tg.Value.store([first, second, file], client))
        self.assertEqual(len(client.batches), 1)
        self.assertEqual(len(client.batches[0]), 2)
        self.assertEqual(client.batches[0][0]["id"], first.id)
        self.assertEqual(client.batches[0][1]["id"], file.id)
        self.assertTrue(all(object_.state.stored for object_ in (first, second, file)))
        await tg.Value.store(file, client)
        self.assertEqual(len(client.batches), 1)

    async def test_overlapping_store_waits_for_claimed_states(self):
        blob = await tg.blob("shared")
        client = BatchClient()
        client.release.clear()
        first = asyncio.create_task(tg.Value.store(blob, client))
        await client.started.wait()
        second = asyncio.create_task(tg.Value.store(blob, client))
        await asyncio.sleep(0)
        self.assertEqual(len(client.batches), 1)
        client.release.set()
        await asyncio.gather(first, second)
        self.assertTrue(blob.state.stored)
        self.assertIsNone(blob.state.store_promise)

    async def test_invalid_batch_response_does_not_mark_any_state_stored(self):
        first = await tg.blob("first")
        second = await tg.blob("second")
        client = BatchClient()
        client.invalid = True
        with self.assertRaisesRegex(ValueError, "invalid object batch output"):
            await tg.Value.store([first, second], client)
        self.assertFalse(first.state.stored)
        self.assertFalse(second.state.stored)

    async def test_data_children_and_strip_proofs_preserve_resolution_options(self):
        blob = await tg.blob("content")
        reference = Referent(
            blob.id,
            {
                "location": {},
                "path": "a",
                "tokens": {"local": ["token"]},
            },
        )
        object_data = {"kind": "object", "value": reference.to_data_string()}
        template_data = {
            "kind": "template",
            "value": {
                "components": [
                    {"kind": "artifact", "value": reference.to_data_string()}
                ]
            },
        }
        mutation_data = {
            "kind": "mutation",
            "value": {"kind": "append", "values": [object_data, template_data]},
        }
        data = {
            "kind": "map",
            "value": {"mutation": mutation_data, "objects": [object_data]},
        }
        self.assertEqual(tg.Value.Data.children(data), [blob.id, blob.id, blob.id])
        stripped = tg.Value.Data.without_location_and_tokens(data)
        self.assertEqual(tg.Value.Data.children(stripped), [blob.id, blob.id, blob.id])
        stripped_ref = Referent.from_data_string(
            stripped["value"]["objects"][0]["value"]
        )
        self.assertEqual(stripped_ref.options, {"path": "a"})
        self.assertIn("tokens", reference.options)

    async def test_value_map_serialization_preserves_insertion_order(self):
        data = tg.Value.to_data({"z": 1, "a": 2})
        self.assertEqual(list(data["value"]), ["z", "a"])
        self.assertEqual(tg.Value.from_data(data), {"z": 1, "a": 2})
        with self.assertRaises(AssertionError):
            tg.Value.assert_(object())
        self.assertIsNone(tg.Value.assert_(None))
