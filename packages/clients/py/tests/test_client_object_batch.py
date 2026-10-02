"""Check object batch ordering, optional children, locations, and proof codecs."""

import unittest
from unittest.mock import AsyncMock

from tangram.client import Client
from tangram.client.object.batch import post_object_batch
from tangram.error import Error
from tangram.http import Body, Response
from tangram.referent import Referent


class ObjectBatchTests(unittest.IsolatedAsyncioTestCase):
    def client(self, output):
        client = Client()
        client.send_with_retry = AsyncMock(
            return_value=Response(200, body=Body.json(output))
        )
        return client

    async def test_wire_order_children_and_output_proofs(self):
        child = Referent("blb_child", {"tokens": {"local": ["proof"]}})
        objects = [
            {
                "id": "blb_child",
                "data": {"kind": "blob", "value": {"kind": "leaf", "bytes": "YWJj"}},
            },
            {
                "id": "fil_parent",
                "children": [child],
                "data": {"kind": "file", "value": {"contents": "blb_child"}},
            },
        ]
        client = self.client(
            {
                "objects": [
                    "blb_child?tokens[local][0]=a",
                    "fil_parent?tokens[local][0]=b",
                ]
            }
        )
        output = await post_object_batch(
            client,
            {
                "objects": objects,
                "location": {"components": [{"name": "cloud", "regions": ["east"]}]},
            },
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.uri), "/objects/batch")
        self.assertEqual(request.headers["accept"], "application/json")
        self.assertEqual(request.headers["content-type"], "application/json")
        data = await request.body.json()
        self.assertEqual(
            [object_["id"] for object_ in data["objects"]], ["blb_child", "fil_parent"]
        )
        self.assertEqual(data["location"], "remote:cloud(east)")
        self.assertNotIn("children", data["objects"][0])
        self.assertEqual(data["objects"][1]["children"], [child.to_data_string()])
        self.assertIs(objects[1]["children"][0], child)
        self.assertEqual(
            [object_.node for object_ in output["objects"]], ["blb_child", "fil_parent"]
        )
        self.assertEqual(output["objects"][1].options, {"tokens": {"local": ["b"]}})

    async def test_null_children_are_omitted_empty_children_are_preserved(self):
        client = self.client({"objects": []})
        await post_object_batch(
            client,
            [
                {"id": "one", "children": None, "data": {}},
                {"id": "two", "children": [], "data": {}},
            ],
        )
        data = await client.send_with_retry.call_args.args[0].body.json()
        self.assertIsNone(data["location"])
        self.assertNotIn("children", data["objects"][0])
        self.assertEqual(data["objects"][1]["children"], [])

    async def test_facade_dictionary_and_list_overloads(self):
        for arg in ({"objects": []}, []):
            client = self.client({"objects": []})
            self.assertEqual(await client.post_object_batch(arg), {"objects": []})
            self.assertEqual(
                await client.send_with_retry.call_args.args[0].body.json(),
                {"objects": [], "location": None},
            )

    async def test_status_errors_preserve_source(self):
        client = Client()
        client.send_with_retry = AsyncMock(
            return_value=Response(
                409, body=Body.json({"message": "failed to store the batch"})
            )
        )
        with self.assertRaises(Error) as caught:
            await post_object_batch(client, [])
        self.assertEqual(await caught.exception.message, "failed to store the batch")
