"""Verify object JSON data, authorization children, framing, and result proofs."""

import json
import unittest
from unittest.mock import AsyncMock

from tangram.client import Client
from tangram.client.object.put import put_object
from tangram.error import Error
from tangram.http import Body, Response
from tangram.referent import Referent


class ObjectPutTests(unittest.IsolatedAsyncioTestCase):
    def client(self):
        client = Client()
        client.send_with_retry = AsyncMock(
            return_value=Response(
                200, body=Body.json({"object": "fil_test?tokens[local][0]=proof"})
            )
        )
        return client

    async def test_request_data_and_output_proofs(self):
        data = {"kind": "file", "value": {"contents": "blb_test"}}
        client = self.client()
        output = await put_object(
            client,
            "object/with?special",
            {
                "data": data,
                "location": {"components": [{"name": "cloud", "regions": ["east"]}]},
                "children": [Referent("blb_test", {"name": "contents"})],
            },
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "PUT")
        self.assertEqual(request.uri.path, "/objects/object%2Fwith%3Fspecial")
        self.assertEqual(request.headers["accept"], "application/json")
        self.assertEqual(request.headers["content-type"], "application/json")
        self.assertEqual(await request.body.json(), data)
        self.assertIn("location=remote%3Acloud%28east%29", request.uri.query)
        self.assertIn("contents", request.uri.query)
        self.assertEqual(output["object"].node, "fil_test")
        self.assertEqual(output["object"].options, {"tokens": {"local": ["proof"]}})

    async def test_missing_and_null_children_have_same_source_defaults(self):
        for children in (None, []):
            client = self.client()
            await put_object(client, "fil_test", {}, children=children)
            request = client.send_with_retry.call_args.args[0]
            self.assertEqual(request.uri.query, "")
            self.assertEqual(await request.body.json(), {})

    async def test_large_child_proofs_are_framed_before_object_data(self):
        data = {"kind": "file", "value": {"contents": "blb_test"}}
        child = Referent("blb_test", {"tokens": {"local": ["x" * 5000]}})
        client = self.client()
        await put_object(client, "fil_test", data, children=[child])
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.headers["x-tg-arg-in-body"], "true")
        self.assertIsNone(request.uri.query)
        body = await request.body.collect()
        length, offset, shift = 0, 0, 0
        while True:
            byte = body[offset]
            offset += 1
            length |= (byte & 127) << shift
            if byte < 128:
                break
            shift += 7
        arg = json.loads(body[offset : offset + length])
        self.assertEqual(arg, {"children": [child.to_data_string()], "location": None})
        self.assertEqual(json.loads(body[offset + length :]), data)

    async def test_facade_source_and_data_overloads(self):
        data = {"kind": "blob", "value": {"kind": "leaf", "bytes": ""}}
        for arg in ({"data": data}, data):
            client = self.client()
            await client.put_object("blb_test", arg)
            request = client.send_with_retry.call_args.args[0]
            self.assertEqual(await request.body.json(), data)

    async def test_status_error_preserves_tangram_error(self):
        client = self.client()
        client.send_with_retry.return_value = Response(
            403, body=Body.json({"message": "failed to put the object"})
        )
        with self.assertRaises(Error) as caught:
            await put_object(client, "fil_test", {})
        self.assertEqual(await caught.exception.message, "failed to put the object")
