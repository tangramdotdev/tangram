"""Check source object query defaults, proof metadata, and missing errors."""

import unittest
from unittest.mock import AsyncMock

from tangram.client import Client
from tangram.client.object.get import get_object, try_get_object
from tangram.error import Error
from tangram.http import Body, Response


class ObjectGetTests(unittest.IsolatedAsyncioTestCase):
    def client(self, output=None, status=200):
        client = Client()
        response = Response(status, body=Body.json(output or {}))
        response.close = AsyncMock()
        client.send_with_retry = AsyncMock(return_value=response)
        return client

    async def test_query_defaults_and_id_escaping(self):
        client = self.client({"data": {"kind": "blob", "value": {}}})
        await try_get_object(
            client, "object/with?special", {"metadata": None, "tokens": None}
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.uri.path, "/objects/object%2Fwith%3Fspecial")
        self.assertEqual(request.headers["accept"], "application/json")
        self.assertEqual(request.uri.query, "metadata=false")
        self.assertEqual(await request.body.collect(), b"")

    async def test_structured_location_and_authorization_are_serialized(self):
        client = self.client()
        await try_get_object(
            client,
            "fil_test",
            {
                "location": {"components": [{"name": "cloud", "regions": ["east"]}]},
                "metadata": True,
                "tokens": {"local": ["secret"]},
            },
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertIn("location=remote%3Acloud%28east%29", request.uri.query)
        self.assertIn("metadata=true", request.uri.query)
        self.assertIn("secret", request.uri.query)

    async def test_object_data_child_proofs_and_tokens_remain_wire_data(self):
        output = {
            "data": {"kind": "file", "value": {"contents": "blb_test"}},
            "children": {"blb_test": {"tokens": {"local": ["child-proof"]}}},
            "tokens": {"local": ["parent-proof"]},
        }
        client = self.client(output)
        self.assertEqual(await get_object(client, "fil_test"), output)

    async def test_missing_optional_returns_none_required_has_id_values(self):
        client = self.client(status=404)
        self.assertIsNone(await try_get_object(client, "fil_test"))
        client.send_with_retry.return_value.close.assert_awaited_once()
        with self.assertRaises(Error) as caught:
            await get_object(client, "fil_test")
        self.assertEqual(await caught.exception.message, "failed to find the object")
        self.assertEqual(await caught.exception.values, {"id": "fil_test"})

    async def test_status_error_remains_tangram_error(self):
        client = self.client({"message": "failed to retrieve the object"}, status=403)
        with self.assertRaises(Error) as caught:
            await try_get_object(client, "fil_test")
        self.assertEqual(
            await caught.exception.message, "failed to retrieve the object"
        )

    async def test_facade_dict_and_keyword_options(self):
        for options in ({"metadata": True}, None):
            client = self.client()
            await client.try_get_object(
                "fil_test", options, tokens={"local": ["secret"]}
            )
            request = client.send_with_retry.call_args.args[0]
            self.assertIn("secret", request.uri.query)
