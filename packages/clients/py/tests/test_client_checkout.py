"""Verify checkout defaults, artifact proofs, and progress decoding."""

import unittest
from unittest.mock import AsyncMock

from tangram.client import Client
from tangram.client.checkout import Checkout, checkout
from tangram.error import Error
from tangram.http import Body, Response
from tangram.referent import Referent


class CheckoutCodecTests(unittest.TestCase):
    def test_default_options_are_omitted(self):
        self.assertEqual(
            Checkout.Arg.to_json(
                {
                    "nodes": [],
                    "dependencies": True,
                    "force": False,
                    "lock": "auto",
                    "ignored": True,
                }
            ),
            {"nodes": []},
        )

    def test_optional_false_and_null_fields_are_preserved(self):
        self.assertEqual(
            Checkout.Arg.to_json(
                {
                    "nodes": [Referent("fil_test")],
                    "dependencies": False,
                    "force": True,
                    "extension": None,
                    "lock": None,
                    "path": None,
                }
            ),
            {
                "nodes": ["fil_test"],
                "dependencies": False,
                "force": True,
                "extension": None,
                "lock": None,
                "path": None,
            },
        )
        self.assertEqual(
            Checkout.Arg.to_json({"nodes": [], "lock": "file"})["lock"], "file"
        )

    def test_nodes_preserve_resolution_options_and_tokens(self):
        node = Referent(
            "fil_test", {"name": "artifact", "tokens": {"local": ["secret"]}}
        )
        output = Checkout.Arg.to_json({"nodes": [node]})
        self.assertEqual(Referent.from_data_string(output["nodes"][0]), node)
        self.assertEqual(
            node.options, {"name": "artifact", "tokens": {"local": ["secret"]}}
        )


class CheckoutEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_and_progress(self):
        async def events():
            yield {"event": "log", "data": '{"message":"checking out"}'}
            yield {"event": "output", "data": '{"paths":["/one","/two"]}'}

        client = Client()
        client.send_with_retry = AsyncMock(
            return_value=Response(200, body=Body.sse(events()))
        )
        stream = await checkout(
            client,
            {
                "nodes": [Referent("fil_one"), Referent("fil_two")],
                "dependencies": False,
            },
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.uri), "/checkout")
        self.assertEqual(request.headers["accept"], "text/event-stream")
        self.assertEqual(request.headers["content-type"], "application/json")
        self.assertEqual(
            await request.body.json(),
            {"nodes": ["fil_one", "fil_two"], "dependencies": False},
        )
        output = [event async for event in stream]
        self.assertEqual(output[1]["value"], {"paths": ["/one", "/two"]})

    async def test_facade_dictionary_and_list_overloads_share_codec(self):
        for arg in ({"nodes": [Referent("fil_test")]}, [Referent("fil_test")]):
            client = Client()
            client.send_with_retry = AsyncMock(return_value=Response(200, body=b""))
            stream = await client.checkout(arg, lock="auto", force=False)
            request = client.send_with_retry.call_args.args[0]
            self.assertEqual(await request.body.json(), {"nodes": ["fil_test"]})
            await stream.aclose()

    async def test_status_error_is_preserved(self):
        client = Client()
        client.send_with_retry = AsyncMock(
            return_value=Response(
                409, body=Body.json({"message": "failed to check out"})
            )
        )
        with self.assertRaises(Error) as caught:
            await checkout(client, {"nodes": []})
        self.assertEqual(await caught.exception.message, "failed to check out")
