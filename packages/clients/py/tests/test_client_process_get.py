"""Check process retrieval requests and response decoding."""

import unittest
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs

from tangram.client.process.get import get_process, try_get_process
from tangram.error import Error
from tangram.http import Body, Response


class ProcessGetTests(unittest.IsolatedAsyncioTestCase):
    def client(self, status=200, output=None, headers=None):
        client = Mock()
        response = Response(status, headers, Body.json(output or {}))
        client.send_with_retry = AsyncMock(return_value=response)
        return client

    async def test_default_request_and_percent_encoded_id(self):
        client = self.client(output={"id": "process", "data": {}})
        self.assertEqual(
            await get_process(client, "process /?"), {"id": "process", "data": {}}
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.uri.path, "/processes/process%20%2F%3F")
        self.assertEqual(request.headers.get("accept"), "application/json")
        self.assertEqual(
            parse_qs(request.uri.query), {"metadata": ["false"], "source": ["auto"]}
        )

    async def test_arg_locations_tokens_and_nullish_defaults(self):
        client = self.client()
        await try_get_process(
            client,
            "process",
            {
                "location": {"components": [{"name": "upstream", "regions": ["west"]}]},
                "metadata": True,
                "source": "remote",
                "tokens": {"token": True},
            },
        )
        request = client.send_with_retry.call_args.args[0]
        query = parse_qs(request.uri.query)
        self.assertEqual(query["location"], ["remote:upstream(west)"])
        self.assertEqual(query["metadata"], ["true"])
        self.assertEqual(query["source"], ["remote"])
        self.assertEqual(query["tokens[token]"], ["true"])
        await try_get_process(
            client, "process", {"metadata": None, "source": None, "tokens": None}
        )
        query = parse_qs(client.send_with_retry.call_args.args[0].uri.query)
        self.assertEqual(query, {"metadata": ["false"], "source": ["auto"]})

    async def test_missing_process_and_failed_response(self):
        client = self.client(404)
        self.assertIsNone(await try_get_process(client, "missing"))
        with self.assertRaisesRegex(ValueError, "failed to find the process"):
            await get_process(client, "missing")
        client = self.client(503, {"message": "unavailable"})
        with self.assertRaises(Error) as caught:
            await try_get_process(client, "process")
        self.assertEqual(await caught.exception.message(), "unavailable")

    async def test_location_and_metadata_header_conversion(self):
        client = self.client(
            output={
                "id": "process",
                "data": {},
                "location": "remote:upstream(west)",
                "metadata": "body",
            },
            headers={"x-tg-process-metadata": '{"progress": 3}'},
        )
        output = await try_get_process(client, "process")
        self.assertEqual(output["location"], {"name": "upstream", "region": "west"})
        self.assertEqual(output["metadata"], {"progress": 3})
        client = self.client(
            output={"location": {"region": "west"}, "metadata": "body"}
        )
        self.assertEqual(
            await try_get_process(client, "process"),
            {"location": {"region": "west"}, "metadata": "body"},
        )
