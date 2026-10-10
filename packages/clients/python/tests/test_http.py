"""Shared HTTP framing regression cases."""

import json
import unittest
from pathlib import Path

from tangram.http import Body, Headers


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_sse_decoding(self):
        fixtures = Path(__file__).parents[3] / "http/fixtures/sse.json"
        for fixture in json.loads(fixtures.read_text()):
            for size in (1, 2, 128):

                async def chunks():
                    data = fixture["input"].encode()
                    for offset in range(0, len(data), size):
                        yield data[offset : offset + size]

                events = [event async for event in Body(chunks()).sse()]
                self.assertEqual(events, fixture["events"])

    def test_header_names(self):
        headers = Headers({"Authorization": "explicit"})
        self.assertEqual(headers.get("AUTHORIZATION"), "explicit")
        headers["Content-Type"] = "application/json"
        self.assertEqual(headers.get("content-type"), "application/json")
