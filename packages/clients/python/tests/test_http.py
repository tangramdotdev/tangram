"""Shared HTTP framing regression cases."""

import json
import unittest
from pathlib import Path

from tangram.client import is_transport_error
from tangram.config import default_http2_config, validate_http2_config
from tangram.error import Error
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

    def test_http2_config_validation(self):
        validate_http2_config(default_http2_config())
        validate_http2_config(
            {"connection_window_size": 65535, "stream_window_size": 1}
        )
        cases = [
            ({"connection_window_size": 65534, "stream_window_size": 1}, "between"),
            ({"connection_window_size": 1 << 20, "stream_window_size": 0}, "between"),
            (
                {
                    "connection_window_size": 1 << 20,
                    "max_concurrent_streams": 0,
                    "stream_window_size": 1,
                },
                "greater than zero",
            ),
            (
                {"connection_window_size": 1 << 20, "stream_window_size": 1 << 20},
                "at least twice",
            ),
        ]
        for config, message in cases:
            with self.assertRaisesRegex(ValueError, message):
                validate_http2_config(config)

    def test_transport_errors(self):
        self.assertFalse(is_transport_error(ValueError("missing TANGRAM_URL")))
        self.assertTrue(is_transport_error(ConnectionRefusedError()))
        self.assertTrue(is_transport_error(TimeoutError()))
        self.assertTrue(
            is_transport_error(
                Error.from_data({"source": {"node": {"kind": "unavailable"}}})
            )
        )
        self.assertFalse(is_transport_error(Error.from_data({"kind": "internal"})))
