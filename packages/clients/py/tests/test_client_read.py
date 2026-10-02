"""Check blob read framing and source request behavior."""

import unittest
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs

from tangram.client import Client
from tangram.client.read import collect_read_stream, read, try_read, try_read_stream
from tangram.error import Error
from tangram.http import Body, Response


class ClientReadTests(unittest.IsolatedAsyncioTestCase):
    def client(self, response):
        client = Mock()
        client.send_with_retry = AsyncMock(return_value=response)
        return client

    async def test_source_request_fields_and_facade_kwargs(self):
        response = Response(200, {"x-tg-position": "12"}, b"abc")
        client = self.client(response)
        self.assertEqual(
            await read(
                client,
                {
                    "blob": "blob_id",
                    "position": "end",
                    "length": -3,
                    "size": 10,
                    "tokens": {"token": True},
                    "extra": True,
                },
            ),
            b"abc",
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.uri.path, "/read")
        self.assertEqual(
            request.headers.to_data(), {"accept": "application/octet-stream"}
        )
        self.assertEqual(
            parse_qs(request.uri.query),
            {
                "blob": ["blob_id"],
                "position": ["end"],
                "length": ["-3"],
                "size": ["10"],
                "tokens[token]": ["true"],
            },
        )
        client = Client()
        client.send_with_retry = AsyncMock(
            return_value=Response(200, {"x-tg-position": "0"}, b"x")
        )
        self.assertEqual(await client.read({"blob": "blob_id", "tokens": None}), b"x")
        self.assertEqual(
            parse_qs(client.send_with_retry.call_args.args[0].uri.query),
            {"blob": ["blob_id"]},
        )

    async def test_positions_advance_with_each_chunk_and_end(self):
        async def chunks():
            yield b"ab"
            yield b"c"

        stream = await try_read_stream(
            self.client(Response(200, {"x-tg-position": "1e1"}, chunks())), "blob_id"
        )
        self.assertEqual(
            [event async for event in stream],
            [
                {"kind": "chunk", "value": {"bytes": b"ab", "position": 10}},
                {"kind": "chunk", "value": {"bytes": b"c", "position": 12}},
                {"kind": "end"},
            ],
        )

    async def test_integer_header_uses_javascript_number_semantics(self):
        for header, position in [("", 0), (" 2.0 ", 2), ("0x10", 16), ("-1", -1)]:
            stream = await try_read_stream(
                self.client(Response(200, {"x-tg-position": header}, b"x")), "id"
            )
            self.assertEqual((await anext(stream))["value"]["position"], position)
            await stream.aclose()
        for header in ["1.5", "NaN", "Infinity", "invalid", "1_0", "١"]:
            stream = await try_read_stream(
                self.client(Response(200, {"x-tg-position": header})), "id"
            )
            with self.assertRaisesRegex(ValueError, "expected an integer"):
                await anext(stream)

    async def test_absent_position_is_allowed_for_empty_body_only(self):
        self.assertEqual(await read(self.client(Response(200)), "id"), b"")
        with self.assertRaisesRegex(ValueError, "expected a position"):
            await read(self.client(Response(200, body=b"x")), "id")

    async def test_missing_and_error_responses(self):
        response = Response(404)
        response.collect = AsyncMock(side_effect=AssertionError("must not collect"))
        client = self.client(response)
        self.assertIsNone(await try_read_stream(client, "id"))
        self.assertIsNone(await try_read(client, "id"))
        with self.assertRaisesRegex(ValueError, "failed to find the blob"):
            await read(client, "id")
        response.collect.assert_not_awaited()
        with self.assertRaises(Error) as caught:
            await read(
                self.client(Response(503, body=Body.json({"message": "failure"}))), "id"
            )
        self.assertEqual(await caught.exception.message(), "failure")

    async def test_collect_stops_at_end_and_closes_iterator(self):
        closed = []

        async def events():
            try:
                yield {"kind": "chunk", "value": {"bytes": b"before", "position": 0}}
                yield {"kind": "end"}
                raise AssertionError("must stop at end")
            finally:
                closed.append(True)

        self.assertEqual(await collect_read_stream(events()), b"before")
        self.assertEqual(closed, [True])
