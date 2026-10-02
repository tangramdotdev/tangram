"""Verify process connection wire codecs and server message decoding."""

import json
import unittest
from unittest.mock import AsyncMock

from tangram.client import Client
from tangram.client.process.connect import (
    connect_arg,
    connect_process,
    encode,
    request_window,
)
from tangram.error import Error
from tangram.http import Body, Response
from tangram.referent import Referent


async def input(values):
    for value in values:
        yield value


class ConnectCodecTests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_and_read_locations_serialize_independently(self):
        location = {"components": [{"name": "cloud", "regions": ["east"]}]}
        arg = {
            "kind": "connect",
            "value": {
                "mode": "run",
                "process": {"command": Referent("cmd_test"), "public": False},
                "reads": {
                    1: {"streams": ["stdout", "stderr"], "location": location},
                    2: {"streams": ["stdout"]},
                },
            },
        }
        data = connect_arg(arg)
        self.assertIsNone(data["value"]["location"])
        self.assertEqual(data["value"]["process"], {"command": "cmd_test"})
        self.assertEqual(
            data["value"]["reads"],
            {
                "1": {"streams": "stdout,stderr", "location": "remote:cloud(east)"},
                "2": {"streams": "stdout", "location": None},
            },
        )
        self.assertEqual(arg["value"]["reads"][1]["streams"], ["stdout", "stderr"])
        self.assertEqual(
            connect_arg(
                {"kind": "read", "value": {"streams": ["stdout"], "position": 7}}
            ),
            {
                "kind": "read",
                "value": {"streams": "stdout", "position": 7, "location": None},
            },
        )

    async def test_write_control_and_receipt_encoding(self):
        chunk = {
            "bytes": b"abc",
            "combined_position": 0,
            "stream_position": 0,
            "stream": "stdin",
        }
        write = connect_arg(
            {"kind": "write", "value": {"data": {"kind": "chunk", "value": chunk}}}
        )
        self.assertEqual(write["value"]["data"]["value"]["bytes"], "YWJj")
        self.assertIsNone(write["value"]["location"])
        self.assertEqual(chunk["bytes"], b"abc")
        for kind in ("cancel", "signal", "tty"):
            self.assertEqual(
                connect_arg({"kind": kind, "value": {"tokens": {}}}),
                {"kind": kind, "value": {"tokens": {}, "location": None}},
            )
        events = [
            event
            async for event in encode(
                input(
                    [
                        {"kind": "ack", "value": {"id": 1}},
                        {
                            "kind": "notification",
                            "value": {
                                "kind": "read",
                                "value": {"id": 2, "progress": {"length": 7}},
                            },
                        },
                        {
                            "kind": "request",
                            "value": {"id": 3, "arg": {"kind": "detach"}},
                        },
                    ]
                )
            )
        ]
        self.assertEqual(json.loads(events[0]["data"]), {"id": 1})
        self.assertEqual(json.loads(events[2]["data"])["arg"], {"kind": "detach"})
        self.assertEqual(request_window, 128)


class ConnectEndpointTests(unittest.IsolatedAsyncioTestCase):
    def client(self, events, *, content_type="text/event-stream; charset=utf-8"):
        response = Response(
            200, {"content-type": content_type}, Body.sse(input(events))
        )
        client = Client()
        client.send = AsyncMock(return_value=response)
        client.send_with_retry = AsyncMock()
        return client

    async def test_request_is_duplex_and_not_retried(self):
        client = self.client([])
        stream = await connect_process(
            client,
            input(
                [
                    {
                        "kind": "request",
                        "value": {
                            "id": 0,
                            "arg": {
                                "kind": "connect",
                                "value": {
                                    "mode": "run",
                                    "process": "prc_test",
                                    "reads": {},
                                },
                            },
                        },
                    },
                ]
            ),
        )
        request = client.send.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.uri), "/processes/connect")
        self.assertEqual(request.headers["accept"], "text/event-stream")
        self.assertEqual(request.headers["content-type"], "text/event-stream")
        events = [event async for event in request.body.sse()]
        self.assertIsNone(json.loads(events[0]["data"])["arg"]["value"]["location"])
        client.send_with_retry.assert_not_awaited()
        await stream.aclose()

    async def test_connect_read_and_chunk_server_outputs_are_decoded(self):
        values = [
            (
                "response",
                {
                    "id": 0,
                    "error": None,
                    "output": {
                        "kind": "connect",
                        "value": {"process": "prc_test", "location": "local"},
                    },
                },
            ),
            (
                "response",
                {
                    "id": 1,
                    "error": None,
                    "output": {
                        "kind": "read",
                        "value": {
                            "kind": "end",
                            "value": {
                                "combined_position": 7,
                                "stream_positions": {"stdout": 7},
                                "ignored": True,
                            },
                        },
                    },
                },
            ),
            (
                "notification",
                {
                    "kind": "read",
                    "value": {
                        "id": 1,
                        "event": {
                            "kind": "chunk",
                            "value": {
                                "bytes": "YWJj",
                                "stream": "stdout",
                                "combined_position": 0,
                                "stream_position": 0,
                            },
                        },
                    },
                },
            ),
            ("notification", {"kind": "outcome", "value": {"exit": 0}}),
            ("ack", {"id": 7}),
        ]
        client = self.client(
            [{"event": kind, "data": json.dumps(value)} for kind, value in values]
        )
        stream = await connect_process(client, input([]))
        messages = [message async for message in stream]
        self.assertEqual(messages[0]["value"]["output"]["value"]["location"], {})
        self.assertNotIn("ignored", messages[1]["value"]["output"]["value"]["value"])
        self.assertEqual(
            messages[2]["value"]["value"]["event"]["value"]["bytes"], b"abc"
        )
        self.assertEqual(messages[3]["value"], values[3][1])
        self.assertEqual(messages[4], {"kind": "ack", "value": {"id": 7}})

    async def test_invalid_event_is_rejected_before_parsing_payload(self):
        client = self.client([{"event": "unknown", "data": "invalid json"}])
        stream = await connect_process(client, input([]))
        with self.assertRaisesRegex(ValueError, "invalid process connect message"):
            _ = [message async for message in stream]

    async def test_invalid_content_type_is_closed(self):
        client = self.client([], content_type="application/json")
        response = client.send.return_value
        response.close = AsyncMock()
        with self.assertRaisesRegex(ValueError, "invalid process connect content type"):
            await connect_process(client, input([]))
        response.close.assert_awaited_once()

    async def test_protocol_and_status_errors_preserve_tangram_errors(self):
        client = self.client(
            [{"event": "error", "data": '{"message":"the connection failed"}'}]
        )
        stream = await connect_process(client, input([]))
        with self.assertRaises(Error) as caught:
            _ = [message async for message in stream]
        self.assertEqual(await caught.exception.message, "the connection failed")
        client.send.return_value = Response(
            409, body=Body.json({"message": "the request failed"})
        )
        with self.assertRaises(Error) as caught:
            await connect_process(client, input([]))
        self.assertEqual(await caught.exception.message, "the request failed")
