import json
import unittest
from copy import deepcopy
from unittest.mock import patch

from tangram.client.process.stdio import read
from tangram.error import Error


async def messages(*values):
    for value in values:
        if isinstance(value, Exception):
            raise value
        yield value


def chunk(position, value):
    return {
        "kind": "notification",
        "value": {
            "kind": "chunk",
            "value": {
                "bytes": value,
                "combined_position": position,
                "stream_position": position,
                "stream": "stdout",
            },
        },
    }


def limit(position):
    return {
        "kind": "response",
        "value": {"kind": "limit", "value": {"position": position}},
    }


class ReadEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_preserves_cursor_and_clips_remaining_length(self):
        calls = []
        first = read.Connection(read.Channel(), messages(chunk(0, b"abc"), OSError()))
        second = read.Connection(
            read.Channel(), messages(chunk(0, b"abcdef"), limit(6))
        )

        async def reconnect(arg):
            calls.append(deepcopy(arg))
            return second

        first.reconnect = reconnect
        output = read.read_process_stdio_all(
            None, "process", {"streams": ["stdout"], "length": 4}, first
        )
        self.assertEqual([item["bytes"] async for item in output], [b"abc", b"def"])
        self.assertEqual(calls, [{"streams": ["stdout"], "length": 1, "position": 3}])
        self.assertTrue(first.input.closed)
        self.assertTrue(second.input.closed)

    async def test_response_finishes_without_consuming_later_messages(self):
        connection = read.Connection(
            read.Channel(), messages(limit(0), ValueError("must not consume"))
        )
        output = read.read_process_stdio_all(
            None, "process", {"streams": ["stdout"]}, connection
        )
        self.assertEqual([item async for item in output], [])
        self.assertEqual(await anext(connection.input), {"kind": "ack"})

    async def test_invalid_position_length_is_terminal(self):
        connection = read.Connection(
            read.Channel(),
            messages(
                {
                    "kind": "notification",
                    "value": {
                        "kind": "position",
                        "value": {"position": 0, "length": 2**53},
                    },
                }
            ),
        )
        output = read.read_process_stdio_all(
            None, "process", {"streams": ["stdout"]}, connection
        )
        with self.assertRaisesRegex(
            read.ProtocolError, "invalid process stdio position"
        ):
            await anext(output)

    async def test_initial_connection_retries_transport_but_not_protocol_errors(self):
        connection = read.Connection(read.Channel(), messages(limit(0)))
        attempts = []

        async def once(client, id, arg):
            attempts.append(arg)
            if len(attempts) == 1:
                raise OSError("closed")
            return connection

        with (
            patch.object(read, "read_process_stdio_once", once),
            patch.object(read, "retry_delay"),
        ):
            output = await read.try_read_process_stdio(
                None, "process", streams=["stdout"]
            )
            self.assertEqual([item async for item in output], [])
        self.assertEqual(len(attempts), 2)

    async def test_decode_error_id_and_reject_malformed_message(self):
        class Response:
            async def close(self):
                pass

            async def sse(self):
                yield {"event": "notification", "data": json.dumps({"kind": "wrong"})}

        with self.assertRaisesRegex(
            read.ProtocolError, "invalid process stdio read notification"
        ):
            await anext(read.decode_server_messages(Response()))
        self.assertIsInstance(read.error_from_data({"message": "error"}), Error)

    async def test_closing_unstarted_stream_closes_both_channels(self):
        closed = []

        class Output:
            async def aclose(self):
                closed.append(True)

        connection = read.Connection(read.Channel(), Output())
        output = read.read_process_stdio_all(
            None, "process", {"streams": ["stdout"]}, connection
        )
        await output.aclose()
        self.assertTrue(connection.input.closed)
        self.assertTrue(closed)

    async def test_request_preserves_location_tokens_and_percent_encodes_id(self):
        from urllib.parse import parse_qs, urlsplit

        class Response:
            status = 200
            headers = {"content-type": "text/event-stream; charset=utf-8"}

            async def close(self):
                pass

            async def sse(self):
                yield {
                    "event": "response",
                    "data": json.dumps({"kind": "limit", "value": {"position": 0}}),
                }

        requests = []

        class Client:
            async def send(self, request):
                requests.append(request)
                return Response()

        connection = await read.read_process_stdio_once(
            Client(),
            "process/part",
            {
                "streams": ["stdout", "stderr"],
                "location": {"components": [{"regions": ["east"]}]},
                "tokens": {"key": "token"},
            },
        )
        uri = str(requests[0].uri)
        self.assertEqual(urlsplit(uri).path, "/processes/process%2Fpart/stdio/read")
        query = parse_qs(urlsplit(uri).query)
        self.assertEqual(query["streams"], ["stdout,stderr"])
        self.assertEqual(query["location"], ["local(east)"])
        self.assertEqual(query["tokens[key]"], ["token"])
        await read.close_output(connection.output)

    async def test_protocol_errors_do_not_retry(self):
        async def once(client, id, arg):
            raise read.ProtocolError("invalid response")

        with (
            patch.object(read, "read_process_stdio_once", once),
            patch.object(read, "retry_delay") as delay,
        ):
            with self.assertRaisesRegex(read.ProtocolError, "invalid response"):
                await read.connect(None, "process", {"streams": ["stdout"]})
            delay.assert_not_called()
