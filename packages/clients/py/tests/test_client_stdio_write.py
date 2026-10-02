import asyncio
import unittest

from tangram.client.process.stdio.write import (
    Channel,
    Connection,
    ProtocolError,
    write_process_stdio_all,
)
from tangram.process.stdio.flow import chunk_size


class WriteTests(unittest.IsolatedAsyncioTestCase):
    def connection(self, responses=None, reconnect=None):
        input = Channel()
        requests = []

        async def output():
            async for message in input:
                if message["kind"] == "ack":
                    continue
                request = message["value"]
                requests.append(request)
                if responses is not None:
                    async for value in responses(request):
                        yield value
                    return
                chunk = request["arg"]
                yield {
                    "kind": "response",
                    "value": {
                        "id": request["id"],
                        "error": None,
                        "output": {
                            "length": len(chunk["value"]["bytes"])
                            if chunk["kind"] == "chunk"
                            else 0,
                            "closed": chunk["kind"] == "end",
                        },
                    },
                }

        return Connection(input, output(), reconnect), requests

    async def chunks(self, chunk):
        yield chunk

    async def test_split_and_complete(self):
        chunk = {
            "stream": "stdin",
            "bytes": b"x" * (chunk_size * 2 + 7),
            "combined_position": 41,
            "stream_position": 23,
        }
        connection, requests = self.connection()
        completed = []
        await asyncio.wait_for(
            write_process_stdio_all(
                None,
                "id",
                {"streams": ["stdin"]},
                self.chunks(chunk),
                connection,
                completed.append,
            ),
            1,
        )
        self.assertEqual(completed, [chunk])
        self.assertEqual(
            [r["arg"]["value"]["stream_position"] for r in requests[:-1]],
            [23, 23 + chunk_size, 23 + chunk_size * 2],
        )
        self.assertEqual(requests[-1]["arg"]["kind"], "end")
        self.assertEqual(
            requests[-1]["arg"]["value"]["combined_position"],
            41 + len(chunk["bytes"]),
        )

    async def test_replay_preserves_request_and_positions(self):
        replacement, replay = self.connection()

        async def fail(request):
            if False:
                yield
            raise ConnectionError("lost")

        async def reconnect():
            return replacement

        connection, original = self.connection(fail, reconnect)
        chunk = {
            "stream": "stdin",
            "bytes": b"hello",
            "combined_position": 11,
            "stream_position": 5,
        }
        await write_process_stdio_all(
            None, "id", {"streams": ["stdin"]}, self.chunks(chunk), connection
        )
        self.assertEqual(original[0], replay[0])

    async def test_partial_closed_does_not_complete(self):
        async def partial(request):
            yield {
                "kind": "response",
                "value": {
                    "id": request["id"],
                    "error": None,
                    "output": {"length": 2, "closed": True},
                },
            }

        connection, _ = self.connection(partial)
        completed = []
        await write_process_stdio_all(
            None,
            "id",
            {"streams": ["stdin"]},
            self.chunks(
                {
                    "stream": "stdin",
                    "bytes": b"hello",
                    "combined_position": 0,
                    "stream_position": 0,
                }
            ),
            connection,
            completed.append,
        )
        self.assertEqual(completed, [])

    async def test_invalid_response_is_terminal(self):
        async def bad(request):
            yield {
                "kind": "response",
                "value": {"id": request["id"] + 1},
            }

        connection, _ = self.connection(bad)
        with self.assertRaisesRegex(ProtocolError, "out-of-order"):
            await write_process_stdio_all(
                None,
                "id",
                {"streams": ["stdin"]},
                self.chunks(
                    {
                        "stream": "stdin",
                        "bytes": b"hello",
                        "combined_position": 0,
                        "stream_position": 0,
                    }
                ),
                connection,
            )
