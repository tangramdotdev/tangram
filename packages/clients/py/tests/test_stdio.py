"""Check stdio cursor recovery and idempotent write replay."""

import base64
import json
import unittest
from copy import deepcopy
from unittest.mock import patch

from tangram.client.process.stdio import read as read_endpoint
from tangram.client.process.stdio import write as write_endpoint
from tangram.process import stdio


def event(kind, value):
    return {"event": kind, "data": json.dumps(value)}


def chunk(position, bytes_):
    return event(
        "notification",
        {
            "kind": "chunk",
            "value": {
                "bytes": base64.b64encode(bytes_).decode(),
                "combined_position": position,
                "stream": "stdout",
                "stream_position": position,
            },
        },
    )


class Response:
    def __init__(self, events):
        self.events = events
        self.closed = False

    async def sse(self):
        if hasattr(self.events, "__aiter__"):
            async for event_ in self.events:
                yield event_
        else:
            for event_ in self.events:
                if isinstance(event_, Exception):
                    raise event_
                yield event_

    async def close(self):
        self.closed = True


class StdioTests(unittest.IsolatedAsyncioTestCase):
    async def test_forward_read_skips_replayed_bytes(self):
        responses = [
            Response([chunk(0, b"abc"), ConnectionError("disconnected")]),
            Response(
                [
                    chunk(0, b"abcdef"),
                    event(
                        "response",
                        {
                            "kind": "end",
                            "value": {
                                "combined_position": 6,
                                "stream_positions": {"stdout": 6},
                            },
                        },
                    ),
                ]
            ),
        ]
        args = []

        async def connect(_client, _id, arg):
            args.append(deepcopy(arg))
            return read_endpoint.Connection(
                read_endpoint.Channel(),
                read_endpoint.decode_server_messages(responses[len(args) - 1]),
            )

        with patch.object(read_endpoint, "connect", connect):
            stream = await read_endpoint.try_read_process_stdio(
                None, "process", {"streams": ["stdout"]}
            )
            self.assertEqual(
                b"".join([chunk_["bytes"] async for chunk_ in stream]), b"abcdef"
            )
        self.assertEqual(args[1]["position"], 3)
        self.assertTrue(all(response.closed for response in responses))

    async def test_reverse_read_resumes_the_clipped_cursor(self):
        responses = [
            Response(
                [
                    event(
                        "notification",
                        {"kind": "position", "value": {"position": 100, "length": -20}},
                    ),
                    chunk(95, b"abcde"),
                    ConnectionError("disconnected"),
                ]
            ),
            Response(
                [
                    chunk(80, b"abcdefghijklmno"),
                    event("response", {"kind": "limit", "value": {"position": 80}}),
                ]
            ),
        ]
        args = []

        async def connect(_client, _id, arg):
            args.append(deepcopy(arg))
            return read_endpoint.Connection(
                read_endpoint.Channel(),
                read_endpoint.decode_server_messages(responses[len(args) - 1]),
            )

        with patch.object(read_endpoint, "connect", connect):
            stream = await read_endpoint.try_read_process_stdio(
                None,
                "process",
                {"streams": ["stdout"], "position": "end", "length": -20},
            )
            self.assertEqual(
                [chunk_["bytes"] async for chunk_ in stream],
                [b"abcde", b"abcdefghijklmno"],
            )
        self.assertEqual(args[1]["position"], 95)
        self.assertEqual(args[1]["length"], -15)

    async def test_read_rejects_a_gap_at_eof(self):
        response = Response(
            [
                chunk(0, b"abc"),
                event(
                    "response",
                    {
                        "kind": "end",
                        "value": {
                            "combined_position": 6,
                            "stream_positions": {"stdout": 6},
                        },
                    },
                ),
            ]
        )
        stream = read_endpoint.read_process_stdio_all(
            None,
            "process",
            {"streams": ["stdout"]},
            read_endpoint.Connection(
                read_endpoint.Channel(), read_endpoint.decode_server_messages(response)
            ),
        )
        with self.assertRaisesRegex(ValueError, "gap at the end"):
            _ = [chunk_ async for chunk_ in stream]
        self.assertTrue(response.closed)

    async def test_write_replays_after_receipt_before_completion(self):
        channels = []
        requests = []
        responses = []

        async def connect(_client, _id, _arg):
            channel = write_endpoint.Channel()
            channels.append(channel)
            index = len(channels) - 1
            recorded = []
            requests.append(recorded)

            async def output():
                async for message in channel:
                    if message["kind"] != "request":
                        continue
                    request = message["value"]
                    recorded.append(deepcopy(request))
                    if index == 0:
                        yield event("ack", {"id": request["id"]})
                        raise ConnectionError("disconnected")
                    is_end = request["arg"]["kind"] == "end"
                    yield event(
                        "response",
                        {
                            "id": request["id"],
                            "error": None,
                            "output": {
                                "closed": is_end,
                                "length": 0
                                if is_end
                                else len(request["arg"]["value"]["bytes"]),
                            },
                        },
                    )

            response = Response(output())
            responses.append(response)
            return write_endpoint.Connection(
                channel, write_endpoint.decode_server_messages(response)
            )

        async def chunks():
            yield {
                "bytes": b"abc",
                "combined_position": 0,
                "stream": "stdin",
                "stream_position": 0,
            }

        with patch.object(write_endpoint, "connect", connect):
            self.assertTrue(
                await write_endpoint.try_write_process_stdio(
                    None, "process", {"streams": ["stdin"]}, chunks()
                )
            )
        self.assertEqual(requests[1][0], requests[0][0])
        self.assertEqual(requests[0][0]["arg"]["value"]["bytes"], b"abc")
        self.assertEqual(requests[1][-1]["arg"]["kind"], "end")
        self.assertTrue(all(response.closed for response in responses))


if __name__ == "__main__":
    unittest.main()


class ReaderWriterTests(unittest.IsolatedAsyncioTestCase):
    async def test_reader_skips_empty_fd_reads_and_replaces_invalid_utf8(self):
        from unittest.mock import AsyncMock

        from tangram import host

        reader = stdio.Reader(fd=123)
        with (
            patch.object(host, "read", AsyncMock(side_effect=[b"", b"\xff", None])),
            patch.object(host, "close", AsyncMock()) as close,
        ):
            self.assertEqual(await reader.text(), "\ufffd")
            close.assert_awaited_once_with(123)

    async def test_writer_rejects_text_and_empty_write_requires_an_available_stream(
        self,
    ):
        writer = stdio.Writer()
        with self.assertRaisesRegex(ValueError, "expected stdio bytes"):
            await writer.write("text")
        with self.assertRaisesRegex(ValueError, "not available"):
            await writer.write(b"")

    async def test_tty_input_restores_raw_mode_on_write_failure(self):
        from unittest.mock import AsyncMock, Mock

        from tangram import host

        client = Mock()
        client.write_process_stdio = AsyncMock(side_effect=RuntimeError("write failed"))
        with (
            patch.object(host, "is_foreground_controlling_tty", return_value=True),
            patch.object(host, "enable_raw_mode", AsyncMock()) as enable,
            patch.object(host, "disable_raw_mode", AsyncMock()) as disable,
        ):
            with self.assertRaisesRegex(RuntimeError, "write failed"):
                await stdio.stdin_task("process", None, {}, "tty", None, client)
        enable.assert_awaited_once_with(0)
        disable.assert_awaited_once_with(0)

    async def test_output_forwarding_combines_streams_and_preserves_authorization(self):
        from unittest.mock import AsyncMock, Mock

        from tangram import host

        async def chunks():
            yield {"stream": "stderr", "bytes": b"error"}
            yield {"stream": "stdout", "bytes": b"output"}

        client = Mock()
        client.try_read_process_stdio = AsyncMock(return_value=chunks())
        with patch.object(host, "write", AsyncMock()) as write:
            await stdio.stdout_stderr_task(
                "process", "location", {"token": True}, "pipe", "tty", client
            )
        client.try_read_process_stdio.assert_awaited_once_with(
            "process",
            {
                "streams": ["stdout", "stderr"],
                "tokens": {"token": True},
                "location": "location",
            },
        )
        self.assertEqual(
            [call.args for call in write.await_args_list],
            [(2, b"error"), (1, b"output")],
        )


class WriteQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_write_waits_for_acknowledgment_and_rejects_early_close(self):
        import asyncio

        queue = stdio.WriteQueue()
        written = asyncio.create_task(queue.write(b"bytes"))
        request = await anext(queue)
        chunk_ = queue.chunk(request, 0, "stdin")
        self.assertFalse(written.done())
        queue.complete(chunk_)
        self.assertEqual(await written, 5)
        queued = asyncio.create_task(queue.write(b"pending"))
        request = await anext(queue)
        queue.chunk(request, 5, "stdin")
        queue.finish()
        with self.assertRaisesRegex(BrokenPipeError, "before the write completed"):
            await queued
