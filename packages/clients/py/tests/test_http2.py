"""Exercise HTTP/2 trailers using actual protocol frames."""

import asyncio
import unittest
from unittest.mock import Mock

from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import RequestReceived

from tangram.error import Error
from tangram.http import Request
from tangram.http2 import Session, Stream


class Writer:
    def __init__(self, reader, trailers):
        self.reader = reader
        self.trailers = trailers
        self.connection = H2Connection(
            H2Configuration(client_side=False, header_encoding="utf-8")
        )
        self.connection.initiate_connection()

    def write(self, bytes_):
        for event in self.connection.receive_data(bytes_):
            if isinstance(event, RequestReceived):
                self.connection.send_headers(event.stream_id, [(":status", "200")])
                self.connection.send_data(event.stream_id, b"before trailers")
                self.connection.send_headers(
                    event.stream_id, self.trailers, end_stream=True
                )
        if data := self.connection.data_to_send():
            self.reader.feed_data(data)

    async def drain(self):
        pass

    def close(self):
        pass

    async def wait_closed(self):
        pass


class Http2Tests(unittest.IsolatedAsyncioTestCase):
    async def test_cancellation_in_a_body_queue_is_propagated_and_released(self):
        session = object.__new__(Session)
        session._release = Mock()
        stream = Stream(asyncio.get_running_loop().create_future())
        stream.chunks.put_nowait(asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await anext(session._body(1, stream))
        session._release.assert_called_once_with(1, stream)

    async def test_error_trailers_surface_after_buffered_body(self):
        reader = asyncio.StreamReader()
        writer = Writer(
            reader,
            [("x-tg-event", "error"), ("x-tg-data", '{"message":"from trailer"}')],
        )
        session = Session(reader, writer, "http", "localhost")
        try:
            response = await session.send(Request("GET", "/"))
            body = response.body.__aiter__()
            self.assertEqual(await anext(body), b"before trailers")
            with self.assertRaises(Error) as raised:
                await anext(body)
            self.assertEqual(await raised.exception.message, "from trailer")
        finally:
            await session.close()

    async def test_error_trailer_requires_data(self):
        reader = asyncio.StreamReader()
        writer = Writer(reader, [("x-tg-event", "error")])
        session = Session(reader, writer, "http", "localhost")
        try:
            response = await session.send(Request("GET", "/"))
            with self.assertRaisesRegex(ValueError, "missing data"):
                await response.collect()
        finally:
            await session.close()


class CoalesceTests(unittest.IsolatedAsyncioTestCase):
    async def test_small_ready_chunks_are_batched_and_empty_chunks_are_ignored(self):
        from tangram.http2 import coalesce

        async def body():
            for chunk in (b"", b"a" * 100, b"b" * 100, b"", b"c" * 100):
                yield chunk

        self.assertEqual(
            [chunk async for chunk in coalesce(body(), 16384)],
            [b"a" * 100 + b"b" * 100 + b"c" * 100],
        )

    async def test_large_chunks_do_not_wait_for_the_next_producer_chunk(self):
        from tangram.http2 import coalesce

        blocked = asyncio.Event()
        closed = asyncio.Event()

        async def body():
            try:
                yield b"a" * 16384
                await blocked.wait()
                yield b"b"
            finally:
                closed.set()

        chunks = coalesce(body(), 16384)
        self.assertEqual(await anext(chunks), b"a" * 16384)
        await chunks.aclose()
        self.assertTrue(closed.is_set())

    async def test_pending_producer_is_preserved_without_canceling_it(self):
        from tangram.http2 import coalesce

        ready = asyncio.Event()
        entered = asyncio.Event()
        canceled = False

        async def body():
            nonlocal canceled
            yield b"first"
            entered.set()
            try:
                await ready.wait()
            except asyncio.CancelledError:
                canceled = True
                raise
            yield b"second"

        chunks = coalesce(body(), 16384)
        self.assertEqual(await anext(chunks), b"first")
        self.assertTrue(entered.is_set())
        self.assertFalse(canceled)
        ready.set()
        self.assertEqual(await anext(chunks), b"second")
        with self.assertRaises(StopAsyncIteration):
            await anext(chunks)
        self.assertFalse(canceled)

    async def test_closing_cancels_pending_producer_and_closes_body(self):
        from tangram.http2 import coalesce

        closed = asyncio.Event()

        async def body():
            try:
                yield b"first"
                await asyncio.Event().wait()
            finally:
                closed.set()

        chunks = coalesce(body(), 16384)
        self.assertEqual(await anext(chunks), b"first")
        await chunks.aclose()
        self.assertTrue(closed.is_set())

    async def test_producer_error_follows_bytes_already_buffered(self):
        from tangram.http2 import coalesce

        async def body():
            yield b"before the error"
            raise ValueError("the body failed")

        chunks = coalesce(body(), 16384)
        self.assertEqual(await anext(chunks), b"before the error")
        with self.assertRaisesRegex(ValueError, "the body failed"):
            await anext(chunks)

    async def test_arbitrary_awaitables_are_supported_by_async_iterator(self):
        from tangram.http2 import coalesce

        class Body:
            index = 0

            def __aiter__(self):
                return self

            def __anext__(self):
                future = asyncio.get_running_loop().create_future()
                self.index += 1
                if self.index <= 2:
                    future.set_result(b"chunk")
                else:
                    future.set_exception(StopAsyncIteration())
                return future

        self.assertEqual(
            [chunk async for chunk in coalesce(Body(), 16384)], [b"chunkchunk"]
        )

    async def test_pending_output_flushes_without_scheduling_a_timer(self):
        from unittest.mock import patch

        from tangram.http2 import coalesce

        async def body():
            yield b"prompt"
            await asyncio.Event().wait()

        chunks = coalesce(body(), 16384)
        loop = asyncio.get_running_loop()
        with patch.object(
            loop, "call_later", side_effect=AssertionError("unexpected batching timer")
        ):
            self.assertEqual(await anext(chunks), b"prompt")
            await chunks.aclose()
