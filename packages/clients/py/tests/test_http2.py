"""Exercise HTTP/2 trailers using actual protocol frames."""

import asyncio
import unittest
from unittest.mock import Mock

from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import PingAckReceived, RequestReceived, StreamReset
from h2.settings import SettingCodes

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


class Peer:
    def __init__(self, reader):
        self.reader = reader
        self.connection = H2Connection(
            H2Configuration(client_side=False, header_encoding="utf-8")
        )
        self.connection.initiate_connection()
        self.requests = asyncio.Queue()
        self.resets = asyncio.Queue()
        self.pings = asyncio.Queue()

    def write(self, data):
        for event in self.connection.receive_data(data):
            if isinstance(event, RequestReceived):
                self.requests.put_nowait(event.stream_id)
            elif isinstance(event, StreamReset):
                self.resets.put_nowait(event.stream_id)
            elif isinstance(event, PingAckReceived):
                self.pings.put_nowait(event)
        self.flush()

    def flush(self):
        if data := self.connection.data_to_send():
            self.reader.feed_data(data)

    async def synchronize(self):
        self.connection.ping(b"12345678")
        self.flush()
        await asyncio.wait_for(self.pings.get(), 1)

    async def drain(self):
        pass

    def close(self):
        pass

    async def wait_closed(self):
        pass


class FlowControlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        reader = asyncio.StreamReader()
        self.peer = Peer(reader)
        self.session = Session(reader, self.peer, "http", "localhost")
        self.addAsyncCleanup(self.session.close)

    async def response(self, body=None):
        request = asyncio.create_task(
            self.session.send(Request("POST", "/", body=body))
        )
        stream_id = await asyncio.wait_for(self.peer.requests.get(), 1)
        self.peer.connection.send_headers(stream_id, [(":status", "200")])
        self.peer.flush()
        return stream_id, await asyncio.wait_for(request, 1)

    async def fill_window(self, stream_id, *, end_stream=False):
        # Padding consumes window credit just like payload bytes.
        connection = self.peer.connection
        while length := min(connection.local_flow_control_window(stream_id), 16384):
            last = length == connection.local_flow_control_window(stream_id)
            connection.send_data(
                stream_id,
                b"x" * (length - 1),
                pad_length=0,
                end_stream=end_stream and last,
            )
        self.peer.flush()
        await self.peer.synchronize()
        self.assertEqual(connection.outbound_flow_control_window, 0)

    async def assert_connection_usable(self):
        stream_id, response = await self.response()
        self.assertEqual(
            self.peer.connection.local_flow_control_window(stream_id), 65535
        )
        self.peer.connection.send_data(stream_id, b"success", end_stream=True)
        self.peer.flush()
        self.assertEqual(await asyncio.wait_for(response.collect(), 1), b"success")

    async def test_closing_unread_responses_restores_connection_credit(self):
        for end_stream in [False, True]:
            with self.subTest(end_stream=end_stream):
                stream_id, response = await self.response()
                await self.fill_window(stream_id, end_stream=end_stream)
                await response.close()
                await response.close()
                self.assertEqual(
                    self.peer.connection.outbound_flow_control_window, 65535
                )
        await self.assert_connection_usable()

    async def test_closing_partial_response_does_not_acknowledge_twice(self):
        stream_id, response = await self.response()
        await self.fill_window(stream_id)
        body = response.body.__aiter__()
        await anext(body)
        await response.close()
        await body.aclose()
        self.assertEqual(self.peer.connection.outbound_flow_control_window, 65535)
        await self.assert_connection_usable()

    async def test_late_data_after_cancellation_is_acknowledged_once(self):
        request = asyncio.create_task(self.session.send(Request("GET", "/")))
        stream_id = await asyncio.wait_for(self.peer.requests.get(), 1)
        connection = self.peer.connection
        connection.send_headers(stream_id, [(":status", "200")])
        while length := min(connection.local_flow_control_window(stream_id), 16384):
            connection.send_data(stream_id, b"x" * length)
        data = connection.data_to_send()
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request
        self.peer.reader.feed_data(data)
        await self.peer.synchronize()
        await self.assert_connection_usable()

    async def test_reset_cancels_blocked_upload_and_restores_credit(self):
        started = asyncio.Event()
        closed = asyncio.Event()

        async def body():
            try:
                started.set()
                await asyncio.Event().wait()
                yield b"unused"
            finally:
                closed.set()

        stream_id, response = await self.response(body())
        await started.wait()
        await self.fill_window(stream_id)
        self.peer.connection.reset_stream(stream_id)
        self.peer.flush()
        await asyncio.wait_for(closed.wait(), 1)
        await self.assert_connection_usable()
        with self.assertRaises(ConnectionError):
            await response.collect()

    async def test_upload_completion_wakes_requests_waiting_for_a_stream(self):
        self.peer.connection.update_settings({SettingCodes.MAX_CONCURRENT_STREAMS: 1})
        self.peer.flush()
        await self.peer.synchronize()
        finish = asyncio.Event()

        async def body():
            await finish.wait()
            if False:
                yield b""

        stream_id, response = await self.response(body())
        self.peer.connection.end_stream(stream_id)
        self.peer.flush()
        await self.peer.synchronize()
        next_request = asyncio.create_task(self.session.send(Request("GET", "/next")))
        self.addAsyncCleanup(self.cancel, next_request)
        await asyncio.sleep(0)
        finish.set()
        next_id = await asyncio.wait_for(self.peer.requests.get(), 1)
        self.peer.connection.send_headers(
            next_id, [(":status", "200")], end_stream=True
        )
        self.peer.flush()
        await (await next_request).close()
        await response.close()

    async def test_closing_response_unblocks_a_pending_body_read(self):
        _, response = await self.response()
        reading = asyncio.create_task(response.collect())
        self.addAsyncCleanup(self.cancel, reading)
        await asyncio.sleep(0)
        await response.close()
        self.assertEqual(await asyncio.wait_for(reading, 1), b"")
        await self.assert_connection_usable()

    async def test_upload_error_preserves_response_error_and_reclaims_credit(self):
        fail = asyncio.Event()

        async def body():
            await fail.wait()
            raise ValueError("upload failed")
            yield b"unused"

        stream_id, response = await self.response(body())
        await self.fill_window(stream_id)
        fail.set()
        self.assertEqual(await asyncio.wait_for(self.peer.resets.get(), 1), stream_id)
        with self.assertRaisesRegex(ValueError, "upload failed"):
            await response.collect()
        await self.assert_connection_usable()

    async def test_canceled_producer_fails_request_without_waiting_for_headers(self):
        async def body():
            raise asyncio.CancelledError()
            yield b"unused"

        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(
                self.session.send(Request("POST", "/", body=body())), 1
            )
        await self.peer.requests.get()
        await self.assert_connection_usable()

    async def test_connection_loss_stops_blocked_upload(self):
        started = asyncio.Event()
        closed = asyncio.Event()

        async def body():
            try:
                started.set()
                await asyncio.Event().wait()
                yield b"unused"
            finally:
                closed.set()

        _, response = await self.response(body())
        await started.wait()
        self.peer.reader.feed_eof()
        await asyncio.wait_for(closed.wait(), 1)
        with self.assertRaises(ConnectionError):
            await response.collect()

    async def test_session_close_waits_for_released_upload_cleanup(self):
        started = asyncio.Event()
        cleaning = asyncio.Event()
        finished = asyncio.Event()
        proceed = asyncio.Event()

        async def body():
            try:
                started.set()
                await asyncio.Event().wait()
                yield b"unused"
            finally:
                cleaning.set()
                await proceed.wait()
                finished.set()

        _, response = await self.response(body())
        await started.wait()
        await response.close()
        await asyncio.wait_for(cleaning.wait(), 1)
        closing = asyncio.create_task(self.session.close())
        await asyncio.sleep(0)
        self.assertFalse(closing.done())
        proceed.set()
        await asyncio.wait_for(closing, 1)
        self.assertTrue(finished.is_set())

    async def cancel(self, task):
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


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
