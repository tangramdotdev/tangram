"""Check the Client facade and JavaScript transport retry contracts."""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from tangram.client import Client, default_retry_options, retry
from tangram.http import Body, Request, Response


class FakeSession:
    def __init__(self, *, closed=False, error=None):
        self.closed = closed
        self.close = AsyncMock(side_effect=self._close)
        self.send = AsyncMock(side_effect=error, return_value=Response(200))

    def _close(self):
        self.closed = True


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_environment_and_authorization_do_not_mutate_request(self):
        session = FakeSession()
        client = Client()
        request = Request("GET", "/health", {"authorization": "custom"})
        with (
            patch.dict(
                "os.environ",
                {"TANGRAM_URL": "http://localhost", "TANGRAM_TOKEN": "secret"},
            ),
            patch("tangram.client.Session.connect", AsyncMock(return_value=session)),
        ):
            self.assertEqual(
                client.arg(), {"url": "http://localhost", "token": "secret"}
            )
            await client.send(request)
        self.assertEqual(request.headers["authorization"], "custom")
        sent = session.send.call_args.args[0]
        self.assertEqual(sent.headers["authorization"], "Bearer secret")
        await client.close()

    async def test_invalid_token_does_not_open_session(self):
        client = Client(url="http://localhost", token=42)
        connect = AsyncMock()
        with patch("tangram.client.Session.connect", connect):
            with self.assertRaisesRegex(ValueError, "invalid TANGRAM_TOKEN"):
                await client.send(Request("GET", "/health"))
        connect.assert_not_awaited()

    async def test_connection_attempts_are_not_retried_again_as_requests(self):
        connect = AsyncMock(side_effect=OSError("offline"))
        client = Client(url="http://localhost")
        with (
            patch("tangram.client.Session.connect", connect),
            patch("tangram.client.asyncio.sleep", AsyncMock()) as sleep,
        ):
            with self.assertRaisesRegex(OSError, "offline"):
                await client.send_with_retry(Request("GET", "/health"))
        self.assertEqual(connect.await_count, 4)
        self.assertEqual(sleep.await_count, 3)

    async def test_request_failure_disconnects_and_preserves_source(self):
        error = ValueError("the stream failed")
        session = FakeSession(error=error)
        client = Client(url="http://localhost")
        client._session = session
        with self.assertRaises(ValueError) as caught:
            await client.send(Request("GET", "/health"))
        self.assertIs(caught.exception, error)
        self.assertIsNone(client._session)
        session.close.assert_awaited_once()

    async def test_request_retries_any_stream_creation_failure(self):
        first = FakeSession(error=ValueError("broken stream"))
        second = FakeSession()
        client = Client(url="http://localhost")
        client._session = first
        with (
            patch("tangram.client.Session.connect", AsyncMock(return_value=second)),
            patch("tangram.client.asyncio.sleep", AsyncMock()),
        ):
            self.assertEqual(
                (await client.send_with_retry(Request("GET", "/health"))).status,
                200,
            )
        first.close.assert_awaited_once()
        second.send.assert_awaited_once()
        await client.close()

    async def test_retry_rejects_streaming_body_before_connecting(self):
        async def chunks():
            yield b"one"

        client = Client(url="http://localhost")
        connect = AsyncMock()
        with patch("tangram.client.Session.connect", connect):
            with self.assertRaisesRegex(ValueError, "streaming body"):
                await client.send_with_retry(
                    Request("POST", "/write", body=Body(chunks()))
                )
        connect.assert_not_awaited()

    async def test_concurrent_connect_and_cancelled_waiter_share_session(self):
        started = asyncio.Event()
        release = asyncio.Event()
        session = FakeSession()

        async def connect(url):
            started.set()
            await release.wait()
            return session

        client = Client(url="http://localhost")
        with patch(
            "tangram.client.Session.connect", AsyncMock(side_effect=connect)
        ) as create:
            first = asyncio.create_task(client._connect())
            await started.wait()
            second = asyncio.create_task(client._connect())
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            release.set()
            self.assertIs(await second, session)
            self.assertIs(await client._connect(), session)
        create.assert_awaited_once()
        await client.close()

    async def test_closed_connections_are_disposed(self):
        first = FakeSession(closed=True)
        second = FakeSession(closed=True)
        third = FakeSession()
        client = Client(url="http://localhost")
        client._session = first
        with patch(
            "tangram.client.Session.connect", AsyncMock(side_effect=[second, third])
        ):
            self.assertIs(await client._connect(), third)
        first.close.assert_awaited_once()
        second.close.assert_awaited_once()
        await client.close()

    async def test_retry_default_timing_matches_javascript(self):
        function = AsyncMock(side_effect=[OSError(), OSError(), OSError(), "ok"])
        with (
            patch("tangram.client.random.random", return_value=0.5),
            patch("tangram.client.asyncio.sleep", AsyncMock()) as sleep,
        ):
            self.assertEqual(await retry(default_retry_options(), function), "ok")
        self.assertEqual(
            [call.args[0] for call in sleep.await_args_list], [0.025, 0.045, 0.085]
        )

    async def test_positional_operation_args_match_keyword_form(self):
        client = Client()
        get = AsyncMock(return_value={"object": None})
        with patch("tangram.client.object.get.get_object", get):
            await client.get_object("fil_example", {"tokens": {"local": ["token"]}})
            await client.get_object("fil_example", tokens={"local": ["token"]})
        self.assertEqual(get.await_args_list[0], get.await_args_list[1])

    def test_native_value_helpers_accept_and_return_wire_data(self):
        client = Client()
        data = {"kind": "map", "value": {"message": "hello"}}
        self.assertEqual(client.parse_value(client.stringify_value(data)), data)


if __name__ == "__main__":
    unittest.main()
