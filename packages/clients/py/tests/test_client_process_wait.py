"""Check lazy process wait promises and SSE outcome semantics."""

import json
import unittest
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import parse_qs

from tangram.client.process.wait import (
    try_wait_process_promise,
    wait_process,
    wait_process_once,
    wait_process_promise,
)
from tangram.error import Error
from tangram.http import Body, Response


class EventResponse:
    status = 200

    def __init__(self, events):
        self.events = events
        self.consumed = 0

    async def sse(self):
        for event in self.events:
            self.consumed += 1
            yield event


def event(kind, value):
    return {"event": kind, "data": json.dumps(value)}


class ProcessWaitTests(unittest.IsolatedAsyncioTestCase):
    def client(self, *responses):
        client = Mock()
        client.send_with_retry = AsyncMock(side_effect=responses)
        return client

    async def test_promise_is_lazy_and_empty_responses_are_retried(self):
        response = EventResponse(
            [event("outcome", {"exit": {"code": 0}, "output": "value"})]
        )
        client = self.client(EventResponse([]), response)
        promise = await try_wait_process_promise(client, "id", {})
        client.send_with_retry.assert_not_awaited()
        self.assertEqual(
            await promise(), {"error": None, "exit": {"code": 0}, "output": "value"}
        )
        self.assertEqual(client.send_with_retry.await_count, 2)

    async def test_request_preserves_arg_and_serializes_location(self):
        client = self.client(EventResponse([event("outcome", {"exit": {"code": 0}})]))
        await wait_process(
            client,
            "id /?",
            {
                "lease": "lease",
                "location": {"components": [{"regions": ["west"]}]},
                "source": "remote",
                "tokens": {"token": True},
            },
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.uri.path, "/processes/id%20%2F%3F/wait")
        self.assertEqual(request.headers.get("accept"), "text/event-stream")
        self.assertEqual(
            parse_qs(request.uri.query),
            {
                "lease": ["lease"],
                "location": ["local(west)"],
                "source": ["remote"],
                "tokens[token]": ["true"],
            },
        )

    async def test_last_outcome_wins_and_stream_is_exhausted(self):
        response = EventResponse(
            [
                event("outcome", {"exit": {"code": 0}, "output": "first"}),
                event("outcome", {"exit": {"code": 1}, "output": "last"}),
            ]
        )
        self.assertEqual(
            (await wait_process(self.client(response), "id"))["output"], "last"
        )
        self.assertEqual(response.consumed, 2)

    async def test_missing_process_is_reported_when_lazy_promise_runs(self):
        client = self.client(Response(404))
        promise = await wait_process_promise(client, "id")
        client.send_with_retry.assert_not_awaited()
        with self.assertRaisesRegex(ValueError, "failed to find the process"):
            await promise()

    async def test_http_error_and_error_sse_variants(self):
        client = self.client(Response(500, body=Body.json({"message": "failure"})))
        with self.assertRaises(Error) as caught:
            await wait_process(client, "id")
        self.assertEqual(await caught.exception.message(), "failure")
        for value, constructor in [
            ("error_id", "with_id"),
            ({"message": "failure"}, "from_data"),
        ]:
            error = Error("failure")
            with patch.object(Error, constructor, return_value=error) as create:
                with self.assertRaises(Error) as caught:
                    await wait_process(
                        self.client(EventResponse([event("error", value)])), "id"
                    )
                self.assertIs(caught.exception, error)
                create.assert_called_once_with(value)

    async def test_output_event_is_rejected_and_empty_once_returns_none(self):
        with self.assertRaisesRegex(ValueError, "invalid process wait event"):
            await wait_process(
                self.client(EventResponse([event("output", {"exit": {"code": 0}})])),
                "id",
            )
        self.assertIsNone(
            await wait_process_once(self.client(EventResponse([])), "id", {})
        )
