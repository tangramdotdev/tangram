import asyncio
import unittest

from tangram.process.connect.channel import Channel
from tangram.process.connect.session import REQUEST_WINDOW, Session
from tangram.process.stdio.flow import capacity


async def messages(*values):
    for value in values:
        yield value


class SessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_response_acknowledgments_precede_queued_operations(self):
        session = Session(None)
        session._input.push({"kind": "request", "value": {"id": 9}})
        pending = asyncio.get_running_loop().create_future()
        session._requests[3] = pending
        await session._receive(
            messages(
                {
                    "kind": "response",
                    "value": {"id": 3, "error": None, "output": {"kind": "signal"}},
                }
            )
        )
        self.assertEqual(
            (await session._input.next())["value"],
            {
                "kind": "ack",
                "value": {"id": 3},
            },
        )
        self.assertEqual((await session._input.next())["value"]["kind"], "request")
        self.assertEqual(await pending, {"kind": "signal"})

    async def test_confirmation_follows_operation(self):
        session = Session(None)
        session._input.push({"kind": "request", "value": {"id": 9}})
        session.confirm()
        session.confirm()
        self.assertEqual((await session._input.next())["value"]["kind"], "request")
        self.assertEqual(
            (await session._input.next())["value"],
            {
                "kind": "ack",
                "value": {"id": 0},
            },
        )
        session._input.close()
        self.assertTrue((await session._input.next())["done"])

    async def test_pending_request_limit_is_separate_from_receipt_credit(self):
        session = Session(None)
        for id in range(REQUEST_WINDOW):
            session._requests[id] = asyncio.get_running_loop().create_future()
        with self.assertRaisesRegex(RuntimeError, "too many process requests"):
            await session._request("signal", {"signal": 1})
        self.assertEqual(session._receipts, set())
        await session._send_request({"kind": "detach"}, 130)
        self.assertIn(130, session._receipts)
        await session.close()

    async def test_read_overflow_fails_session_without_blocking_control(self):
        session = Session(None)
        read = session._reads[1] = Channel(capacity)
        event = {"kind": "position", "value": {"position": 0}}
        for _ in range(capacity):
            read.push(event)
        await session._receive(
            messages(
                {
                    "kind": "notification",
                    "value": {"kind": "read", "value": {"id": 1, "event": event}},
                }
            )
        )
        self.assertTrue(session._closed)
        self.assertIsInstance(session._error, RuntimeError)
        for _ in range(capacity):
            self.assertFalse((await read.next())["done"])
        with self.assertRaisesRegex(RuntimeError, "queue is full"):
            await read.next()

    async def test_write_requests_submit_concurrently_and_emit_in_input_order(self):
        session = Session(None)
        pending = {}

        async def request(kind, value):
            self.assertEqual(kind, "write")
            key = value["data"]
            future = pending[key] = asyncio.get_running_loop().create_future()
            return {"kind": "write", "value": await future}

        session._request = request
        connection = session.write({})
        for id in (1, 2):
            connection.input.push(
                {
                    "kind": "request",
                    "value": {"id": id, "arg": id},
                }
            )
        await asyncio.sleep(0)
        self.assertEqual(set(pending), {1, 2})
        pending[2].set_result("second")
        first = asyncio.create_task(connection.output.__anext__())
        await asyncio.sleep(0)
        self.assertFalse(first.done())
        pending[1].set_result("first")
        self.assertEqual((await first)["value"]["id"], 1)
        self.assertEqual((await connection.output.__anext__())["value"]["id"], 2)
        connection.input.close()
        await session.close()

    async def test_buffered_iterator_steps_allow_read_consumer_to_run(self):
        session = Session(None)
        session._initial_reads[1] = {"streams": ["stdout"]}
        session._reads[1] = Channel(capacity)
        count = capacity * 3
        events = [
            {
                "kind": "notification",
                "value": {
                    "kind": "read",
                    "value": {
                        "id": 1,
                        "event": {
                            "kind": "chunk",
                            "value": {
                                "bytes": b"x",
                                "stream": "stdout",
                                "stream_position": index,
                                "combined_position": index,
                            },
                        },
                    },
                },
            }
            for index in range(count)
        ]
        events.append(
            {
                "kind": "response",
                "value": {
                    "id": 1,
                    "error": None,
                    "output": {
                        "kind": "read",
                        "value": {
                            "kind": "end",
                            "value": {
                                "combined_position": count,
                                "stream_positions": {"stdout": count},
                            },
                        },
                    },
                },
            }
        )

        async def consume():
            return [chunk async for chunk in session.read(["stdout"])]

        consumer = asyncio.create_task(consume())
        await session._receive(messages(*events))
        self.assertEqual(len(await consumer), count)
        self.assertIsNone(session._error)

    async def test_eof_before_outcome_has_specific_wait_error(self):
        session = Session(None)
        await session._receive(messages())
        with self.assertRaisesRegex(ConnectionError, "before completion"):
            await session.wait()

    async def test_detach_after_outcome_does_not_send_request(self):
        session = Session(None)
        await session._receive(
            messages(
                {
                    "kind": "notification",
                    "value": {"kind": "outcome", "value": {"exit": 0, "error": None}},
                }
            )
        )
        await session.detach()
        self.assertEqual((await session.wait())["exit"], 0)
        self.assertEqual(session._next_id, 1)
