import asyncio
import unittest

from tangram.process import Process


class FakeClient:
    def __init__(self, handler):
        self.handler = handler
        self.opens = []
        self.tasks = []
        self.outputs = []

    async def connect_process(self, input):
        queue = asyncio.Queue()
        self.outputs.append(queue)
        number = len(self.opens)
        self.opens.append(None)

        async def receive():
            async for message in input:
                if message["kind"] != "request":
                    continue
                request = message["value"]
                id, arg = request["id"], request["arg"]
                if arg["kind"] == "connect":
                    self.opens[number] = arg["value"]
                    queue.put_nowait({"kind": "ack", "value": {"id": id}})
                    queue.put_nowait(
                        {
                            "kind": "response",
                            "value": {
                                "id": id,
                                "output": {
                                    "kind": "connect",
                                    "value": {
                                        "process": "prc_test",
                                        "lease": "lease_test",
                                        "location": {"name": "runner"},
                                        "tokens": {"runner": ["proof"]},
                                    },
                                },
                            },
                        }
                    )
                    for read_id, read in arg["value"]["reads"].items():
                        await self.handler(number, "read", read_id, read, queue)
                else:
                    await self.handler(number, arg["kind"], id, arg.get("value"), queue)

        self.tasks.append(asyncio.create_task(receive()))

        async def output():
            while (message := await queue.get()) is not None:
                if isinstance(message, Exception):
                    raise message
                yield message

        return output()

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


def response(queue, id, kind, value=None):
    queue.put_nowait({"kind": "ack", "value": {"id": id}})
    queue.put_nowait(
        {
            "kind": "response",
            "value": {
                "id": id,
                "output": {"kind": kind, "value": value},
            },
        }
    )


def read_chunk(queue, id, bytes_, position):
    queue.put_nowait(
        {
            "kind": "notification",
            "value": {
                "kind": "read",
                "value": {
                    "id": id,
                    "event": {
                        "kind": "chunk",
                        "value": {
                            "bytes": bytes_,
                            "stream": "stdout",
                            "stream_position": position,
                            "combined_position": position,
                        },
                    },
                },
            },
        }
    )


class ProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_reconnect_keeps_lease_and_skips_overlap(self):
        async def handler(number, kind, id, value, queue):
            if kind != "read":
                return
            queue.put_nowait(
                {
                    "kind": "notification",
                    "value": {
                        "kind": "read",
                        "value": {
                            "id": id,
                            "event": {
                                "kind": "position",
                                "value": {"position": 0},
                            },
                        },
                    },
                }
            )
            read_chunk(queue, id, b"a", 0)
            if number == 0:
                queue.put_nowait(None)
            else:
                read_chunk(queue, id, b"b", 1)
                response(
                    queue,
                    id,
                    "read",
                    {
                        "kind": "end",
                        "value": {
                            "combined_position": 2,
                            "stream_positions": {"stdout": 2},
                        },
                    },
                )

        client = FakeClient(handler)
        process = await Process.connect("prc_test", client=client, reads=["stdout"])
        try:
            self.assertEqual(b"".join([chunk async for chunk in process.read()]), b"ab")
            self.assertEqual(client.opens[1]["process"], "prc_test")
            self.assertEqual(client.opens[1]["lease"], "lease_test")
            self.assertEqual(client.opens[1]["tokens"], {"runner": ["proof"]})
            self.assertEqual(client.opens[1]["reads"][1]["position"], 1)
        finally:
            await process.close()
            await client.close()

    async def test_write_reconnect_replays_original_positions(self):
        writes = []

        async def handler(number, kind, id, value, queue):
            if kind == "write":
                writes.append(value["data"])
                if number == 0:
                    queue.put_nowait(None)
                else:
                    response(queue, id, "write", {"length": 3, "closed": False})

        client = FakeClient(handler)
        process = await Process.connect("prc_test", client=client)
        try:
            await process.write(b"abc")
            self.assertEqual(writes[0], writes[1])
            self.assertEqual(process._write_position, 3)
        finally:
            await process.close()
            await client.close()

    async def test_signal_is_not_replayed(self):
        signals = []

        async def handler(number, kind, id, value, queue):
            if kind == "signal":
                signals.append(value)
                queue.put_nowait(None)

        client = FakeClient(handler)
        process = await Process.connect("prc_test", client=client)
        try:
            with self.assertRaises(ConnectionError):
                await process.signal("TERM")
            self.assertEqual(len(signals), 1)
            self.assertEqual(len(client.opens), 1)
        finally:
            await process.close()
            await client.close()

    async def test_wait_reconnect(self):
        async def handler(number, kind, id, value, queue):
            pass

        client = FakeClient(handler)
        process = await Process.connect("prc_test", client=client)
        try:
            session = process.connection.session
            client.outputs[0].put_nowait(ConnectionError("lost transport"))
            while not session.closed:
                await asyncio.sleep(0)
            wait = asyncio.create_task(process.wait())
            while process.connection.session is session:
                await asyncio.sleep(0)
            process.connection.session._wait.set_result({"exit": 0, "error": None})
            self.assertEqual((await wait)["exit"], 0)
        finally:
            await process.close()
            await client.close()


class StdioHandleTests(unittest.IsolatedAsyncioTestCase):
    async def test_reader_keeps_one_stream_until_eof(self):
        from tangram.process.stdio import Reader

        class Process:
            async def read_stdio(self, **options):
                async def chunks():
                    for position, bytes_ in [(0, b"a"), (1, b"bc")]:
                        yield {
                            "stream": "stdout",
                            "stream_position": position,
                            "bytes": bytes_,
                        }

                return chunks()

        reader = Reader(Process(), "stdout")
        self.assertEqual(await reader.read(), b"a")
        self.assertEqual(await reader.read_all(), b"bc")
        self.assertIsNone(await reader.read())

    async def test_writer_write_all_closes_after_last_write(self):
        from tangram.process.stdio import Writer

        calls = []

        class Client:
            async def write_process_stdio(self, id, chunks, complete, **arg):
                async for chunk in chunks:
                    calls.append(chunk["bytes"])
                    complete(chunk)
                calls.append(None)

        class Process:
            id = "prc_test"
            location = None
            tokens = {}
            connection = None

            async def load(self):
                pass

        from unittest.mock import patch

        from tangram.client import client

        writer = Writer(Process())
        with patch.object(client, "write_process_stdio", Client().write_process_stdio):
            await writer.write_all(b"abc")
        self.assertEqual(calls, [b"abc", None])
        with self.assertRaises(BrokenPipeError):
            await writer.write(b"d")

    async def test_context_cancels_owned_process(self):
        calls = []

        class Client:
            async def cancel_process(self, id, options=None, **arg):
                calls.append((id, options if options is not None else arg))

        async with Process("prc_test", client=Client(), lease="lease_test"):
            pass
        self.assertEqual(calls[0][1]["lease"], "lease_test")

    async def test_completed_spawn_outcome_does_not_cancel_on_disposal(self):
        from unittest.mock import AsyncMock

        client = type("Client", (), {"cancel_process": AsyncMock()})()
        async with Process(
            "prc_test", client=client, lease="lease_test", outcome={"exit": 0}
        ):
            pass
        client.cancel_process.assert_not_awaited()

    async def test_waited_process_does_not_cancel_on_disposal(self):
        class Client:
            async def cancel_process(self, id, options=None, **arg):
                raise AssertionError("should not cancel a completed process")

        async with Process(
            "prc_test", client=Client(), lease="lease_test", outcome={"exit": 0}
        ) as process:
            await process.wait()
