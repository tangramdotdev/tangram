"""A duplex process connect session with independent stdio flow control."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import TYPE_CHECKING, Self, cast

if TYPE_CHECKING:
    from ...client.process.connect import ConnectArgObject, ServerResponseOutput
    from ...client.process.spawn import OutputObject
    from ...value import ValueType
    from ..outcome import ProcessOutcome
    from ..stdio import StdioChunk, Stream

from ...client import Client
from ...client import client as default_client
from ...config import validate_receiver
from ...error import Error
from ...http.flow import Receiver
from ..outcome import Outcome
from ..stdio import validate_output
from ..stdio.flow import capacity
from .channel import Channel


class Session:
    def __init__(self, client: Client):
        self.client = client
        self._spawn_output: OutputObject | None = None
        self._input: Channel[dict] = Channel(
            capacity(client.stdio) * (client.stdio["max_reads"] + 1) + 256
        )
        self._requests: dict[int, asyncio.Future] = {}
        self._pending: dict[int, str] = {}
        self._reads: dict[int, Channel[dict]] = {}
        self._next_id = 1
        self._initial_reads: dict[int, dict] = {}
        self._ready = False
        self._selected = asyncio.get_running_loop().create_future()
        self._outcome_received = False
        self._wait = asyncio.get_running_loop().create_future()
        self._receiver: asyncio.Task | None = None
        self._output = None
        self._closed = False
        self._error: Exception | None = None
        self._write_position = 0
        self._write_lock = asyncio.Lock()

    @classmethod
    async def connect(
        cls,
        process: str | dict,
        *,
        client: Client | None = None,
        mode="run",
        reads=None,
        **options,
    ):
        connection = cls(client or default_client)
        reads = {
            index + 1: ({"streams": [read]} if isinstance(read, str) else dict(read))
            for index, read in enumerate(reads or [])
        }
        if len(reads) > connection.client.stdio["max_reads"]:
            raise ValueError("too many process reads")
        connection._next_id = len(reads) + 1
        for id in reads:
            reads[id].setdefault("flow", connection.client.stdio)
            validate_receiver(connection.client.stdio, reads[id]["flow"])
            connection._reads[id] = Channel(capacity(reads[id]["flow"]))
        arg = cast(
            "ConnectArgObject",
            {"mode": mode, "process": process, "reads": reads, **options},
        )
        try:
            _, messages = await connection.client.connect_process(
                arg, connection._messages()
            )
            connection._output = messages
            connection._receiver = asyncio.create_task(connection._receive(messages))
            connection.output = await connection._selected
            connection._initial_reads = dict(reads)
            return connection
        except BaseException:
            await connection.close()
            raise

    @classmethod
    async def spawn(cls, arg: dict, **options):
        return await cls.connect(arg, **options)

    @property
    def output(self) -> OutputObject:
        if self._spawn_output is None:
            raise ValueError("the session has no spawn output")
        return self._spawn_output

    @output.setter
    def output(self, output: OutputObject) -> None:
        self._spawn_output = output

    @property
    def closed(self):
        return self._closed

    @property
    def id(self):
        return self.output["process"]

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def _messages(self):
        async for message in self._input:
            yield message

    async def _receive(self, messages):
        try:
            async for message in messages:
                # Yield between buffered events, as JavaScript iterator awaits do.
                await asyncio.sleep(0)
                kind, value = message["kind"], message["value"]

                if kind == "ack":
                    continue
                if kind == "response":
                    id = value["id"]
                    self._pending.pop(id, None)
                    read = self._reads.get(id)
                    if read is not None:
                        if value.get("error") is not None:
                            read.close(Error.from_data(value["error"]))
                            self._reads.pop(id)
                            self._input.push({"kind": "ack", "value": {"id": id}}, True)
                        elif (value.get("output") or {}).get("kind") == "read":
                            read.push({"kind": "response", "value": value})
                        else:
                            raise ValueError("expected a process read response")
                        continue
                    self._input.push({"kind": "ack", "value": {"id": id}}, True)
                    pending = self._requests.pop(id, None)
                    if pending is None or pending.done():
                        continue
                    if value.get("error") is not None:
                        pending.set_exception(Error.from_data(value["error"]))
                    elif value.get("output") is not None:
                        pending.set_result(value["output"])
                    else:
                        pending.set_exception(ValueError("invalid process response"))
                    continue
                if (
                    value["kind"] == "progress"
                    and value["value"]["kind"] == "output"
                    and not self._selected.done()
                ):
                    self._selected.set_result(value["value"]["value"])
                if value["kind"] == "outcome":
                    self._outcome_received = True
                    if not self._wait.done():
                        self._wait.set_result(Outcome.from_data(value["value"]))
                elif value["kind"] == "read":
                    read = value["value"]
                    if read["id"] in self._reads:
                        self._reads[read["id"]].push(read["event"])
            self._finish()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._finish(error)

    def _finish(self, error=None):
        if not self._selected.done():
            self._selected.set_exception(
                error
                or ConnectionError("the process connection closed before selection")
            )
            self._selected.exception()
        if self._closed:
            return
        self._closed = True
        self._error = error
        self._pending.clear()
        self._input.close()
        for pending in self._requests.values():
            if not pending.done():
                pending.set_exception(
                    error or ConnectionError("the process connection closed")
                )
                pending.exception()
        self._requests.clear()
        for read in self._reads.values():
            read.close(error)
        if not self._wait.done():
            if error is None:
                self._wait.set_result(None)
            else:
                self._wait.set_exception(error)
                self._wait.exception()

    async def _send_request(self, arg, id):
        if self._closed:
            raise self._error or ConnectionError("the process connection closed")
        kind = arg["kind"]
        limit = (
            self.client.stdio["limits"]["messages"] + 1
            if kind == "write"
            else self.client.stdio["max_reads"]
            if kind == "read"
            else 64
        )
        count = sum(pending == kind for pending in self._pending.values())
        if count >= limit or (
            kind == "read" and len(self._reads) > self.client.stdio["max_reads"]
        ):
            raise RuntimeError("too many pending process requests of this kind")
        if kind != "close":
            self._pending[id] = kind
        try:
            if not self._input.push(
                {"kind": "request", "value": {"id": id, "arg": arg}}, kind == "detach"
            ):
                raise ConnectionError("the process connection closed")
        except BaseException:
            self._pending.pop(id, None)
            raise

    async def _request(self, kind, value=None) -> ServerResponseOutput:
        if self._closed:
            raise self._error or ConnectionError("the process connection closed")
        id = self._next_id
        self._next_id += 1
        pending = asyncio.get_running_loop().create_future()
        self._requests[id] = pending
        arg = {"kind": kind, **({"value": value} if value is not None else {})}
        try:
            await self._send_request(arg, id)
            self.ready()
        except BaseException as error:
            self._requests.pop(id, None)
            pending.set_exception(error)
        return await pending

    def ready(self):
        if not self._ready:
            self._ready = True
            self._input.push({"kind": "notification", "value": {"kind": "ready"}})

    def has_initial(self, arg):
        arg = {"flow": self.client.stdio, **arg}
        return any(
            matches_read(initial, arg) for initial in self._initial_reads.values()
        )

    async def read(self, streams: list[Stream], **options) -> AsyncIterator[StdioChunk]:
        arg = {"flow": self.client.stdio, "streams": streams, **options}
        validate_receiver(self.client.stdio, arg["flow"])
        initial = next(
            (id for id, read in self._initial_reads.items() if matches_read(read, arg)),
            None,
        )
        self._initial_reads.pop(initial, None)
        if initial is None:
            id = self._next_id
            self._next_id += 1
            queue = self._reads[id] = Channel(capacity(arg["flow"]))
            try:
                await self._send_request({"kind": "read", "value": arg}, id)
            except BaseException:
                self._reads.pop(id, None)
                raise
        else:
            id = initial
            queue = self._reads[id]
        self.ready()
        receiver = Receiver(arg["flow"]["limits"])
        position = None
        finished = False
        forward = options.get("length") is None or options["length"] >= 0
        try:
            while True:
                result = await queue.next()
                if result["done"]:
                    raise ConnectionError("the process connection closed")
                event = result["value"]
                if event["kind"] == "response":
                    response = event["value"]
                    if response.get("error") is not None:
                        raise Error.from_data(response["error"])
                    output = response["output"]
                    if output["kind"] != "read":
                        raise ValueError("expected a process read response")
                    validate_output(
                        output["value"], streams, position or 0, len(streams) > 1
                    )
                    consumption = receiver.flush()
                    if consumption is not None:
                        self._input.push(
                            {
                                "kind": "notification",
                                "value": {
                                    "kind": "read_consumption",
                                    "value": {"id": id, "consumption": consumption},
                                },
                            },
                            True,
                        )
                    self._input.push({"kind": "ack", "value": {"id": id}}, True)
                    finished = True
                    return
                if event["kind"] == "position":
                    position = event["value"]["position"]
                    continue
                if event["kind"] != "chunk":
                    raise ValueError("invalid process read event")
                chunk = event["value"]
                if chunk["stream"] not in streams:
                    raise ValueError("unexpected process stdio stream")
                bytes_ = chunk["bytes"]
                receiver.receive(len(bytes_))
                start = chunk[
                    "combined_position" if len(streams) > 1 else "stream_position"
                ]
                end = start + len(bytes_)
                if position is not None and (start if forward else end) != position:
                    raise ValueError("encountered a gap in the process stdio stream")
                position = end if forward else start
                yield chunk
                consumption = receiver.consume(len(bytes_))
                if consumption is not None:
                    self._input.push(
                        {
                            "kind": "notification",
                            "value": {
                                "kind": "read_consumption",
                                "value": {"id": id, "consumption": consumption},
                            },
                        },
                        True,
                    )
        finally:
            self._reads.pop(id, None)
            self._pending.pop(id, None)
            if not finished and not self._closed and self._error is None:
                close_id = self._next_id
                self._next_id += 1
                await self._send_request({"kind": "close", "value": id}, close_id)

    async def wait(self) -> ProcessOutcome[ValueType]:
        self.ready()
        outcome = await asyncio.shield(self._wait)
        if outcome is None:
            raise ConnectionError("the process connection closed before completion")
        return outcome

    async def signal(self, signal: int | dict):
        response = await self._request(
            "signal", signal if isinstance(signal, dict) else {"signal": signal}
        )
        if response["kind"] != "signal":
            raise ValueError("expected a signal response")

    async def cancel(self, arg=None):
        response = await self._request(
            "cancel", arg if arg is not None else {"lease": self.output["lease"]}
        )
        if response["kind"] != "cancel":
            raise ValueError("expected a cancel response")

    async def tty(self, arg):
        response = await self._request("tty", arg)
        if response["kind"] != "tty":
            raise ValueError("expected a tty response")

    async def set_tty_size(self, **size):
        await self.tty({"size": size})

    def write(self, arg):
        output: Channel[dict] = Channel(capacity(self.client.stdio))
        previous = None

        async def request(message):
            value = {
                "data": message["value"]["arg"],
                **{key: arg[key] for key in ("location", "tokens") if key in arg},
            }
            response = await self._request("write", value)
            if response["kind"] != "write":
                raise ValueError("expected a write response")
            return {
                "kind": "response",
                "value": {
                    "error": None,
                    "id": message["value"]["id"],
                    "output": response["value"],
                },
            }

        async def ordered(task, before):
            try:
                if before is not None:
                    await before
                output.push(await task)
            except Exception as error:
                output.close(error)

        def push(message):
            nonlocal previous
            if message["kind"] == "ack":
                return True
            task = asyncio.create_task(request(message))
            task.add_done_callback(
                lambda task: task.exception() if not task.cancelled() else None
            )
            previous = asyncio.create_task(ordered(task, previous))
            return True

        return SimpleNamespace(
            input=SimpleNamespace(push=push, close=output.close), output=output
        )

    async def detach(self) -> None:
        if not self._outcome_received:
            try:
                response = await self._request("detach")
            except Exception:
                if self._outcome_received:
                    return
                raise
            if response["kind"] != "detach":
                raise ValueError("expected a detach response")
        self._input.close()

    async def close(self) -> None:
        self._finish()
        if self._receiver is not None:
            self._receiver.cancel()
            await asyncio.gather(self._receiver, return_exceptions=True)
        if self._output is not None:
            await self._output.aclose()


def matches_read(initial, arg):
    return (
        initial.get("flow") == arg.get("flow")
        and initial["streams"] == arg["streams"]
        and (initial.get("position") or 0) == (arg.get("position") or 0)
        and all(
            initial.get(key) == arg.get(key) for key in ("length", "size", "timeout")
        )
    )
