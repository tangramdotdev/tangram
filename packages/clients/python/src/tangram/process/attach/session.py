"""A duplex process attach session."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Self, cast

if TYPE_CHECKING:
    from ...client.process.attach import AttachArgObject, ServerResponseOutput
    from ...client.process.spawn import OutputObject
    from ...value import ValueType
    from ..outcome import ProcessOutcome

from ...client import Client
from ...client import client as default_client
from ...error import Error
from ..outcome import Outcome
from .channel import Channel


class Session:
    def __init__(self, client: Client):
        self.client = client
        self._spawn_output: OutputObject | None = None
        self._input: Channel[dict] = Channel(client.process["attach"]["max_messages"])
        self._requests: dict[int, asyncio.Future] = {}
        self._pending: dict[int, str] = {}
        self._next_id = 1
        self._ready = False
        self._selected = asyncio.get_running_loop().create_future()
        self._outcome_received = False
        self._wait = asyncio.get_running_loop().create_future()
        self._receiver: asyncio.Task | None = None
        self._output = None
        self._closed = False
        self._error: Exception | None = None

    @classmethod
    async def attach(
        cls,
        process: str | dict,
        *,
        client: Client | None = None,
        mode="run",
        **options,
    ):
        connection = cls(client or default_client)
        arg = cast(
            "AttachArgObject",
            {"mode": mode, "process": process, **options},
        )
        try:
            _, messages = await connection.client.attach_process(
                arg, connection._messages()
            )
            connection._output = messages
            connection._receiver = asyncio.create_task(connection._receive(messages))
            connection.output = await connection._selected
            return connection
        except BaseException:
            await connection.close()
            raise

    @classmethod
    async def spawn(cls, arg: dict, **options):
        return await cls.attach(arg, **options)

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
        if len(self._pending) >= self.client.process["attach"]["max_requests"]:
            raise RuntimeError("too many pending process requests")
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
