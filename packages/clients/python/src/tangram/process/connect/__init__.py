"""Process connections that reconnect sessions for resumable operations."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from ...client import Client
    from ...value import ValueType
    from ..outcome import ProcessOutcome
    from ..stdio import StdioChunk, StdioWriteOutput

from ...error import Error
from ...location import Arg as LocationArg
from .session import Session


class Connection:
    """Reconnect a process session while preserving the process's lease."""

    def __init__(self, session: Session):
        self.initial = session
        self.session = session
        self._opening = None
        self.closed = False

    @classmethod
    async def open(cls, process, **options) -> Self:
        session = await Session.connect(process, **options)
        session.ready()
        connection = cls(session)
        if options.get("mode") == "spawn":
            await connection.close()
        return connection

    async def ensure_session(self, read=None) -> Session:
        if self.closed:
            raise ConnectionError("the process connection was closed")
        if not self.session._closed and self.session._error is None:
            return self.session
        if self._opening is None:
            output = self.session.output
            process_id = output["process"]
            if not isinstance(process_id, str):
                raise ValueError("expected a sandboxed process")

            async def open():
                location = output.get("location")
                session = await Session.connect(
                    process_id,
                    client=self.session.client,
                    lease=output.get("lease"),
                    location=None
                    if location is None
                    else LocationArg.from_location(location),
                    tokens=output.get("tokens") or {},
                    reads=[] if read is None else [read],
                )
                if self.closed:
                    await session.close()
                    raise ConnectionError("the process connection was closed")
                previous = self.session
                self.session = session
                await previous.close()
                return session

            self._opening = asyncio.create_task(open())
        opening = self._opening
        try:
            return await asyncio.shield(opening)
        finally:
            if opening.done() and self._opening is opening:
                self._opening = None

    async def wait(self) -> ProcessOutcome[ValueType]:
        while True:
            try:
                return await self.session.wait()
            except Exception as error:
                if self.closed or isinstance(error, Error):
                    raise
                await self.ensure_session()

    async def request(self, kind, value=None):
        # Control operations are never replayed after an ambiguous response.
        return await (await self.ensure_session())._request(kind, value)

    async def signal(self, arg):
        await (await self.ensure_session()).signal(arg)

    async def cancel(self, arg):
        await (await self.ensure_session()).cancel(arg)

    async def tty(self, arg):
        await (await self.ensure_session()).tty(arg)

    def stdio_client(self):
        async def read(id, options=None, **arg):
            return self.read({**(options or {}), **arg})

        async def write(id, input, options=None, *, complete=None, **arg):
            options = {**(options or {}), **arg}
            position = 0
            positions = {stream: 0 for stream in options["streams"]}
            async for chunk in input:
                if chunk["stream"] not in positions:
                    raise ValueError("unexpected process stdio stream")
                output = await self.write(
                    {"kind": "chunk", "value": chunk}, options=options
                )
                if output["length"] != len(chunk["bytes"]):
                    raise BrokenPipeError(
                        "the process stdio closed before writing the chunk"
                    )
                position = chunk["combined_position"] + output["length"]
                positions[chunk["stream"]] = chunk["stream_position"] + output["length"]
                if complete is not None:
                    complete(chunk)
                if output["closed"]:
                    return
            await self.write(
                {
                    "kind": "end",
                    "value": {
                        "combined_position": position,
                        "stream_positions": positions,
                    },
                },
                options=options,
            )

        async def tty(id, options=None, **arg):
            await self.tty({**(options or {}), **arg})

        return SimpleNamespace(
            set_process_tty_size=tty,
            try_read_process_stdio=read,
            write_process_stdio=write,
        )

    async def read(self, arg) -> AsyncIterator[StdioChunk]:
        arg = dict(arg)
        streams = arg.pop("streams")
        forward = arg.get("length") is None or arg["length"] >= 0
        cursor = arg.get("position", 0)
        cursor = cursor if type(cursor) is int else None
        while True:
            initial_arg = {"streams": streams, **arg}
            session = (
                self.initial
                if self.initial.has_initial(initial_arg)
                else await self.ensure_session(initial_arg)
            )
            try:
                async for chunk in session.read(streams, **arg):
                    chunk: StdioChunk = {**chunk}
                    bytes_ = chunk["bytes"]
                    key = "combined_position" if len(streams) > 1 else "stream_position"
                    start = chunk[key]
                    end = start + len(bytes_)
                    if cursor is not None:
                        if (forward and start > cursor) or (
                            not forward and end < cursor
                        ):
                            raise ValueError(
                                "encountered a gap in the process stdio stream"
                            )
                        if forward and start < cursor:
                            overlap = min(len(bytes_), cursor - start)
                            bytes_ = bytes_[overlap:]
                            chunk["combined_position"] += overlap
                            chunk["stream_position"] += overlap
                        elif not forward and end > cursor:
                            bytes_ = bytes_[: max(0, cursor - start)]
                    if not bytes_:
                        continue
                    chunk["bytes"] = bytes_
                    cursor = chunk[key] + (len(bytes_) if forward else 0)
                    arg["position"] = cursor
                    if arg.get("length") is not None:
                        arg["length"] += (-1 if forward else 1) * len(bytes_)
                    yield chunk
                return
            except (OSError, ConnectionError):
                if self.closed:
                    raise
                await self.ensure_session({"streams": streams, **arg})

    async def write(self, data, *, options=None) -> StdioWriteOutput:
        while True:
            session = await self.ensure_session()
            try:
                output = await session._request(
                    "write",
                    {
                        "data": data,
                        **{
                            key: options[key]
                            for key in ("location", "tokens")
                            if options is not None and key in options
                        },
                    },
                )
                if output["kind"] != "write":
                    raise ValueError("expected a process write response")
                return output["value"]
            except (OSError, ConnectionError):
                if self.closed:
                    raise
                # Replay the original positions to deduplicate a committed write.
                await self.ensure_session()

    async def close_initial(self, stream):
        initial = self.initial
        id = next(
            (
                id
                for id, arg in initial._initial_reads.items()
                if arg["streams"] == [stream]
            ),
            None,
        )
        if id is not None:
            initial._initial_reads.pop(id)
            initial._reads.pop(id, None)
            if not initial._closed and initial._error is None:
                await initial._request("close", id)

    async def detach(self) -> None:
        if self.closed:
            return
        if not self.session._closed and self.session._error is None:
            await self.session.detach()
        await self.close()

    async def close(self) -> None:
        self.closed = True
        await self.session.close()
        if self.initial is not self.session:
            await self.initial.close()
        if self._opening is not None:
            self._opening.cancel()
            await asyncio.gather(self._opening, return_exceptions=True)


async def connect(id, options=None, *, client: Client | None = None, **kwargs):
    """Connect to an existing process with its referrer options and initial reads."""
    from .. import Process

    return await Process.connect(id, client=client, **{**(options or {}), **kwargs})


async def spawn(arg, options=None, mode="spawn", *, client: Client | None = None):
    """Dispatch a prepared spawn argument to the sandboxed or local backend."""
    from ..spawn import spawn_sandboxed, spawn_unsandboxed

    if "sandbox" not in arg:
        return await spawn_unsandboxed(arg, options or {}, client=client)
    return await spawn_sandboxed(arg, options or {}, mode, client=client)
