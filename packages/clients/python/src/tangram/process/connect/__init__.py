"""Process connections that reconnect sessions for resumable operations."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from ...client import Client
    from ...client.process.spawn import OutputObject
    from ...value import ValueType
    from ..outcome import ProcessOutcome

from ...client import is_transport_error
from ...error import Error
from ...location import Arg as LocationArg
from .session import Session


class Connection:
    """Reconnect a process session while preserving the process's lease."""

    def __init__(self, session: Session):
        self.session = session
        self._opening = None
        self._closing = asyncio.Event()
        self.closed = False

    @classmethod
    async def open(cls, process, **options) -> Self:
        session = await Session.connect(process, **options)
        session.ready()
        connection = cls(session)
        if options.get("mode") == "spawn":
            await connection.close()
        return connection

    @property
    def output(self) -> OutputObject:
        return self.session.output

    async def ensure_session(self) -> Session:
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
                if self.closed or (
                    isinstance(error, Error) and not is_transport_error(error)
                ):
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

    @staticmethod
    def nonce_stdio_client(connection, client):
        """Get a stdio client for a process located by its connect's nonce.

        Its reads end when the connection closes once it opens.
        """

        async def read(process, arg=None, **options):
            stream = await client.try_read_process_stdio(process, arg, **options)
            try:
                opened = await connection
            except Exception:
                opened = None
            if stream is None or opened is None:
                return stream
            return BoundStream(stream, opened._closing)

        return SimpleNamespace(
            set_process_tty_size=client.set_process_tty_size,
            try_read_process_stdio=read,
            write_process_stdio=client.write_process_stdio,
        )

    def stdio_client(self):
        client = self.session.client

        async def read(id, arg=None, **options):
            stream = await client.try_read_process_stdio(id, arg, **options)
            if stream is None:
                return None
            return BoundStream(stream, self._closing)

        async def tty(id, options=None, **arg):
            await self.tty({**(options or {}), **arg})

        return SimpleNamespace(
            set_process_tty_size=tty,
            try_read_process_stdio=read,
            write_process_stdio=client.write_process_stdio,
        )

    async def detach(self) -> None:
        if self.closed:
            return
        if not self.session._closed and self.session._error is None:
            await self.session.detach()
        await self.close()

    async def close(self) -> None:
        self.closed = True
        self._closing.set()
        await self.session.close()
        if self._opening is not None:
            self._opening.cancel()
            await asyncio.gather(self._opening, return_exceptions=True)


class BoundStream:
    def __init__(self, stream, closing: asyncio.Event):
        self._stream = stream
        self._closing = closing

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self):
        async def step():
            try:
                return True, await anext(self._stream)
            except StopAsyncIteration:
                return False, None

        next_ = asyncio.ensure_future(step())
        closing = asyncio.ensure_future(self._closing.wait())
        try:
            await asyncio.wait({next_, closing}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            closing.cancel()
        if not next_.done():
            next_.cancel()
            await asyncio.gather(next_, return_exceptions=True)
            await self.aclose()
            raise StopAsyncIteration
        ok, chunk = next_.result()
        if not ok:
            raise StopAsyncIteration
        return chunk

    async def aclose(self) -> None:
        close = getattr(self._stream, "aclose", None)
        if close is not None:
            await close()


async def connect(id, options=None, *, client: Client | None = None, **kwargs):
    """Connect to an existing process with its referrer options."""
    from .. import Process

    return await Process.connect(id, client=client, **{**(options or {}), **kwargs})


async def spawn(arg, options=None, mode="spawn", *, client: Client | None = None):
    """Dispatch a prepared spawn argument to the sandboxed or local backend."""
    from ..spawn import spawn_sandboxed, spawn_unsandboxed

    if "sandbox" not in arg:
        return await spawn_unsandboxed(arg, options or {}, client=client)
    return await spawn_sandboxed(arg, options or {}, mode, client=client)
