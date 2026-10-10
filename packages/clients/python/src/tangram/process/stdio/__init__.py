"""Process stdio streams, including cursor-based transport reconnection."""

from __future__ import annotations

import asyncio
import base64
import builtins
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Literal, NotRequired, Self, TypedDict

from ...client.process.stdio.read import validate_output as validate_output
from ...config import Config

if TYPE_CHECKING:
    from ...client import Client
    from ...client.process.stdio.write import KeywordOptions as WriteOptions
    from ...location import ArgObject as LocationArgObject


type Stream = Literal["stdin", "stdout", "stderr"]


class StdioChunk(TypedDict):
    bytes: bytes
    combined_position: int
    stream: Stream
    stream_position: int
    timestamp: NotRequired[int | float | None]


class StdioEnd(TypedDict):
    combined_position: int
    stream_positions: dict[Stream, int]


class StdioChunkEvent(TypedDict):
    kind: Literal["chunk"]
    value: StdioChunk


class StdioPosition(TypedDict):
    length: int | None
    position: int


class StdioPositionEvent(TypedDict):
    kind: Literal["position"]
    value: StdioPosition


type StdioReadEvent = StdioChunkEvent | StdioPositionEvent


class StdioEndOutput(TypedDict):
    kind: Literal["end"]
    value: StdioEnd


class StdioLimitPosition(TypedDict):
    position: int


class StdioLimitOutput(TypedDict):
    kind: Literal["limit", "timeout"]
    value: StdioLimitPosition


type StdioReadOutput = StdioEndOutput | StdioLimitOutput


class ReadArgObject(TypedDict):
    flow: NotRequired[Config]
    streams: list[Stream]
    length: NotRequired[int | None]
    location: NotRequired[LocationArgObject | None]
    position: NotRequired[int | str | None]
    size: NotRequired[int | None]
    timeout: NotRequired[int | float | None]
    tokens: NotRequired[dict[str, list[str]] | None]


class WriteArgObject(TypedDict):
    data: StdioChunkEvent | StdioEndOutput
    location: NotRequired[LocationArgObject | None]
    tokens: NotRequired[dict[str, list[str]] | None]


class StdioWriteOutput(TypedDict):
    closed: bool
    length: int


class Chunk:
    @staticmethod
    def from_data(data):
        return {**data, "bytes": base64.b64decode(data["bytes"])}

    @staticmethod
    def to_data(chunk):
        return {**chunk, "bytes": base64.b64encode(chunk["bytes"]).decode()}


class End:
    @staticmethod
    def from_data(data):
        return {
            "combined_position": data["combined_position"],
            "stream_positions": data["stream_positions"],
        }

    to_data = from_data


class Read:
    class Output:
        @staticmethod
        def from_data(data):
            return (
                {"kind": "end", "value": End.from_data(data["value"])}
                if data["kind"] == "end"
                else data
            )

        @staticmethod
        def validate(output, streams, position):
            validate_output(output, streams, position, len(streams) > 1)


class Write:
    class Data:
        @staticmethod
        def to_data(data):
            value = (
                Chunk.to_data(data["value"])
                if data["kind"] == "chunk"
                else End.to_data(data["value"])
            )
            return {"kind": data["kind"], "value": value}


class Reader:
    """A process stream reader retaining its cursor between calls."""

    def __init__(self, process=None, stream="stdout", *, fd=None, unavailable=False):
        self.process = process
        self.stream = stream
        self.fd = fd
        self.available = not unavailable
        self.position = 0
        self._iterator = None
        self._buffer = b""
        self._ended = False

    def set_process(self, process):
        self.process = process

    def __aiter__(self) -> AsyncGenerator[builtins.bytes, None]:
        return self._iterate()

    async def _iterate(self) -> AsyncGenerator[builtins.bytes, None]:
        if not self.available:
            raise ValueError(f"{self.stream} is not available")
        if self.fd is not None:
            from ... import host

            while True:
                chunk = await host.read(self.fd, 4096)
                if chunk is None:
                    break
                if chunk:
                    yield chunk
            fd, self.fd = self.fd, None
            self.process = None
            await host.close(fd)
            return
        if self.process is None:
            raise ValueError(f"{self.stream} is not available")
        chunks = await self.process.read_stdio(
            streams=[self.stream], position=self.position
        )
        try:
            async for chunk in chunks:
                if chunk["stream"] != self.stream:
                    raise ValueError("invalid process stdio stream")
                self.position = chunk["stream_position"] + len(chunk["bytes"])
                if chunk["bytes"]:
                    yield chunk["bytes"]
        finally:
            await chunks.aclose()

    async def read(self, length: int | None = None) -> builtins.bytes | None:
        if self._ended:
            return None if length is None else b""
        if length is None:
            if self._buffer:
                output, self._buffer = self._buffer, b""
                return output
            if self._iterator is None:
                self._iterator = self._iterate()
            try:
                return await anext(self._iterator)
            except StopAsyncIteration:
                self._ended = True
                return None
        if length < 0:
            raise ValueError("invalid read length")
        if self._iterator is None:
            self._iterator = self._iterate()
        while len(self._buffer) < length:
            try:
                self._buffer += await anext(self._iterator)
            except StopAsyncIteration:
                self._ended = True
                break
        output, self._buffer = self._buffer[:length], self._buffer[length:]
        return output

    async def read_all(self) -> builtins.bytes:
        chunks = []
        if self._buffer:
            chunks.append(self._buffer)
            self._buffer = b""
        while (chunk := await self.read()) is not None:
            chunks.append(chunk)
        return b"".join(chunks)

    async def bytes(self) -> builtins.bytes:
        return await self.read_all()

    async def read_all_to_string(self) -> str:
        return (await self.read_all()).decode(errors="replace")

    async def text(self) -> str:
        return await self.read_all_to_string()

    async def close(self) -> None:
        self._ended = True
        if self._iterator is not None:
            await self._iterator.aclose()
        elif self.process is not None and self.process.connection is not None:
            await self.process.connection.close_initial(self.stream)
        if self.fd is not None:
            from ... import host

            await host.close(self.fd)
            self.fd = None


class Writer:
    """A process stdin writer sharing the process's ordered write cursor."""

    def __init__(
        self, process=None, stream: Stream = "stdin", *, fd=None, unavailable=False
    ):
        self.process = process
        self.stream = stream
        self.fd = fd
        self.available = not unavailable
        self.closed = False
        self._input = None
        self._input_promise = None
        self._task = None

    def set_process(self, process):
        self.process = process

    async def write(self, bytes_: bytes | bytearray | memoryview) -> int:
        if not isinstance(bytes_, (bytes, bytearray, memoryview)):
            raise ValueError("expected stdio bytes")
        if not self.available:
            raise ValueError(f"{self.stream} is not available")
        if self.closed:
            raise BrokenPipeError("the process stdio writer is closed")
        if self.fd is None and self.process is None:
            raise ValueError(f"{self.stream} is not available")
        if len(bytes_) == 0:
            return 0
        if self.fd is not None:
            from ... import host

            await host.write(self.fd, builtins.bytes(bytes_))
        elif self.process is not None:
            if isinstance(self.process.id, int):
                raise ValueError(f"{self.stream} is not available")
            queue = await self._input_with_process()
            return await queue.write(bytes_)
        else:
            raise ValueError(f"{self.stream} is not available")
        return len(bytes_)

    async def write_all(self, bytes_: bytes | bytearray | memoryview) -> None:
        if not isinstance(bytes_, (bytes, bytearray, memoryview)):
            raise ValueError("expected stdio bytes")
        position = 0
        while position < len(bytes_):
            count = await self.write(bytes_[position:])
            if count == 0:
                raise BrokenPipeError("failed to write stdin")
            position += count
        await self.close()

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            if self.fd is not None:
                from ... import host

                await host.close(self.fd)
                self.fd = None
            elif (
                self.available
                and self.process is not None
                and isinstance(self.process.id, str)
            ):
                queue = await self._input_with_process()
                self.process = None
                queue.close()
                assert self._task is not None
                await self._task

    async def _create_input(self):
        from ...client import client as default_client

        process = self.process
        if process is None or not isinstance(process.id, str):
            raise ValueError(f"{self.stream} is not available")
        location = process.location
        if location is None and process.connection is None:
            await process.load()
            location = process.location
        queue = WriteQueue()
        client = (
            process.connection.stdio_client()
            if process.connection is not None
            else default_client
        )
        arg: WriteOptions = {"streams": [self.stream], "tokens": process.tokens}
        if location is not None:
            arg["location"] = location

        async def run():
            try:
                await client.write_process_stdio(
                    process.id,
                    write_chunks(queue, self.stream),
                    complete=queue.complete,
                    **arg,
                )
                queue.finish()
            except BaseException as error:
                queue.fail(error)
                raise

        self._input = queue
        self._task = asyncio.create_task(run())
        self._task.add_done_callback(
            lambda task: None if task.cancelled() else task.exception()
        )
        return queue

    async def _input_with_process(self):
        if self._input is not None:
            return self._input
        if self._input_promise is None:
            self._input_promise = asyncio.create_task(self._create_input())
        promise = self._input_promise
        try:
            return await asyncio.shield(promise)
        finally:
            if promise.done() and self._input_promise is promise:
                self._input_promise = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()


class WriteQueue:
    def __init__(self):
        self.closed = False
        self.error = None
        self.values = asyncio.Queue()
        self.pending = {}

    def chunk(self, request, position: int, stream: Stream) -> StdioChunk:
        chunk: StdioChunk = {
            "bytes": request[0],
            "combined_position": position,
            "stream": stream,
            "stream_position": position,
        }
        self.pending[id(chunk)] = request
        return chunk

    def complete(self, chunk):
        request = self.pending.pop(id(chunk), None)
        if request is not None and not request[1].done():
            request[1].set_result(len(chunk["bytes"]))

    def close(self):
        self.closed = True
        self.values.put_nowait(None)

    def fail(self, error):
        if self.error is not None:
            return
        self.error = error
        self.closed = True
        while not self.values.empty():
            request = self.values.get_nowait()
            if request is not None and not request[1].done():
                request[1].set_exception(error)
        for request in self.pending.values():
            if not request[1].done():
                request[1].set_exception(error)
        self.pending.clear()
        self.values.put_nowait(None)

    def finish(self):
        if not self.values.empty() or self.pending or not self.closed:
            self.fail(BrokenPipeError("stdin closed before the write completed"))

    async def write(self, bytes_: bytes | bytearray | memoryview) -> int:
        if self.error is not None:
            raise self.error
        if self.closed:
            raise BrokenPipeError("stdin is closed")
        future = asyncio.get_running_loop().create_future()
        self.values.put_nowait((bytes_, future))
        return await future

    def __aiter__(self):
        return self

    async def __anext__(self):
        request = await self.values.get()
        if request is None:
            if self.error is not None:
                raise self.error
            raise StopAsyncIteration
        return request


async def write_chunks(input, stream: Stream) -> AsyncGenerator[StdioChunk, None]:
    position = 0
    async for request in input:
        chunk = input.chunk(request, position, stream)
        position += len(chunk["bytes"])
        yield chunk


async def task(
    id,
    location,
    tokens,
    stdin,
    stdout,
    stderr,
    tty,
    connection=None,
    *,
    client: Client | None = None,
):
    """Forward inherited stdio and terminal resize events until output ends."""
    from ... import host
    from ...client import client as default_client

    client = (
        connection.stdio_client()
        if connection is not None
        else client or default_client
    )
    stdin_closing = False
    stdin_error = None
    resize_error = None
    output_error = None
    stopper = await host.stopper_open() if stdin is not None else None
    listener = host.listen_signal("sigwinch") if tty else None

    async def input_task():
        nonlocal stdin_error
        try:
            await stdin_task(id, location, tokens, stdin, stopper, client)
        except Exception as error:
            if not stdin_closing:
                stdin_error = error

    async def resize_task():
        nonlocal resize_error
        try:
            await sigwinch_task(id, location, tokens, listener, client)
        except Exception as error:
            resize_error = error

    input_future = asyncio.create_task(input_task()) if stopper is not None else None
    resize_future = asyncio.create_task(resize_task()) if listener is not None else None
    try:
        try:
            await stdout_stderr_task(id, location, tokens, stdout, stderr, client)
        except Exception as error:
            output_error = error
        if stopper is not None:
            stdin_closing = True
            await host.stopper_stop(stopper)
    finally:
        try:
            await cleanup(stopper, listener)
        finally:
            pending = [
                future for future in (input_future, resize_future) if future is not None
            ]
            if pending:
                await asyncio.gather(*pending)
    for error in (stdin_error, resize_error, output_error):
        if error is not None:
            raise error


async def cleanup(stopper, listener) -> None:
    from ... import host

    try:
        if listener is not None:
            await listener.close()
    finally:
        if stopper is not None:
            await host.stopper_close(stopper)


async def stdin_task(id, location, tokens, stdin, stopper, client) -> None:
    from ... import host

    raw = stdin == "tty" and host.is_foreground_controlling_tty(0)
    if raw:
        await host.enable_raw_mode(0)
    error = None
    try:

        async def input():
            position = 0
            while True:
                bytes_ = await host.read(0, 4096, stopper)
                if bytes_ is None:
                    return
                if not bytes_:
                    continue
                yield {
                    "bytes": bytes_,
                    "combined_position": position,
                    "stream": "stdin",
                    "stream_position": position,
                }
                position += len(bytes_)

        arg = {"streams": ["stdin"], "tokens": tokens}
        if location is not None:
            arg["location"] = location
        await client.write_process_stdio(id, input(), **arg)
    except Exception as caught:
        error = caught
    finally:
        if raw:
            try:
                await host.disable_raw_mode(0)
            except Exception as caught:
                if error is None:
                    error = caught
    if error is not None:
        raise error


async def stdout_stderr_task(id, location, tokens, stdout, stderr, client) -> None:
    from ... import host

    streams = [
        stream
        for stream, mode in (("stdout", stdout), ("stderr", stderr))
        if mode is not None
    ]
    if not streams:
        return
    arg = {"streams": streams, "tokens": tokens}
    if location is not None:
        arg["location"] = location
    iterator = await client.try_read_process_stdio(id, arg)
    if iterator is None:
        return
    try:
        async for chunk in iterator:
            await host.write(1 if chunk["stream"] == "stdout" else 2, chunk["bytes"])
    finally:
        if hasattr(iterator, "aclose"):
            await iterator.aclose()


async def sigwinch_task(id, location, tokens, listener, client) -> None:
    from ... import host

    async for _ in listener:
        size = host.get_tty_size()
        if size is None:
            continue
        arg = {"size": size, "tokens": tokens}
        if location is not None:
            arg["location"] = location
        await client.set_process_tty_size(id, arg)


class Stdio:
    Chunk = Chunk
    End = End
    Read = Read
    Write = Write
    Reader = Reader
    Writer = Writer
    Stream = Literal["stdin", "stdout", "stderr"]
