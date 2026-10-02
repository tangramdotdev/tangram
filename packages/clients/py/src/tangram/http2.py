"""A duplex HTTP/2 transport using asyncio and hyper-h2."""

import asyncio
import json
import ssl
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass, field
from urllib.parse import unquote, urlsplit

from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import (
    ConnectionTerminated,
    DataReceived,
    RemoteSettingsChanged,
    ResponseReceived,
    StreamEnded,
    StreamReset,
    TrailersReceived,
    WindowUpdated,
)

from .http import Request, Response


@dataclass
class Stream:
    headers: asyncio.Future[tuple[int, dict[str, str]]]
    # The receive window bounds data buffered until the consumer acknowledges it.
    chunks: asyncio.Queue[tuple[bytes, int] | BaseException | None] = field(
        default_factory=asyncio.Queue
    )
    writer: asyncio.Task[None] | None = None
    ended: bool = False


class Session:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        scheme: str,
        authority: str,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.scheme = scheme
        self.authority = authority
        self.connection = H2Connection(
            H2Configuration(client_side=True, header_encoding="utf-8")
        )
        self.streams: dict[int, Stream] = {}
        self.changed = asyncio.Event()
        self.closed = False
        self.connection.initiate_connection()
        self._flush()
        self.reader_task = asyncio.create_task(self._read())

    @classmethod
    async def connect(cls, url: str) -> "Session":
        if url.startswith("http+unix://"):
            path = unquote(url.removeprefix("http+unix://"))
            reader, writer = await asyncio.open_unix_connection(path)
            return cls(reader, writer, "http", "localhost")
        uri = urlsplit(url)
        if uri.scheme not in ("http", "https") or uri.hostname is None:
            raise ValueError("invalid HTTP/2 URL")
        context = None
        if uri.scheme == "https":
            context = ssl.create_default_context()
            context.set_alpn_protocols(["h2"])
        reader, writer = await asyncio.open_connection(
            uri.hostname,
            uri.port or (443 if context else 80),
            ssl=context,
        )
        if (
            context
            and writer.get_extra_info("ssl_object").selected_alpn_protocol() != "h2"
        ):
            writer.close()
            await writer.wait_closed()
            raise ConnectionError("failed to negotiate the HTTP/2 protocol")
        return cls(reader, writer, uri.scheme, uri.netloc)

    async def send(self, request: Request) -> Response:
        while True:
            if self.closed:
                raise ConnectionError("the HTTP/2 session is closed")
            self.changed.clear()
            if (
                self.connection.open_outbound_streams
                < self.connection.remote_settings.max_concurrent_streams
            ):
                break
            await self.changed.wait()
        stream_id = self.connection.get_next_available_stream_id()
        stream = Stream(asyncio.get_running_loop().create_future())
        self.streams[stream_id] = stream
        headers = [
            (":method", request.method),
            (":scheme", self.scheme),
            (":authority", self.authority),
            (":path", str(request.uri)),
        ]
        for key, value in request.headers.items():
            if isinstance(value, list):
                headers.extend((key.lower(), item) for item in value)
            else:
                headers.append((key.lower(), str(value)))
        try:
            self.connection.send_headers(
                stream_id, headers, end_stream=request.body is None
            )
            self._flush()
            if request.body is not None:
                stream.writer = asyncio.create_task(
                    self._write_body(stream_id, stream, request.body)
                )
            # Read response headers while the request body is still being produced.
            status, headers = await stream.headers
            return Response(
                status,
                headers,
                self._body(stream_id, stream),
                close=lambda: self._release(stream_id, stream),
            )
        except BaseException:
            self._release(stream_id, stream)
            raise

    async def _write_body(
        self, stream_id: int, stream: Stream, body: AsyncIterable[bytes]
    ) -> None:
        try:
            async for chunk in coalesce(body, self.connection.max_outbound_frame_size):
                offset = 0
                while offset < len(chunk):
                    self.changed.clear()
                    if self.closed:
                        raise ConnectionError("the HTTP/2 session is closed")
                    length = min(
                        self.connection.local_flow_control_window(stream_id),
                        self.connection.max_outbound_frame_size,
                    )
                    if length <= 0:
                        await self.changed.wait()
                        continue
                    data = chunk[offset : offset + length]
                    self.connection.send_data(stream_id, data)
                    offset += len(data)
                    self._flush()
                    await self.writer.drain()
                    await asyncio.sleep(0)
            self.connection.end_stream(stream_id)
            self._flush()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fail_stream(stream, error)
            self._release(stream_id, stream)

    async def _body(self, stream_id: int, stream: Stream) -> AsyncIterator[bytes]:
        try:
            while True:
                chunk = await stream.chunks.get()
                if isinstance(chunk, BaseException):
                    raise chunk
                if chunk is None:
                    return
                data, length = chunk
                try:
                    yield data
                finally:
                    if not self.closed:
                        self.connection.acknowledge_received_data(length, stream_id)
                        self._flush()
        finally:
            self._release(stream_id, stream)

    async def _read(self) -> None:
        try:
            while data := await self.reader.read(65536):
                for event in self.connection.receive_data(data):
                    if isinstance(event, ConnectionTerminated):
                        raise ConnectionError(
                            f"the HTTP/2 session terminated: {event.error_code}"
                        )
                    if isinstance(event, (RemoteSettingsChanged, WindowUpdated)):
                        self.changed.set()
                    stream_id = getattr(event, "stream_id", None)
                    stream = self.streams.get(stream_id)
                    if stream is None:
                        continue
                    if isinstance(event, ResponseReceived):
                        headers = {
                            key.decode()
                            if isinstance(key, bytes)
                            else key: value.decode()
                            if isinstance(value, bytes)
                            else value
                            for key, value in event.headers
                        }
                        if not stream.headers.done():
                            stream.headers.set_result(
                                (int(headers.pop(":status")), headers)
                            )
                    elif isinstance(event, DataReceived):
                        stream.chunks.put_nowait(
                            (event.data, event.flow_controlled_length)
                        )
                    elif isinstance(event, TrailersReceived):
                        headers = dict(event.headers)
                        if headers.get("x-tg-event") == "error":
                            from .error import Error

                            try:
                                data = headers.get("x-tg-data")
                                if data is None:
                                    raise ValueError("missing data")
                                error = Error.from_data(json.loads(data))
                            except Exception as error_:
                                error = error_
                            self._fail_stream(stream, error)
                    elif isinstance(event, StreamEnded):
                        stream.ended = True
                        stream.chunks.put_nowait(None)
                        self.changed.set()
                    elif isinstance(event, StreamReset):
                        self._fail_stream(
                            stream,
                            ConnectionError(
                                f"the HTTP/2 stream reset: {event.error_code}"
                            ),
                        )
                        self.changed.set()
                self._flush()
                await self.writer.drain()
            raise ConnectionError("the HTTP/2 connection closed")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.closed = True
            self.changed.set()
            for stream in list(self.streams.values()):
                if not stream.ended:
                    self._fail_stream(stream, error)
            self.writer.close()

    def _flush(self) -> None:
        data = self.connection.data_to_send()
        if data:
            self.writer.write(data)

    @staticmethod
    def _fail_stream(stream: Stream, error: BaseException) -> None:
        if not stream.headers.done():
            stream.headers.set_exception(error)
        stream.chunks.put_nowait(error)

    def _release(self, stream_id: int, stream: Stream) -> None:
        self.streams.pop(stream_id, None)
        if stream.writer and not stream.writer.done():
            stream.writer.cancel()
        if not self.closed:
            try:
                self.connection.reset_stream(stream_id)
                self._flush()
            except Exception:
                # A fully closed stream does not need a reset.
                pass
        self.changed.set()

    async def close(self) -> None:
        self.closed = True
        self.changed.set()
        tasks = [self.reader_task]
        for stream in self.streams.values():
            if not stream.ended:
                self._fail_stream(
                    stream, ConnectionError("the HTTP/2 session is closed")
                )
            if stream.writer:
                tasks.append(stream.writer)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.streams.clear()
        self.writer.close()
        await self.writer.wait_closed()


async def coalesce(body: AsyncIterable[bytes], size: int) -> AsyncIterator[bytes]:
    """Combine ready chunks and flush when the producer becomes pending."""
    iterator = body.__aiter__()
    pending = None
    finished = False
    try:
        while not finished:
            if pending is None:
                pending = asyncio.ensure_future(anext(iterator))
            try:
                chunk = await pending
            except StopAsyncIteration:
                return
            pending = None
            buffer = bytearray(chunk)
            while len(buffer) < size:
                pending = asyncio.ensure_future(anext(iterator))
                # Poll the next chunk without waiting for future input.
                if not pending.done():
                    await asyncio.sleep(0)
                if not pending.done():
                    break
                try:
                    buffer.extend(pending.result())
                except StopAsyncIteration:
                    finished = True
                except Exception:
                    # Send buffered bytes before surfacing the producer error.
                    break
                pending = None
                if finished:
                    break
            if buffer:
                yield bytes(buffer)
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        close = getattr(iterator, "aclose", None)
        if close is not None:
            await close()
