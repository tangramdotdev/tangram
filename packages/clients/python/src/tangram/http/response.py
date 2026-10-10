from __future__ import annotations

import asyncio
import inspect
import json
import math
from collections import deque
from collections.abc import AsyncIterable, AsyncIterator, Callable, Mapping
from typing import Protocol, cast

from .body import Body, SseEvent
from .encoding import require_json
from .headers import Headers, HeaderValue


class ResponseStream(Protocol):
    """The HTTP/2 event stream expected by the JavaScript adapter."""

    def once(self, event: str, callback: Callable[..., None]) -> object: ...
    def on(self, event: str, callback: Callable[..., None]) -> object: ...
    def close(self) -> None: ...


class Response:
    def __init__(
        self,
        status: int,
        headers: Headers | Mapping[str, HeaderValue] | None = None,
        body: Body | AsyncIterable[str | bytes] | bytes | str | None = None,
        *,
        close: Callable[[], None] | None = None,
    ):
        self.body = body if isinstance(body, Body) else Body(body)
        self.headers = headers if isinstance(headers, Headers) else Headers(headers)
        self.status = status
        self._close = close
        self._body_source = body

    @staticmethod
    async def from_stream(stream: ResponseStream) -> Response:
        chunks: deque[bytes] = deque()
        error = None
        failed = False
        notify = asyncio.Event()
        done = False
        response: asyncio.Future[Response] = asyncio.get_running_loop().create_future()

        async def body() -> AsyncIterator[bytes]:
            try:
                while True:
                    if chunks:
                        yield chunks.popleft()
                    elif failed:
                        raise cast(BaseException, error)
                    elif done:
                        break
                    else:
                        notify.clear()
                        await notify.wait()
            finally:
                if not done:
                    stream.close()

        body_ = Body(body())

        def fail(error_: BaseException) -> None:
            nonlocal error, failed, done
            error = error_
            failed = True
            done = True
            notify.set()
            if not response.done():
                response.set_exception(error_)

        def receive_response(headers: Mapping[str, HeaderValue]) -> None:
            try:
                headers_ = Headers(headers)
                value = headers_.get(":status")
                # JavaScript's Number conversion accepts an empty status as zero.
                status = (
                    math.nan
                    if value is None
                    else 0.0
                    if value.strip() == ""
                    else float(value)
                )
                if not math.isfinite(status) or not status.is_integer():
                    raise ValueError("invalid status")
                if not response.done():
                    response.set_result(
                        Response(
                            int(status),
                            headers_,
                            body_,
                            close=lambda: None if done else stream.close(),
                        )
                    )
            except Exception as error:
                fail(error)

        def receive_data(chunk: bytes) -> None:
            chunks.append(chunk)
            notify.set()

        def receive_trailers(headers: Mapping[str, HeaderValue]) -> None:
            headers_ = Headers(headers)
            if headers_.get("x-tg-event") == "error":
                data = headers_.get("x-tg-data")
                if data is None:
                    fail(ValueError("missing data"))
                else:
                    from ..error import Error

                    try:
                        fail(Error.from_data(json.loads(data)))
                    except Exception as error:
                        fail(error)

        def end() -> None:
            nonlocal done
            done = True
            notify.set()

        stream.once("error", fail)
        stream.once("response", receive_response)
        stream.on("data", receive_data)
        stream.once("trailers", receive_trailers)
        stream.once("end", end)

        def closed() -> None:
            if not done:
                from ..http2 import TransportError

                fail(
                    TransportError("the HTTP/2 stream closed before the response ended")
                )

        stream.once("close", closed)
        return await response

    async def collect(self) -> bytes:
        try:
            return await self.body.collect()
        finally:
            await self.close()

    async def body_header(self) -> object:
        source = self.body.__aiter__()
        buffer = b""
        offset = 0

        async def read(length: int) -> bytes:
            nonlocal buffer, offset
            output = bytearray()
            while len(output) < length:
                if offset == len(buffer):
                    try:
                        buffer = await anext(source)
                    except StopAsyncIteration:
                        raise ValueError(
                            "the response ended inside the header"
                        ) from None
                    offset = 0
                    continue
                count = min(length - len(output), len(buffer) - offset)
                output.extend(buffer[offset : offset + count])
                offset += count
            return bytes(output)

        try:
            require_json(self.headers.get("content-type"))
            length = 0
            for index in range(10):
                byte = (await read(1))[0]
                length += (byte & 127) << (7 * index)
                if length > 1_048_576:
                    raise ValueError("header too large")
                if byte < 128:
                    break
            else:
                raise ValueError("invalid header length")
            header = json.loads(await read(length))

            async def body() -> AsyncIterator[bytes]:
                try:
                    if offset < len(buffer):
                        yield buffer[offset:]
                    async for chunk in source:
                        yield chunk
                finally:
                    close = getattr(source, "aclose", None)
                    if close is not None:
                        await close()

            self.body = Body(body())
            return header
        except BaseException:
            close = getattr(source, "aclose", None)
            if close is not None:
                await close()
            await self.close()
            raise

    async def json(self) -> object:
        try:
            return await self.body.json()
        finally:
            await self.close()

    async def sse(self) -> AsyncIterator[SseEvent]:
        try:
            async for event in self.body.sse():
                yield event
        finally:
            await self.close()

    async def close(self) -> None:
        if self._close is not None:
            self._close()
            self._close = None
        close = getattr(self._body_source, "aclose", None)
        # An active generator unwinds in its consumer after the transport closes.
        running = inspect.isasyncgen(self._body_source) and self._body_source.ag_running
        if close is not None and not running:
            await close()
