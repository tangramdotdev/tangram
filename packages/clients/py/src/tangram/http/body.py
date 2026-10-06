import builtins
import json
import re
from collections.abc import AsyncIterable, AsyncIterator, Callable, Coroutine
from typing import Any, NotRequired, TypedDict, overload


def json_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode()


class SseEvent(TypedDict):
    data: str
    event: NotRequired[str]


class _JsonMethod:
    @overload
    def __get__(
        self, instance: None, owner: type["Body"]
    ) -> Callable[[object], "Body"]: ...
    @overload
    def __get__(
        self, instance: "Body", owner: type["Body"]
    ) -> Callable[[], Coroutine[Any, Any, object]]: ...
    def __get__(self, instance: "Body | None", owner: type["Body"]):
        return owner._json if instance is None else instance._read_json


class _SseMethod:
    @overload
    def __get__(
        self, instance: None, owner: type["Body"]
    ) -> Callable[[AsyncIterable[SseEvent]], "Body"]: ...
    @overload
    def __get__(
        self, instance: "Body", owner: type["Body"]
    ) -> Callable[[], AsyncIterator[SseEvent]]: ...
    def __get__(self, instance: "Body | None", owner: type["Body"]):
        return owner._sse if instance is None else instance._read_sse


class Body:
    SseEvent = SseEvent

    def __init__(
        self,
        body: AsyncIterable[str | builtins.bytes] | builtins.bytes | str | None = None,
    ):
        if isinstance(body, str):
            body = body.encode()
        self._replayable = body is None or isinstance(body, bytes)
        self._body = (
            empty()
            if body is None
            else single(body)
            if isinstance(body, bytes)
            else body
        )

    @staticmethod
    def bytes(value: builtins.bytes) -> "Body":
        body = Body(single(value))
        body._replayable = True
        return body

    @staticmethod
    def empty() -> "Body":
        return Body()

    @staticmethod
    def _json(value: object) -> "Body":
        return Body.bytes(json_bytes(value))

    @staticmethod
    def _sse(events: AsyncIterable[SseEvent]) -> "Body":
        return Body(encode_sse(events))

    @staticmethod
    def text(value: str) -> "Body":
        return Body.bytes(value.encode())

    @property
    def replayable(self) -> bool:
        return self._replayable

    def prepend(self, value: builtins.bytes) -> "Body":
        body = self

        class Output:
            async def __aiter__(self) -> AsyncIterator[builtins.bytes]:
                yield value
                async for chunk in body:
                    yield chunk

        output = Body(Output())
        output._replayable = self._replayable
        return output

    async def collect(self) -> builtins.bytes:
        chunks = []
        async for chunk in self:
            chunks.append(chunk)
        return concat(chunks)

    async def _read_json(self) -> object:
        value = await self.collect()
        string = value.decode("utf-8-sig", errors="replace")
        return json.loads(string)

    json = _JsonMethod()

    def _read_sse(self) -> AsyncIterator[SseEvent]:
        return decode_sse(self)

    sse = _SseMethod()

    def __aiter__(self) -> AsyncIterator[builtins.bytes]:
        return normalize(self._body).__aiter__()


async def decode_sse(body: AsyncIterable[bytes]) -> AsyncIterator[SseEvent]:
    buffer = ""
    async for chunk in body:
        buffer += chunk.decode("utf-8-sig", errors="replace")
        while True:
            index = buffer.find("\n\n")
            length = 2
            if index == -1:
                index = buffer.find("\r\n\r\n")
                length = 4
            if index == -1:
                break
            block = buffer[:index]
            buffer = buffer[index + length :]
            event = parse_sse(block)
            if event is not None:
                yield event
    event = parse_sse(buffer)
    if event is not None:
        yield event


async def encode_sse(events: AsyncIterable[SseEvent]) -> AsyncIterator[bytes]:
    async for event in events:
        yield format_sse(event).encode()


def empty() -> AsyncIterable[bytes]:
    class Empty:
        async def __aiter__(self) -> AsyncIterator[builtins.bytes]:
            for value in ():
                yield value

    return Empty()


def single(value: bytes) -> AsyncIterable[bytes]:
    class Single:
        async def __aiter__(self) -> AsyncIterator[builtins.bytes]:
            yield value

    return Single()


async def normalize(body: AsyncIterable[str | bytes]) -> AsyncIterator[bytes]:
    async for chunk in body:
        yield chunk.encode() if isinstance(chunk, str) else chunk


def parse_sse(block: str) -> SseEvent | None:
    event = None
    data = []
    for line in re.split(r"\r\n|\r|\n", block):
        if line == "" or line.startswith(":"):
            continue
        index = line.find(":")
        name = line if index == -1 else line[:index]
        value = "" if index == -1 else line[index + 1 :]
        if value.startswith(" "):
            value = value[1:]
        if name == "event":
            event = value
        elif name == "data":
            data.append(value)
    if event is None and not data:
        return None
    output: SseEvent = {"data": "\n".join(data)}
    if event is not None:
        output["event"] = event
    return output


def format_sse(event: SseEvent) -> str:
    lines = []
    if "event" in event:
        lines.append(f"event: {event['event']}")
    for line in event["data"].split("\n"):
        lines.append(f"data: {line}")
    lines.extend(["", ""])
    return "\n".join(lines)


def concat(chunks: list[bytes]) -> bytes:
    return b"".join(chunks)
