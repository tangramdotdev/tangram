"""Blob reading, translated from client/read.ts."""

from typing import TYPE_CHECKING, TypedDict, Unpack, cast

if TYPE_CHECKING:
    from tangram.client import Client

import math
import re
from collections.abc import AsyncIterator
from typing import Any, Literal, NotRequired

from tangram.error import Error, ErrorData
from tangram.http import Request, Stream


class KeywordOptions(TypedDict, total=False):
    length: int | None
    position: int | str | None
    size: int | None
    tokens: dict[str, list[str]] | None


class ReadChunk(TypedDict):
    bytes: bytes
    position: int


class Read:
    class Options(TypedDict):
        position: NotRequired[int | str | None]
        length: NotRequired[int | None]
        size: NotRequired[int | None]

    class Arg(Options):
        blob: str
        tokens: NotRequired[dict[str, list[str]] | None]

    class Event:
        Chunk = ReadChunk

        class ChunkEvent(TypedDict):
            kind: Literal["chunk"]
            value: ReadChunk

        class End(TypedDict):
            kind: Literal["end"]


async def read(
    client: "Client",
    arg: Read.Arg | str | None = None,
    **options: Unpack[KeywordOptions],
) -> bytes:
    stream = await try_read_stream(client, arg, **options)
    if stream is None:
        raise ValueError("failed to find the blob")
    return await collect_read_stream(stream)


async def try_read(
    client: "Client",
    arg: Read.Arg | str | None = None,
    **options: Unpack[KeywordOptions],
) -> bytes | None:
    stream = await try_read_stream(client, arg, **options)
    return None if stream is None else await collect_read_stream(stream)


async def try_read_stream(
    client: "Client",
    arg: Read.Arg | str | None = None,
    **options: Unpack[KeywordOptions],
) -> Stream[Read.Event.ChunkEvent | Read.Event.End] | None:
    arg_: dict[str, Any] = {
        **(arg if isinstance(arg, dict) else {"blob": arg}),
        **options,
    }
    request = Request("GET", "/read", {"accept": "application/octet-stream"}).arg(
        {
            "blob": arg_["blob"],
            "length": arg_.get("length"),
            "position": arg_.get("position"),
            "size": arg_.get("size"),
            "tokens": {} if arg_.get("tokens") is None else arg_["tokens"],
        }
    )
    response = await client.send_with_retry(request)
    if response.status == 404:
        await response.close()
        return None
    elif response.status < 200 or response.status >= 300:
        raise Error.from_data(cast(ErrorData, await response.json()))
    return Stream(decode_read_events(response), response.close)


async def collect_read_stream(stream):
    chunks = []
    try:
        async for event in stream:
            if event["kind"] == "end":
                break
            chunks.append(event["value"]["bytes"])
    finally:
        if hasattr(stream, "aclose"):
            await stream.aclose()
    return b"".join(chunks)


async def decode_read_events(
    response,
) -> AsyncIterator[Read.Event.ChunkEvent | Read.Event.End]:
    position = response.headers.get("x-tg-position")
    next_position = None
    if position is not None:
        try:
            stripped = position.strip()
            if stripped and not (
                re.fullmatch(
                    r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?",
                    stripped,
                )
                or re.fullmatch(r"0[xX][0-9a-fA-F]+|0[oO][0-7]+|0[bB][01]+", stripped)
            ):
                raise ValueError("expected an integer")
            number = (
                float(int(stripped, 0))
                if stripped.lower().startswith(("0x", "0o", "0b"))
                else float(stripped)
                if stripped
                else 0.0
            )
            if not math.isfinite(number) or not number.is_integer():
                raise ValueError("expected an integer")
            next_position = int(number)
        except ValueError:
            raise ValueError("expected an integer") from None
    async for bytes_ in response.body:
        if next_position is None:
            raise ValueError("expected a position")
        yield {
            "kind": "chunk",
            "value": {"bytes": bytes_, "position": next_position},
        }
        next_position += len(bytes_)
    yield {"kind": "end"}
