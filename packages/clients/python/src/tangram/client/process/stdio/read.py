"""Resumable stdio reads, translated from client/process/stdio/read.ts."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, TypedDict, Unpack

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.process.stdio import ReadArgObject, StdioChunk
    from tangram.process.stdio import Stream as StdioStream

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from tangram.error import Error
from tangram.http import Body, Request, Stream
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject


@dataclass
class Connection:
    output: AsyncIterator[dict[str, Any]]


class KeywordOptions(TypedDict, total=False):
    length: int | None
    location: LocationArgObject | None
    position: int | str | None
    process: str
    size: int | None
    streams: list[StdioStream]
    timeout: int | float | None
    tokens: dict[str, list[str]] | None


class ProtocolError(ValueError):
    pass


class Acks:
    """The position through which the reader has the read's bytes.

    Each connection sends it, so the runner can release the bytes.
    """

    def __init__(self, position: int):
        self._changed = asyncio.Event()
        self._closed = False
        self._position = position

    def close(self):
        self._closed = True
        self._changed.set()

    def set(self, position: int):
        if position <= self._position:
            return
        self._position = position
        self._changed.set()

    async def events(self):
        sent = None
        while True:
            if sent != self._position:
                sent = self._position
                yield {"data": json.dumps(sent), "event": "ack"}
                continue
            if self._closed:
                return
            self._changed.clear()
            await self._changed.wait()


async def try_read_process_stdio(
    client: Client,
    arg: ReadArgObject | None = None,
    **options: Unpack[KeywordOptions],
) -> Stream[StdioChunk] | None:
    options = {**(arg or {}), **options}
    position = options.get("position")
    acks = Acks(position if isinstance(position, int) else 0)
    connection = await connect(client, options, acks)
    if connection is None:
        acks.close()
        return None
    return read_process_stdio_all(client, options, connection, acks)


def read_process_stdio_all(
    client: Client, arg, connection: Connection, acks: Acks
) -> Stream[StdioChunk]:
    state: dict[str, Any] = {"canceled": False, "connection": connection}
    output = read_process_stdio_all_inner(client, arg, state, acks)

    async def close():
        state["canceled"] = True
        acks.close()
        await close_output(state["connection"].output)

    return Stream(output, close)


async def read_process_stdio_all_inner(client: Client, arg, state, acks: Acks):
    from tangram.process.stdio import Read

    connection = state["connection"]
    combined = len(arg["streams"]) > 1
    forward = arg.get("length") is None or arg["length"] >= 0
    next_arg: dict[str, Any] = {**arg, "streams": list(arg["streams"])}
    position = (
        None if isinstance(arg.get("position"), str) else arg.get("position") or 0
    )
    reconnects = 0
    try:
        while not state["canceled"]:
            message = None
            error = None
            try:
                message = await anext(connection.output)
            except StopAsyncIteration:
                pass
            except Exception as error_:
                if state["canceled"]:
                    return
                if not is_retryable_stream_error(error_):
                    raise
                error = error_
            if state["canceled"]:
                return
            if message is None:
                connection, reconnects = await reconnect(
                    client, next_arg, connection, acks, reconnects, error
                )
                state["connection"] = connection
                continue
            if message["kind"] == "response":
                Read.Output.validate(message["value"], arg["streams"], position or 0)
                return
            if message["value"]["kind"] == "position":
                value = message["value"]["value"]
                if (
                    not safe_integer(value["position"])
                    or value["position"] < 0
                    or (
                        value["length"] is not None
                        and not safe_integer(value["length"])
                    )
                ):
                    raise ProtocolError("invalid process stdio position")
                position = value["position"]
                next_arg.update(value)
                continue
            if message["value"]["kind"] != "chunk":
                raise ProtocolError("invalid process stdio read notification")
            reconnects = 0
            chunk = message["value"]["value"]
            if chunk["stream"] not in arg["streams"]:
                raise ProtocolError("invalid process stdio stream")
            start = chunk["combined_position"] if combined else chunk["stream_position"]
            end = start + len(chunk["bytes"])
            if not safe_integer(end):
                raise ProtocolError("the stdio position is too large")
            if position is not None:
                if (forward and end <= position) or (not forward and start >= position):
                    continue
                if (forward and start > position) or (not forward and end < position):
                    raise ProtocolError("encountered a gap in the stdio stream")
                if forward and start < position:
                    overlap = position - start
                    chunk = {
                        **chunk,
                        "bytes": chunk["bytes"][overlap:],
                        "combined_position": chunk["combined_position"] + overlap,
                        "stream_position": chunk["stream_position"] + overlap,
                    }
                elif not forward and end > position:
                    chunk = {**chunk, "bytes": chunk["bytes"][: position - start]}
            length = len(chunk["bytes"])
            position = (
                chunk["combined_position"] if combined else chunk["stream_position"]
            ) + (length if forward else 0)
            if next_arg.get("length") is not None:
                if next_arg["length"] >= 0:
                    next_arg["length"] -= min(length, next_arg["length"])
                else:
                    next_arg["length"] += min(length, abs(next_arg["length"]))
            next_arg["position"] = position
            # The consumer has the chunk once it is yielded.
            if forward:
                acks.set(position)
            yield chunk
    finally:
        acks.close()
        await close_output(connection.output)


async def connect(client: Client, arg, acks: Acks) -> Connection | None:
    from tangram.client import retry

    return await retry(
        client.stream_retry,
        lambda: read_process_stdio_once(client, arg, acks),
        is_retryable_error,
    )


async def reconnect(
    client: Client, arg, connection, acks: Acks, attempt: int, error
) -> tuple[Connection, int]:
    """Reconnect after the given number of consecutive failed attempts.

    Return the new connection and the number of consecutive failed attempts,
    including this one.
    """
    from tangram.client import backoff

    await close_output(connection.output)
    while True:
        # Fail once the consecutive attempts exhaust the stream retry options.
        if attempt >= client.stream_retry["max_retries"]:
            if error is not None:
                raise error
            raise ConnectionError("the process stdio stream ended before completion")

        # Back off, so that an empty stream does not reconnect in a tight loop.
        await backoff(client.stream_retry, attempt)
        attempt += 1

        # Connect.
        try:
            next_ = await read_process_stdio_once(client, arg, acks)
        except Exception as error_:
            if not is_retryable_error(error_):
                raise
            error = error_
            continue
        if next_ is None:
            raise ValueError("failed to find the process")

        return next_, attempt


async def read_process_stdio_once(client: Client, arg, acks: Acks):
    request = Request(
        "POST",
        "/processes/stdio/read",
        {"accept": "text/event-stream", "content-type": "text/event-stream"},
        Body.sse(acks.events()),
    ).arg(
        {
            **arg,
            "location": None
            if arg.get("location") is None
            else LocationArg.to_data_string(arg["location"]),
            "streams": ",".join(arg["streams"]),
            "tokens": arg.get("tokens") or {},
        }
    )
    try:
        response = await client.send(request)
        if response.status == 404:
            await response.close()
            return None
        if not 200 <= response.status < 300:
            raise await response_error(response)
        if (
            response.headers.get("content-type", "").split(";", 1)[0]
            != "text/event-stream"
        ):
            raise ProtocolError("invalid process stdio response content type")
    except BaseException:
        if "response" in locals():
            await response.close()
        raise
    return Connection(Stream(decode_server_messages(response), response.close))


async def decode_server_messages(response):
    from tangram.process.stdio import Chunk, Read

    try:
        async for event in response.sse():
            try:
                if event.get("event") == "error":
                    raise error_from_data(json.loads(event["data"]))
                if event.get("event") == "notification":
                    value = json.loads(event["data"])
                    if value["kind"] == "chunk":
                        yield {
                            "kind": "notification",
                            "value": {
                                "kind": "chunk",
                                "value": Chunk.from_data(value["value"]),
                            },
                        }
                    elif value["kind"] == "position":
                        yield {"kind": "notification", "value": value}
                    else:
                        raise ProtocolError("invalid process stdio read notification")
                elif event.get("event") == "response":
                    value = Read.Output.from_data(json.loads(event["data"]))
                    if value["kind"] not in ("end", "limit", "timeout"):
                        raise ProtocolError("invalid process stdio read request")
                    yield {"kind": "response", "value": value}
                else:
                    raise ProtocolError("invalid process stdio read message")
            except Exception as error:
                if is_terminal_error(error):
                    raise
                raise ProtocolError(
                    "failed to deserialize a process stdio message"
                ) from error
    finally:
        await response.close()


async def response_error(response):
    try:
        return error_from_data(await response.json())
    except Error:
        raise
    except Exception as error:
        raise ProtocolError("failed to deserialize the error response") from error


def error_from_data(data):
    return Error.with_id(data) if isinstance(data, str) else Error.from_data(data)


def is_terminal_error(error):
    return isinstance(error, (Error, ProtocolError))


def is_retryable_error(error):
    from tangram.client import is_transport_error

    return not isinstance(error, ProtocolError) and is_transport_error(error)


def is_retryable_stream_error(error):
    from tangram.client import is_transport_error

    # Retry an open stream's error unless the server sent it.
    return not isinstance(error, ProtocolError) and (
        not isinstance(error, Error) or is_transport_error(error)
    )


def safe_integer(value):
    return (
        type(value) in (int, float) and abs(value) <= 2**53 - 1 and value == int(value)
    )


async def close_output(output):
    if hasattr(output, "aclose"):
        await output.aclose()


def validate_output(output, streams, position, combined=None):
    kind, value = output["kind"], output["value"]
    if kind in ("limit", "timeout"):
        expected = value["position"]
    elif kind == "end":
        expected = (
            value["combined_position"]
            if len(streams) > 1
            else value["stream_positions"].get(streams[0])
        )
    else:
        raise ProtocolError("invalid process stdio read output")
    if not safe_integer(expected) or expected < 0:
        raise ProtocolError("invalid stdio read completion position")
    if (position < expected) if kind == "end" else (position != expected):
        raise ProtocolError("encountered a gap at the end of the stdio read")
