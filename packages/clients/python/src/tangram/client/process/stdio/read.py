"""Resumable stdio reads, translated from client/process/stdio/read.ts."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, TypedDict, Unpack

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.process.stdio import ReadArgObject, StdioChunk
    from tangram.process.stdio import Stream as StdioStream

import asyncio
import json
from collections import deque
from dataclasses import dataclass
from typing import Any

from tangram.config import StdioConfig, validate_stdio_receiver
from tangram.error import Error
from tangram.http import Body, Request, Stream, percent_encode
from tangram.http.flow import Receiver
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject


@dataclass
class Connection:
    input: Channel
    output: AsyncIterator[dict[str, Any]]
    reconnect: Callable[..., Awaitable[Connection | None]] | None = None


class KeywordOptions(TypedDict, total=False):
    flow: StdioConfig
    length: int | None
    location: LocationArgObject | None
    position: int | str | None
    size: int | None
    streams: list[StdioStream]
    timeout: int | float | None
    tokens: dict[str, list[str]] | None


class ProtocolError(ValueError):
    pass


async def try_read_process_stdio(
    client: Client,
    id: str,
    arg: ReadArgObject | None = None,
    **options: Unpack[KeywordOptions],
) -> Stream[StdioChunk] | None:
    options = {"flow": client.stdio, **(arg or {}), **options}
    validate_stdio_receiver(client.stdio, options["flow"])
    connection = await connect(client, id, options)
    if connection is None:
        return None
    return read_process_stdio_all(client, id, options, connection)


def read_process_stdio_all(
    client: Client, id: str, arg, connection: Connection
) -> Stream[StdioChunk]:
    state: dict[str, Any] = {"canceled": False, "connection": connection}
    output = read_process_stdio_all_inner(client, id, arg, state)

    async def close():
        state["canceled"] = True
        state["connection"].input.close()
        await close_output(state["connection"].output)

    return Stream(output, close)


async def read_process_stdio_all_inner(client: Client, id, arg, state):
    from tangram.process.stdio import Read

    connection = state["connection"]
    combined = len(arg["streams"]) > 1
    forward = arg.get("length") is None or arg["length"] >= 0
    next_arg: dict[str, Any] = {**arg, "streams": list(arg["streams"])}
    window = Receiver(arg.get("flow", client.stdio)["limits"])
    pending = None
    position = (
        None if isinstance(arg.get("position"), str) else arg.get("position") or 0
    )
    try:
        while not state["canceled"]:
            consumption = None if pending is None else window.consume(pending)
            pending = None
            if consumption is not None:
                connection.input.push({"kind": "notification", "value": consumption})
            message = None
            try:
                message = await anext(connection.output)
            except StopAsyncIteration:
                pass
            except Exception as error:
                if state["canceled"]:
                    return
                if is_terminal_error(error):
                    raise
            if state["canceled"]:
                return
            if message is None:
                connection = state["connection"] = await reconnect(
                    client, id, next_arg, connection
                )
                window = Receiver(arg.get("flow", client.stdio)["limits"])
                continue
            if message["kind"] == "response":
                Read.Output.validate(message["value"], arg["streams"], position or 0)
                consumption = window.flush()
                if consumption is not None:
                    connection.input.push(
                        {"kind": "notification", "value": consumption}
                    )
                connection.input.push({"kind": "ack"})
                connection.input.close()
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
            chunk = message["value"]["value"]
            window.receive(len(chunk["bytes"]))
            pending = len(chunk["bytes"])
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
            yield chunk
    finally:
        connection.input.close()
        await close_output(connection.output)


async def connect(client: Client, id: str, arg) -> Connection | None:
    attempt = 0
    while True:
        try:
            return await read_process_stdio_once(client, id, arg)
        except Exception as error:
            if is_terminal_error(error):
                raise
            await retry_delay(attempt)
            attempt += 1


async def reconnect(client: Client, id, arg, connection):
    connection.input.close()
    await close_output(connection.output)
    next_ = (
        await connect(client, id, arg)
        if connection.reconnect is None
        else await connection.reconnect(arg)
    )
    if next_ is None:
        raise ValueError("failed to find the process")
    return next_


async def read_process_stdio_once(client: Client, id, arg):
    input = Channel()
    request = Request(
        "POST",
        f"/processes/{percent_encode(id)}/stdio/read",
        {"accept": "text/event-stream", "content-type": "text/event-stream"},
        Body.sse(encode_client_messages(input)),
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
            input.close()
            return None
        if not 200 <= response.status < 300:
            raise await response_error(response)
        if (
            response.headers.get("content-type", "").split(";", 1)[0]
            != "text/event-stream"
        ):
            raise ProtocolError("invalid process stdio response content type")
    except BaseException:
        input.close()
        if "response" in locals():
            await response.close()
        raise
    return Connection(input, Stream(decode_server_messages(response), response.close))


async def encode_client_messages(input):
    async for message in input:
        yield {
            "data": json.dumps(None if message["kind"] == "ack" else message["value"]),
            "event": message["kind"],
        }


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


async def retry_delay(attempt):
    await asyncio.sleep(min(0.01 * 2 ** min(attempt, 7), 1))


class Channel:
    def __init__(self):
        self.closed = False
        self.values = deque()
        self.waiters = deque()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        while self.waiters:
            waiter = self.waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self

    async def __anext__(self) -> dict[str, Any]:
        if self.values:
            return self.values.popleft()
        if self.closed:
            raise StopAsyncIteration
        waiter = asyncio.get_running_loop().create_future()
        self.waiters.append(waiter)
        value = await waiter
        if value is None:
            raise StopAsyncIteration
        return value

    def push(self, value: dict[str, Any]) -> bool:
        if self.closed:
            return False
        while self.waiters:
            waiter = self.waiters.popleft()
            if not waiter.done():
                waiter.set_result(value)
                return True
        self.values.append(value)
        return True

    async def aclose(self) -> None:
        self.close()


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
