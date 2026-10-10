"""Process stdio writes, translated from client/process/stdio/write.ts."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.process.stdio import StdioChunk
    from tangram.process.stdio import Stream as StdioStream

import asyncio
import json
from collections import deque
from dataclasses import dataclass
from typing import Any

from tangram.error import Error
from tangram.http import Body, Request, percent_encode
from tangram.http.flow import Consumption, Sender
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject


@dataclass
class Connection:
    input: Channel
    output: AsyncIterator[dict[str, Any]]
    reconnect: Callable[..., Awaitable[Connection | None]] | None = None


class KeywordOptions(TypedDict, total=False):
    location: LocationArgObject | None
    streams: list[StdioStream]
    tokens: dict[str, list[str]] | None


class ProtocolError(ValueError):
    pass


async def write_process_stdio(
    client: Client,
    id: str,
    arg=None,
    input: AsyncIterable[StdioChunk] | None = None,
    complete: Callable[[StdioChunk], None] | None = None,
    **options,
) -> None:
    output = await try_write_process_stdio(client, id, arg, input, complete, **options)
    if output is None:
        raise ValueError("failed to find the process")


async def try_write_process_stdio(
    client: Client,
    id: str,
    arg=None,
    input: AsyncIterable[StdioChunk] | None = None,
    complete: Callable[[StdioChunk], None] | None = None,
    **options,
) -> bool | None:
    if not isinstance(arg, dict):
        input = arg if arg is not None else input
        arg = {}
    arg = {**arg, **options}
    complete = complete if complete is not None else arg.pop("complete", None)
    connection = await connect(client, id, arg)
    if connection is None:
        await close_output(input)
        return None
    await write_process_stdio_all(client, id, arg, input, connection, complete)
    return True


async def write_process_stdio_all(
    client: Client, id, arg, input, connection, complete=None
):
    pending = deque()
    consumption: Consumption = {"bytes": 0, "messages": 0}
    flow = client.stdio
    window = Sender(flow["limits"])
    remaining = None
    input_event = None
    output_event = None
    input_ended = False
    next_id = 0
    combined_position = 0
    stream_positions = {stream: 0 for stream in arg["streams"]}
    try:
        while True:
            while remaining is not None and window.available(1):
                chunk, offset = remaining
                if chunk["stream"] not in arg["streams"]:
                    raise ProtocolError("invalid process stdio stream")
                length = min(
                    flow["max_message_size"],
                    len(chunk["bytes"]) - offset,
                    window.remaining_bytes(),
                )
                if length == 0:
                    if complete is not None:
                        complete(chunk)
                    remaining = None
                    break
                value = {
                    **chunk,
                    "bytes": chunk["bytes"][offset : offset + length],
                    "combined_position": chunk["combined_position"] + offset,
                    "stream_position": chunk["stream_position"] + offset,
                }
                combined_position = value["combined_position"] + length
                stream_positions[value["stream"]] = value["stream_position"] + length
                if not safe_integer(combined_position) or not safe_integer(
                    stream_positions[value["stream"]]
                ):
                    raise ProtocolError("invalid stdio position")
                window.send(length)
                offset += length
                last = offset == len(chunk["bytes"])
                pending.append(
                    {
                        "original": chunk if last else None,
                        "request": {
                            "arg": {"kind": "chunk", "value": value},
                            "id": next_id,
                        },
                        "sent": False,
                    }
                )
                next_id += 1
                remaining = None if last else (chunk, offset)
            if input_ended and not pending:
                pending.append(
                    {
                        "request": {
                            "arg": {
                                "kind": "end",
                                "value": {
                                    "combined_position": combined_position,
                                    "stream_positions": dict(stream_positions),
                                },
                            },
                            "id": next_id,
                        },
                        "sent": False,
                    }
                )
                next_id += 1
            for value in pending:
                if not value["sent"]:
                    connection.input.push(
                        {"kind": "request", "value": value["request"]}
                    )
                    value["sent"] = True
            if (
                window.available(1)
                and remaining is None
                and not input_ended
                and input_event is None
            ):
                input_event = asyncio.create_task(next_input(input))
            if output_event is None:
                output_event = asyncio.create_task(next_output(connection.output))
            if input_event is None:
                event = await output_event
            else:
                await asyncio.wait(
                    (output_event, input_event), return_when=asyncio.FIRST_COMPLETED
                )
                event = (
                    output_event.result()
                    if output_event.done()
                    else input_event.result()
                )
            if event["kind"] == "input_error":
                raise event["error"]
            if event["kind"] == "input":
                input_event = None
                if event["done"]:
                    input_ended = True
                else:
                    remaining = (event["value"], 0)
                continue
            output_event = None
            if event["kind"] == "output_error" and is_terminal_error(event["error"]):
                raise event["error"]
            if event["kind"] == "output_error" or event["done"]:
                connection = await reconnect(client, id, arg, connection)
                for value in pending:
                    value["sent"] = False
                continue
            message = event["value"]
            if message["kind"] == "ack":
                continue
            response = message["value"]
            connection.input.push({"kind": "ack", "value": {"id": response["id"]}})
            if not pending or pending[0]["request"]["id"] != response["id"]:
                raise ProtocolError("received an out-of-order stdio write response")
            value = pending.popleft()
            if response["error"] is not None:
                raise error_from_data(response["error"])
            if response["output"] is None:
                raise ProtocolError("missing the stdio write output")
            closed, length = response["output"]["closed"], response["output"]["length"]
            request_arg = value["request"]["arg"]
            expected = (
                len(request_arg["value"]["bytes"])
                if request_arg["kind"] == "chunk"
                else 0
            )
            if request_arg["kind"] == "chunk":
                consumption = {
                    "bytes": consumption["bytes"] + expected,
                    "messages": consumption["messages"] + 1,
                }
                window.update(consumption)
            if (
                not safe_integer(length)
                or length < 0
                or length > expected
                or (not closed and length != expected)
            ):
                raise ProtocolError("invalid process stdio write length")
            if (
                length == expected
                and value.get("original") is not None
                and complete is not None
            ):
                complete(value["original"])
            if request_arg["kind"] == "end" and not closed:
                raise ProtocolError("the stdio end was not confirmed")
            if closed or request_arg["kind"] == "end":
                return
    finally:
        connection.input.close()
        tasks = [task for task in (input_event, output_event) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await close_output(connection.output)
        await close_output(input)


async def next_input(input):
    return await next_event(input, "input")


async def next_output(output):
    return await next_event(output, "output")


async def next_event(iterator, kind):
    try:
        return {"kind": kind, "done": False, "value": await anext(iterator)}
    except StopAsyncIteration:
        return {"kind": kind, "done": True}
    except Exception as error:
        return {"kind": kind + "_error", "error": error}


async def connect(client: Client, id: str, arg) -> Connection | None:
    attempt = 0
    while True:
        try:
            return await write_process_stdio_once(client, id, arg)
        except Exception as error:
            if is_terminal_error(error):
                raise
            await retry_delay(attempt)
            attempt += 1


async def reconnect(client: Client, id, arg, connection):
    connection.input.close()
    await close_output(connection.output)
    next = (
        await connect(client, id, arg)
        if connection.reconnect is None
        else await connection.reconnect()
    )
    if next is None:
        raise ValueError("failed to find the process")
    return next


async def write_process_stdio_once(client: Client, id, arg):
    input = Channel()
    request = Request(
        "POST",
        f"/processes/{percent_encode(id)}/stdio/write",
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
            input.close()
            await response.close()
            return None
        if not 200 <= response.status < 300:
            input.close()
            raise await response_error(response)
        if (
            response.headers.get("content-type", "").split(";", 1)[0]
            != "text/event-stream"
        ):
            input.close()
            await response.close()
            raise ProtocolError("invalid process stdio response content type")
        return Connection(input, decode_server_messages(response))
    except BaseException:
        input.close()
        raise


async def encode_client_messages(input):
    from tangram.process.stdio import Write

    async for message in input:
        value = message["value"]
        if message["kind"] == "request":
            value = {**value, "arg": Write.Data.to_data(value["arg"])}
        yield {"data": json.dumps(value), "event": message["kind"]}


async def decode_server_messages(response):
    try:
        async for event in response.sse():
            try:
                if event.get("event") == "error":
                    raise error_from_data(json.loads(event["data"]))
                if event.get("event") not in ("ack", "response"):
                    raise ProtocolError("invalid process stdio write message")
                yield {"kind": event["event"], "value": json.loads(event["data"])}
            except (Error, ProtocolError):
                raise
            except Exception as error:
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
    finally:
        await response.close()


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
