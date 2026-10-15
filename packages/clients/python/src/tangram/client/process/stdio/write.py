"""Process stdio writes, translated from client/process/stdio/write.ts."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Callable
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
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject


@dataclass
class Connection:
    input: Channel
    output: AsyncIterator[dict[str, Any]]


class KeywordOptions(TypedDict, total=False):
    location: LocationArgObject | None
    streams: list[StdioStream]
    tokens: dict[str, list[str]] | None


MAX_CHUNK_SIZE = 32 * 1024


class ProtocolError(ValueError):
    pass


class NotFoundError(ValueError):
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
    try:
        await write_process_stdio_all(client, id, arg, input, connection, complete)
    except NotFoundError:
        return None
    return True


async def write_process_stdio_all(
    client: Client, id, arg, input, connection, complete=None
):
    pending = deque()
    written = 0
    input_event = None
    output_event = None
    drained_event = None
    input_ended = False
    combined_position = 0
    reconnects = 0
    stream_positions = {stream: 0 for stream in arg["streams"]}
    try:
        while True:
            # Send the data the server has not reported written.
            for value in pending:
                if not value["sent"]:
                    connection.input.push(resend(value["data"], written))
                    value["sent"] = True

            # Read more input only once the request body drains, so HTTP/2
            # backpressure bounds the pending data.
            drained = not connection.input.values
            if drained and not input_ended and input_event is None:
                input_event = asyncio.create_task(next_input(input))
            if not drained and drained_event is None:
                drained_event = asyncio.create_task(connection.input.wait_drained())
            if output_event is None:
                output_event = asyncio.create_task(next_output(connection.output))
            tasks = [
                task
                for task in (output_event, input_event, drained_event)
                if task is not None
            ]
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if drained_event is not None and drained_event.done():
                drained_event = None
            if input_event is not None and input_event.done():
                event = input_event.result()
                input_event = None
                if event["kind"] == "input_error":
                    raise event["error"]
                if event["done"]:
                    input_ended = True
                    pending.append(
                        {
                            "data": {
                                "kind": "end",
                                "value": {
                                    "combined_position": combined_position,
                                    "stream_positions": dict(stream_positions),
                                },
                            },
                            "end": combined_position,
                            "original": None,
                            "sent": False,
                        }
                    )
                else:
                    chunk = event["value"]
                    if chunk["stream"] not in arg["streams"]:
                        raise ProtocolError("invalid process stdio stream")
                    combined_position, stream_positions[chunk["stream"]] = split_chunk(
                        chunk, pending
                    )
                    if not chunk["bytes"] and complete is not None:
                        complete(chunk)
            if not output_event.done():
                continue
            event = output_event.result()
            output_event = None
            if event["kind"] == "output_error" and not is_retryable_stream_error(
                event["error"]
            ):
                raise event["error"]
            if event["kind"] == "output_error" or event["done"]:
                error = event["error"] if event["kind"] == "output_error" else None
                connection, reconnects = await reconnect(
                    client, id, arg, connection, reconnects, error
                )
                for value in pending:
                    value["sent"] = False
                continue
            output = event["value"]
            closed, position = output["closed"], output["position"]
            if not safe_integer(position) or position < 0:
                raise ProtocolError("invalid process stdio write position")
            reconnects = 0
            written = max(written, position)
            while (
                pending
                and pending[0]["data"]["kind"] == "chunk"
                and pending[0]["end"] <= written
            ):
                value = pending.popleft()
                if value["original"] is not None and complete is not None:
                    complete(value["original"])
            if closed:
                return
    finally:
        connection.input.close()
        tasks = [
            task
            for task in (input_event, output_event, drained_event)
            if task is not None
        ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await close_output(connection.output)
        await close_output(input)


def resend(data, written):
    """Skip the bytes of a resent chunk that the server already wrote."""
    if data["kind"] != "chunk" or data["value"]["combined_position"] >= written:
        return data
    chunk = data["value"]
    offset = written - chunk["combined_position"]
    value = {
        **chunk,
        "bytes": chunk["bytes"][offset:],
        "combined_position": chunk["combined_position"] + offset,
        "stream_position": chunk["stream_position"] + offset,
    }
    return {"kind": "chunk", "value": value}


def split_chunk(chunk, pending):
    """Split a chunk into parts, completing the chunk once its last part is written."""
    combined_position = chunk["combined_position"]
    stream_position = chunk["stream_position"]
    length = len(chunk["bytes"])
    offset = 0
    while offset < length:
        size = min(MAX_CHUNK_SIZE, length - offset)
        part = {
            **chunk,
            "bytes": chunk["bytes"][offset : offset + size],
            "combined_position": chunk["combined_position"] + offset,
            "stream_position": chunk["stream_position"] + offset,
        }
        offset += size
        combined_position = part["combined_position"] + size
        stream_position = part["stream_position"] + size
        if not safe_integer(combined_position) or not safe_integer(stream_position):
            raise ProtocolError("invalid stdio position")
        pending.append(
            {
                "data": {"kind": "chunk", "value": part},
                "end": combined_position,
                "original": chunk if offset == length else None,
                "sent": False,
            }
        )
    return combined_position, stream_position


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
    from tangram.client import retry

    return await retry(
        client.stream_retry,
        lambda: write_process_stdio_once(client, id, arg),
        is_retryable_error,
    )


async def reconnect(
    client: Client, id, arg, connection, attempt: int, error
) -> tuple[Connection, int]:
    """Reconnect after the given number of consecutive failed attempts.

    Return the new connection and the number of consecutive failed attempts,
    including this one.
    """
    from tangram.client import backoff

    connection.input.close()
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
            next_ = await write_process_stdio_once(client, id, arg)
        except Exception as error_:
            if not is_retryable_error(error_):
                raise
            error = error_
            continue
        if next_ is None:
            raise ValueError("failed to find the process")

        return next_, attempt


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

    # Write the request body immediately, without waiting for the response headers.
    response = asyncio.ensure_future(client.send(request))
    response.add_done_callback(
        lambda task: None if task.cancelled() else task.exception()
    )
    return Connection(input, receive_server_messages(response, input))


async def receive_server_messages(response, input):
    try:
        response = await response
    except BaseException:
        input.close()
        raise
    if response.status == 404:
        input.close()
        await response.close()
        raise NotFoundError("failed to find the process")
    if not 200 <= response.status < 300:
        input.close()
        raise await response_error(response)
    if response.headers.get("content-type", "").split(";", 1)[0] != "text/event-stream":
        input.close()
        await response.close()
        raise ProtocolError("invalid process stdio response content type")
    async for message in decode_server_messages(response):
        yield message


async def encode_client_messages(input):
    from tangram.process.stdio import Write

    async for data in input:
        yield {"data": json.dumps(Write.Data.to_data(data)), "event": "data"}


async def decode_server_messages(response):
    try:
        async for event in response.sse():
            try:
                if event.get("event") == "error":
                    raise error_from_data(json.loads(event["data"]))
                if event.get("event") != "output":
                    raise ProtocolError("invalid process stdio write message")
                yield json.loads(event["data"])
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


def is_retryable_error(error):
    from tangram.client import is_transport_error

    return not isinstance(error, (NotFoundError, ProtocolError)) and is_transport_error(
        error
    )


def is_retryable_stream_error(error):
    from tangram.client import is_transport_error

    # Retry an open stream's error unless the server sent it.
    return not isinstance(error, (NotFoundError, ProtocolError)) and (
        not isinstance(error, Error) or is_transport_error(error)
    )


class Channel:
    def __init__(self):
        self.closed = False
        self.drained = []
        self.values = deque()
        self.waiters = deque()

    async def wait_drained(self) -> None:
        if not self.values or self.closed:
            return
        waiter = asyncio.get_running_loop().create_future()
        self.drained.append(waiter)
        await waiter

    def _notify_drained(self) -> None:
        while self.drained:
            waiter = self.drained.pop()
            if not waiter.done():
                waiter.set_result(None)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._notify_drained()
        while self.waiters:
            waiter = self.waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self

    async def __anext__(self) -> dict[str, Any]:
        if self.values:
            value = self.values.popleft()
            if not self.values:
                self._notify_drained()
            return value
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
