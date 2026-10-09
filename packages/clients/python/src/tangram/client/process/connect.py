"""Bidirectional process connections, translated from connect.ts."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tangram.client import Client

import json
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any, Literal, NotRequired, TypedDict, cast

from ...error import Error, ErrorData
from ...http import Body, Request, Stream, Uri
from ...http.body import SseEvent
from ...location import ArgObject as LocationArgObject
from ...process.stdio import (
    ReadArgObject,
    StdioReadEvent,
    StdioReadOutput,
    StdioWriteOutput,
    WriteArgObject,
)
from .cancel import Cancel
from .signal import Signal
from .spawn import ArgObject as SpawnArgObject
from .spawn import Spawn, location_arg_to_string
from .wait import Wait


class Tagged[K: str, V](TypedDict):
    kind: K
    value: V


class EmptyTagged[K: str](TypedDict):
    kind: K


class ConnectArgObject(Wait.Arg):
    mode: Literal["run", "spawn"]
    process: SpawnArgObject | str
    reads: dict[int, ReadArgObject]


class Header(TypedDict):
    pass


class ConnectOptions(Wait.Arg):
    reads: NotRequired[list[ReadArgObject]]


class TtySize(TypedDict):
    cols: int
    rows: int


class TtyArg(TypedDict):
    size: TtySize
    location: NotRequired[LocationArgObject | None]
    tokens: NotRequired[dict[str, list[str]] | None]


type ClientRequestArg = (
    Tagged[Literal["cancel"], Cancel.Arg]
    | Tagged[Literal["close"], int]
    | EmptyTagged[Literal["detach"]]
    | Tagged[Literal["read"], ReadArgObject]
    | Tagged[Literal["signal"], Signal.Arg]
    | Tagged[Literal["tty"], TtyArg]
    | Tagged[Literal["write"], WriteArgObject]
)


class Receipt(TypedDict):
    id: int


class RequestMessage(TypedDict):
    arg: ClientRequestArg
    id: int


class ReadProgress(TypedDict):
    consumed: int


class ReadNotification(TypedDict):
    id: int
    progress: ReadProgress


type ClientMessage = (
    Tagged[Literal["ack"], Receipt]
    | Tagged[
        Literal["notification"],
        Tagged[Literal["read"], ReadNotification] | EmptyTagged[Literal["ready"]],
    ]
    | Tagged[Literal["request"], RequestMessage]
)
type ServerResponseOutput = (
    Tagged[Literal["cancel"], Cancel.Output]
    | Tagged[Literal["read"], StdioReadOutput]
    | Tagged[Literal["write"], StdioWriteOutput]
    | EmptyTagged[Literal["close", "detach", "signal", "tty"]]
)


class ReadEventNotification(TypedDict):
    id: int
    event: StdioReadEvent


class ResponseMessage(TypedDict):
    error: dict[str, Any] | None
    id: int
    output: ServerResponseOutput | None


type ServerMessage = (
    Tagged[Literal["ack"], Receipt]
    | Tagged[
        Literal["notification"],
        Tagged[Literal["progress"], dict[str, Any]]
        | Tagged[Literal["read"], ReadEventNotification]
        | Tagged[Literal["outcome"], dict[str, Any]],
    ]
    | Tagged[Literal["response"], ResponseMessage]
)


class Connect:
    Mode = Literal["run", "spawn"]
    Options = ConnectOptions
    Arg = ConnectArgObject
    Header = Header
    ClientRequestArg = ClientRequestArg
    ClientMessage = ClientMessage
    ServerResponseOutput = ServerResponseOutput
    ServerMessage = ServerMessage


async def connect_process(
    client: Client, arg: ConnectArgObject, input: AsyncIterable[ClientMessage]
) -> tuple[Header, Stream[ServerMessage]]:
    request = Request(
        {
            "body": Body.sse(encode(input)),
            "headers": {
                "accept": "text/event-stream",
                "content-type": "text/event-stream",
            },
            "method": "POST",
            "uri": Uri({"path": "/processes/connect"}),
        }
    )
    process = arg["process"]
    # Preserve native spawn values that cannot round-trip through a query.
    if not isinstance(process, str):
        request.headers["x-tg-arg-in-body"] = "true"
    request.arg(
        {
            **location_arg(arg),
            "process": process
            if isinstance(process, str)
            else Spawn.Arg.to_json(process),
            "reads": {str(id): stdio_arg(read) for id, read in arg["reads"].items()},
        }
    )
    response = await client.send(request)
    if not 200 <= response.status < 300:
        raise Error.from_data(cast(ErrorData, await response.json()))
    if response.headers.get("content-type", "").split(";", 1)[0] != "text/event-stream":
        await response.close()
        raise ValueError("invalid process connect content type")

    header = cast(Header, await response.body_header())
    events = response.sse()

    async def messages():
        from ...process.stdio import Chunk, Read

        async for event in events:
            kind = event.get("event")
            if kind == "error":
                raise Error.from_data(json.loads(event["data"]))
            if kind not in ("ack", "notification", "response"):
                raise ValueError("invalid process connect message")
            value = json.loads(event["data"])
            if kind == "response":
                output = value.get("output")
                if output is not None and output["kind"] == "read":
                    output["value"] = Read.Output.from_data(output["value"])
            if (
                kind == "notification"
                and value["kind"] == "progress"
                and value["value"]["kind"] == "output"
            ):
                value["value"]["value"] = Spawn.Output.from_json(
                    value["value"]["value"]
                )
            if kind == "notification" and value["kind"] == "read":
                read = value["value"]["event"]
                if read["kind"] == "chunk":
                    read["value"] = Chunk.from_data(read["value"])
            yield cast(ServerMessage, {"kind": kind, "value": value})

    return header, Stream(messages(), response.close)


async def encode(input: AsyncIterable[ClientMessage]) -> AsyncIterator[SseEvent]:
    async for message in input:
        value: Any = message["value"]
        if message["kind"] == "request":
            value = {**value, "arg": connect_arg(value["arg"])}
        yield {"event": message["kind"], "data": json.dumps(value, allow_nan=False)}


def connect_arg(arg):
    from ...process.stdio import Write

    kind = arg["kind"]
    if kind == "read":
        return {"kind": kind, "value": stdio_arg(arg["value"])}
    if kind == "write":
        value = arg["value"]
        return {
            "kind": kind,
            "value": location_arg({**value, "data": Write.Data.to_data(value["data"])}),
        }
    if kind in ("cancel", "signal", "tty"):
        return {"kind": kind, "value": location_arg(arg["value"])}
    return arg


def location_arg(arg):
    location = arg.get("location")
    return {
        **arg,
        "location": None if location is None else location_arg_to_string(location),
    }


def stdio_arg(arg):
    return {**location_arg(arg), "streams": ",".join(arg["streams"])}
