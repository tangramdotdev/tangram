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
from ...referent import ReferentData
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
    nonce: NotRequired[str | None]
    open: NotRequired[list[str]]
    process: SpawnArgObject | str


class Header(TypedDict):
    sync: NotRequired[ReferentData[str]]


class ConnectOptions(Wait.Arg):
    pass


class TtySize(TypedDict):
    cols: int
    rows: int


class TtyArg(TypedDict):
    size: TtySize
    location: NotRequired[LocationArgObject | None]
    tokens: NotRequired[dict[str, list[str]] | None]


type ClientRequestArg = (
    Tagged[Literal["cancel"], Cancel.Arg]
    | EmptyTagged[Literal["detach"]]
    | Tagged[Literal["signal"], Signal.Arg]
    | Tagged[Literal["tty"], TtyArg]
)


class Receipt(TypedDict):
    id: int


class RequestMessage(TypedDict):
    arg: ClientRequestArg
    id: int


type ClientMessage = (
    Tagged[Literal["ack"], Receipt]
    | Tagged[Literal["notification"], EmptyTagged[Literal["ready"]]]
    | Tagged[Literal["request"], RequestMessage]
)
type ServerResponseOutput = (
    Tagged[Literal["cancel"], Cancel.Output]
    | EmptyTagged[Literal["detach", "signal", "tty"]]
)


class ResponseMessage(TypedDict):
    error: dict[str, Any] | None
    id: int
    output: ServerResponseOutput | None


type ServerMessage = (
    Tagged[Literal["ack"], Receipt]
    | Tagged[
        Literal["notification"],
        Tagged[Literal["progress"], dict[str, Any]]
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
        async for event in events:
            kind = event.get("event")
            if kind == "error":
                raise Error.from_data(json.loads(event["data"]))
            if kind not in ("ack", "notification", "response"):
                raise ValueError("invalid process connect message")
            value = json.loads(event["data"])
            if (
                kind == "notification"
                and value["kind"] == "progress"
                and value["value"]["kind"] == "output"
            ):
                value["value"]["value"] = Spawn.Output.from_json(
                    value["value"]["value"]
                )
            yield cast(ServerMessage, {"kind": kind, "value": value})

    return header, Stream(messages(), response.close)


async def encode(input: AsyncIterable[ClientMessage]) -> AsyncIterator[SseEvent]:
    async for message in input:
        value: Any = message["value"]
        if message["kind"] == "request":
            value = {**value, "arg": connect_arg(value["arg"])}
        yield {"event": message["kind"], "data": json.dumps(value, allow_nan=False)}


def connect_arg(arg):
    kind = arg["kind"]
    if kind in ("cancel", "signal", "tty"):
        return {"kind": kind, "value": location_arg(arg["value"])}
    return arg


def location_arg(arg):
    location = arg.get("location")
    return {
        **arg,
        "location": None if location is None else location_arg_to_string(location),
    }
