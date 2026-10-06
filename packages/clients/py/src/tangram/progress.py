"""Progress events, translated from progress.ts."""

from __future__ import annotations

import json
from collections.abc import AsyncIterable, AsyncIterator, Callable
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict, cast

if TYPE_CHECKING:
    from .diagnostic import DiagnosticData

from .error import Error
from .http import Response

IndicatorFormat = Literal["normal", "bytes"]
Level = Literal["success", "info", "warning", "error"]


class DiagnosticEvent(TypedDict):
    kind: Literal["diagnostic"]
    value: DiagnosticData


class IndicatorsEvent(TypedDict):
    kind: Literal["indicators"]
    value: list[Indicator]


class LogEvent(TypedDict):
    kind: Literal["log"]
    value: Log


class OutputEvent[T](TypedDict):
    kind: Literal["output"]
    value: T


type Event[T] = DiagnosticEvent | IndicatorsEvent | LogEvent | OutputEvent[T]


class Indicator(TypedDict):
    current: NotRequired[float | None]
    format: IndicatorFormat
    name: str
    title: str
    total: NotRequired[float | None]


class Log(TypedDict):
    level: NotRequired[Level | None]
    message: str


async def progress[T](
    response: Response, convert: Callable[[Any], T] = lambda value: value
) -> AsyncIterator[Event[T]]:
    async for event in response.sse():
        kind = event.get("event")
        if kind == "error":
            data = json.loads(event["data"])
            raise (
                Error.with_id(data) if isinstance(data, str) else Error.from_data(data)
            )
        elif kind is None:
            parsed = json.loads(event["data"])
            yield (
                {"kind": "output", "value": convert(parsed["value"])}
                if parsed["kind"] == "output"
                else parsed
            )
        elif kind in ("diagnostic", "indicators", "log", "output"):
            value = json.loads(event["data"])
            yield cast(
                Event[T],
                {
                    "kind": kind,
                    "value": convert(value) if kind == "output" else value,
                },
            )
        else:
            raise ValueError("invalid progress event")


async def last_output[T](events: AsyncIterable[Event[T]]) -> T | None:
    output: T | None = None
    async for event in events:
        if event["kind"] == "output":
            output = event["value"]
    return output


class Progress:
    Event = Event
    Indicator = Indicator
    IndicatorFormat = IndicatorFormat
    Log = Log
    Level = Level
    decode = staticmethod(progress)
    last_output = staticmethod(last_output)
