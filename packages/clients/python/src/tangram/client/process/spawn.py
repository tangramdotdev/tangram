"""Process spawning and its source-aligned wire codecs."""

from collections.abc import AsyncIterable, AsyncIterator
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict, cast

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.process.outcome import Outcome

from ...http import Stream
from ...location import Arg as LocationArg
from ...location import ArgObject as LocationArgObject
from ...location import Location, LocationObject
from ...object import Object
from ...progress import Event
from ...referent import Referent

type Stdio = Literal["inherit", "log", "null", "pipe", "tty"]


class CommandArgObject(TypedDict):
    executable: Referent[Any]
    args: NotRequired[list[Any]]
    cwd: NotRequired[str | None]
    env: NotRequired[dict[str, Any]]
    host: NotRequired[str | None]
    stdin: NotRequired[Referent[str] | None]
    user: NotRequired[str | None]


class ArgObject(TypedDict):
    cached: NotRequired[bool]
    cache_location: NotRequired[LocationArgObject | None]
    checksum: NotRequired[str | None]
    command: Referent[CommandArgObject | str] | Object | str
    debug: NotRequired[bool | dict[str, Any] | None]
    location: NotRequired[LocationArgObject | None]
    parent: NotRequired[str | None]
    public: NotRequired[bool]
    retry: NotRequired[bool]
    sandbox: NotRequired[dict[str, Any] | str | None]
    stderr: NotRequired[Stdio]
    stdin: NotRequired[Stdio]
    stdout: NotRequired[Stdio]
    tty: NotRequired[bool | dict[str, Any] | None]


class OutputObject(TypedDict):
    process: int | str
    cached: NotRequired[bool]
    command: NotRequired[str | None]
    lease: NotRequired[str | None]
    location: NotRequired[LocationObject | None]
    outcome: NotRequired["Outcome.Data | None"]
    tokens: NotRequired[dict[str, list[str]] | None]


class Arg:
    @staticmethod
    def to_json(arg: ArgObject) -> dict[str, Any]:
        output = {}
        if "cached" in arg:
            output["cached"] = arg["cached"]
        if "cache_location" in arg:
            location = arg["cache_location"]
            output["cache_location"] = (
                None if location is None else location_arg_to_string(location)
            )
        if "checksum" in arg:
            output["checksum"] = arg["checksum"]
        command = as_referent(arg["command"])
        output["command"] = (
            command.to_data_string()
            if isinstance(command.node, str)
            else command.to_data(CommandArg.to_json)
        )
        if "debug" in arg:
            output["debug"] = arg["debug"]
        if "location" in arg:
            location = arg["location"]
            output["location"] = (
                None if location is None else location_arg_to_string(location)
            )
        if "parent" in arg:
            output["parent"] = arg["parent"]
        if arg.get("public"):
            output["public"] = arg["public"]
        if arg.get("retry"):
            output["retry"] = arg["retry"]
        if "sandbox" in arg:
            output["sandbox"] = arg["sandbox"]
        for stream in ("stderr", "stdin", "stdout"):
            if stream in arg and arg[stream] != "inherit":
                output[stream] = arg[stream]
        if "tty" in arg:
            output["tty"] = arg["tty"]
        return output


class CommandArg:
    @staticmethod
    def to_json(arg: CommandArgObject) -> dict[str, Any]:
        output = {
            **arg,
            "executable": as_referent(arg["executable"]).to_data(),
        }
        if arg.get("stdin") is not None:
            output["stdin"] = as_referent(arg["stdin"]).to_data()
        return output


class Output:
    @staticmethod
    def from_json(output: dict[str, Any]) -> OutputObject:
        result = {
            key: value
            for key, value in output.items()
            if key not in ("location", "outcome")
        }
        if "location" in output:
            location = output["location"]
            result["location"] = (
                Location.from_data_string(location)
                if isinstance(location, str)
                else location
            )
        if "outcome" in output:
            result["outcome"] = output["outcome"]
        return cast(OutputObject, result)


class Spawn:
    Arg = Arg
    CommandArg = CommandArg
    Output = Output


async def spawn_process(
    client: "Client", arg: ArgObject
) -> AsyncIterator[Event[OutputObject]]:
    events = await try_spawn_process(client, arg)
    return map_spawn_events(events)


async def try_spawn_process(
    client: "Client", arg: ArgObject
) -> Stream[Event[OutputObject | None]]:
    return await client._progress(
        "/processes/spawn",
        Arg.to_json(arg),
        lambda output: None if output is None else Output.from_json(output),
    )


async def map_spawn_events(
    events: AsyncIterable[Event[OutputObject | None]],
) -> AsyncIterator[Event[OutputObject]]:
    try:
        async for event in events:
            if event["kind"] == "output" and event["value"] is None:
                raise ValueError("expected a process")
            yield cast(Event[OutputObject], event)
    finally:
        close = getattr(events, "aclose", None)
        if callable(close):
            await close()


def as_referent(value):
    if isinstance(value, Object):
        return value.to_referent()
    if isinstance(value, Referent):
        return value
    return (
        Referent.from_data_string(value)
        if isinstance(value, str)
        else Referent.from_data(value)
    )


def location_arg_to_string(value):
    if isinstance(value, str):
        return value
    if "components" in value:
        return LocationArg.to_data_string(value)
    return Location.to_data_string(value)


spawn_data = Arg.to_json
