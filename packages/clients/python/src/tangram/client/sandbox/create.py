"""Sandbox creation, translated from client/sandbox/create.ts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict, cast

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.sandbox import SandboxData, SandboxOutput

from tangram.error import Error, ErrorData
from tangram.http import Body, Request
from tangram.location import Location, LocationObject


class DataArgObject(TypedDict, total=False):
    cpu: int | float | None
    host: str | None
    hostname: str | None
    isolation: dict[str, Literal["container", "seatbelt", "vm"]] | None
    location: str | None
    memory: int | float | None
    mounts: list[dict[str, Any]]
    network: dict[str, Any] | None
    owner: str | None
    ttl: int | float | None


class WireOutput(TypedDict):
    data: SandboxData
    location: NotRequired[str | LocationObject | None]
    tokens: NotRequired[dict[str, list[str]] | None]


async def create_sandbox(client: Client, arg: DataArgObject) -> SandboxOutput:
    body = Body.json(arg)
    request = Request(
        "POST",
        "/sandboxes",
        {"accept": "application/json", "content-type": "application/json"},
        body,
    )
    response = await client.send_with_retry(request)
    if response.status < 200 or response.status >= 300:
        raise Error.from_data(cast(ErrorData, await response.json()))
    output = cast(WireOutput, await response.json())
    location = output.get("location")
    if isinstance(location, str):
        output["location"] = Location.from_data_string(location)
    return cast("SandboxOutput", output)
