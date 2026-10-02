"""Sandbox retrieval, translated from client/sandbox/get.ts."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict, cast

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.sandbox import SandboxOutput

from tangram.error import Error, ErrorDataObject
from tangram.http import Request, percent_encode
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject
from tangram.location import Location

from .create import WireOutput


class ArgObject(TypedDict, total=False):
    location: LocationArgObject | None
    source: str
    tokens: dict[str, list[str]] | None


async def get_sandbox(
    client: Client, id: str, arg: ArgObject | None = None, **options
) -> SandboxOutput:
    output = await try_get_sandbox(client, id, arg, **options)
    if output is None:
        raise ValueError("failed to find the sandbox")
    return output


async def try_get_sandbox(
    client: Client, id: str, arg: ArgObject | None = None, **options
) -> SandboxOutput | None:
    options = {**(arg or {}), **options}
    request = Request(
        "GET", f"/sandboxes/{percent_encode(id)}", {"accept": "application/json"}
    ).arg(
        {
            "source": "auto" if options.get("source") is None else options["source"],
            "tokens": {} if options.get("tokens") is None else options["tokens"],
            "location": None
            if options.get("location") is None
            else LocationArg.to_data_string(options["location"]),
        }
    )
    response = await client.send_with_retry(request)
    if response.status == 404:
        await response.close()
        return None
    elif response.status < 200 or response.status >= 300:
        raise Error.from_data(cast(ErrorDataObject, await response.json()))
    output = cast(WireOutput, await response.json())
    location = output.get("location")
    if isinstance(location, str):
        output["location"] = Location.from_data_string(location)
    return cast("SandboxOutput", output)
