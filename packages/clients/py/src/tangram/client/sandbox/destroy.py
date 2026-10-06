"""Sandbox destruction, translated from client/sandbox/destroy.ts."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict, cast

if TYPE_CHECKING:
    from tangram.client import Client

from tangram.error import Error, ErrorData
from tangram.http import Body, Request, percent_encode
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject


class ArgObject(TypedDict, total=False):
    location: LocationArgObject | None


async def destroy_sandbox(
    client: Client, id: str, arg: ArgObject | None = None, **options
) -> None:
    destroyed = await try_destroy_sandbox(client, id, arg, **options)
    if destroyed is None:
        raise ValueError("failed to find the sandbox")
    elif not destroyed:
        raise ValueError("the sandbox was already destroyed")


async def try_destroy_sandbox(
    client: Client, id: str, arg: ArgObject | None = None, **options
) -> bool | None:
    options = {**(arg or {}), **options}
    body = Body.json(
        {
            "location": None
            if options.get("location") is None
            else LocationArg.to_data_string(options["location"]),
        }
    )
    request = Request(
        "POST",
        f"/sandboxes/{percent_encode(id)}/destroy",
        {"content-type": "application/json"},
        body,
    )
    response = await client.send_with_retry(request)
    if response.status == 404:
        await response.close()
        return None
    elif response.status == 409:
        await response.close()
        return False
    elif response.status < 200 or response.status >= 300:
        raise Error.from_data(cast(ErrorData, await response.json()))
    await response.close()
    return True
