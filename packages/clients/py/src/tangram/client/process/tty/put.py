"""Process TTY resizing, translated from client/process/tty/put.ts."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict, Unpack, cast

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.client.process.connect import TtyArg

from tangram.client.process.connect import TtySize
from tangram.error import Error, ErrorData
from tangram.http import Body, Request, percent_encode
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject


class KeywordOptions(TypedDict, total=False):
    location: LocationArgObject | None
    size: TtySize
    tokens: dict[str, list[str]] | None


async def set_process_tty_size(
    client: Client,
    id: str,
    arg: TtyArg | None = None,
    **options: Unpack[KeywordOptions],
) -> None:
    found = await try_set_process_tty_size(client, id, arg, **options)
    if not found:
        raise ValueError("failed to find the process")


async def try_set_process_tty_size(
    client: Client,
    id: str,
    arg: TtyArg | None = None,
    **options: Unpack[KeywordOptions],
) -> bool | None:
    options = {**(arg or {}), **options}
    location = options.get("location")
    body = Body.json(
        {
            **options,
            "location": None
            if location is None
            else LocationArg.to_data_string(location),
        }
    )
    request = Request(
        "PUT",
        f"/processes/{percent_encode(id)}/tty/size",
        {"content-type": "application/json"},
        body,
    )
    response = await client.send_with_retry(request)
    if response.status == 404:
        await response.close()
        return None
    elif response.status < 200 or response.status >= 300:
        raise Error.from_data(cast(ErrorData, await response.json()))
    await response.close()
    return True
