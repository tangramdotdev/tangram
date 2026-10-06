"""Process signals, translated from client/process/signal.ts."""

from typing import TYPE_CHECKING, TypedDict, Unpack, cast

if TYPE_CHECKING:
    from tangram.client import Client

from typing import Any, NotRequired

from tangram.error import Error, ErrorData
from tangram.http import Body, Request, percent_encode
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject


class KeywordOptions(TypedDict, total=False):
    location: LocationArgObject | None
    signal: str
    tokens: dict[str, list[str]] | None


class Signal:
    class Arg(TypedDict):
        location: NotRequired[LocationArgObject | None]
        signal: str
        tokens: NotRequired[dict[str, list[str]] | None]


async def signal_process(
    client: "Client",
    id: str,
    arg: Signal.Arg | None = None,
    **options: Unpack[KeywordOptions],
) -> None:
    found = await try_signal_process(client, id, arg, **options)
    if not found:
        raise ValueError("failed to find the process")


async def try_signal_process(
    client: "Client",
    id: str,
    arg: Signal.Arg | None = None,
    **options: Unpack[KeywordOptions],
) -> bool | None:
    arg_: dict[str, Any] = {**(arg or {}), **options}
    body = Body.json(
        {
            **arg_,
            "location": None
            if arg_.get("location") is None
            else LocationArg.to_data_string(arg_["location"]),
        }
    )
    request = Request(
        "POST",
        f"/processes/{percent_encode(id)}/signal",
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
