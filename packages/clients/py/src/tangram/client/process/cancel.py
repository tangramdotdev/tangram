"""Process cancellation, translated from client/process/cancel.ts."""

from typing import TYPE_CHECKING, TypedDict, Unpack, cast

if TYPE_CHECKING:
    from tangram.client import Client

from typing import Any, NotRequired

from tangram.error import Error, ErrorDataObject
from tangram.http import Request, percent_encode
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject


class KeywordOptions(TypedDict, total=False):
    lease: str
    location: LocationArgObject | None


class Cancel:
    class Arg(TypedDict):
        lease: str
        location: NotRequired[LocationArgObject | None]

    class Output(TypedDict):
        released: bool


async def cancel_process(
    client: "Client",
    id: str,
    arg: Cancel.Arg | None = None,
    **options: Unpack[KeywordOptions],
) -> Cancel.Output:
    output = await try_cancel_process(client, id, arg, **options)
    if output is None:
        raise ValueError("failed to find the process")
    return output


async def try_cancel_process(
    client: "Client",
    id: str,
    arg: Cancel.Arg | None = None,
    **options: Unpack[KeywordOptions],
) -> Cancel.Output | None:
    arg_: dict[str, Any] = {**(arg or {}), **options}
    request = Request("POST", f"/processes/{percent_encode(id)}/cancel").arg(
        {
            "lease": arg_["lease"],
            "location": None
            if arg_.get("location") is None
            else LocationArg.to_data_string(arg_["location"]),
        }
    )
    response = await client.send_with_retry(request)
    if response.status == 404:
        await response.close()
        return None
    elif response.status < 200 or response.status >= 300:
        raise Error.from_data(cast(ErrorDataObject, await response.json()))
    return cast(Cancel.Output, await response.json())
