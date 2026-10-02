"""Process storage, translated from client/process/put.ts."""

from typing import TYPE_CHECKING, TypedDict, cast

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.process import ProcessDataObject

from typing import Any, NotRequired

from tangram.error import Error, ErrorDataObject
from tangram.http import Body, Request, percent_encode
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject


class KeywordOptions(TypedDict, total=False):
    location: LocationArgObject | None


class Put:
    class Arg(TypedDict):
        data: "ProcessDataObject"
        location: NotRequired[LocationArgObject | None]

    class Output(TypedDict):
        tokens: NotRequired[dict[str, list[str]] | None]


async def put_process(
    client: "Client",
    id: str,
    arg: "Put.Arg | ProcessDataObject | None" = None,
    *,
    data=None,
    **options,
) -> Put.Output:
    arg_: dict[str, Any] = dict(arg or {})
    if not arg_:
        arg_ = {}
    elif "data" not in arg_:
        arg_ = {"data": arg_}
    arg_ = {**arg_, **options}
    if data is not None:
        arg_["data"] = data
    body = Body.json(
        {
            **arg_,
            "data": arg_["data"],
            "location": None
            if arg_.get("location") is None
            else LocationArg.to_data_string(arg_["location"]),
        }
    )
    request = Request(
        "PUT",
        f"/processes/{percent_encode(id)}",
        {"accept": "application/json", "content-type": "application/json"},
        body,
    )
    response = await client.send_with_retry(request)
    if response.status < 200 or response.status >= 300:
        raise Error.from_data(cast(ErrorDataObject, await response.json()))
    return cast(Put.Output, await response.json())
