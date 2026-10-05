"""Process retrieval, translated from client/process/get.ts."""

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.process import ProcessDataObject

import json
from typing import Any, NotRequired, TypedDict

from tangram.error import Error, ErrorData
from tangram.http import Request, percent_encode
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject
from tangram.location import Location, LocationObject


class Get:
    class Arg(TypedDict):
        location: NotRequired[LocationArgObject | None]
        metadata: NotRequired[bool]
        source: NotRequired[str]
        tokens: NotRequired[dict[str, list[str]] | None]

    class Output(TypedDict):
        data: "ProcessDataObject"
        id: str
        location: NotRequired[LocationObject | None]
        metadata: NotRequired[object]
        source: NotRequired[str]
        tokens: NotRequired[dict[str, list[str]] | None]


class WireOutput(TypedDict):
    data: "ProcessDataObject"
    id: str
    location: NotRequired[str | LocationObject | None]
    metadata: NotRequired[object]
    source: NotRequired[str]
    tokens: NotRequired[dict[str, list[str]] | None]


async def get_process(
    client: "Client", id: str, arg: Get.Arg | None = None, **options
) -> Get.Output:
    output = await try_get_process(client, id, arg, **options)
    if output is None:
        raise ValueError("failed to find the process")
    return output


async def try_get_process(
    client: "Client", id: str, arg: Get.Arg | None = None, **options
) -> Get.Output | None:
    arg_: dict[str, Any] = {**(arg or {}), **options}
    request = Request(
        "GET", f"/processes/{percent_encode(id)}", {"accept": "application/json"}
    ).arg(
        {
            "location": None
            if arg_.get("location") is None
            else LocationArg.to_data_string(arg_["location"]),
            "metadata": False if arg_.get("metadata") is None else arg_["metadata"],
            "source": "auto" if arg_.get("source") is None else arg_["source"],
            "tokens": {} if arg_.get("tokens") is None else arg_["tokens"],
        }
    )
    response = await client.send_with_retry(request)
    if response.status == 404:
        await response.close()
        return None
    elif response.status < 200 or response.status >= 300:
        raise Error.from_data(cast(ErrorData, await response.json()))
    output = cast(WireOutput, await response.json())
    location = output.get("location")
    if isinstance(location, str):
        output["location"] = Location.from_data_string(location)
    metadata = response.headers.get("x-tg-process-metadata")
    if metadata is not None:
        output["metadata"] = json.loads(metadata)
    return cast(Get.Output, output)
