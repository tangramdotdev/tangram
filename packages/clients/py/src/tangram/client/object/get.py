"""Object retrieval and proof metadata, translated from get.ts."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.object import Object

from ...error import Error, ErrorDataObject
from ...http import Request, percent_encode
from ...location import Arg as LocationArg
from ...location import Location


async def get_object(
    client: Client, id: str, arg: Object.Get.Arg | None = None, **options
) -> Object.Get.Output:
    output = await try_get_object(client, id, arg, **options)
    if output is None:
        raise await Error.new("failed to find the object", {"values": {"id": id}})
    return output


async def try_get_object(
    client: Client, id: str, arg: Object.Get.Arg | None = None, **options
) -> Object.Get.Output | None:
    options = {**(arg or {}), **options}
    location = options.get("location")
    request = Request(
        "GET", f"/objects/{percent_encode(id)}", {"accept": "application/json"}
    ).arg(
        {
            "location": None if location is None else location_to_data_string(location),
            "metadata": False
            if options.get("metadata") is None
            else options["metadata"],
            "tokens": {} if options.get("tokens") is None else options["tokens"],
        }
    )
    response = await client.send_with_retry(request)
    if response.status == 404:
        await response.close()
        return None
    elif not 200 <= response.status < 300:
        raise Error.from_data(cast(ErrorDataObject, await response.json()))
    return cast("Object.Get.Output", await response.json())


def location_to_data_string(location):
    if isinstance(location, str):
        return location
    if "components" in location:
        return LocationArg.to_data_string(location)
    return Location.to_data_string(location)
