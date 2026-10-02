"""Object insertion and proofs, translated from put.ts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.object import Object

from ...http import percent_encode
from ...location import Arg as LocationArg
from ...location import Location
from ...referent import Referent


async def put_object(
    client: Client, id: str, data: Mapping[str, Any], *, children=None, location=None
) -> Object.Put.Output:
    if "data" in data:
        arg = data
        data = arg["data"]
        children = arg.get("children")
        location = arg.get("location")
    output = await client._json(
        "PUT",
        f"/objects/{percent_encode(id)}",
        data=data,
        arg={
            "children": [child_to_data_string(child) for child in children or []],
            "location": None if location is None else location_to_data_string(location),
        },
    )
    output = cast(dict[str, str], output)
    return {"object": Referent.from_data_string(output["object"])}


def child_to_data_string(child):
    if isinstance(child, Referent):
        return child.to_data_string()
    if isinstance(child, dict):
        return Referent.from_data(child).to_data_string()
    return child


def location_to_data_string(location):
    if isinstance(location, str):
        return location
    if "components" in location:
        return LocationArg.to_data_string(location)
    return Location.to_data_string(location)
