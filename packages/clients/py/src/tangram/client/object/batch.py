"""Object batch insertion, translated from batch.ts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from tangram.client import Client
    from tangram.object import Object

from ...location import Arg as LocationArg
from ...location import Location
from ...referent import Referent


async def post_object_batch(
    client: Client,
    objects: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    location=None,
) -> Object.Batch.Output:
    arg = (
        dict(objects)
        if isinstance(objects, dict)
        else {"objects": objects, "location": location}
    )
    location = arg.get("location")
    data = {
        **arg,
        "location": None if location is None else location_to_data_string(location),
        "objects": [object_to_data(object_) for object_ in arg["objects"]],
    }
    output = cast(
        dict[str, list[str]], await client._json("POST", "/objects/batch", data=data)
    )
    return {
        "objects": [Referent.from_data_string(object_) for object_ in output["objects"]]
    }


def object_to_data(object_):
    output = {**object_, "data": object_["data"]}
    children = object_.get("children")
    if children is None:
        output.pop("children", None)
    else:
        output["children"] = [
            child.to_data_string() if isinstance(child, Referent) else child
            for child in children
        ]
    return output


def location_to_data_string(location):
    if isinstance(location, str):
        return location
    if "components" in location:
        return LocationArg.to_data_string(location)
    return Location.to_data_string(location)
