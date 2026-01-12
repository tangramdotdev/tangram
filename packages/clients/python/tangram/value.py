"""Value type and serialization."""

from __future__ import annotations

import base64
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    TypedDict,
    Union,
)

if TYPE_CHECKING:
    from tangram.mutation import Mutation
    from tangram.object_ import Object_
    from tangram.placeholder import Placeholder
    from tangram.template import Template

# Value: Union of all valid Tangram value types.
Value = Union[
    None,
    bool,
    int,
    float,
    str,
    List["Value"],
    Dict[str, "Value"],
    "Object_",
    bytes,
    "Mutation[Any]",
    "Template",
    "Placeholder",
]


class MapData(TypedDict, total=False):
    """Serialized map data."""

    kind: str
    value: Dict[str, "ValueData"]


class ObjectRefData(TypedDict):
    """Serialized object reference data."""

    kind: str
    value: str


class BytesData(TypedDict):
    """Serialized bytes data."""

    kind: str
    value: str  # base64 encoded


class MutationData(TypedDict):
    """Serialized mutation data."""

    kind: str
    value: Dict[str, Any]


class TemplateData(TypedDict):
    """Serialized template data."""

    kind: str
    value: Dict[str, Any]


class PlaceholderData(TypedDict):
    """Serialized placeholder data."""

    kind: str
    value: Dict[str, Any]


# ValueData: Serialized form of Value.
ValueData = Union[
    None,
    bool,
    int,
    float,
    str,
    List["ValueData"],
    MapData,
    ObjectRefData,
    BytesData,
    MutationData,
    TemplateData,
    PlaceholderData,
]


def to_data(value: Value) -> ValueData:
    """Serialize a Value to ValueData."""
    from tangram.mutation import Mutation
    from tangram.object_ import Object_
    from tangram.placeholder import Placeholder
    from tangram.template import Template

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    elif isinstance(value, list):
        return [to_data(v) for v in value]
    elif Object_.is_(value):
        return {"kind": "object", "value": value.id}
    elif isinstance(value, bytes):
        return {"kind": "bytes", "value": base64.b64encode(value).decode("ascii")}
    elif isinstance(value, Mutation):
        return {"kind": "mutation", "value": value.to_data()}
    elif isinstance(value, Template):
        return {"kind": "template", "value": value.to_data()}
    elif isinstance(value, Placeholder):
        return {"kind": "placeholder", "value": value.to_data()}
    elif isinstance(value, dict):
        return {"kind": "map", "value": {k: to_data(v) for k, v in value.items()}}
    else:
        raise ValueError(f"invalid value type: {type(value)}")


def from_data(data: ValueData) -> Value:
    """Deserialize ValueData to a Value."""
    from tangram.mutation import Mutation
    from tangram.object_ import Object_
    from tangram.placeholder import Placeholder
    from tangram.template import Template

    if data is None or isinstance(data, (bool, int, float, str)):
        return data
    elif isinstance(data, list):
        return [from_data(d) for d in data]
    elif isinstance(data, dict):
        kind = data.get("kind")
        if kind == "map":
            return {k: from_data(v) for k, v in data["value"].items()}
        elif kind == "object":
            return Object_.with_id(data["value"])
        elif kind == "bytes":
            return base64.b64decode(data["value"])
        elif kind == "mutation":
            return Mutation.from_data(data["value"])
        elif kind == "template":
            return Template.from_data(data["value"])
        elif kind == "placeholder":
            return Placeholder.from_data(data["value"])
        else:
            raise ValueError(f"unknown value data kind: {kind}")
    else:
        raise ValueError(f"invalid value data type: {type(data)}")


def is_value(value: Any) -> bool:
    """Check if a value is a valid Tangram Value."""
    from tangram.mutation import Mutation
    from tangram.object_ import Object_
    from tangram.placeholder import Placeholder
    from tangram.template import Template

    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return True
    elif isinstance(value, (Object_, Mutation, Template, Placeholder)):
        return True
    elif isinstance(value, list):
        return all(is_value(v) for v in value)
    elif isinstance(value, dict):
        return all(isinstance(k, str) and is_value(v) for k, v in value.items())
    else:
        return False


def is_array(value: Any) -> bool:
    """Check if value is an array of valid Values."""
    if not isinstance(value, list):
        return False
    return all(is_value(v) for v in value)


def is_map(value: Any) -> bool:
    """Check if value is a map of string keys to Values."""
    if not isinstance(value, dict):
        return False
    return all(isinstance(k, str) and is_value(v) for k, v in value.items())


def objects(value: Any) -> List[Any]:
    """Extract all Object references from a value recursively."""
    from tangram.object_ import Object_

    result: List[Any] = []
    if Object_.is_(value):
        result.append(value)
    elif isinstance(value, list):
        for v in value:
            result.extend(objects(v))
    elif isinstance(value, dict):
        for v in value.values():
            result.extend(objects(v))
    return result


async def store(value: Any) -> None:
    """Store all objects in the value, matching JS Value.store behavior.

    This collects all unstored objects in reverse topological order,
    then stores them in a single batch.
    """
    from tangram.handle import get_handle
    from tangram.object_ import Object_
    from tangram.resolve import resolve

    resolved = await resolve(value)

    # Collect all unstored objects in reverse topological order.
    unstored: List[Any] = []
    stack = [obj for obj in objects(resolved) if not obj.state.stored]

    while stack:
        obj = stack.pop()
        unstored.append(obj)
        if obj.state.object is None:
            continue
        children = Object_.Object_.children(obj.state.object)
        stack.extend([child for child in children if not child.state.stored])

    unstored.reverse()
    if not unstored:
        return

    # Store the objects.
    objects_to_post: List[Dict[str, Any]] = []
    for obj in unstored:
        if obj.state.object is None:
            continue
        data = Object_.Object_.to_data(obj.state.object)
        id_ = get_handle().object_id(data)
        obj.state.id = id_
        objects_to_post.append({"id": id_, "data": data})

    if objects_to_post:
        await get_handle().post_object_batch({"objects": objects_to_post})

    # Mark all objects stored.
    for obj in unstored:
        obj.state.stored = True
