"""Tangram values and their JSON and TGON representations."""

from __future__ import annotations

import asyncio
import base64
import json
import math
from collections.abc import Awaitable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, TypedDict, TypeGuard, cast

from .. import _native, authorization
from ..module import Module
from ..mutation import UNSET as UNSET
from ..mutation import Mutation
from ..placeholder import Placeholder
from ..placeholder import output as output
from ..referent import Referent
from ..template import Template

if TYPE_CHECKING:
    from ..client import Client
    from ..location import LocationObject
    from ..module import ModuleDataObject
    from ..mutation import DataType as MutationDataType
    from ..object import Object
    from ..placeholder import Data as PlaceholderData
    from ..template import TemplateDataType
    from .print import Options as PrinterOptions

# Explicit recursive inputs substitute for Unresolved[Value] until mapped types exist.
type ValueType = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | bytearray
    | memoryview
    | Sequence[ValueType]
    | Mapping[str, ValueType]
    | Object
    | Mutation[Any]
    | Module
    | Template
    | Placeholder
)
type ValueInput = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | bytearray
    | memoryview
    | Sequence[ValueInput]
    | Mapping[str, ValueInput]
    | Object
    | Mutation[Any]
    | Module
    | Template
    | Placeholder
    | Awaitable[ValueInput]
)


class ObjectValueData(TypedDict):
    kind: Literal["object"]
    value: str


class BytesValueData(TypedDict):
    kind: Literal["bytes"]
    value: str


class MapValueData(TypedDict):
    kind: Literal["map"]
    value: dict[str, ValueData]


class MutationValueData(TypedDict):
    kind: Literal["mutation"]
    value: MutationDataType


class ModuleValueData(TypedDict):
    kind: Literal["module"]
    value: ModuleDataObject


class TemplateValueData(TypedDict):
    kind: Literal["template"]
    value: TemplateDataType


class PlaceholderValueData(TypedDict):
    kind: Literal["placeholder"]
    value: PlaceholderData


type ValueData = (
    None
    | bool
    | int
    | float
    | str
    | list[ValueData]
    | ObjectValueData
    | BytesValueData
    | MapValueData
    | MutationValueData
    | ModuleValueData
    | TemplateValueData
    | PlaceholderValueData
)


class Value:
    type Type = ValueType
    type Input = ValueInput
    type PrintOptions = PrinterOptions

    @staticmethod
    def parse(value: str) -> ValueType:
        return Value.from_data(cast(ValueData, json.loads(_native.parse_value(value))))

    @staticmethod
    def stringify(value: ValueType) -> str:
        return _native.stringify_value(
            json.dumps(Value.to_data(value), allow_nan=False)
        )

    @staticmethod
    def print(value: ValueType, options: PrinterOptions | None = None) -> str:
        from .print import Printer

        return Printer(options).print(value)

    @staticmethod
    def to_data(value: ValueType) -> ValueData:
        from ..object import Object

        if value is None or isinstance(value, (bool, str)):
            return value
        if isinstance(value, (int, float)):
            try:
                valid = math.isfinite(value) and (
                    not isinstance(value, int) or int(float(value)) == value
                )
            except OverflowError:
                valid = False
            if not valid:
                raise ValueError("the number cannot be represented by a Tangram value")
            return value
        if isinstance(value, (bytes, bytearray, memoryview)):
            return {"kind": "bytes", "value": base64.b64encode(value).decode()}
        if isinstance(value, Sequence):
            return [Value.to_data(child) for child in value]
        if isinstance(value, Mapping):
            if any(not isinstance(key, str) for key in value):
                raise TypeError("Tangram map keys must be strings")
            return {
                "kind": "map",
                "value": {key: Value.to_data(child) for key, child in value.items()},
            }
        if isinstance(value, Object):
            return {"kind": "object", "value": value.to_referent().to_data_string()}
        if isinstance(value, Placeholder):
            return {"kind": "placeholder", "value": {"name": value.name}}
        if isinstance(value, Mutation):
            return {"kind": "mutation", "value": value.to_data()}
        if isinstance(value, Module):
            return {"kind": "module", "value": value.to_data()}
        if isinstance(value, Template):
            return {"kind": "template", "value": value.to_data()}
        raise TypeError(f"invalid Tangram value {type(value).__name__}")

    @staticmethod
    def from_data(data: ValueData) -> ValueType:
        from ..object import Object

        if isinstance(data, list):
            return [Value.from_data(child) for child in data]
        if data is None or isinstance(data, (bool, int, float, str)):
            return data
        if data["kind"] == "map":
            return {key: Value.from_data(child) for key, child in data["value"].items()}
        if data["kind"] == "bytes":
            return base64.b64decode(data["value"], validate=True)
        if data["kind"] == "object":
            return Object.with_referent(Referent.from_data_string(data["value"]))
        if data["kind"] == "placeholder":
            return Placeholder(data["value"]["name"])
        if data["kind"] == "mutation":
            return Mutation.from_data(data["value"])
        if data["kind"] == "module":
            return Module.from_data(data["value"])
        if data["kind"] == "template":
            return Template.from_data(data["value"])
        raise ValueError("invalid Tangram value kind")

    @staticmethod
    def is_(value: object) -> TypeGuard[ValueType]:
        from ..object import Object

        return (
            value is None
            or isinstance(
                value,
                (
                    bool,
                    int,
                    float,
                    str,
                    bytes,
                    bytearray,
                    memoryview,
                    Object,
                    Mutation,
                    Module,
                    Template,
                    Placeholder,
                ),
            )
            or Value.is_array(value)
            or Value.is_map(value)
        )

    @staticmethod
    def expect(value: object) -> ValueType:
        if not Value.is_(value):
            raise AssertionError("assertion failed")
        return value

    @staticmethod
    def assert_(value: object) -> None:
        if not Value.is_(value):
            raise AssertionError("assertion failed")

    @staticmethod
    def is_array(value: object) -> TypeGuard[Sequence[ValueType]]:
        return (
            isinstance(value, Sequence)
            and not isinstance(value, (str, bytes, bytearray, memoryview))
            and all(Value.is_(child) for child in value)
        )

    @staticmethod
    def is_map(value: object) -> TypeGuard[Mapping[str, ValueType]]:
        return isinstance(value, Mapping) and all(
            isinstance(key, str) and Value.is_(child) for key, child in value.items()
        )

    @staticmethod
    def objects(value: ValueType) -> list[Object]:
        from ..object import Object

        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray, memoryview)
        ):
            return [object_ for child in value for object_ in Value.objects(child)]
        if Value.is_map(value):
            return [
                object_ for child in value.values() for object_ in Value.objects(child)
            ]
        if isinstance(value, Object):
            return [value]
        if isinstance(value, Mutation):
            return value.objects()
        if isinstance(value, Module):
            return Module.children(value)
        if isinstance(value, Template):
            return value.objects()
        return []

    @staticmethod
    def inherit_location(value: ValueType, location: LocationObject | None) -> None:
        for object_ in Value.objects(value):
            from ..object import Object

            Object.inherit_location(object_, location)

    @staticmethod
    def inherit_tokens(value: ValueType, tokens: dict[str, list[str]]) -> None:
        for object_ in Value.objects(value):
            object_._inherit_tokens(tokens)

    @staticmethod
    async def store(value: ValueType, client: Client | None = None) -> None:
        if client is None:
            from ..client import client

        while True:
            # Collect all unstored states with children before parents.
            pending = set()
            states = []
            stack = [(False, object_) for object_ in Value.objects(value)]
            visited = set()
            while stack:
                expanded, object_ = stack.pop()
                if expanded:
                    states.append(object_)
                    continue
                identity = id(object_)
                if identity in visited:
                    continue
                visited.add(identity)
                if object_._stored:
                    continue
                if object_._storing is not None:
                    pending.add(object_._storing)
                    continue
                stack.append((True, object_))
                if object_._value is not None:
                    stack.extend((False, child) for child in object_._children())

            # Wait for overlapping store promises and plan the batch again.
            if pending:
                await asyncio.gather(*(asyncio.shield(task) for task in pending))
                continue
            if not states:
                return

            # Claim the states and start the store promise.
            task = asyncio.create_task(_store_states(states, client))
            for state in states:
                state._storing = task

            def clear(completed):
                for state in states:
                    if state._storing is completed:
                        state._storing = None

            task.add_done_callback(clear)
            await asyncio.shield(task)
            return

    class Data:
        type Type = ValueData

        @staticmethod
        def children(data: ValueData) -> list[str]:
            if data is None or isinstance(data, (bool, int, float, str)):
                return []
            if isinstance(data, list):
                return [child for value in data for child in Value.Data.children(value)]
            if data["kind"] == "map":
                return [
                    child
                    for value in data["value"].values()
                    for child in Value.Data.children(value)
                ]
            if data["kind"] == "object":
                return [Referent.from_data_string(data["value"]).node]
            if data["kind"] == "mutation":
                return Mutation.Data.children(data["value"])
            if data["kind"] == "module":
                return Module.Data.children(data["value"])
            if data["kind"] == "template":
                return Template.Data.children(data["value"])
            return []

        @staticmethod
        def without_location_and_tokens(data: ValueData) -> ValueData:
            if isinstance(data, list):
                return [Value.Data.without_location_and_tokens(value) for value in data]
            if data is None or isinstance(data, (bool, int, float, str)):
                return data
            if data["kind"] == "map":
                value = {
                    key: Value.Data.without_location_and_tokens(value)
                    for key, value in data["value"].items()
                }
            elif data["kind"] == "object":
                referent = Referent.from_data_string(data["value"])
                value = referent.without_location_and_tokens().to_data_string()
            elif data["kind"] == "mutation":
                value = Mutation.Data.without_location_and_tokens(data["value"])
            elif data["kind"] == "module":
                value = Module.Data.without_location_and_tokens(data["value"])
            elif data["kind"] == "template":
                value = Template.Data.without_location_and_tokens(data["value"])
            else:
                return cast(ValueData, dict(data))
            return cast(ValueData, {**data, "value": value})


async def _store_states(states, client):
    # Create the batch.
    objects: list[dict[str, Any]] = []
    state_group_indices = {}
    state_groups = []
    for state in states:
        if state._value is None:
            raise ValueError("expected the object to be loaded")
        from ..object import Object

        data = Object.Data.without_location_and_tokens(Object.to_data(state))
        data = json.loads(_native.normalize_object(json.dumps(data)))
        id_ = _native.object_id(json.dumps(data))
        state._id = id_
        children = [child.to_referent() for child in state._children()]
        group_index = state_group_indices.get(id_)
        if group_index is None:
            group_index = len(state_groups)
            objects.append({"children": children, "data": data, "id": id_})
            state_group_indices[id_] = group_index
            state_groups.append([])
        else:
            objects[group_index]["children"].extend(children)
        state_groups[group_index].append(state)

    # Store the batch.
    output = await client.post_object_batch(objects)

    # Update the states after validating the complete response.
    nodes = output["objects"]
    if len(nodes) != len(state_groups):
        raise ValueError("invalid object batch output")
    for states, node in zip(state_groups, nodes):
        if not states or any(state.id != node.node for state in states):
            raise ValueError("invalid object batch output")
    for states, node in zip(state_groups, nodes):
        for state in states:
            tokens = authorization.clone(node.options.get("tokens"))
            state.tokens = authorization.inherit(tokens, state.tokens, state.id)
            state._location = node.options.get("location")
            state._stored = True
