"""Structured locations and the canonical location argument grammar."""

from __future__ import annotations

import re
from typing import ClassVar, NotRequired, TypedDict, TypeGuard, cast


class LocalObject(TypedDict):
    region: NotRequired[str | None]


class RemoteObject(LocalObject):
    name: str


type LocationObject = LocalObject | RemoteObject


class LocalComponentObject(TypedDict):
    regions: NotRequired[list[str] | None]


class RemoteComponentObject(LocalComponentObject):
    name: str


type ComponentObject = LocalComponentObject | RemoteComponentObject


class ArgObject(TypedDict):
    components: list[ComponentObject]


class Parsed[T](TypedDict):
    index: int
    value: T


class Location:
    Local: ClassVar[type[Local]]
    Remote: ClassVar[type[Remote]]
    Arg: ClassVar[type[Arg]]
    type Type = LocationObject

    @staticmethod
    def is_(value: object) -> TypeGuard[LocationObject]:
        return Local.is_(value) or Remote.is_(value)

    @staticmethod
    def to_data_string(value: LocationObject) -> str:
        arg = Arg.from_location(value)
        return Arg.to_data_string(arg)

    @staticmethod
    def from_data_string(data: str) -> LocationObject:
        arg = Arg.from_data_string(data)
        location = Arg.to_location(arg)
        if location is None:
            raise ValueError("expected exactly one location")
        return location


class Local:
    @staticmethod
    def is_(value: object) -> TypeGuard[LocalObject]:
        return (
            isinstance(value, dict)
            and not any(
                key in value for key in ("components", "name", "regions", "remote")
            )
            and (value.get("region") is None or isinstance(value["region"], str))
        )


class Remote:
    @staticmethod
    def is_(value: object) -> TypeGuard[RemoteObject]:
        return (
            isinstance(value, dict)
            and not any(key in value for key in ("components", "regions", "remote"))
            and "name" in value
            and isinstance(value["name"], str)
            and (value.get("region") is None or isinstance(value["region"], str))
        )


class Arg:
    Component: ClassVar[type[Component]]
    LocalComponent: ClassVar[type[LocalComponent]]
    RemoteComponent: ClassVar[type[RemoteComponent]]
    type Type = ArgObject

    @staticmethod
    def is_(value: object) -> TypeGuard[ArgObject]:
        return (
            isinstance(value, dict)
            and isinstance(value.get("components"), list)
            and all(Component.is_(component) for component in value["components"])
        )

    @staticmethod
    def to_data_string(value: ArgObject) -> str:
        output = []
        for component in value["components"]:
            regions = [
                region for region in component.get("regions") or [] if len(region) > 0
            ]
            if "name" in component:
                string = "remote"
                if cast(RemoteComponentObject, component)["name"] != "default":
                    string += ":" + cast(RemoteComponentObject, component)["name"]
                if regions:
                    string += "(" + ",".join(regions) + ")"
                output.append(string)
                continue
            string = "local"
            if regions:
                string += "(" + ",".join(regions) + ")"
            output.append(string)
        return ",".join(output)

    @staticmethod
    def from_data_string(data: str) -> ArgObject:
        index = skip_whitespace(data, 0)
        components: list[ComponentObject] = []
        while index < len(data):
            component = parse_component(data, index)
            components.append(component["value"])
            index = skip_whitespace(data, component["index"])
            if index >= len(data):
                break
            if data[index] != ",":
                raise ValueError("invalid location arg")
            index = skip_whitespace(data, index + 1)
        return {"components": components}

    @staticmethod
    def from_location(value: LocationObject) -> ArgObject:
        if not isinstance(value, dict):
            raise TypeError("invalid location")
        region = value.get("region")
        if "name" not in value:
            return {"components": [{} if region is None else {"regions": [region]}]}
        component: RemoteComponentObject = {"name": cast(RemoteObject, value)["name"]}
        if region is not None:
            component["regions"] = [region]
        return {"components": [component]}

    @staticmethod
    def to_location(value: ArgObject) -> LocationObject | None:
        if len(value["components"]) != 1:
            return None
        component = value["components"][0]
        regions = component.get("regions")
        if "name" not in component:
            if regions is not None and len(regions) != 1:
                return None
            return {} if regions is None else {"region": regions[0]}
        if regions is not None and len(regions) != 1:
            return None
        output: RemoteObject = {"name": cast(RemoteComponentObject, component)["name"]}
        if regions is not None:
            output["region"] = regions[0]
        return output


class Component:
    @staticmethod
    def is_(value: object) -> TypeGuard[ComponentObject]:
        return LocalComponent.is_(value) or RemoteComponent.is_(value)


class LocalComponent:
    @staticmethod
    def is_(value: object) -> TypeGuard[LocalComponentObject]:
        return (
            isinstance(value, dict)
            and not any(
                key in value for key in ("components", "name", "region", "remote")
            )
            and (
                value.get("regions") is None
                or isinstance(value["regions"], list)
                and all(isinstance(region, str) for region in value["regions"])
            )
        )


class RemoteComponent:
    @staticmethod
    def is_(value: object) -> TypeGuard[RemoteComponentObject]:
        return (
            isinstance(value, dict)
            and not any(key in value for key in ("components", "region"))
            and "name" in value
            and isinstance(value["name"], str)
            and (
                value.get("regions") is None
                or isinstance(value["regions"], list)
                and all(isinstance(region, str) for region in value["regions"])
            )
        )


Location.Local = Local
Location.Remote = Remote
Location.Arg = Arg
Arg.Component = Component
Arg.LocalComponent = LocalComponent
Arg.RemoteComponent = RemoteComponent


def parse_component(data: str, index: int) -> Parsed[ComponentObject]:
    if data.startswith("local", index):
        regions = parse_optional_regions(data, index + len("local"))
        return {
            "value": {} if regions["value"] is None else {"regions": regions["value"]},
            "index": regions["index"],
        }
    if not data.startswith("remote", index):
        raise ValueError("invalid location arg")
    next_index = index + len("remote")
    name = "default"
    if data[next_index : next_index + 1] == ":":
        parsed = parse_name(data, next_index + 1)
        name = parsed["value"]
        next_index = parsed["index"]
    regions = parse_optional_regions(data, next_index)
    value: RemoteComponentObject = {"name": name}
    if regions["value"] is not None:
        value["regions"] = regions["value"]
    return {"value": value, "index": regions["index"]}


def parse_optional_regions(data: str, index: int) -> Parsed[list[str] | None]:
    index = skip_whitespace(data, index)
    if data[index : index + 1] != "(":
        return {"value": None, "index": index}
    index += 1
    regions = []
    while True:
        index = skip_whitespace(data, index)
        parsed = parse_name(data, index)
        regions.append(parsed["value"])
        index = skip_whitespace(data, parsed["index"])
        if data[index : index + 1] == ",":
            index += 1
            continue
        if data[index : index + 1] != ")":
            raise ValueError("invalid location arg")
        return {"value": regions, "index": index + 1}


def parse_name(data: str, index: int) -> Parsed[str]:
    next_index = index
    while next_index < len(data) and is_name_char(data[next_index]):
        next_index += 1
    if next_index == index:
        raise ValueError("invalid location arg")
    return {"value": data[index:next_index], "index": next_index}


def skip_whitespace(data: str, index: int) -> int:
    while index < len(data) and data[index] in (
        "\t\n\v\f\r \u00a0\u1680\u2000\u2001\u2002\u2003"
        "\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028"
        "\u2029\u202f\u205f\u3000\ufeff"
    ):
        index += 1
    return index


def is_name_char(char: str) -> bool:
    return re.fullmatch(r"[A-Za-z0-9_-]", char) is not None
