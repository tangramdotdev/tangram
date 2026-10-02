"""References to nodes with resolution and authorization options."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import ClassVar, NotRequired, Self, TypedDict, cast, overload
from urllib.parse import quote, unquote

from .authorization import Tokens
from .location import Location, LocationObject

_NAMES = ("artifact", "id", "location", "name", "path", "tag", "tokens")


class Options(TypedDict, total=False):
    artifact: str | None
    id: str | None
    location: LocationObject | None
    name: str | None
    path: str | None
    tag: str | None
    tokens: dict[str, list[str]] | None


class DataOptions(TypedDict, total=False):
    artifact: str | None
    id: str | None
    location: str | None
    name: str | None
    path: str | None
    tag: str | None
    tokens: dict[str, list[str]] | None


class ReferentData[T](TypedDict):
    node: T
    options: NotRequired[DataOptions | None]


class Data:
    Options = DataOptions


type ReferentOptions = Options


@dataclass
class Referent[T]:
    Options = Options
    Data: ClassVar[type[Data]] = Data

    node: T
    options: ReferentOptions | None = field(default_factory=dict)

    @classmethod
    def with_node_and_local_tokens[U](cls, node: U, tokens: list[str]) -> Referent[U]:
        return cls.with_node_and_tokens(node, Tokens.with_local(tokens))

    @classmethod
    def with_node_and_tokens[U](
        cls, node: U, tokens: dict[str, list[str]]
    ) -> Referent[U]:
        return Referent(node, {"tokens": tokens} if not Tokens.is_empty(tokens) else {})

    @overload
    def to_data(self) -> ReferentData[T]: ...

    @overload
    def to_data[U](self, encode: Callable[[T], U]) -> ReferentData[U]: ...

    def to_data(
        self, encode: Callable[[T], object] | None = None
    ) -> ReferentData[object]:
        node = self.node if encode is None else encode(self.node)
        raw = dict(self.options or {})
        options = {name: raw[name] for name in _NAMES if raw.get(name) is not None}
        if "location" in options:
            options["location"] = Location.to_data_string(
                cast(LocationObject, options["location"])
            )
        return {"node": node, "options": cast(DataOptions, options)}

    def to_data_string(self, encode: Callable[[T], str] = str) -> str:
        string = str(encode(self.node))
        params = []
        for name in _NAMES[:-1]:
            value = (self.options or {}).get(name)
            if value is not None:
                if name == "location":
                    value = Location.to_data_string(value)
                params.append(f"{name}={encode_uri_component(cast(str, value))}")
        for location, tokens in ((self.options or {}).get("tokens") or {}).items():
            for index, token in enumerate(tokens):
                params.append(
                    f"tokens[{encode_uri_component(location)}][{index}]={encode_uri_component(token)}"
                )
        return string + ("?" + "&".join(params) if params else "")

    @classmethod
    @overload
    def from_data[U](cls, data: ReferentData[U]) -> Referent[U]: ...

    @classmethod
    @overload
    def from_data[U, V](
        cls, data: ReferentData[U], decode: Callable[[U], V]
    ) -> Referent[V]: ...

    @classmethod
    def from_data(
        cls,
        data: ReferentData[object],
        decode: Callable[[object], object] | None = None,
    ) -> Referent[object]:
        assert isinstance(data, dict)
        node = data["node"] if decode is None else decode(data["node"])
        raw = dict(data.get("options") or {})
        options = {name: raw[name] for name in _NAMES if raw.get(name) is not None}
        if "location" in options:
            options["location"] = Location.from_data_string(
                cast(str, options["location"])
            )
        return Referent(node, cast(Options, options))

    @classmethod
    @overload
    def from_data_string(cls, data: str) -> Referent[str]: ...

    @classmethod
    @overload
    def from_data_string[U](
        cls, data: str, decode: Callable[[str], U]
    ) -> Referent[U]: ...

    @classmethod
    def from_data_string(
        cls, data: str, decode: Callable[[str], object] | None = None
    ) -> Referent[object]:
        node, separator, query = data.partition("?")
        decoded_node = node if decode is None else decode(node)
        options: dict[str, object] = {}
        for param in query.split("?", 1)[0].split("&") if separator else []:
            name, separator, value = param.partition("=")
            if not separator:
                raise ValueError("missing value")
            value = value.split("=", 1)[0]
            if name in ("artifact", "id", "name", "path", "tag"):
                options[name] = decode_uri_component(value)
            elif name == "location":
                options[name] = Location.from_data_string(decode_uri_component(value))
            else:
                match = re.fullmatch(r"tokens\[(.*?)\]\[([0-9]+)\]", name)
                if match is None:
                    raise ValueError("invalid key")
                tokens = cast(
                    dict[str, list[str]], options.setdefault("tokens", {})
                ).setdefault(decode_uri_component(match[1]), [])
                if int(match[2]) != len(tokens):
                    raise ValueError("invalid token index")
                tokens.append(decode_uri_component(value))
        return Referent(decoded_node, cast(Options, options))

    def without_token(self) -> Self:
        options = None if self.options is None else dict(self.options)
        if options is not None:
            options.pop("tokens", None)
        return type(self)(self.node, cast(Options | None, options))

    def without_location_and_tokens(self) -> Self:
        referent = self.without_token()
        if referent.options is not None:
            referent.options.pop("location", None)
        return referent


def encode_uri_component(value: str) -> str:
    return quote(value, safe="-._~!*'()")


def decode_uri_component(value: str) -> str:
    if re.search(r"%(?![0-9A-Fa-f]{2})", value):
        raise ValueError("invalid URI component")
    return unquote(value, errors="strict")
