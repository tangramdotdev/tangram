"""References with from __future__ import annotations

import re
from collections.abc import Callablesolution and authorization options."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import ClassVar, NotRequired, TypedDict, cast, overload

from .location import Arg, ArgObject
from .referent import decode_uri_component, encode_uri_component


class Options(TypedDict, total=False):
    artifact: str | None
    get: str | None
    id: str | None
    location: ArgObject | None
    name: str | None
    path: str | None
    source: str | None
    tag: str | None
    tokens: dict[str, list[str]] | None


class DataOptions(TypedDict, total=False):
    artifact: str | None
    get: str | None
    id: str | None
    location: str | None
    name: str | None
    path: str | None
    source: str | None
    tag: str | None
    tokens: dict[str, list[str]] | None


class ReferenceData[T](TypedDict):
    node: T
    options: NotRequired[DataOptions | None]


type ReferenceOptions = Options


@dataclass
class Reference[T]:
    String: ClassVar[type[str]] = str
    Object: ClassVar[type[Reference]]
    Options = Options
    Data: ClassVar[type[Data]]

    node: T
    options: ReferenceOptions | None = field(default_factory=dict)

    @overload
    def to_data(self) -> ReferenceData[T]: ...

    @overload
    def to_data[U](self, encode: Callable[[T], U]) -> ReferenceData[U]: ...

    def to_data(
        self, encode: Callable[[T], object] | None = None
    ) -> ReferenceData[object]:
        node = self.node if encode is None else encode(self.node)
        options = {
            key: (self.options or {})[key]
            for key in (
                "artifact",
                "get",
                "id",
                "location",
                "name",
                "path",
                "source",
                "tag",
                "tokens",
            )
            if (self.options or {}).get(key) is not None
        }
        if "location" in options:
            options["location"] = Arg.to_data_string(
                cast(ArgObject, options["location"])
            )
        return {"node": node, "options": cast(DataOptions, options)}

    @classmethod
    @overload
    def from_data[U](cls, data: ReferenceData[U]) -> Reference[U]: ...

    @classmethod
    @overload
    def from_data[U, V](
        cls, data: ReferenceData[U], decode: Callable[[U], V]
    ) -> Reference[V]: ...

    @classmethod
    def from_data(
        cls,
        data: ReferenceData[object],
        decode: Callable[[object], object] | None = None,
    ) -> Reference[object]:
        assert isinstance(data, dict)
        node = data["node"] if decode is None else decode(data["node"])
        raw = dict(data.get("options") or {})
        options = {
            key: raw[key]
            for key in (
                "artifact",
                "get",
                "id",
                "location",
                "name",
                "path",
                "source",
                "tag",
                "tokens",
            )
            if (data.get("options") or {}).get(key) is not None
        }
        if "location" in options:
            options["location"] = Arg.from_data_string(cast(str, options["location"]))
        return Reference(node, cast(Options, options))

    def to_data_string(self, encode: Callable[[T], str] = str) -> str:
        node = encode(self.node)
        string = str(node)
        params = []
        for name in (
            "artifact",
            "get",
            "id",
            "location",
            "name",
            "path",
            "source",
            "tag",
        ):
            value = (self.options or {}).get(name)
            if value is None:
                continue
            if name == "location":
                value = Arg.to_data_string(value)
            params.append(f"{name}={encode_uri_component(cast(str, value))}")
        for location, tokens in ((self.options or {}).get("tokens") or {}).items():
            for index, token in enumerate(tokens):
                params.append(
                    f"tokens[{encode_uri_component(location)}][{index}]={encode_uri_component(token)}"
                )
        return string + ("?" + "&".join(params) if params else "")

    @classmethod
    @overload
    def from_data_string(cls, data: str) -> Reference[str]: ...

    @classmethod
    @overload
    def from_data_string[U](
        cls, data: str, decode: Callable[[str], U]
    ) -> Reference[U]: ...

    @classmethod
    def from_data_string(
        cls, data: str, decode: Callable[[str], object] | None = None
    ) -> Reference[object]:
        node, separator, query = data.partition("?")
        decoded_node = node if decode is None else decode(node)
        options: dict[str, object] = {}
        for param in query.split("?", 1)[0].split("&") if separator else []:
            key, separator, value = param.partition("=")
            if not separator:
                raise ValueError("missing value")
            value = value.split("=", 1)[0]
            if key in ("artifact", "get", "id", "name", "path", "source", "tag"):
                options[key] = decode_uri_component(value)
            elif key == "location":
                options[key] = Arg.from_data_string(decode_uri_component(value))
            else:
                match = re.fullmatch(r"tokens\[(.*?)\]\[([0-9]+)\]", key)
                if match is None:
                    raise ValueError("invalid key")
                tokens = cast(
                    dict[str, list[str]], options.setdefault("tokens", {})
                ).setdefault(decode_uri_component(match[1]), [])
                if int(match[2]) != len(tokens):
                    raise ValueError("invalid token index")
                tokens.append(decode_uri_component(value))
        return Reference(decoded_node, cast(Options, options))

    @staticmethod
    @overload
    def without_tokens(data: str) -> str: ...

    @staticmethod
    @overload
    def without_tokens[U: ReferenceData](data: U) -> U: ...

    @staticmethod
    def without_tokens(
        data: str | ReferenceData[object],
    ) -> str | ReferenceData[object]:
        if isinstance(data, str):
            reference = Reference.from_data_string(data)
            if reference.options is not None:
                reference.options.pop("tokens", None)
            return reference.to_data_string()
        output = dict(data)
        if output.get("options") is not None:
            output["options"] = {
                key: value
                for key, value in cast(DataOptions, output["options"]).items()
                if key != "tokens"
            }
        return cast(ReferenceData[object], output)


class Data:
    Options = DataOptions
    without_tokens = staticmethod(Reference.without_tokens)


Reference.Object = Reference
Reference.Data = Data
