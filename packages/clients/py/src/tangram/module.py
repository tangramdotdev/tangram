"""Module descriptors, sources, and authorization-bearing referents."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, NotRequired, Self, TypedDict, cast

if TYPE_CHECKING:
    from .graph import Pointer
    from .object import Object
    from .range import Range

from .referent import Referent, ReferentData, ReferentOptions

type ModuleSource = str | Object | Pointer | int


class ModuleReferentArg(TypedDict):
    node: ModuleSource
    options: NotRequired[ReferentOptions | None]


class ModuleConstructorArg(TypedDict):
    kind: str
    referent: Referent[ModuleSource] | ModuleReferentArg


class ModuleDataObject(TypedDict):
    kind: str
    referent: ReferentData[str]


class ModuleLocationObject(TypedDict):
    module: Module
    range: Range


class ModuleLocationData(TypedDict):
    module: ModuleDataObject
    range: Range


@dataclass(init=False)
class Module:
    __tangram_atomic__ = True
    kind: str
    referent: Referent[ModuleSource]

    Kind = Literal[
        "js",
        "py",
        "ts",
        "dts",
        "object",
        "artifact",
        "blob",
        "directory",
        "file",
        "symlink",
        "graph",
        "command",
    ]

    def __init__[T: ModuleSource](
        self,
        kind: str | ModuleConstructorArg,
        referent: Referent[T] | ModuleReferentArg | None = None,
    ):
        if isinstance(kind, str):
            module_kind = kind
            source = referent
        else:
            module_kind = kind["kind"]
            source = kind["referent"]
        if source is None:
            raise TypeError("missing module referent")
        self.kind = module_kind
        self.referent = cast(
            "Referent[ModuleSource]",
            (
                Referent(source["node"], source.get("options") or {})
                if isinstance(source, dict)
                else source
            ),
        )

    class Source:
        @staticmethod
        def to_data_string(value):
            from .object import edge_string

            if isinstance(value, str):
                return value if value.startswith((".", "/")) else "./" + value
            return edge_string(value)

    def to_referent(self) -> Referent[ModuleSource]:
        from . import authorization

        options: ReferentOptions = {**(self.referent.options or {})}
        options["tokens"] = {
            location: list(entry)
            for location, entry in (options.get("tokens") or {}).items()
        }
        tokens = options["tokens"] or {}
        for child in self.children():
            child_options = child.to_referent().options or {}
            options["tokens"] = authorization.inherit(
                tokens, child_options.get("tokens") or {}, child.id
            )
            if options.get("location") is None:
                options["location"] = child_options.get("location")
        return Referent(self.referent.node, options)

    def to_data(self) -> ModuleDataObject:
        from .object import edge_string

        return {"kind": self.kind, "referent": self.to_referent().to_data(edge_string)}

    @classmethod
    def from_data(cls, data: ModuleDataObject) -> Self:
        return cls(
            data["kind"], Referent.from_data(data["referent"], cls._decode_source)
        )

    @staticmethod
    def _decode_source(source):
        from .object import Object, edge_from_data

        if isinstance(source, str) and source.startswith((".", "/")):
            return source
        edge = edge_from_data(source)
        return Object.with_id(edge) if isinstance(edge, str) else edge

    @staticmethod
    def source_to_data_string(source):
        return Module.Source.to_data_string(source)

    def to_data_string(self) -> str:
        from .referent import encode_uri_component

        string = self.to_referent().to_data_string(self.source_to_data_string)
        return (
            string
            + ("&" if "?" in string else "?")
            + "kind="
            + encode_uri_component(self.kind)
        )

    @classmethod
    def from_data_string(cls, data: str) -> Self:
        from .referent import decode_uri_component

        parts = data.split("?")
        node = parts[0]
        query = parts[1] if len(parts) > 1 else None
        kind = None
        options = []
        for param in query.split("&") if query is not None else []:
            parts = param.split("=")
            key = parts[0]
            if len(parts) < 2:
                raise ValueError("missing value")
            value = parts[1]
            if key == "kind":
                kind = decode_uri_component(value)
            else:
                options.append(param)
        if kind is None:
            raise ValueError("missing module kind")
        referent = Referent.from_data_string(
            node + ("?" + "&".join(options) if options else ""), cls._decode_source
        )
        return cls(kind, referent)

    def children(self) -> list[Object]:
        from .object import Object, objects

        children = (
            [] if isinstance(self.referent.node, str) else objects(self.referent.node)
        )
        root = (self.referent.options or {}).get("id")
        if (
            self.kind == "py"
            and root is not None
            and all(child.id != root for child in children)
        ):
            children.append(Object.with_id(root))
        for child in children:
            Object.inherit_location(
                child, (self.referent.options or {}).get("location")
            )
            Object.inherit_tokens(
                child, (self.referent.options or {}).get("tokens") or {}
            )
        return children

    def without_token(self) -> Self:
        return type(self)(self.kind, self.referent.without_token())

    @staticmethod
    def data_children(data):
        return Module.Data.children(data)

    @staticmethod
    def data_without_location_and_tokens(data):
        return Module.Data.without_location_and_tokens(data)

    class Data:
        @staticmethod
        def children(data):
            from .graph import Pointer

            referent = data["referent"]
            if isinstance(referent, str):
                referent = Referent.from_data_string(referent).to_data()
            source = referent["node"]
            if isinstance(source, str) and source.startswith((".", "/")):
                children = []
            elif isinstance(source, int) or (
                isinstance(source, str) and source.isascii() and source.isdecimal()
            ):
                children = []
            elif isinstance(source, str) and "index=" not in source:
                children = [source]
            else:
                children = [Pointer.from_data(source).graph.id]
            root = (referent.get("options") or {}).get("id")
            if data["kind"] == "py" and root is not None and root not in children:
                children.append(root)
            return children

        @staticmethod
        def without_location_and_tokens(data):
            referent = (
                Referent.from_data_string(data["referent"])
                if isinstance(data["referent"], str)
                else Referent.from_data(data["referent"])
            )
            referent = referent.without_location_and_tokens()
            output = (
                referent.to_data_string()
                if isinstance(data["referent"], str)
                else referent.to_data()
            )
            return {**data, "referent": output}

    class Location(dict):
        @staticmethod
        def to_data(value: ModuleLocationObject) -> ModuleLocationData:
            return {
                "module": Module.to_data(value["module"]),
                "range": value["range"],
            }

        @staticmethod
        def from_data(data: ModuleLocationData) -> ModuleLocationObject:
            return {"module": Module.from_data(data["module"]), "range": data["range"]}

        @staticmethod
        def children(value):
            return Module.children(value["module"])

        class Data:
            @staticmethod
            def children(data):
                return Module.Data.children(data["module"])

            @staticmethod
            def without_location_and_tokens(data):
                return {
                    **data,
                    "module": Module.Data.without_location_and_tokens(data["module"]),
                }
