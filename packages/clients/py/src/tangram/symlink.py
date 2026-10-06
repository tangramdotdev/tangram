from __future__ import annotations

import builtins
from typing import TYPE_CHECKING, ClassVar, Required, Self, TypedDict, cast, overload

import tangram as tg

from .builder import Builder
from .mutation import UNSET
from .object import Object
from .resolve import Unresolved, resolve
from .template import Template

if TYPE_CHECKING:
    from .client import Client
    from .directory import Directory
    from .file import File
    from .graph import Pointer, PointerWireData, SymlinkWirePayload


type ArtifactValue = Directory | File | Symlink


class SymlinkArgObject(TypedDict, total=False):
    artifact: Unresolved[ArtifactValue | Pointer | None]
    path: Unresolved[str | None]


type SymlinkInput = Unresolved[
    str
    | ArtifactValue
    | Template
    | SymlinkArgObject
    | Pointer
    | None
    | list[SymlinkInput]
    | tuple[SymlinkInput, ...]
]


type SymlinkWireData = SymlinkWirePayload | PointerWireData


class SymlinkValue(TypedDict):
    artifact: ArtifactValue | Pointer | int | None
    path: str | None


class SymlinkConstructorArg(TypedDict, total=False):
    id: str
    object: SymlinkValue | Pointer
    stored: Required[bool]
    tokens: dict[str, list[str]] | None


class Symlink(Object):
    kind = "symlink"
    Builder: ClassVar[type[SymlinkBuilder]]
    Arg: ClassVar[type[SymlinkArg]]
    Data: ClassVar[type[SymlinkData]]
    Object: ClassVar[type[SymlinkObject]]
    Id = str
    ConstructorArg = SymlinkConstructorArg

    @overload
    def __init__(self, path: SymlinkConstructorArg, **options) -> None: ...

    @overload
    def __init__(
        self,
        path: str | None = None,
        *,
        artifact: ArtifactValue | Pointer | int | None = None,
        value: SymlinkValue | Pointer | None = None,
        **options,
    ) -> None: ...

    def __init__(self, path=None, *, artifact=None, value=None, **options) -> None:
        if isinstance(path, dict) and any(
            key in path for key in ("object", "id", "stored")
        ):
            arg = path
            super().__init__(
                arg.get("object"), id=arg.get("id"), tokens=arg.get("tokens")
            )
            self._stored = arg["stored"]
            return
        if path is not None or artifact is not None:
            value = {"artifact": artifact, "path": path}
        super().__init__(value, **options)

    async def object(self, client: Client | None = None) -> SymlinkValue | Pointer:
        return await self.load(client)

    async def load(self, client: Client | None = None) -> SymlinkValue | Pointer:
        from .graph import Pointer

        value = await super().load(client)
        if isinstance(value, Pointer):
            value.graph.state.inherit_location(self.state.location)
        return cast("SymlinkValue | Pointer", value)

    @classmethod
    async def new(cls, *args: SymlinkInput, client: Client | None = None) -> Self:
        from .graph import Graph

        if len(args) == 1:
            resolved = await resolve(args[0])
            if isinstance(resolved, cls):
                return resolved
            arg = await cls.arg_resolved(resolved, client=client)
        else:
            arg = await cls.arg(*args, client=client)
        if Graph.Arg.Pointer.is_(arg):
            return cls.with_object(Graph.Pointer.from_arg(arg))
        arg = cast(SymlinkArgObject, arg)
        object_ = {"artifact": None, "path": None}
        if arg.get("artifact", UNSET) is not UNSET:
            object_["artifact"] = Graph.Edge.from_arg(arg["artifact"])
        if arg.get("path", UNSET) is not UNSET:
            object_["path"] = arg["path"]
        return cls.with_object(object_)

    @classmethod
    async def arg(
        cls, *args: SymlinkInput, client: Client | None = None
    ) -> SymlinkArgObject | Pointer:
        return await cls.arg_resolved(*(await resolve(args)), client=client)

    @classmethod
    async def arg_resolved(
        cls, *args, client: Client | None = None
    ) -> SymlinkArgObject | Pointer:
        from .graph import Graph

        output: SymlinkArgObject = {}
        for resolved in args:
            arg = await cls.arg_resolved_inner(resolved, client=client)
            if Graph.Arg.Pointer.is_(arg):
                if len(args) == 1:
                    return arg
                raise ValueError("cannot merge a graph pointer with symlink fields")
            output = {**output, **cast(SymlinkArgObject, arg)}
        return output

    @classmethod
    async def arg_resolved_inner(cls, resolved, client: Client | None = None):
        from .artifact import Artifact

        if isinstance(resolved, str):
            return {"path": resolved}
        elif Artifact.is_(resolved):
            return {"artifact": resolved}
        elif isinstance(resolved, Template):
            components = resolved.components
            assert len(components) <= 2
            first = components[0] if components else None
            if isinstance(first, str) and len(components) == 1:
                return {"path": first}
            elif Artifact.is_(first) and len(components) == 1:
                return {"artifact": first}
            elif Artifact.is_(first) and isinstance(components[1], str):
                assert components[1].startswith("/")
                return {"artifact": first, "path": components[1][1:]}
            raise ValueError("invalid template")
        elif isinstance(resolved, cls):
            return {
                "artifact": await resolved.artifact(client),
                "path": await resolved.path(client),
            }
        return resolved

    @classmethod
    def expect(cls, value: builtins.object) -> Self:
        assert isinstance(value, cls)
        return value

    @classmethod
    def assert_(cls, value: builtins.object) -> None:
        assert isinstance(value, cls)

    def _encode(self, value):
        return SymlinkObject.to_data(value)

    def _decode(self, value):
        return SymlinkObject.from_data(value)

    def _children(self):
        return SymlinkObject.children(self._value) if self._value is not None else []

    @tg.property
    async def artifact(self, client: Client | None = None) -> ArtifactValue | None:
        from .graph import Pointer
        from .object import dereference

        object_ = await self.object(client)
        in_graph = isinstance(object_, Pointer)
        object_ = await dereference(object_, client)
        artifact = object_.get("artifact")
        assert in_graph or type(artifact) not in (int, float)
        if isinstance(artifact, Pointer):
            artifact = await artifact.graph.get(artifact.index, client)
        if artifact is not None:
            Object.inherit_location(artifact, self.state.location)
            Object.inherit_tokens(artifact, self.state.tokens)
        return artifact

    @tg.property
    async def path(self, client: Client | None = None) -> str | None:
        from .object import dereference

        return (await dereference(await self.object(client), client)).get("path")

    async def resolve(self, client: Client | None = None) -> ArtifactValue | None:
        from .directory import Directory

        artifact = await self.artifact(client)
        if isinstance(artifact, Symlink):
            artifact = await artifact.resolve(client)
        path = await self.path(client)
        if artifact is None and path is not None:
            raise ValueError("cannot resolve a symlink with no artifact")
        elif artifact is not None and path is None:
            return artifact
        elif isinstance(artifact, Directory) and path is not None:
            return await artifact.try_get(path, client)
        raise ValueError("invalid symlink")


class SymlinkBuilder(Builder[Symlink]):
    type = Symlink

    def __init__(self, *args: SymlinkInput, **options) -> None:
        super().__init__(*(arg for arg in args if arg is not None), **options)

    def artifact(self, artifact: Unresolved[ArtifactValue | Pointer | None]) -> Self:
        return self._push({"artifact": artifact})

    def path(self, path: Unresolved[str | None]) -> Self:
        return self._push({"path": path})


class SymlinkArg:
    Object = SymlinkArgObject


class SymlinkObject:
    @staticmethod
    def to_data(object_) -> SymlinkWireData:
        from .graph import Graph, Pointer

        if isinstance(object_, Pointer) or "index" in object_:
            return Graph.Pointer.to_data(
                Graph.Pointer.from_arg(object_)
                if isinstance(object_, dict)
                else object_
            )
        return Graph.Symlink.to_data(object_)

    @staticmethod
    def from_data(data: SymlinkWireData) -> SymlinkValue | Pointer:
        from .graph import Graph

        if Graph.Data.Pointer.is_(data):
            return Graph.Pointer.from_data(data)
        return Graph.Symlink.from_data(data)

    @staticmethod
    def children(object_: SymlinkValue | Pointer) -> list[Object]:
        from .graph import Graph, Pointer

        if isinstance(object_, Pointer) or "index" in object_:
            return Graph.Pointer.children(
                Graph.Pointer.from_arg(object_)
                if isinstance(object_, dict)
                else object_
            )
        return Graph.Symlink.children(object_)


class SymlinkData:
    @staticmethod
    def children(data: SymlinkWireData) -> list[str]:
        from .graph import Graph

        if Graph.Data.Pointer.is_(data):
            return Graph.Data.Pointer.children(data)
        return Graph.Data.Symlink.children(data)


setattr(Symlink, "Builder", SymlinkBuilder)
setattr(Symlink, "Arg", SymlinkArg)
setattr(Symlink, "Object", SymlinkObject)
setattr(Symlink, "Data", SymlinkData)

symlink = SymlinkBuilder
