from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING, ClassVar, Required, Self, TypedDict, cast, overload

from . import path
from .async_property import async_property
from .builder import Builder
from .object import Object
from .resolve import Unresolved, resolve

if TYPE_CHECKING:
    from .blob import Blob
    from .client import Client
    from .file import File
    from .graph import (
        DirectoryBranchWireData,
        DirectoryLeafWireData,
        Pointer,
        PointerWireData,
    )
    from .symlink import Symlink


type ArtifactValue = Directory | File | Symlink
type DirectoryEntryInput = Unresolved[
    ArtifactValue
    | Blob
    | Pointer
    | str
    | bytes
    | bytearray
    | memoryview
    | Mapping[str, DirectoryEntryInput]
    | None
]


class DirectoryChildInput(TypedDict):
    directory: DirectoryInput
    count: Unresolved[int]
    last: Unresolved[str]


class DirectoryBranchInput(TypedDict):
    children: Unresolved[list[Unresolved[DirectoryChildInput]]]


type DirectoryInput = Unresolved[
    Directory
    | DirectoryBranchInput
    | Pointer
    | Mapping[str, DirectoryEntryInput]
    | list[DirectoryInput]
    | tuple[DirectoryInput, ...]
]


type DirectoryWireData = (
    DirectoryLeafWireData | DirectoryBranchWireData | PointerWireData
)


class DirectoryLeaf(TypedDict):
    entries: dict[str, ArtifactValue | Pointer | int]


class DirectoryChild(TypedDict):
    directory: Directory | Pointer | int
    count: int
    last: str


class DirectoryBranch(TypedDict):
    children: list[DirectoryChild]


type DirectoryValue = DirectoryLeaf | DirectoryBranch | Pointer


class DirectoryConstructorArg(TypedDict, total=False):
    id: str
    object: DirectoryValue
    stored: Required[bool]
    tokens: dict[str, list[str]] | None


class Directory(Object):
    Builder: ClassVar[type[DirectoryBuilder]]
    Arg: ClassVar[type[DirectoryArg]]
    Data: ClassVar[type[DirectoryData]]
    Object: ClassVar[type[DirectoryObject]]
    kind = "directory"
    ConstructorArg = DirectoryConstructorArg

    @overload
    def __init__(self, entries: DirectoryConstructorArg, **options) -> None: ...

    @overload
    def __init__(
        self,
        entries: Mapping[str, ArtifactValue | Pointer] | None = None,
        *,
        value: DirectoryValue | None = None,
        **options,
    ) -> None: ...

    def __init__(self, entries=None, *, value=None, **options) -> None:
        if isinstance(entries, dict) and any(
            key in entries for key in ("object", "id", "stored")
        ):
            arg = entries
            super().__init__(
                arg.get("object"), id=arg.get("id"), tokens=arg.get("tokens")
            )
            self._stored = arg["stored"]
            return
        if entries is not None:
            value = {"entries": dict(entries)}
        super().__init__(value, **options)

    async def object(self, client: Client | None = None) -> DirectoryValue:
        return await self.load(client)

    async def load(self, client: Client | None = None) -> DirectoryValue:
        return cast(DirectoryValue, await super().load(client))

    @classmethod
    async def new(cls, *args: DirectoryInput, client: Client | None = None) -> Self:
        from .blob import Blob
        from .file import File
        from .graph import Pointer
        from .symlink import Symlink

        if len(args) == 1:
            arg = await resolve(args[0])
            if isinstance(arg, cls):
                return arg
            if isinstance(arg, Pointer):
                return cls.with_pointer(arg)
            if isinstance(arg, dict) and "index" in arg and "graph" in arg:
                return cls.with_pointer(
                    Pointer(arg["graph"], arg["index"], arg["kind"])
                )
            if DirectoryArgBranch.is_(arg):
                children = []
                for child in arg["children"]:
                    directory = await cls.new(child["directory"], client=client)
                    children.append(
                        {
                            "directory": directory,
                            "count": child["count"],
                            "last": child["last"],
                        }
                    )
                return cls.with_object({"children": children})
            resolved = [arg]
        else:
            resolved = await resolve(args)
        entries: dict[str, ArtifactValue | Pointer] = {}
        for arg in resolved:
            if isinstance(arg, cls):
                for name, entry in (await arg.entries(client)).items():
                    existing_entry = entries.get(name)
                    if isinstance(existing_entry, cls) and isinstance(entry, cls):
                        entry = await cls.new(existing_entry, entry, client=client)
                    entries[name] = entry
            elif isinstance(arg, dict):
                for key, value in arg.items():
                    from .mutation import UNSET

                    if value is UNSET:
                        continue
                    components = path.components(key)
                    if not components:
                        raise ValueError("the path must have at least one component")
                    first_component, *trailing_components = components
                    if not path.Component.is_normal(first_component):
                        raise ValueError("all path components must be normal")
                    name = first_component
                    existing_entry = entries.get(name)
                    if not isinstance(existing_entry, cls):
                        existing_entry = None
                    if trailing_components:
                        trailing_path = path.from_components(trailing_components)
                        new_entry = await cls.new(
                            *([existing_entry] if existing_entry is not None else []),
                            {trailing_path: value},
                            client=client,
                        )
                        entries[name] = new_entry
                    elif value is None:
                        entries.pop(name, None)
                    elif isinstance(value, (int, float)) and not isinstance(
                        value, bool
                    ):
                        raise ValueError(
                            "cannot use number as directory entry without kind"
                        )
                    elif isinstance(value, Pointer):
                        entries[name] = value
                    elif isinstance(value, dict) and isinstance(
                        value.get("index"), (int, float)
                    ):
                        entries[name] = Pointer(
                            value["graph"], value["index"], value["kind"]
                        )
                    elif isinstance(value, (str, bytes, bytearray, memoryview, Blob)):
                        entries[name] = await File.new(value, client=client)
                    elif isinstance(value, (File, Symlink)):
                        entries[name] = value
                    else:
                        entries[name] = await cls.new(
                            *([existing_entry] if existing_entry is not None else []),
                            value,
                            client=client,
                        )
            else:
                raise TypeError("invalid directory argument")
        return cls.with_object({"entries": entries})

    def _decode(self, value):
        from .graph import Pointer
        from .object import edge_from_data

        if "graph" in value:
            return Pointer.from_data(value)
        if "entries" in value:
            return {
                "entries": {
                    name: edge_from_data(child)
                    for name, child in value["entries"].items()
                }
            }
        return {
            "children": [
                {**child, "directory": edge_from_data(child["directory"])}
                for child in value["children"]
            ]
        }

    async def get(self, arg: str, client: Client | None = None) -> ArtifactValue:
        artifact = await self.try_get(arg, client)
        if artifact is None:
            raise ValueError(f'failed to get the directory entry "{arg}"')
        return artifact

    async def try_get(
        self, arg: str, client: Client | None = None
    ) -> ArtifactValue | None:
        from .symlink import Symlink

        components = path.components(arg)
        artifact = self
        parents = []
        while True:
            if not components:
                break
            component = components.pop(0)
            if component == path.Component.root:
                raise ValueError("invalid path")
            if component == ".":
                continue
            if component == "..":
                if not parents:
                    raise ValueError("path is external")
                artifact = parents.pop()
                continue
            if not isinstance(artifact, Directory):
                return None
            entries = await artifact.entries(client)
            entry = entries.get(component)
            if entry is None:
                return None
            parents.append(artifact)
            artifact = entry
            if isinstance(entry, Symlink):
                artifact_ = await entry.artifact(client)
                path_ = await entry.path(client)
                if artifact_ is None and path_ is not None:
                    if not parents:
                        raise ValueError("path is external")
                    artifact = parents.pop()
                    components[:0] = path.components(path_)
                elif artifact_ is not None and path_ is None:
                    return artifact_
                elif isinstance(artifact_, Directory) and path_ is not None:
                    return await artifact_.try_get(path_, client)
                else:
                    raise ValueError("invalid symlink")
        return artifact

    @async_property
    async def entries(self, client: Client | None = None) -> dict[str, ArtifactValue]:
        entries = {}
        async for name, artifact in self._iterate(client):
            entries[name] = artifact
        return entries

    async def walk(
        self, client: Client | None = None
    ) -> AsyncIterator[tuple[str, ArtifactValue]]:
        async for name, artifact in self._iterate(client):
            yield name, artifact
            if isinstance(artifact, Directory):
                async for entry_name, entry_artifact in artifact.walk(client):
                    yield path.join(name, entry_name), entry_artifact

    def __aiter__(self) -> AsyncIterator[tuple[str, ArtifactValue]]:
        return self._iterate()

    async def _iterate(
        self, client: Client | None = None
    ) -> AsyncIterator[tuple[str, ArtifactValue]]:
        from .graph import DirectoryLeafNode, Pointer

        object_ = await self.object(client)
        if isinstance(object_, Pointer):
            graph = object_.graph
            nodes = await graph.nodes(client)
            node = nodes[object_.index]
            if node["kind"] != "directory":
                raise TypeError("expected a directory")
            if "entries" in node:
                for name, edge in cast(DirectoryLeafNode, node)["entries"].items():
                    if type(edge) is int:
                        artifact = await graph.get(edge, client)
                    elif isinstance(edge, Pointer):
                        artifact = await edge.graph.get(edge.index, client)
                    else:
                        artifact = edge
                    Object.inherit_location(artifact, self.state.location)
                    Object.inherit_tokens(artifact, self.state.tokens)
                    yield name, cast(ArtifactValue, artifact)
            else:
                for child in node["children"]:
                    child_directory = await Directory.resolve_edge_in_graph(
                        child["directory"], graph, client
                    )
                    child_directory.state.inherit_location(self.state.location)
                    child_directory.state.inherit_tokens(self.state.tokens)
                    async for entry in child_directory._iterate(client):
                        yield entry
        elif "entries" in object_:
            for name, edge in cast(DirectoryLeaf, object_)["entries"].items():
                if type(edge) is int:
                    raise TypeError("expected an object")
                if isinstance(edge, Pointer):
                    artifact = await edge.graph.get(edge.index, client)
                else:
                    artifact = edge
                Object.inherit_location(artifact, self.state.location)
                Object.inherit_tokens(artifact, self.state.tokens)
                yield name, cast(ArtifactValue, artifact)
        else:
            for child in object_["children"]:
                child_directory = await Directory.resolve_edge(
                    child["directory"], client
                )
                child_directory.state.inherit_location(self.state.location)
                child_directory.state.inherit_tokens(self.state.tokens)
                async for entry in child_directory._iterate(client):
                    yield entry

    @staticmethod
    async def resolve_edge(edge, client: Client | None = None):
        from .graph import Pointer

        if type(edge) is int:
            raise TypeError("missing graph")
        if isinstance(edge, Pointer):
            artifact = await edge.graph.get(edge.index, client)
            return Directory.expect(artifact)
        return edge

    @staticmethod
    async def resolve_edge_in_graph(edge, graph, client: Client | None = None):
        from .graph import Pointer

        if type(edge) is int:
            return Directory.expect(await graph.get(edge, client))
        if isinstance(edge, Pointer):
            return Directory.expect(await edge.graph.get(edge.index, client))
        return edge


class DirectoryBuilder(Builder[Directory]):
    type = Directory

    def __init__(self, *args: DirectoryInput, client: Client | None = None) -> None:
        super().__init__(*args, client=client)

    def entries(self, entries: Unresolved[Mapping[str, DirectoryEntryInput]]) -> Self:
        return self._push(entries)

    def entry(self, path: str, value: DirectoryEntryInput) -> Self:
        return self._push({path: value})


class DirectoryArgBranch:
    @staticmethod
    def is_(value):
        return isinstance(value, dict) and isinstance(value.get("children"), list)


class DirectoryArg:
    Branch = DirectoryArgBranch
    BranchInput = DirectoryBranchInput
    Child = DirectoryChildInput
    Leaf = Mapping[str, DirectoryEntryInput]


class DirectoryObject:
    @staticmethod
    def to_data(object_) -> DirectoryWireData:
        return Directory.with_object(object_).to_data()["value"]

    @staticmethod
    def from_data(data: DirectoryWireData) -> DirectoryValue:
        return cast(DirectoryValue, Directory.from_data(data)._value)

    @staticmethod
    def children(object_: DirectoryValue) -> list[Object]:
        return Directory.with_object(object_)._children()


class DirectoryData:
    @staticmethod
    def children(data: DirectoryWireData) -> list[str]:
        return [child.id for child in Directory.from_data(data)._children()]


setattr(Directory, "Builder", DirectoryBuilder)
setattr(Directory, "Arg", DirectoryArg)
setattr(Directory, "Object", DirectoryObject)
setattr(Directory, "Data", DirectoryData)

directory = DirectoryBuilder
