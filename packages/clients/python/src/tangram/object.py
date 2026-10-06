from __future__ import annotations

import asyncio
import builtins
import json
from collections.abc import Mapping
from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    NotRequired,
    Self,
    TypedDict,
    cast,
)

import tangram as tg

from . import _native, authorization
from .module import Module
from .mutation import Mutation
from .referent import Referent
from .template import Template

if TYPE_CHECKING:
    from .blob import BlobData
    from .client import Client
    from .command import CommandData
    from .directory import DirectoryData
    from .error import ErrorData
    from .file import FileData
    from .graph import GraphData
    from .symlink import SymlinkData


class Object:
    __tangram_atomic__ = True
    kind: Any
    Id: Any
    Object: ClassVar[Any]
    Data: ClassVar[Any]
    Get: ClassVar[type[ObjectGet]]
    Batch: ClassVar[type[ObjectBatch]]
    Put: ClassVar[type[ObjectPut]]

    def __init__(self, value=None, *, id=None, location=None, tokens=None):
        if value is None and id is None:
            raise ValueError("missing object value or ID")
        self._id = id
        self._value = value
        self._location = deepcopy(location)
        self.tokens = deepcopy(tokens or {})
        self._stored = id is not None
        self._loading: asyncio.Task | None = None
        self._storing: asyncio.Task | None = None
        if self.tokens:
            authorization.normalize(self.tokens, self.id)

    @property
    def _object_kind(self):
        return self.kind

    @property
    def location(self):
        return self._location

    @location.setter
    def location(self, value):
        self._location = value

    @property
    def id(self) -> str:
        if self._id is None:
            data = ObjectDataNamespace.without_location_and_tokens(Object.to_data(self))
            self._id = _native.object_id(json.dumps(data, allow_nan=False))
        return self._id

    @classmethod
    def is_(cls, value):
        return isinstance(value, cls)

    @classmethod
    def expect(cls, value: builtins.object) -> Self:
        if not isinstance(value, cls):
            raise TypeError(f"expected a {cls.__name__.lower()}")
        return value

    @classmethod
    def assert_(cls, value: builtins.object) -> None:
        cls.expect(value)

    @staticmethod
    def inherit_location(object_, location):
        if object_.state.location is None:
            object_.state.location = location

    @staticmethod
    def inherit_tokens(object_, tokens):
        object_._inherit_tokens(tokens)

    @classmethod
    def with_id(cls, id: str) -> Self:
        from .blob import Blob
        from .command import Command
        from .directory import Directory
        from .error import Error
        from .file import File
        from .graph import Graph
        from .symlink import Symlink

        types = {
            "blb": Blob,
            "cmd": Command,
            "dir": Directory,
            "err": Error,
            "fil": File,
            "gph": Graph,
            "sym": Symlink,
        }
        type_ = types.get(id[:3])
        if type_ is None or (cls is not Object and cls is not type_):
            raise ValueError(f"invalid object id: {id}")
        return cast(Self, type_(id=id))

    @classmethod
    def with_referent(cls, referent: Referent) -> Self:
        value = cls.with_id(referent.node)
        value._location = (referent.options or {}).get("location")
        value.tokens = deepcopy((referent.options or {}).get("tokens") or {})
        return value

    @classmethod
    def with_object(cls, value) -> Self:
        if cls is Object:
            type_ = _kind_type(value["kind"])
            return type_.with_object(value["value"])
        result = cls.__new__(cls)
        Object.__init__(result, value)
        return result

    @classmethod
    def from_data(cls, data) -> Self:
        from .blob import Blob
        from .command import Command
        from .directory import Directory
        from .error import Error
        from .file import File
        from .graph import Graph
        from .symlink import Symlink

        if cls is Object:
            types = {
                type_.__name__.lower(): type_
                for type_ in (Blob, Command, Directory, Error, File, Graph, Symlink)
            }
            return cast(Self, types[data["kind"]].from_data(data["value"]))
        result = cls.with_object({})
        result._value = result._decode(data)
        return result

    @classmethod
    def with_pointer(cls, pointer) -> Self:
        if cls.kind != pointer.kind:
            raise ValueError("unexpected graph pointer kind")
        return cls.with_object(pointer)

    async def object(self, client: Client | None = None):
        return await self.load(client)

    def unload(self) -> None:
        if self._stored:
            self._value = None

    def to_referent(self) -> Referent:
        return Referent(
            self.id,
            {
                "location": self.state.location,
                "tokens": self.state.collect_tokens(),
            },
        )

    def _collect_tokens(self, visited=None):
        return self.state.collect_tokens()

    def to_data(self):
        if self._value is None:
            raise ValueError("the object has not been loaded")
        return {"kind": self._object_kind, "value": self._encode(self._value)}

    async def load(self, client: Client | None = None):
        value = await self._load_inner(client)
        for child in self._children():
            child.state.inherit_tokens(self.tokens)
        return value

    async def _load_inner(self, client: Client | None = None):
        if self._value is not None:
            return self._value
        if self._loading is None:
            self._loading = asyncio.create_task(self._load(client))
        task = self._loading
        try:
            return await asyncio.shield(task)
        finally:
            if task.done():
                self._loading = None

    async def _load(self, client: Client | None):
        if client is None:
            from .client import client
        output = await client.get_object(
            self.id, location=self._location, tokens=self.tokens
        )
        if output["data"]["kind"] != self._object_kind:
            raise ValueError("unexpected object kind")
        if output.get("tokens"):
            tokens = authorization.clone(output["tokens"])
            self.tokens = authorization.inherit(tokens, self.tokens, self.id)
        self._value = self._decode(output["data"]["value"])
        for child in self._children():
            child._inherit_tokens(
                ((output.get("children") or {}).get(child.id) or {}).get("tokens")
            )
        return self._value

    async def store(self, client: Client | None = None) -> str:
        from .value import Value

        await Value.store(self, client)
        return self.id

    def _inherit_tokens(self, tokens):
        self.tokens = authorization.inherit(self.tokens, tokens or {}, self.id)

    @tg.property
    async def children(self, client: Client | None = None) -> list[Object]:
        await self.load(client)
        children = self._children()
        for child in children:
            child.state.inherit_location(self._location)
        return children

    def _children(self):
        return objects(self._value)

    @property
    def state(self) -> ObjectState:
        return ObjectState(self)

    def _encode(self, value):
        return encode(value)

    def _decode(self, value):
        return deepcopy(value)

    def __eq__(self, other):
        return isinstance(other, Object) and self.id == other.id

    def __repr__(self):
        return f"{type(self).__name__}.with_id({self.id!r})"


class ObjectState:
    """Expose the shared lazy object state, matching the JavaScript State fields."""

    def __init__(self, object_):
        if isinstance(object_, dict):
            arg = object_
            if arg.get("object") is not None:
                object_ = Object.with_object(arg["object"])
            else:
                object_ = Object.with_id(arg["id"])
            object_._id = arg.get("id")
            object_._stored = arg.get("stored", False)
            object_._location = deepcopy(arg.get("location"))
            object_.tokens = deepcopy(arg.get("tokens") or {})
        self._object = object_
        if object_.tokens:
            authorization.normalize(object_.tokens, self.id)

    @property
    def id(self):
        return self._object.id

    @id.setter
    def id(self, value):
        self._object._id = value

    @property
    def object(self):
        if self._object._value is None:
            return None
        return {"kind": self._object._object_kind, "value": self._object._value}

    @object.setter
    def object(self, value):
        self._object._value = None if value is None else value["value"]

    @property
    def stored(self):
        return self._object._stored

    @stored.setter
    def stored(self, value):
        self._object._stored = value

    @property
    def location(self):
        return deepcopy(self._object._location)

    @location.setter
    def location(self, value):
        self._object._location = deepcopy(value)

    @property
    def tokens(self):
        return authorization.clone(self._object.tokens)

    @tokens.setter
    def tokens(self, value):
        self._object.tokens = authorization.clone(value)
        if self._object.tokens:
            authorization.normalize(self._object.tokens, self.id)

    def inherit_location(self, location):
        Object.inherit_location(self._object, location)

    def inherit_tokens(self, tokens):
        self._object._inherit_tokens(tokens)

    def collect_tokens(self):
        locations = set()
        visited = set()
        stack = [self._object]
        while stack:
            state = stack.pop()
            if id(state) in visited:
                continue
            visited.add(id(state))
            locations.update(state.tokens)
            if state._value is not None:
                stack.extend(state._children())
        tokens = {}
        for location in locations:
            visited = set()
            stack = [(self._object, False)]
            while stack:
                state, covered = stack.pop()
                key = (id(state), covered)
                if key in visited:
                    continue
                visited.add(key)
                inherited = covered
                for token in state.tokens.get(location, []):
                    if not inherited or (
                        authorization.Token.resource(token) or ""
                    ).startswith("syn_"):
                        tokens.setdefault(location, []).append(token)
                    covered |= authorization.Token.authorizes_object_subtree(
                        token, state.id
                    )
                if state._value is not None:
                    stack.extend((child, covered) for child in state._children())
        authorization.normalize(tokens)
        return tokens

    @property
    def kind(self):
        return self._object._object_kind

    @tg.property
    async def children(self, client: Client | None = None) -> list[Object]:
        return await self._object.children(client)

    def start_store_promise(self, promise):
        if self.stored or self.store_promise is not None:
            raise ValueError("the object state cannot start a store promise")
        self._object._storing = promise

    def finish_store(self, referent):
        if self.id != referent.node:
            raise ValueError("invalid object batch output")
        self.location = (referent.options or {}).get("location")
        self.stored = True
        tokens = authorization.clone((referent.options or {}).get("tokens"))
        self._object.tokens = authorization.inherit(
            tokens, self._object.tokens, self.id
        )

    def clear_store_promise(self, promise):
        if self.store_promise is promise:
            self._object._storing = None

    @property
    def load_promise(self):
        return self._object._loading

    @property
    def store_promise(self):
        return self._object._storing

    async def load(self, client: Client | None = None):
        await self._object.load(client)
        return self.object

    def unload(self) -> None:
        self._object.unload()


def edge_string(value):
    from .graph import Pointer

    if isinstance(value, Object):
        return value.id
    if isinstance(value, Pointer):
        return value.to_data_string()
    return str(value)


def edge_from_data(value):
    from .graph import Pointer

    if value is None:
        return None
    if type(value) is int or (isinstance(value, str) and value.isdecimal()):
        return int(value)
    if isinstance(value, dict) or value.startswith("graph="):
        return Pointer.from_data(value)
    if value.startswith(("blb_", "cmd_", "dir_", "err_", "fil_", "gph_", "sym_")):
        return Object.with_id(value)
    return value


def encode(value):
    from .command import CommandValue
    from .graph import Pointer

    if isinstance(value, CommandValue):
        return value.to_data()
    if isinstance(value, Object):
        return value.id
    if isinstance(value, Pointer):
        return value.to_data()
    if isinstance(value, Referent):
        return value.to_data_string(edge_string)
    if isinstance(value, Mapping):
        return {key: encode(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode(child) for child in value]
    return value


def objects(value) -> list[Object]:
    from .command import CommandValue
    from .graph import Pointer

    if isinstance(value, CommandValue):
        return objects(value.value)
    if isinstance(value, Template):
        return objects(value.components)
    if isinstance(value, Module):
        return objects(value.referent.node)
    if isinstance(value, Mutation):
        return objects([value.value, value.values, value.template])
    if isinstance(value, Object):
        return [value]
    if isinstance(value, Pointer):
        return [value.graph]
    if isinstance(value, Referent):
        return objects(value.node)
    if isinstance(value, Mapping):
        return [child for value in value.values() for child in objects(value)]
    if isinstance(value, (list, tuple)):
        return [child for value in value for child in objects(value)]
    return []


async def dereference(value, client: Client | None):
    from .graph import Pointer

    if not isinstance(value, Pointer):
        return value
    nodes = await value.graph.nodes(client)
    node = nodes[value.index]
    if node["kind"] != value.kind:
        raise ValueError("unexpected graph node kind")

    def edge(child):
        from .graph import Pointer

        if type(child) is int:
            if child < 0 or child >= len(nodes):
                raise ValueError("invalid graph node index")
            return Pointer(value.graph, child, nodes[child]["kind"])
        return child

    node = dict(node)
    if node["kind"] == "directory":
        if "entries" in node:
            node["entries"] = {
                name: edge(child) for name, child in node["entries"].items()
            }
        else:
            node["children"] = [
                {**child, "directory": edge(child["directory"])}
                for child in node["children"]
            ]
    elif node["kind"] == "file":
        node["dependencies"] = {
            name: None
            if child is None
            else Referent(edge(child.node), deepcopy(child.options))
            for name, child in node.get("dependencies", {}).items()
        }
    elif node["kind"] == "symlink":
        node["artifact"] = edge(node.get("artifact"))
    return node


def dependency(value):
    if value is None or isinstance(value, Referent):
        return value
    if isinstance(value, dict) and "node" in value:
        return Referent(value["node"], value.get("options") or {})
    return Referent(value)


setattr(Object, "State", ObjectState)


class ObjectId(str):
    @staticmethod
    def kind(id):
        kinds = {
            "blb": "blob",
            "dir": "directory",
            "fil": "file",
            "sym": "symlink",
            "gph": "graph",
            "cmd": "command",
            "err": "error",
        }
        if id[:3] not in kinds:
            raise ValueError(f"invalid object id: {id}")
        return kinds[id[:3]]


class ObjectObject:
    @staticmethod
    def to_data(object_):
        return Object.to_data(Object.with_object(object_))

    @staticmethod
    def from_data(data):
        object_ = Object.from_data(data)
        return {"kind": object_._object_kind, "value": object_._value}

    @staticmethod
    def children(object_):
        return Object.with_object(object_)._children()


class ObjectDataNamespace:
    @staticmethod
    def children(data):
        namespace = _kind_type(data["kind"]).__dict__.get("Data")
        if namespace is not None and hasattr(namespace, "children"):
            return namespace.children(data["value"])
        return [child.id for child in Object.from_data(data)._children()]

    @staticmethod
    def without_location_and_tokens(data):
        kind = data["kind"]
        if kind in ("blob", "directory", "symlink"):
            return dict(data)
        namespace = _kind_type(kind).__dict__.get("Data")
        if namespace is not None and hasattr(namespace, "without_location_and_tokens"):
            value = namespace.without_location_and_tokens(data["value"])
        elif kind == "file":
            value = _file_data_without_location_and_tokens(data["value"])
        elif kind == "graph":
            value = {
                **data["value"],
                "nodes": [
                    _file_data_without_location_and_tokens(node)
                    if node["kind"] == "file"
                    else dict(node)
                    for node in data["value"]["nodes"]
                ],
            }
        elif kind == "error":
            value = _error_data_without_location_and_tokens(data["value"])
        else:
            raise ValueError("invalid object kind")
        return {**data, "value": value}


def _kind_type(kind):
    from .blob import Blob
    from .command import Command
    from .directory import Directory
    from .error import Error
    from .file import File
    from .graph import Graph
    from .symlink import Symlink

    types = {
        "blob": Blob,
        "command": Command,
        "directory": Directory,
        "error": Error,
        "file": File,
        "graph": Graph,
        "symlink": Symlink,
    }
    return types[kind]


def _referent_without_location_and_tokens(data):
    referent = (
        Referent.from_data_string(data)
        if isinstance(data, str)
        else Referent.from_data(data)
    ).without_location_and_tokens()
    return referent.to_data_string() if isinstance(data, str) else referent.to_data()


def _file_data_without_location_and_tokens(data):
    from .reference import Reference

    if not isinstance(data, dict):
        return data
    output = dict(data)
    if "dependencies" in data:
        output["dependencies"] = {
            Reference.without_tokens(reference): None
            if dependency is None
            else _referent_without_location_and_tokens(dependency)
            for reference, dependency in data["dependencies"].items()
        }
    return output


def _error_data_without_location_and_tokens(data):
    from .diagnostic import Diagnostic

    output = dict(data)
    for field in ("diagnostics", "stack"):
        if data.get(field) is not None:
            output[field] = [
                Diagnostic.Data.without_location_and_tokens(value)
                if field == "diagnostics"
                else _error_location_without_location_and_tokens(value)
                for value in data[field]
            ]
    if data.get("location") is not None:
        output["location"] = _error_location_without_location_and_tokens(
            data["location"]
        )
    if data.get("source") is not None:
        source = data["source"]
        if isinstance(source, str):
            output["source"] = _referent_without_location_and_tokens(source)
        else:
            referent = (
                Referent.from_data_string(source)
                if isinstance(source, str)
                else Referent.from_data(source)
            ).without_location_and_tokens()
            if not isinstance(referent.node, str):
                referent.node = _error_data_without_location_and_tokens(referent.node)
            output["source"] = referent.to_data()
    return output


def _error_location_without_location_and_tokens(data):
    file = data["file"]
    if file["kind"] == "module":
        file = {**file, "value": Module.Data.without_location_and_tokens(file["value"])}
    return {**data, "file": dict(file)}


Object.Id = ObjectId
Object.Object = ObjectObject
Object.Data = ObjectDataNamespace


class ObjectDataBlob(TypedDict):
    kind: Literal["blob"]
    value: BlobData


class ObjectDataCommand(TypedDict):
    kind: Literal["command"]
    value: CommandData


class ObjectDataDirectory(TypedDict):
    kind: Literal["directory"]
    value: DirectoryData


class ObjectDataError(TypedDict):
    kind: Literal["error"]
    value: ErrorData


class ObjectDataFile(TypedDict):
    kind: Literal["file"]
    value: FileData


class ObjectDataGraph(TypedDict):
    kind: Literal["graph"]
    value: GraphData


class ObjectDataSymlink(TypedDict):
    kind: Literal["symlink"]
    value: SymlinkData


type ObjectData = (
    ObjectDataBlob
    | ObjectDataCommand
    | ObjectDataDirectory
    | ObjectDataError
    | ObjectDataFile
    | ObjectDataGraph
    | ObjectDataSymlink
)


class ObjectGet:
    class Arg(TypedDict, total=False):
        location: str | dict[str, builtins.object] | None
        metadata: bool
        tokens: dict[str, list[str]] | None

    class Child(TypedDict, total=False):
        tokens: dict[str, list[str]] | None

    class Output(TypedDict):
        data: ObjectData
        children: NotRequired[dict[str, ObjectGet.Child]]
        tokens: NotRequired[dict[str, list[str]] | None]


class ObjectBatch:
    class Object(TypedDict):
        id: str
        data: ObjectData
        children: NotRequired[list[Referent] | None]

    class Arg(TypedDict):
        objects: list[ObjectBatch.Object]
        location: NotRequired[str | dict[str, builtins.object] | None]

    class Output(TypedDict):
        objects: list[Referent]


class ObjectPut:
    class Arg(TypedDict):
        data: ObjectData
        children: NotRequired[list[Referent] | None]
        location: NotRequired[str | dict[str, builtins.object] | None]

    class Output(TypedDict):
        object: Referent


Object.Get = ObjectGet
Object.Batch = ObjectBatch
Object.Put = ObjectPut


setattr(Object, "kind", staticmethod(lambda object_: object_._object_kind))
