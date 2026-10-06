from __future__ import annotations

import builtins
from typing import TYPE_CHECKING, ClassVar, Required, Self, TypedDict, cast, overload

from ..args import Args
from ..async_property import async_property
from ..builder import Builder
from ..mutation import UNSET, Mutation
from ..object import Object
from ..referent import Referent
from ..resolve import TemplateString, Unresolved, is_template_string, resolve

if TYPE_CHECKING:
    from ..blob import Blob, BlobInput
    from ..client import Client
    from ..graph import FileWirePayload, Pointer, PointerWireData


class FileArgObject(TypedDict, total=False):
    contents: BlobInput
    dependencies: Unresolved[dict[str, FileDependencyInput]] | None
    executable: Unresolved[bool | Mutation | None]
    module: Unresolved[str | Mutation | None]


type FileDependencyInput = Unresolved[Object | Referent | Pointer | None]
type FileInput = Unresolved[
    File
    | BlobInput
    | FileArgObject
    | Pointer
    | TemplateString
    | list[FileInput]
    | tuple[FileInput, ...]
]


class FileResolvedArgObject(TypedDict, total=False):
    contents: BlobInput
    dependencies: dict[str, Referent | Object | Pointer | None] | None
    executable: bool | None
    module: str | None


type FileWireData = FileWirePayload | PointerWireData


class FileValue(TypedDict):
    contents: Blob
    dependencies: dict[str, Referent | None]
    executable: bool
    module: str | None


class FileConstructorArg(TypedDict, total=False):
    id: str
    object: FileValue | Pointer
    stored: Required[bool]
    tokens: dict[str, list[str]] | None


class File(Object):
    kind = "file"
    ConstructorArg = FileConstructorArg
    Builder: ClassVar[type[FileBuilder]]
    Arg: ClassVar[type[FileArgNamespace]]
    Data: ClassVar[type[FileData]]
    Object: ClassVar[type[FileObject]]

    @overload
    def __init__(self, contents: FileConstructorArg, **options) -> None: ...

    @overload
    def __init__(
        self,
        contents: str | builtins.bytes | Blob | None = None,
        *,
        dependencies: dict[str, Referent | Object | Pointer | None] | None = None,
        executable: bool = False,
        module: str | None = None,
        value: FileValue | Pointer | None = None,
        **options,
    ) -> None: ...

    def __init__(
        self,
        contents=None,
        *,
        dependencies=None,
        executable=False,
        module=None,
        value=None,
        **options,
    ):
        from ..blob import Blob
        from ..object import dependency

        if isinstance(contents, dict) and any(
            key in contents for key in ("object", "id", "stored")
        ):
            arg = contents
            super().__init__(
                arg.get("object"), id=arg.get("id"), tokens=arg.get("tokens")
            )
            self._stored = arg["stored"]
            return
        if contents is not None:
            contents = (
                Blob(contents) if isinstance(contents, (str, bytes)) else contents
            )
            value = {
                "contents": contents,
                "dependencies": {
                    name: dependency(value)
                    for name, value in (dependencies or {}).items()
                },
                "executable": executable,
                "module": module,
            }
        super().__init__(value, **options)

    async def object(self, client: Client | None = None) -> FileValue | Pointer:
        return await self.load(client)

    async def load(self, client: Client | None = None) -> FileValue | Pointer:
        from ..graph import Pointer

        value = await super().load(client)
        if isinstance(value, Pointer):
            value.graph.state.inherit_location(self.state.location)
        return cast("FileValue | Pointer", value)

    @classmethod
    async def new(
        cls, *args: FileInput, client: Client | None = None, **options
    ) -> Self:
        from ..blob import Blob
        from ..graph import Pointer

        resolved_args, options = await resolve([args, options])
        resolved_args = [
            Pointer(arg["graph"], arg["index"], arg["kind"])
            if isinstance(arg, dict) and "index" in arg and "graph" in arg
            else arg
            for arg in resolved_args
        ]
        if (
            len(resolved_args) == 1
            and not options
            and isinstance(resolved_args[0], cls)
        ):
            return resolved_args[0]
        if len(resolved_args) == 1 and isinstance(resolved_args[0], Pointer):
            return cls.with_pointer(resolved_args[0])
        from ..object import dependency

        state = await cls.arg_resolved(*resolved_args, options, client=client)
        dependencies = {}
        for name, child in (state.get("dependencies") or {}).items():
            child = dependency(child)
            if (
                child is not None
                and isinstance(child.node, dict)
                and "index" in child.node
            ):
                child = Referent(
                    Pointer(
                        child.node["graph"], child.node["index"], child.node["kind"]
                    ),
                    child.options,
                )
            dependencies[name] = child
        return cls(
            await Blob.new(state.get("contents"), client=client),
            dependencies=dependencies,
            executable=state.get("executable") or False,
            module=state.get("module"),
        )

    @classmethod
    async def arg(
        cls, *args: FileInput, client: Client | None = None
    ) -> FileResolvedArgObject:
        return await cls.arg_resolved(*(await resolve(args)), client=client)

    @classmethod
    async def arg_resolved(
        cls, *args, client: Client | None = None
    ) -> FileResolvedArgObject:
        from ..blob import Blob

        async def map(arg):

            if arg is UNSET:
                return {}
            if is_template_string(arg):
                from ..template import string_text

                return {"contents": string_text(arg)}
            if isinstance(arg, cls):
                return {
                    "contents": await arg.contents(client),
                    "dependencies": await arg.dependencies(client),
                }
            if isinstance(arg, (str, bytes, bytearray, memoryview, Blob)):
                return {"contents": arg}
            return arg

        async def contents(a, b):

            if b is UNSET:
                raise AssertionError()
            if b is None:
                return await Blob.new(client=client)
            if a is UNSET or a is None:
                return await Blob.new(b, client=client)
            return await Blob.new(a, b, client=client)

        return cast(
            FileResolvedArgObject,
            await Args.apply_resolved(
                args, map=map, reduce={"contents": contents, "dependencies": "merge"}
            ),
        )

    def _encode(self, value):
        return FileObject.to_data(value)

    def _decode(self, value):
        from ..graph import Graph, Pointer

        if Graph.Data.Pointer.is_(value):
            return Pointer.from_data(value)
        return Graph.File.from_data(value)

    @async_property
    async def contents(self, client: Client | None = None) -> Blob:
        from ..object import dereference

        contents = (await dereference(await self.load(client), client))["contents"]
        Object.inherit_location(contents, self.state.location)
        Object.inherit_tokens(contents, self.state.tokens)
        return contents

    @async_property
    async def text(self, client: Client | None = None) -> str:
        return await (await self.contents(client)).text(client)

    @async_property
    async def executable(self, client: Client | None = None) -> bool:
        from ..object import dereference

        return (await dereference(await self.load(client), client)).get(
            "executable", False
        )

    @async_property
    async def dependencies(
        self, client: Client | None = None
    ) -> dict[str, Referent | None]:
        from ..graph import Pointer
        from ..object import dereference

        value = await self.load(client)
        graph = value.graph if isinstance(value, Pointer) else None
        object_ = await dereference(value, client)
        result = {}
        for reference, dependency in object_.get("dependencies", {}).items():
            if dependency is None:
                result[reference] = None
                continue
            node = dependency.node
            if type(node) is int:
                if graph is None:
                    raise AssertionError("missing graph")
                node = await graph.get(node, client)
            elif isinstance(node, Pointer):
                node = await node.graph.get(node.index, client)
            if node is not None:
                location = dependency.options.get("location")
                Object.inherit_location(
                    node, self.state.location if location is None else location
                )
                Object.inherit_tokens(node, dependency.options.get("tokens") or {})
                Object.inherit_tokens(node, self.state.tokens)
            result[reference] = Referent(node, dependency.options)
        return result

    @async_property
    async def module(self, client: Client | None = None) -> str | None:
        from ..object import dereference

        return (await dereference(await self.load(client), client)).get("module")

    @async_property
    async def dependency_objects(self, client: Client | None = None) -> list[Object]:
        return [
            dependency.node
            for dependency in (await self.dependencies(client)).values()
            if dependency is not None and dependency.node is not None
        ]

    @staticmethod
    def raw(strings, *placeholders, **options):
        return FileBuilder(True, strings, *placeholders, **options)

    @async_property
    async def length(self, client: Client | None = None) -> int:
        return await (await self.contents(client)).length(client)

    async def read(
        self, options=None, client: Client | None = None, **kwargs
    ) -> builtins.bytes:
        if options is not None and not isinstance(options, dict):
            client, options = options, None
        options = {**(options or {}), **kwargs}
        return await (await self.contents(client)).read(options, client=client)

    @async_property
    async def bytes(self, client: Client | None = None, **options) -> builtins.bytes:
        return await self.read(client, **options)


class FileBuilder(Builder[File]):
    type = File

    @overload
    def __init__(self, *args: FileInput, **options) -> None: ...

    @overload
    def __init__(
        self,
        raw: bool,
        strings: list[str] | TemplateString,
        *placeholders: str,
        **options,
    ) -> None: ...

    def __init__(self, *args, **options) -> None:
        from ..template import unindent

        raw = False
        if args and isinstance(args[0], bool):
            raw, *args = args
        self._raw = raw
        if args and isinstance(args[0], list) and hasattr(args[0], "raw"):
            strings, *placeholders = args
            components = []
            for index, string in enumerate(strings[:-1]):
                components.extend([string, placeholders[index]])
            components.append(strings[-1])
            string = "".join(components)
            if not raw:
                string = "".join(unindent([string]))
            args = [string]
        super().__init__(*args, **options)

    async def _create(self) -> File:
        from ..template import string_text

        args = await resolve(self._args)
        if self._raw and args and is_template_string(args[0]):
            args[0] = string_text(args[0], raw=True)
        return await File.new(*args, client=self._client)

    def contents(self, contents: BlobInput) -> Self:
        return self._push({"contents": contents})

    def dependencies(
        self, dependencies: Unresolved[dict[str, FileDependencyInput]]
    ) -> Self:
        return self._push({"dependencies": dependencies})

    def dependency(self, reference: str, value: FileDependencyInput) -> Self:
        return self.dependencies({reference: value})

    def executable(self, executable: Unresolved[bool | Mutation | None] = True) -> Self:
        return self._push({"executable": executable})

    def module(self, module: Unresolved[str | Mutation | None]) -> Self:
        return self._push({"module": module})


class FileArgNamespace:
    Object = FileArgObject


class FileObject:
    @staticmethod
    def to_data(object_) -> FileWireData:
        from ..graph import Pointer
        from ..object import edge_string

        if isinstance(object_, Pointer):
            return object_.to_data()
        data: FileWirePayload = {"contents": object_["contents"].id}
        dependencies = object_.get("dependencies") or {}
        if dependencies:
            data["dependencies"] = {
                reference: None
                if dependency is None
                else dependency.without_token().to_data_string(
                    lambda node: "" if node is None else edge_string(node)
                )
                for reference, dependency in dependencies.items()
            }
        if object_.get("executable", False) is not False:
            data["executable"] = object_["executable"]
        if object_.get("module") is not None:
            data["module"] = object_["module"]
        return data

    @staticmethod
    def from_data(data: FileWireData) -> FileValue | Pointer:
        return cast("FileValue | Pointer", File.from_data(data)._value)

    @staticmethod
    def children(object_: FileValue | Pointer) -> list[Object]:
        from ..graph import Graph, Pointer

        return (
            Graph.Pointer.children(object_)
            if isinstance(object_, Pointer)
            else Graph.File.children(object_)
        )


class FileData:
    @staticmethod
    def children(data: FileWireData) -> list[str]:
        from ..graph import Graph

        return (
            Graph.Data.Pointer.children(data)
            if Graph.Data.Pointer.is_(data)
            else Graph.Data.File.children(data)
        )

    @staticmethod
    def without_location_and_tokens(data):
        from ..reference import Reference

        if isinstance(data, str) or "index" in data:
            return dict(data) if isinstance(data, dict) else data
        result = dict(data)
        if "dependencies" not in data:
            return result
        result["dependencies"] = {
            Reference.without_tokens(reference): None
            if dependency is None
            else (
                (
                    Referent.from_data_string(dependency)
                    if isinstance(dependency, str)
                    else Referent.from_data(dependency)
                )
                .without_location_and_tokens()
                .to_data_string()
            )
            for reference, dependency in data.get("dependencies", {}).items()
        }
        return result


setattr(File, "Builder", FileBuilder)
setattr(File, "Object", FileObject)
setattr(File, "Data", FileData)

file = FileBuilder
