from __future__ import annotations

import asyncio
import base64
import builtins
from typing import TYPE_CHECKING, ClassVar, Required, Self, TypedDict, cast, overload

if TYPE_CHECKING:
    from .client import Client

import tangram as tg

from .args import Args
from .builder import Builder
from .mutation import Mutation
from .object import Object
from .resolve import Unresolved, resolve


class BlobChild(TypedDict):
    blob: Blob
    length: int


class BlobChildInput(TypedDict):
    blob: Unresolved[Blob]
    length: Unresolved[int]


class BlobArgObject(TypedDict, total=False):
    children: Unresolved[list[Unresolved[BlobChildInput]] | Mutation | None]


class BlobResolvedArgObject(TypedDict, total=False):
    children: list[BlobChild] | None


class BlobLeaf(TypedDict):
    bytes: bytes


class BlobBranch(TypedDict):
    children: list[BlobChild]


class BlobDataChild(TypedDict):
    blob: str
    length: int


class BlobDataLeaf(TypedDict):
    bytes: str


class BlobDataBranch(TypedDict):
    children: list[BlobDataChild]


class ReadOptions(TypedDict, total=False):
    position: int | str | None
    length: int | None
    size: int | None


type BlobValue = BlobLeaf | BlobBranch
type BlobData = BlobDataLeaf | BlobDataBranch
type BlobArg = str | bytes | bytearray | memoryview | Blob | BlobArgObject | None
type BlobInput = Unresolved[BlobArg | list[BlobInput] | tuple[BlobInput, ...]]


class BlobConstructorArg(TypedDict, total=False):
    id: str
    object: BlobValue
    stored: Required[bool]
    tokens: dict[str, list[str]] | None


class Blob(Object):
    kind = "blob"
    ConstructorArg = BlobConstructorArg
    ReadOptions = ReadOptions
    Builder: ClassVar[type[BlobBuilder]]
    Arg: ClassVar[type[BlobArgNamespace]]
    Data: ClassVar[type[BlobDataNamespace]]
    Object: ClassVar[type[BlobObject]]

    @overload
    def __init__(self, value: BlobConstructorArg, **options) -> None: ...

    @overload
    def __init__(
        self,
        value: BlobValue | str | builtins.bytes | bytearray | memoryview | None = None,
        **options,
    ) -> None: ...

    def __init__(self, value=None, **options) -> None:
        if isinstance(value, dict) and any(
            key in value for key in ("object", "id", "stored")
        ):
            arg = value
            super().__init__(
                arg.get("object"), id=arg.get("id"), tokens=arg.get("tokens")
            )
            self._stored = arg["stored"]
            return
        if isinstance(value, str):
            value = value.encode()
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = {"bytes": bytes(value)}
        super().__init__(value, **options)

    async def object(self, client: Client | None = None) -> BlobValue:
        return await self.load(client)

    async def load(self, client: Client | None = None) -> BlobValue:
        return cast(BlobValue, await super().load(client))

    @classmethod
    async def new(cls, *args: BlobInput, client: Client | None = None) -> Blob:
        resolved_args = await resolve(args)
        if len(resolved_args) == 1 and isinstance(resolved_args[0], cls):
            return resolved_args[0]
        arg = await cls.arg_resolved(*resolved_args, client=client)
        children = arg.get("children") or []
        if not children:
            return cls(b"")
        if len(children) == 1:
            return children[0]["blob"]
        return cls({"children": children})

    @staticmethod
    def leaf(*args: BlobInput) -> BlobBuilder:
        return BlobBuilder.leaf(*args)

    @staticmethod
    def branch(*args: BlobInput) -> BlobBuilder:
        return BlobBuilder.branch(*args)

    @classmethod
    async def arg(
        cls, *args: BlobInput, client: Client | None = None
    ) -> BlobResolvedArgObject:
        return await cls.arg_resolved(*(await resolve(args)), client=client)

    @classmethod
    async def arg_resolved(
        cls, *args, client: Client | None = None
    ) -> BlobResolvedArgObject:
        async def map(arg):
            if arg is None:
                return {"children": []}
            if isinstance(arg, (str, bytes, bytearray, memoryview)):
                bytes_ = arg.encode() if isinstance(arg, str) else bytes(arg)
                blob = cls.with_object({"bytes": bytes_})
                length = len(bytes_)
                return {"children": [{"blob": blob, "length": length}]}
            if isinstance(arg, cls):
                length = await arg.length(client)
                return {"children": [{"blob": arg, "length": length}]}
            return arg

        return cast(
            BlobResolvedArgObject,
            await Args.apply_resolved(args, map=map, reduce={"children": "append"}),
        )

    def _encode(self, value):
        return BlobObject.to_data(value)

    def _decode(self, value):
        return BlobObject.from_data(value)

    @tg.property
    async def length(self, client: Client | None = None) -> int:
        object_ = await self.object(client)
        if "children" in object_:
            return sum(
                child["length"] for child in cast(BlobBranch, object_)["children"]
            )
        return len(object_["bytes"])

    async def read(self, options=None, client: Client | None = None, **kwargs):
        if options is not None and not isinstance(options, dict):
            client, options = options, None
        options = {**(options or {}), **kwargs}
        if client is None:
            from .client import client
        id_ = await self.store(client)
        return await client.read(
            id_,
            position=options.get("position"),
            length=options.get("length"),
            size=options.get("size"),
            tokens=self.tokens,
        )

    @tg.property
    async def bytes(self, client: Client | None = None) -> builtins.bytes:
        return await self.read(client=client)

    @tg.property
    async def text(self, client: Client | None = None) -> str:
        return (await self.bytes(client)).decode(errors="replace")

    @staticmethod
    def raw(*args: BlobInput, **options) -> BlobBuilder:
        return BlobBuilder(True, *args, **options)


class BlobBuilder(Builder[Blob]):
    type = Blob

    def __init__(self, *args: BlobInput | bool, **options) -> None:
        from .template import unindent

        parsed_args = list(args)
        raw = False
        if parsed_args and isinstance(parsed_args[0], bool):
            raw = parsed_args.pop(0)
        if (
            parsed_args
            and isinstance(parsed_args[0], list)
            and hasattr(parsed_args[0], "raw")
        ):
            strings = cast(list[str], parsed_args[0])
            placeholders = cast(list[str], parsed_args[1:])
            components = []
            for index, string in enumerate(strings[:-1]):
                components.extend([string, placeholders[index]])
            components.append(strings[-1])
            string = "".join(components)
            if not raw:
                string = "".join(unindent([string]))
            parsed_args = [string]
        super().__init__(*parsed_args, **options)

    @classmethod
    def leaf(cls, *args: BlobInput) -> Self:
        builder = cls(*args)
        builder._mode = "leaf"
        return builder

    @staticmethod
    async def create_leaf(*args: BlobInput, client: Client | None = None) -> Blob:
        resolved = await resolve(args)

        async def bytes_for(arg):
            if arg is None:
                return b""
            if isinstance(arg, str):
                return arg.encode()
            if isinstance(arg, (bytes, bytearray, memoryview)):
                return bytes(arg)
            return await Blob.expect(arg).bytes(client)

        objects = await asyncio.gather(*(bytes_for(arg) for arg in resolved))
        return Blob.with_object({"bytes": b"".join(objects)})

    @classmethod
    def branch(cls, *args: BlobInput) -> Self:
        builder = cls(*args)
        builder._mode = "branch"
        return builder

    @staticmethod
    async def create_branch(*args: BlobInput, client: Client | None = None) -> Blob:
        arg = await Blob.arg(*args, client=client)
        return Blob.with_object({"children": arg.get("children") or []})

    async def _create(self) -> Blob:
        if getattr(self, "_mode", None) == "leaf":
            return await self.create_leaf(*self._args, client=self._client)
        if getattr(self, "_mode", None) == "branch":
            return await self.create_branch(*self._args, client=self._client)
        return await super()._create()


class BlobArgNamespace:
    Object = BlobArgObject


class BlobObject:
    @staticmethod
    def to_data(object_: BlobValue) -> BlobData:
        if "bytes" in object_:
            return {
                "bytes": base64.b64encode(cast(BlobLeaf, object_)["bytes"]).decode()
            }
        return {
            "children": [
                {"blob": child["blob"].id, "length": child["length"]}
                for child in object_["children"]
            ]
        }

    @staticmethod
    def from_data(data: BlobData) -> BlobValue:
        if "bytes" in data:
            return {
                "bytes": base64.b64decode(
                    cast(BlobDataLeaf, data)["bytes"], validate=True
                )
            }
        return {
            "children": [
                {"blob": Blob.with_id(child["blob"]), "length": child["length"]}
                for child in data["children"]
            ]
        }

    @staticmethod
    def children(object_: BlobValue) -> list[Object]:
        return [child["blob"] for child in object_.get("children", [])]


class BlobDataNamespace:
    @staticmethod
    def children(data: BlobData) -> list[str]:
        return [child["blob"] for child in data.get("children", [])]


Blob.Arg = BlobArgNamespace
Blob.Builder = BlobBuilder
Blob.Data = BlobDataNamespace
Blob.Object = BlobObject

blob = BlobBuilder
