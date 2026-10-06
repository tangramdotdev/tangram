from __future__ import annotations

import traceback
from typing import TYPE_CHECKING, ClassVar, Literal, NotRequired, Self, TypedDict, cast

if TYPE_CHECKING:
    from .client import Client
    from .diagnostic import DiagnosticData, DiagnosticObject
    from .module import ModuleDataObject
    from .range import Range

import tangram as tg

from .args import Args
from .builder import Builder
from .diagnostic import Diagnostic
from .module import Module
from .object import Object
from .referent import Referent, ReferentData
from .resolve import Unresolved, resolve

type ErrorKind = Literal[
    "argument", "internal", "missing", "unauthenticated", "unauthorized", "unavailable"
]


class ErrorFileRecord[K: str, V](TypedDict):
    kind: K
    value: V


type ErrorFileObject = (
    ErrorFileRecord[Literal["internal"], str]
    | ErrorFileRecord[Literal["module"], Module]
)
type ErrorFileData = (
    ErrorFileRecord[Literal["internal"], str]
    | ErrorFileRecord[Literal["module"], ModuleDataObject]
)


class ErrorLocationObject(TypedDict):
    symbol: str | None
    file: ErrorFileObject
    range: Range


class ErrorLocationData(TypedDict):
    symbol: NotRequired[str | None]
    file: ErrorFileData
    range: Range


class ErrorData(TypedDict, total=False):
    code: str | None
    diagnostics: list[DiagnosticData] | None
    kind: ErrorKind | None
    location: ErrorLocationData | None
    message: str | None
    source: ReferentData[ErrorData | str] | str | None
    stack: list[ErrorLocationData] | None
    values: dict[str, str]


class ErrorObjectValue(TypedDict):
    code: str | None
    diagnostics: list[DiagnosticObject] | None
    kind: ErrorKind | None
    location: ErrorLocationObject | None
    message: str | None
    source: Referent[ErrorObjectValue | Error] | None
    stack: list[ErrorLocationObject] | None
    values: dict[str, str]


class ErrorArgObject(TypedDict, total=False):
    code: Unresolved[str | None]
    diagnostics: Unresolved[list[Unresolved[DiagnosticObject]] | None]
    kind: Unresolved[ErrorKind | None]
    location: Unresolved[ErrorLocationObject | None]
    message: Unresolved[str | None]
    source: Unresolved[Referent[ErrorObjectValue | Error] | None]
    stack: Unresolved[list[Unresolved[ErrorLocationObject]] | None]
    values: Unresolved[dict[str, Unresolved[str]] | None]


class ErrorConstructorArg(TypedDict, total=False):
    id: str
    object: ErrorObjectValue
    stored: bool
    tokens: dict[str, list[str]] | None


class Error(Object, Exception):
    Builder: ClassVar[type[ErrorBuilder]]
    _object_kind = "error"
    Kind = ErrorKind
    Arg: ClassVar[type[ErrorArg]]
    ConstructorArg = ErrorConstructorArg
    Location: ClassVar[type[ErrorLocation]]
    File: ClassVar[type[ErrorFile]]

    def __init__(self, message=None, *, value=None, stored=None, **options):
        from .object import Object

        if isinstance(message, dict):
            constructor = message
            value = constructor.get("object", value)
            options = {
                **options,
                **{
                    key: constructor[key]
                    for key in ("id", "tokens")
                    if key in constructor
                },
            }
            stored = constructor.get("stored", stored)
            message = None
        if message is not None:
            value = {"message": message}
        Object.__init__(self, value, **options)
        if stored is not None:
            self._stored = stored
        Exception.__init__(
            self,
            message
            or (value or {}).get("message")
            or options.get("id")
            or "Tangram error",
        )

    @classmethod
    def with_object(cls, value):
        return cls(value=value)

    @classmethod
    def sync(cls, first=None, second=None) -> Self:
        object_ = empty_object(capture_stack())
        args = [{"message": first}, second] if isinstance(first, str) else [first]
        for arg in args:
            if arg is not None:
                for key in object_:
                    if key in arg:
                        object_.update(
                            {key: arg[key] if key != "values" else arg[key] or {}}
                        )
        object_["kind"] = error_kind(object_)
        return cls.with_object(object_)

    @classmethod
    async def new(cls, *args, client: Client | None = None) -> Self:
        args = await resolve(args)
        if (
            args
            and isinstance(args[-1], cls)
            and all(
                isinstance(arg, dict) and set(arg) <= {"stack"} for arg in args[:-1]
            )
        ):
            return args[-1]
        arg = await cls.arg_resolved({"stack": capture_stack()}, *args, client=client)
        object_ = {key: arg.get(key) for key in empty_object()}
        object_["values"] = arg.get("values") or {}
        object_["kind"] = error_kind(object_)
        return cls.with_object(object_)

    @classmethod
    async def arg(cls, *args, client: Client | None = None):
        return await cls.arg_resolved(*(await resolve(args)), client=client)

    @classmethod
    async def arg_resolved(cls, *args, client: Client | None = None):
        async def map(arg):
            if isinstance(arg, str):
                return {"message": arg}
            if isinstance(arg, cls):
                return await arg.object(client)
            return arg

        return await Args.apply_resolved(
            args,
            map=map,
            reduce={key: "set" for key in empty_object() if key != "values"}
            | {"values": "merge"},
        )

    def to_data(self) -> ErrorData:
        if self._value is None:
            raise ValueError("the error has not been loaded")
        return ErrorObject.to_data(self._value)

    def to_data_or_id(self) -> ErrorData | str:
        if self.state.stored:
            return self.to_referent().to_data_string()
        return self.to_data()

    def _encode(self, value):
        return ErrorObject.to_data(value)

    def _decode(self, value):
        object_ = ErrorObject.from_data(value)
        Exception.__init__(self, object_.get("message") or self._id or "Tangram error")
        return object_

    def _children(self):
        return ErrorObject.children(self._value)

    @tg.property
    async def kind(self, client: Client | None = None) -> ErrorKind | None:
        return error_kind(await self.load(client))

    @tg.property
    async def location(
        self, client: Client | None = None
    ) -> ErrorLocationObject | None:
        return (await self.load(client)).get("location")

    @tg.property
    async def code(self, client: Client | None = None) -> str | None:
        return (await self.load(client)).get("code")

    @tg.property
    async def diagnostics(
        self, client: Client | None = None
    ) -> list[DiagnosticObject] | None:
        return (await self.load(client)).get("diagnostics")

    @tg.property
    async def message(self, client: Client | None = None) -> str | None:
        return (await self.load(client)).get("message")

    @tg.property
    async def source(self, client: Client | None = None) -> Referent[Error] | None:
        source = (await self.load(client)).get("source")
        if source is None:
            return None
        source = as_referent(source)
        if isinstance(source.node, Error):
            return source
        return Referent(Error.with_object(source.node), source.options)

    @tg.property
    async def stack(
        self, client: Client | None = None
    ) -> list[ErrorLocationObject] | None:
        return (await self.load(client)).get("stack")

    @tg.property
    async def values(self, client: Client | None = None) -> dict[str, str]:
        return (await self.load(client)).get("values") or {}

    @classmethod
    def from_data(cls, data: ErrorData | str) -> Self:
        return (
            cls.with_referent(Referent.from_data_string(data))
            if isinstance(data, str)
            else cls.with_object(ErrorObject.from_data(data))
        )


def error_kind(value) -> ErrorKind | None:
    from .object import dependency

    if value.get("kind") is not None:
        return value["kind"]
    source = value.get("source")
    if source is None:
        return None
    source = dependency(source).node
    if isinstance(source, Error):
        source = source._value
    return error_kind(source) if isinstance(source, dict) else None


class ErrorBuilder(Builder[Error]):
    type = Error

    def __init__(self, *args, client: Client | None = None, **options):
        super().__init__({"stack": capture_stack()}, *args, client=client, **options)

    sync = staticmethod(Error.sync)

    def code(self, code: Unresolved[str | None]) -> Self:
        return self._push({"code": code})

    def diagnostics(
        self, diagnostics: Unresolved[list[Unresolved[DiagnosticObject]] | None]
    ) -> Self:
        return self._push({"diagnostics": diagnostics})

    def kind(self, kind: Unresolved[ErrorKind | None]) -> Self:
        return self._push({"kind": kind})

    def location(self, location: Unresolved[ErrorLocationObject | None]) -> Self:
        return self._push({"location": location})

    def message(self, message: Unresolved[str | None]) -> Self:
        return self._push({"message": message})

    def source(
        self, source: Unresolved[Referent[ErrorObjectValue | Error] | Error | None]
    ) -> Self:
        return self._push({"source": source})

    def stack(
        self, stack: Unresolved[list[Unresolved[ErrorLocationObject]] | None]
    ) -> Self:
        return self._push({"stack": stack})

    def values(self, values: Unresolved[dict[str, Unresolved[str]] | None]) -> Self:
        return self._push({"values": values})

    def value(self, key: str, value: Unresolved[str]) -> Self:
        return self.values({key: value})


setattr(Error, "Builder", ErrorBuilder)

error = ErrorBuilder


def capture_stack() -> list[ErrorLocationObject]:
    """Capture Python frames as internal locations, excluding this helper."""
    return [
        {
            "symbol": frame.name,
            "file": {"kind": "internal", "value": frame.filename},
            "range": {
                "start": {"line": (frame.lineno or 1) - 1, "character": 0},
                "end": {"line": (frame.lineno or 1) - 1, "character": 0},
            },
        }
        for frame in reversed(traceback.extract_stack()[:-2])
    ]


def empty_object(stack: list[ErrorLocationObject] | None = None) -> ErrorObjectValue:
    return {
        "code": None,
        "diagnostics": None,
        "kind": None,
        "location": None,
        "message": None,
        "source": None,
        "stack": stack,
        "values": {},
    }


def as_referent(value):
    from .object import dependency

    return dependency(value)


class ErrorObject:
    kind = staticmethod(error_kind)

    @staticmethod
    def to_data(object_: ErrorObjectValue) -> ErrorData:
        data: ErrorData = {}
        for key in ("code", "kind", "message"):
            if object_.get(key) is not None:
                data.update({key: object_[key]})
        diagnostics = object_.get("diagnostics")
        if diagnostics is not None:
            data["diagnostics"] = [Diagnostic.to_data(item) for item in diagnostics]
        location = object_.get("location")
        if location is not None:
            data["location"] = ErrorLocation.to_data(location)
        if object_.get("source") is not None:

            def node_to_data(node):
                if isinstance(node, Error):
                    return node.id if node.state.stored else node.to_data()
                return ErrorObject.to_data(node)

            data["source"] = as_referent(object_["source"]).to_data(node_to_data)
        stack = object_.get("stack")
        if stack is not None:
            data["stack"] = [ErrorLocation.to_data(item) for item in stack]
        if object_.get("values"):
            data["values"] = object_["values"]
        return data

    @staticmethod
    def from_data(data: ErrorData) -> ErrorObjectValue:
        object_ = empty_object()
        object_["code"] = data.get("code")
        object_["kind"] = data.get("kind")
        object_["message"] = data.get("message")
        object_["values"] = data.get("values") or {}
        diagnostics = data.get("diagnostics")
        if diagnostics is not None:
            object_["diagnostics"] = [
                Diagnostic.from_data(item) for item in diagnostics
            ]
        location = data.get("location")
        if location is not None:
            object_["location"] = ErrorLocation.from_data(location)
        source = data.get("source")
        if source is not None:
            object_["source"] = (
                Referent.from_data_string(
                    source,
                    lambda node: (
                        Error.with_id(node)
                        if isinstance(node, str)
                        else ErrorObject.from_data(node)
                    ),
                )
                if isinstance(source, str)
                else Referent.from_data(
                    source,
                    lambda node: (
                        Error.with_id(node)
                        if isinstance(node, str)
                        else ErrorObject.from_data(node)
                    ),
                )
            )
        stack = data.get("stack")
        if stack is not None:
            object_["stack"] = [ErrorLocation.from_data(item) for item in stack]
        return object_

    @staticmethod
    def children(object_) -> list[Object]:
        if object_ is None:
            return []
        children = [
            child
            for item in object_.get("diagnostics") or []
            for child in Diagnostic.children(item)
        ]
        if object_.get("location") is not None:
            children.extend(ErrorLocation.children(object_["location"]))
        children.extend(
            child
            for item in object_.get("stack") or []
            for child in ErrorLocation.children(item)
        )
        if object_.get("source") is not None:
            node = as_referent(object_["source"]).node
            children.extend(
                [node] if isinstance(node, Error) else ErrorObject.children(node)
            )
        return children


class ErrorLocation:
    @staticmethod
    def to_data(value: ErrorLocationObject) -> ErrorLocationData:
        file = value["file"]
        file_data: ErrorFileData = (
            {"kind": "module", "value": Module.to_data(file["value"])}
            if file["kind"] == "module"
            else {"kind": "internal", "value": file["value"]}
        )
        data: ErrorLocationData = {"file": file_data, "range": value["range"]}
        if value.get("symbol") is not None:
            data["symbol"] = value["symbol"]
        return data

    @staticmethod
    def from_data(data: ErrorLocationData) -> ErrorLocationObject:
        file = data["file"]
        file_object: ErrorFileObject = (
            {"kind": "module", "value": Module.from_data(file["value"])}
            if file["kind"] == "module"
            else {"kind": "internal", "value": file["value"]}
        )
        return {
            "symbol": data.get("symbol"),
            "file": file_object,
            "range": data["range"],
        }

    @staticmethod
    def children(value) -> list[Object]:
        return ErrorFile.children(value["file"])


class ErrorFile:
    @staticmethod
    def children(value) -> list[Object]:
        return Module.children(value["value"]) if value["kind"] == "module" else []


class ErrorDataNamespace:
    Location: ClassVar[type[ErrorDataLocation]]
    File: ClassVar[type[ErrorDataFile]]

    @staticmethod
    def children(data) -> list[str]:
        children = [
            child
            for item in data.get("diagnostics") or []
            for child in Diagnostic.Data.children(item)
        ]
        if data.get("location") is not None:
            children.extend(ErrorDataLocation.children(data["location"]))
        children.extend(
            child
            for item in data.get("stack") or []
            for child in ErrorDataLocation.children(item)
        )
        source = data.get("source")
        if source is not None:
            if isinstance(source, str):
                node = source.split("?")[0]
                children.extend([node] if node else [])
            elif isinstance(source["node"], str):
                children.append(source["node"])
            else:
                children.extend(ErrorDataNamespace.children(source["node"]))
        return children

    @staticmethod
    def without_location_and_tokens(data) -> ErrorData:
        output: ErrorData = {**data}
        if data.get("diagnostics") is not None:
            output["diagnostics"] = [
                Diagnostic.Data.without_location_and_tokens(item)
                for item in data["diagnostics"]
            ]
        if data.get("location") is not None:
            output["location"] = ErrorDataLocation.without_location_and_tokens(
                data["location"]
            )
        source = data.get("source")
        if source is not None:
            referent = (
                Referent.from_data_string(source)
                if isinstance(source, str)
                else Referent.from_data(source)
            ).without_location_and_tokens()
            if isinstance(source, str):
                output["source"] = referent.to_data_string()
            else:
                if not isinstance(source["node"], str):
                    referent = cast("Referent[ErrorData | str]", referent)
                    referent.node = ErrorDataNamespace.without_location_and_tokens(
                        source["node"]
                    )
                output["source"] = cast(
                    "ReferentData[ErrorData | str]", referent.to_data()
                )
        if data.get("stack") is not None:
            output["stack"] = [
                ErrorDataLocation.without_location_and_tokens(item)
                for item in data["stack"]
            ]
        return output


class ErrorDataLocation:
    @staticmethod
    def children(data) -> list[str]:
        return ErrorDataFile.children(data["file"])

    @staticmethod
    def without_location_and_tokens(data) -> ErrorLocationData:
        return {**data, "file": ErrorDataFile.without_location_and_tokens(data["file"])}


class ErrorDataFile:
    @staticmethod
    def children(data) -> list[str]:
        return Module.Data.children(data["value"]) if data["kind"] == "module" else []

    @staticmethod
    def without_location_and_tokens(data: ErrorFileData) -> ErrorFileData:
        if data["kind"] == "module":
            return {
                **data,
                "value": Module.Data.without_location_and_tokens(data["value"]),
            }
        return {**data}


class ErrorArg(dict):
    Object = ErrorArgObject


Error.Arg = ErrorArg
Error.ConstructorArg = ErrorConstructorArg
Error.Object = ErrorObject
Error.Data = ErrorDataNamespace
Error.Location = ErrorLocation
Error.File = ErrorFile
ErrorDataNamespace.Location = ErrorDataLocation
ErrorDataNamespace.File = ErrorDataFile
