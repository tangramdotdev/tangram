"""Error class as an Object type.

This module mirrors the JavaScript packages/clients/js/src/error.ts.
Note: Named error_ to avoid conflict with Python's built-in Exception.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict, Union

from tangram.diagnostic import Diagnostic, DiagnosticData
from tangram.diagnostic import from_data as diagnostic_from_data
from tangram.diagnostic import to_data as diagnostic_to_data
from tangram.module_ import Module, ModuleData
from tangram.module_ import from_data as module_from_data
from tangram.module_ import to_data as module_to_data
from tangram.object_ import Object_
from tangram.range_ import Range
from tangram.referent import Referent
from tangram.referent import from_data as referent_from_data
from tangram.referent import to_data as referent_to_data


class File(TypedDict):
    """Error file location."""

    kind: str  # "internal" or "module"
    value: Union[str, Module]


class FileData(TypedDict):
    """Serialized error file location."""

    kind: str
    value: Union[str, ModuleData]


class Location(TypedDict):
    """Error location."""

    symbol: Optional[str]
    file: File
    range: Range


class LocationData(TypedDict):
    """Serialized error location."""

    symbol: Optional[str]
    file: FileData
    range: Range


def location_to_data(value: Location) -> LocationData:
    """Convert error location to data."""
    file: FileData
    if value["file"]["kind"] == "module":
        file = {"kind": "module", "value": module_to_data(value["file"]["value"])}
    else:
        file = value["file"]  # type: ignore
    return {
        "symbol": value.get("symbol"),
        "file": file,
        "range": value["range"],
    }


def location_from_data(data: LocationData) -> Location:
    """Convert data to error location."""
    file: File
    if data["file"]["kind"] == "module":
        file = {"kind": "module", "value": module_from_data(data["file"]["value"])}
    else:
        file = data["file"]  # type: ignore
    return {
        "symbol": data.get("symbol"),
        "file": file,
        "range": data["range"],
    }


class ErrorObject(TypedDict, total=False):
    """Error object data."""

    code: Optional[str]
    diagnostics: Optional[List[Diagnostic]]
    location: Optional[Location]
    message: Optional[str]
    source: Optional[Referent[Union["ErrorObject", "Error_"]]]
    stack: Optional[List[Location]]
    values: Dict[str, str]


class ErrorData(TypedDict, total=False):
    """Serialized error data."""

    code: str
    diagnostics: List[DiagnosticData]
    location: LocationData
    message: str
    source: Any  # Referent.Data
    stack: List[LocationData]
    values: Dict[str, str]


class ErrorArg(TypedDict, total=False):
    """Error constructor arguments."""

    code: Optional[str]
    diagnostics: Optional[List[Diagnostic]]
    location: Optional[Location]
    message: str
    source: Optional[Referent[Union[ErrorObject, "Error_"]]]
    stack: Optional[List[Location]]
    values: Optional[Dict[str, str]]


def error(
    first_arg: Optional[Union[str, ErrorArg]] = None,
    second_arg: Optional[ErrorArg] = None,
) -> "Error_":
    """Create a new error."""
    obj: ErrorObject = {"values": {}}

    if first_arg is not None and isinstance(first_arg, str):
        obj["message"] = first_arg
        if second_arg is not None:
            if "code" in second_arg:
                obj["code"] = second_arg["code"]
            if "diagnostics" in second_arg:
                obj["diagnostics"] = second_arg["diagnostics"]
            if "location" in second_arg:
                obj["location"] = second_arg["location"]
            if "message" in second_arg:
                obj["message"] = second_arg["message"]
            if "source" in second_arg:
                obj["source"] = second_arg["source"]
            if "stack" in second_arg:
                obj["stack"] = second_arg["stack"]
            if "values" in second_arg:
                obj["values"] = second_arg.get("values") or {}
    elif first_arg is not None and isinstance(first_arg, dict):
        if "code" in first_arg:
            obj["code"] = first_arg["code"]
        if "diagnostics" in first_arg:
            obj["diagnostics"] = first_arg["diagnostics"]
        if "location" in first_arg:
            obj["location"] = first_arg["location"]
        if "message" in first_arg:
            obj["message"] = first_arg["message"]
        if "source" in first_arg:
            obj["source"] = first_arg["source"]
        if "stack" in first_arg:
            obj["stack"] = first_arg["stack"]
        if "values" in first_arg:
            obj["values"] = first_arg.get("values") or {}

    return Error_.with_object(obj)


class Error_(Exception):
    """An error is an object with code, message, diagnostics, and location."""

    _state: Object_.State

    def __init__(
        self,
        *,
        id: Optional[str] = None,
        object: Optional[ErrorObject] = None,
        stored: bool = False,
    ) -> None:
        """Initialize an error."""
        # Initialize Exception with the message if available.
        message = object.get("message", "") if object else ""
        super().__init__(message)
        obj = {"kind": "error", "value": object} if object is not None else None
        self._state = Object_.State(id=id, object=obj, stored=stored)

    @property
    def state(self) -> Object_.State:
        """Get the object state."""
        return self._state

    @classmethod
    def with_id(cls, id: str) -> "Error_":
        """Create an error with the given ID."""
        return cls(id=id, stored=True)

    @classmethod
    def with_object(cls, object: ErrorObject) -> "Error_":
        """Create an error with the given object."""
        return cls(object=object, stored=False)

    @classmethod
    def from_data(cls, data: ErrorData) -> "Error_":
        """Create an error from serialized data."""
        return cls.with_object(object_from_data(data))

    @staticmethod
    def to_data(value: "Error_") -> ErrorData:
        """Convert an error to data."""
        from tangram.assert_ import assert_

        obj = value.state.object
        assert_(obj is not None and obj.get("kind") == "error")
        return object_to_data(obj["value"])

    @staticmethod
    def expect(value: Any) -> "Error_":
        """Assert that value is an Error and return it."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Error_))
        return value

    @staticmethod
    def assert_(value: Any) -> None:
        """Assert that value is an Error."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Error_))

    @property
    def id(self) -> str:
        """Get the error ID."""
        from tangram.assert_ import assert_

        id = self._state.id
        assert_(Object_.Id.kind(id) == "error")
        return id

    async def object(self) -> ErrorObject:
        """Get the error object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "error")
        return obj.get("value", obj)

    async def load(self) -> ErrorObject:
        """Load the error object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "error")
        return obj.get("value", obj)

    def unload(self) -> None:
        """Unload the error data."""
        self._state.unload()

    async def store(self) -> str:
        """Store the error and return its ID."""
        import tangram as tg

        await tg.Value.store(self)
        return self.id

    async def children(self) -> List[Object_]:
        """Get child objects."""
        return await self._state.children()

    async def code(self) -> Optional[str]:
        """Get the error code."""
        obj = await self.object()
        return obj.get("code")

    async def diagnostics(self) -> Optional[List[Diagnostic]]:
        """Get the error diagnostics."""
        obj = await self.object()
        return obj.get("diagnostics")

    async def location(self) -> Optional[Location]:
        """Get the error location."""
        obj = await self.object()
        return obj.get("location")

    async def message(self) -> Optional[str]:
        """Get the error message."""
        obj = await self.object()
        return obj.get("message")

    async def source(self) -> Optional[Referent["Error_"]]:
        """Get the error source."""
        obj = await self.object()
        source = obj.get("source")
        if source is None:
            return None
        if isinstance(source.item, Error_):
            return source
        else:
            return Referent(Error_.with_object(source.item), source.options)

    async def stack(self) -> Optional[List[Location]]:
        """Get the error stack."""
        obj = await self.object()
        return obj.get("stack")

    async def values(self) -> Dict[str, str]:
        """Get the error values."""
        obj = await self.object()
        return obj.get("values", {})

    # Nested Object namespace.
    class Object:
        """Namespace for Error.Object operations."""

        @staticmethod
        def to_data(obj: ErrorObject) -> ErrorData:
            """Convert object to data."""
            return object_to_data(obj)

        @staticmethod
        def from_data(data: ErrorData) -> ErrorObject:
            """Convert data to object."""
            return object_from_data(data)

        @staticmethod
        def children(obj: Optional[ErrorObject]) -> List[Object_]:
            """Get child objects from error object."""
            if obj is None:
                return []
            children: List[Object_] = []
            source = obj.get("source")
            if source is not None and isinstance(source.item, Error_):
                children.append(source.item)
            return children


def object_to_data(obj: ErrorObject) -> ErrorData:
    """Convert error object to data."""
    data: ErrorData = {}
    if obj.get("code") is not None:
        data["code"] = obj["code"]
    if obj.get("diagnostics") is not None:
        data["diagnostics"] = [diagnostic_to_data(d) for d in obj["diagnostics"]]
    if obj.get("location") is not None:
        data["location"] = location_to_data(obj["location"])
    if obj.get("message") is not None:
        data["message"] = obj["message"]
    if obj.get("source") is not None:
        source = obj["source"]

        def item_to_data(item: Union[ErrorObject, Error_]) -> Any:
            if isinstance(item, Error_):
                if item.state.stored:
                    return item.id
                else:
                    inner_obj = item.state.object
                    if inner_obj is not None and inner_obj.get("kind") == "error":
                        return object_to_data(inner_obj["value"])
                    return {}
            else:
                return object_to_data(item)

        data["source"] = referent_to_data(source, item_to_data)
    if obj.get("stack") is not None:
        data["stack"] = [location_to_data(loc) for loc in obj["stack"]]
    if obj.get("values") and len(obj["values"]) > 0:
        data["values"] = obj["values"]
    return data


def object_from_data(data: ErrorData) -> ErrorObject:
    """Convert data to error object."""
    obj: ErrorObject = {"values": {}}
    if "code" in data:
        obj["code"] = data["code"]
    if "diagnostics" in data:
        obj["diagnostics"] = [diagnostic_from_data(d) for d in data["diagnostics"]]
    if "location" in data:
        obj["location"] = location_from_data(data["location"])
    if "message" in data:
        obj["message"] = data["message"]
    if "source" in data:

        def item_from_data(item: Any) -> Union[ErrorObject, Error_]:
            if isinstance(item, str):
                return Error_.with_id(item)
            else:
                return object_from_data(item)

        obj["source"] = referent_from_data(data["source"], item_from_data)
    if "stack" in data:
        obj["stack"] = [location_from_data(loc) for loc in data["stack"]]
    if "values" in data:
        obj["values"] = data.get("values") or {}
    return obj


# Type aliases.
Error_.Id = str
Error_.Arg = ErrorArg
Error_.Data = ErrorData
