"""File class for Tangram.

This module mirrors the JavaScript packages/clients/js/src/file.ts.
"""

from __future__ import annotations

import base64
from typing import Any, Dict, List, Optional, Union

from tangram.object_ import Object_


async def file(*args: Any) -> "File":
    """Create a new file."""
    return await File.new(*args)


class File:
    """A file is an artifact with contents, dependencies, and executable flag."""

    _state: Object_.State

    def __init__(
        self,
        *,
        id: Optional[str] = None,
        object: Optional[Dict[str, Any]] = None,
        stored: bool = False,
    ) -> None:
        """Initialize a file."""
        obj = {"kind": "file", "value": object} if object is not None else None
        self._state = Object_.State(id=id, object=obj, stored=stored)

    @property
    def state(self) -> Object_.State:
        """Get the object state."""
        return self._state

    @classmethod
    def with_id(cls, id: str) -> "File":
        """Create a file with the given ID."""
        return cls(id=id, stored=True)

    @classmethod
    def with_object(cls, object: Dict[str, Any]) -> "File":
        """Create a file with the given object."""
        return cls(object=object, stored=False)

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "File":
        """Create a file from serialized data."""
        return cls.with_object(File.Object.from_data(data))

    @classmethod
    async def new(cls, *args: Any) -> "File":
        """Create a new file from arguments."""
        import tangram as tg

        arg = await cls.arg(*args)
        contents = await tg.blob(arg.get("contents"))
        dependencies = {}
        for reference, value in (arg.get("dependencies") or {}).items():
            dependency: Optional[Dict[str, Any]] = None
            if value is None:
                dependency = None
            elif isinstance(value, int) or (isinstance(value, dict) and "index" in value) or Object_.is_(value):
                item = value
                dependency = {"item": item, "options": {}}
            elif isinstance(value, dict):
                item = value.get("item")
                dependency = {"item": item, "options": value.get("options", {})}
            dependencies[reference] = dependency
        executable = arg.get("executable", False)
        object = {"contents": contents, "dependencies": dependencies, "executable": executable}
        return cls.with_object(object)

    @classmethod
    async def arg(cls, *args: Any) -> Dict[str, Any]:
        """Process file arguments using Args.apply."""
        import tangram as tg

        async def map_fn(arg: Any) -> Dict[str, Any]:
            if arg is None:
                return {}
            elif isinstance(arg, (str, bytes)):
                return {"contents": arg}
            elif isinstance(arg, tg.Blob):
                return {"contents": arg}
            elif isinstance(arg, File):
                return {
                    "contents": await arg.contents(),
                    "dependencies": await arg.dependencies(),
                }
            elif isinstance(arg, dict):
                return arg
            else:
                return {}

        async def contents_reducer(a: Any, b: Any) -> Any:
            return await tg.blob(a, b)

        return await tg.Args.apply(
            args=list(args),
            map_fn=map_fn,
            reduce_spec={
                "contents": contents_reducer,
                "dependencies": "merge",
            },
        )

    @staticmethod
    def expect(value: Any) -> "File":
        """Assert that value is a File and return it."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, File))
        return value

    @staticmethod
    def assert_(value: Any) -> None:
        """Assert that value is a File."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, File))

    @property
    def id(self) -> str:
        """Get the file ID."""
        from tangram.assert_ import assert_

        id = self._state.id
        assert_(Object_.Id.kind(id) == "file")
        return id

    async def object(self) -> Dict[str, Any]:
        """Get the file object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "file")
        return obj.get("value", obj)

    async def load(self) -> Dict[str, Any]:
        """Load the file object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "file")
        return obj.get("value", obj)

    def unload(self) -> None:
        """Unload the file data."""
        self._state.unload()

    async def store(self) -> str:
        """Store the file and return its ID."""
        import tangram as tg

        await tg.Value.store(self)
        return self.id

    async def children(self) -> List[Object_]:
        """Get child objects."""
        return await self._state.children()

    async def contents(self) -> "tg.Blob":
        """Get the file contents as a blob."""
        import tangram as tg

        obj = await self.object()
        contents = obj.get("contents")
        if contents is None:
            return await tg.blob(b"")
        if isinstance(contents, tg.Blob):
            return contents
        return tg.Blob.with_id(contents)

    async def dependencies(self) -> Dict[str, Any]:
        """Get the file dependencies."""
        obj = await self.object()
        dependencies = obj.get("dependencies", {})
        result = {}
        for reference, dependency in dependencies.items():
            if dependency is None:
                result[reference] = None
            else:
                item = dependency.get("item")
                options = dependency.get("options", {})
                obj_item: Optional[Object_] = None
                if item is None:
                    obj_item = None
                elif isinstance(item, Object_):
                    obj_item = item
                elif isinstance(item, str):
                    obj_item = Object_.with_id(item)
                result[reference] = {"item": obj_item, "options": options}
        return result

    async def dependency_objects(self) -> List[Object_]:
        """Get all dependency objects."""
        dependencies = await self.dependencies()
        items = []
        for dependency in dependencies.values():
            if dependency is not None and dependency.get("item") is not None:
                items.append(dependency["item"])
        return items

    async def executable(self) -> bool:
        """Check if the file is executable."""
        obj = await self.object()
        return obj.get("executable", False)

    async def length(self) -> int:
        """Get the length of the file contents."""
        contents = await self.contents()
        return await contents.length()

    async def read(
        self,
        position: Optional[int] = None,
        length: Optional[int] = None,
        size: Optional[int] = None,
    ) -> bytes:
        """Read file contents."""
        contents = await self.contents()
        return await contents.read(position=position, length=length, size=size)

    async def bytes(self) -> bytes:
        """Get the file contents as bytes."""
        contents = await self.contents()
        return await contents.bytes()

    async def text(self) -> str:
        """Get the file contents as text."""
        contents = await self.contents()
        return await contents.text()

    # Nested Object namespace.
    class Object:
        """Namespace for File.Object operations."""

        @staticmethod
        def to_data(obj: Dict[str, Any]) -> Dict[str, Any]:
            """Convert object to data."""
            from tangram.blob import Blob

            data: Dict[str, Any] = {}
            contents = obj.get("contents")
            if contents is not None:
                if isinstance(contents, Blob):
                    data["contents"] = contents.id
                else:
                    data["contents"] = contents
            dependencies = obj.get("dependencies", {})
            if dependencies:
                data["dependencies"] = {}
                for reference, dependency in dependencies.items():
                    if dependency is None:
                        data["dependencies"][reference] = None
                    else:
                        item = dependency.get("item")
                        if item is None:
                            item_data = None
                        elif hasattr(item, "id"):
                            item_data = item.id
                        else:
                            item_data = item
                        data["dependencies"][reference] = {
                            "item": item_data,
                            "options": dependency.get("options", {}),
                        }
            executable = obj.get("executable", False)
            if executable:
                data["executable"] = executable
            return data

        @staticmethod
        def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
            """Convert data to object."""
            from tangram.blob import Blob

            obj: Dict[str, Any] = {}
            contents = data.get("contents")
            if contents is not None:
                obj["contents"] = Blob.with_id(contents)
            dependencies_data = data.get("dependencies", {})
            dependencies = {}
            for reference, dependency_data in dependencies_data.items():
                if dependency_data is None:
                    dependencies[reference] = None
                else:
                    item_data = dependency_data.get("item")
                    if item_data is None:
                        item = None
                    elif isinstance(item_data, str):
                        item = Object_.with_id(item_data)
                    else:
                        item = item_data
                    dependencies[reference] = {
                        "item": item,
                        "options": dependency_data.get("options", {}),
                    }
            obj["dependencies"] = dependencies
            obj["executable"] = data.get("executable", False)
            return obj

        @staticmethod
        def children(obj: Optional[Dict[str, Any]]) -> List[Object_]:
            """Get child objects from file object."""
            from tangram.blob import Blob

            if obj is None:
                return []
            result: List[Object_] = []
            contents = obj.get("contents")
            if contents is not None:
                if isinstance(contents, Blob):
                    result.append(contents)
            dependencies = obj.get("dependencies", {})
            for dependency in dependencies.values():
                if dependency is not None:
                    item = dependency.get("item")
                    if item is not None and isinstance(item, Object_):
                        result.append(item)
            return result


# Type aliases.
File.Id = str
File.Arg = Union[None, str, bytes, "tg.Blob", "File", Dict[str, Any]]
File.Data = Dict[str, Any]
