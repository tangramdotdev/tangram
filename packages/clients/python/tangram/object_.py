"""Object base class and state management.

This module mirrors the JavaScript packages/clients/js/src/object.ts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, TypeVar, Union

if TYPE_CHECKING:
    pass

T = TypeVar("T", bound="Object_")


class Object_:
    """Union type for all Tangram objects.

    This is the base class and also serves as a namespace for object utilities.
    """

    _state: "Object_.State"

    class State:
        """State management for objects with lazy loading."""

        def __init__(
            self,
            *,
            id: Optional[str] = None,
            object: Optional[Dict[str, Any]] = None,
            stored: bool = False,
        ) -> None:
            """Initialize the object state."""
            self._id = id
            self._object = object
            self._stored = stored

        @property
        def id(self) -> str:
            """Get the object ID, computing it if necessary."""
            if self._id is not None:
                return self._id
            if self._object is None:
                raise RuntimeError("object has no id and no object")
            from tangram.handle import get_handle

            data = Object_.Object_.to_data(self._object)
            self._id = get_handle().object_id(data)
            return self._id

        @id.setter
        def id(self, value: str) -> None:
            """Set the object ID."""
            self._id = value

        @property
        def object(self) -> Optional[Dict[str, Any]]:
            """Get the object."""
            return self._object

        @object.setter
        def object(self, value: Optional[Dict[str, Any]]) -> None:
            """Set the object."""
            self._object = value

        @property
        def stored(self) -> bool:
            """Check if the object is stored."""
            return self._stored

        @stored.setter
        def stored(self, value: bool) -> None:
            """Set the stored flag."""
            self._stored = value

        @property
        def kind(self) -> str:
            """Get the object kind."""
            if self._object is not None:
                return self._object.get("kind", "")
            return Object_.Id.kind(self._id) if self._id else ""

        async def load(self) -> Dict[str, Any]:
            """Load the object data from the handle."""
            if self._object is None:
                if self._id is None:
                    raise RuntimeError("cannot load object without id")
                from tangram.handle import get_handle

                data = await get_handle().get_object(self._id)
                self._object = Object_.Object_.from_data(data)
            return self._object

        def unload(self) -> None:
            """Unload the object data if stored."""
            if self._stored:
                self._object = None

        async def children(self) -> List["Object_"]:
            """Get child objects."""
            await self.load()
            return Object_.Object_.children(self._object)

    class Id:
        """Namespace for Object.Id operations."""

        @staticmethod
        def kind(id: str) -> str:
            """Get the object kind from its ID prefix."""
            if len(id) < 3:
                raise ValueError(f"invalid object id: {id}")
            prefix = id[:3]
            if prefix == "blb":
                return "blob"
            elif prefix == "dir":
                return "directory"
            elif prefix == "fil":
                return "file"
            elif prefix == "sym":
                return "symlink"
            elif prefix == "gph":
                return "graph"
            elif prefix == "cmd":
                return "command"
            elif prefix == "err":
                return "error"
            else:
                raise ValueError(f"invalid object id: {id}")

    class Object_:
        """Namespace for Object.Object operations."""

        @staticmethod
        def to_data(obj: Dict[str, Any]) -> Dict[str, Any]:
            """Convert object to data."""
            kind = obj.get("kind")
            value = obj.get("value")
            if kind == "blob":
                from tangram.blob import Blob

                return {"kind": "blob", "value": Blob.Object.to_data(value)}
            elif kind == "directory":
                from tangram.directory import Directory

                return {"kind": "directory", "value": Directory.Object.to_data(value)}
            elif kind == "file":
                from tangram.file import File

                return {"kind": "file", "value": File.Object.to_data(value)}
            elif kind == "symlink":
                from tangram.symlink import Symlink

                return {"kind": "symlink", "value": Symlink.Object.to_data(value)}
            elif kind == "graph":
                from tangram.graph import Graph

                return {"kind": "graph", "value": Graph.Object.to_data(value)}
            elif kind == "command":
                from tangram.command import Command

                return {"kind": "command", "value": Command.Object.to_data(value)}
            elif kind == "error":
                from tangram.error_ import Error_

                return {"kind": "error", "value": Error_.Object.to_data(value)}
            else:
                return obj

        @staticmethod
        def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
            """Convert data to object."""
            kind = data.get("kind")
            value = data.get("value")
            if kind == "blob":
                from tangram.blob import Blob

                return {"kind": "blob", "value": Blob.Object.from_data(value)}
            elif kind == "directory":
                from tangram.directory import Directory

                return {"kind": "directory", "value": Directory.Object.from_data(value)}
            elif kind == "file":
                from tangram.file import File

                return {"kind": "file", "value": File.Object.from_data(value)}
            elif kind == "symlink":
                from tangram.symlink import Symlink

                return {"kind": "symlink", "value": Symlink.Object.from_data(value)}
            elif kind == "graph":
                from tangram.graph import Graph

                return {"kind": "graph", "value": Graph.Object.from_data(value)}
            elif kind == "command":
                from tangram.command import Command

                return {"kind": "command", "value": Command.Object.from_data(value)}
            elif kind == "error":
                from tangram.error_ import Error_

                return {"kind": "error", "value": Error_.Object.from_data(value)}
            else:
                return data

        @staticmethod
        def children(obj: Optional[Dict[str, Any]]) -> List["Object_"]:
            """Get child objects from an object."""
            if obj is None:
                return []
            kind = obj.get("kind")
            value = obj.get("value")
            if kind == "blob":
                from tangram.blob import Blob

                return Blob.Object.children(value)
            elif kind == "directory":
                from tangram.directory import Directory

                return Directory.Object.children(value)
            elif kind == "file":
                from tangram.file import File

                return File.Object.children(value)
            elif kind == "symlink":
                from tangram.symlink import Symlink

                return Symlink.Object.children(value)
            elif kind == "graph":
                from tangram.graph import Graph

                return Graph.Object.children(value)
            elif kind == "command":
                from tangram.command import Command

                return Command.Object.children(value)
            elif kind == "error":
                from tangram.error_ import Error_

                return Error_.Object.children(value)
            else:
                return []

    @staticmethod
    def with_id(id: str) -> "Object_":
        """Create an object from its ID."""
        prefix = id[:3] if len(id) >= 3 else ""
        if prefix == "blb":
            from tangram.blob import Blob

            return Blob.with_id(id)
        elif prefix == "dir":
            from tangram.directory import Directory

            return Directory.with_id(id)
        elif prefix == "fil":
            from tangram.file import File

            return File.with_id(id)
        elif prefix == "sym":
            from tangram.symlink import Symlink

            return Symlink.with_id(id)
        elif prefix == "gph":
            from tangram.graph import Graph

            return Graph.with_id(id)
        elif prefix == "cmd":
            from tangram.command import Command

            return Command.with_id(id)
        elif prefix == "err":
            from tangram.error_ import Error_

            return Error_.with_id(id)
        else:
            raise ValueError(f"invalid object id: {id}")

    @staticmethod
    def is_(value: Any) -> bool:
        """Check if value is an Object."""
        from tangram.blob import Blob
        from tangram.command import Command
        from tangram.directory import Directory
        from tangram.error_ import Error_
        from tangram.file import File
        from tangram.graph import Graph
        from tangram.symlink import Symlink

        return isinstance(value, (Blob, Directory, File, Symlink, Graph, Command, Error_))

    @staticmethod
    def expect(value: Any) -> "Object_":
        """Assert value is an Object and return it."""
        from tangram.assert_ import assert_

        assert_(Object_.is_(value))
        return value

    @staticmethod
    def assert_(value: Any) -> None:
        """Assert value is an Object."""
        from tangram.assert_ import assert_

        assert_(Object_.is_(value))

    @staticmethod
    def kind(obj: "Object_") -> str:
        """Get the kind of an object."""
        from tangram.blob import Blob
        from tangram.command import Command
        from tangram.directory import Directory
        from tangram.error_ import Error_
        from tangram.file import File
        from tangram.graph import Graph
        from tangram.symlink import Symlink

        if isinstance(obj, Blob):
            return "blob"
        elif isinstance(obj, Directory):
            return "directory"
        elif isinstance(obj, File):
            return "file"
        elif isinstance(obj, Symlink):
            return "symlink"
        elif isinstance(obj, Graph):
            return "graph"
        elif isinstance(obj, Command):
            return "command"
        elif isinstance(obj, Error_):
            return "error"
        else:
            from tangram.assert_ import unreachable

            return unreachable()


# Backward compatibility aliases.
ObjectState = Object_.State


def id_kind(id: str) -> Optional[str]:
    """Extract the object kind from an ID. Deprecated - use Object_.Id.kind()."""
    try:
        return Object_.Id.kind(id)
    except ValueError:
        return None
