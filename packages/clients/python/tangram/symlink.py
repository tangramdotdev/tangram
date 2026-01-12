"""Symlink class for Tangram.

This module mirrors the JavaScript packages/clients/js/src/symlink.ts.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from tangram.object_ import Object_


async def symlink(arg: Any) -> "Symlink":
    """Create a new symlink."""
    return await Symlink.new(arg)


class Symlink:
    """A symlink is an artifact that points to another location."""

    _state: Object_.State

    def __init__(
        self,
        *,
        id: Optional[str] = None,
        object: Optional[Dict[str, Any]] = None,
        stored: bool = False,
    ) -> None:
        """Initialize a symlink."""
        obj = {"kind": "symlink", "value": object} if object is not None else None
        self._state = Object_.State(id=id, object=obj, stored=stored)

    @property
    def state(self) -> Object_.State:
        """Get the object state."""
        return self._state

    @classmethod
    def with_id(cls, id: str) -> "Symlink":
        """Create a symlink with the given ID."""
        return cls(id=id, stored=True)

    @classmethod
    def with_object(cls, object: Dict[str, Any]) -> "Symlink":
        """Create a symlink with the given object."""
        return cls(object=object, stored=False)

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "Symlink":
        """Create a symlink from serialized data."""
        return cls.with_object(Symlink.Object.from_data(data))

    @classmethod
    async def new(cls, arg: Any) -> "Symlink":
        """Create a new symlink from an argument."""
        resolved = await cls.arg(arg)
        return cls.with_object({
            "artifact": resolved.get("artifact"),
            "path": resolved.get("path"),
        })

    @classmethod
    async def arg(cls, arg: Any) -> Dict[str, Any]:
        """Process symlink argument."""
        import tangram as tg

        resolved = await tg.resolve(arg)

        if isinstance(resolved, str):
            return {"path": resolved}
        elif _is_artifact(resolved):
            return {"artifact": resolved}
        elif isinstance(resolved, tg.Template):
            components = resolved.components
            if len(components) > 2:
                raise RuntimeError("invalid template for symlink")
            first_component = components[0] if components else None
            second_component = components[1] if len(components) > 1 else None

            if isinstance(first_component, str) and second_component is None:
                return {"path": first_component}
            elif _is_artifact(first_component) and second_component is None:
                return {"artifact": first_component, "path": None}
            elif _is_artifact(first_component) and isinstance(second_component, str):
                if not second_component.startswith("/"):
                    raise RuntimeError("path must start with /")
                return {"artifact": first_component, "path": second_component[1:]}
            else:
                raise RuntimeError("invalid template")
        elif isinstance(resolved, Symlink):
            artifact = await resolved.artifact()
            path = await resolved.path()
            return {"artifact": artifact, "path": path}
        elif isinstance(resolved, dict):
            return resolved
        else:
            raise RuntimeError(f"unsupported symlink argument type: {type(resolved)}")

    @staticmethod
    def expect(value: Any) -> "Symlink":
        """Assert that value is a Symlink and return it."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Symlink))
        return value

    @staticmethod
    def assert_(value: Any) -> None:
        """Assert that value is a Symlink."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Symlink))

    @property
    def id(self) -> str:
        """Get the symlink ID."""
        from tangram.assert_ import assert_

        id = self._state.id
        assert_(Object_.Id.kind(id) == "symlink")
        return id

    async def object(self) -> Dict[str, Any]:
        """Get the symlink object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "symlink")
        return obj.get("value", obj)

    async def load(self) -> Dict[str, Any]:
        """Load the symlink object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "symlink")
        return obj.get("value", obj)

    def unload(self) -> None:
        """Unload the symlink data."""
        self._state.unload()

    async def store(self) -> str:
        """Store the symlink and return its ID."""
        import tangram as tg

        await tg.Value.store(self)
        return self.id

    async def children(self) -> List[Object_]:
        """Get child objects."""
        return await self._state.children()

    async def artifact(self) -> Optional[Object_]:
        """Get the artifact this symlink points to."""
        obj = await self.object()
        artifact = obj.get("artifact")
        if artifact is None:
            return None
        if isinstance(artifact, Object_):
            return artifact
        if isinstance(artifact, str):
            return Object_.with_id(artifact)
        return None

    async def path(self) -> Optional[str]:
        """Get the path component of this symlink."""
        obj = await self.object()
        return obj.get("path")

    async def resolve(self) -> Optional[Object_]:
        """Resolve the symlink to its target artifact.

        Recursively resolves symlinks and follows paths into directories.
        """
        from tangram.directory import Directory

        artifact = await self.artifact()

        if isinstance(artifact, Symlink):
            artifact = await artifact.resolve()

        path = await self.path()

        if artifact is None and path is not None:
            raise RuntimeError("cannot resolve a symlink with no artifact")
        elif artifact is not None and path is None:
            return artifact
        elif isinstance(artifact, Directory) and path is not None:
            return await artifact.try_get(path)
        else:
            raise RuntimeError("invalid symlink")

    # Nested Object namespace.
    class Object:
        """Namespace for Symlink.Object operations."""

        @staticmethod
        def to_data(obj: Dict[str, Any]) -> Dict[str, Any]:
            """Convert object to data."""
            data: Dict[str, Any] = {}
            artifact = obj.get("artifact")
            if artifact is not None:
                if hasattr(artifact, "id"):
                    data["artifact"] = artifact.id
                else:
                    data["artifact"] = artifact
            path = obj.get("path")
            if path is not None:
                data["path"] = path
            return data

        @staticmethod
        def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
            """Convert data to object."""
            obj: Dict[str, Any] = {}
            artifact_data = data.get("artifact")
            if artifact_data is not None:
                if isinstance(artifact_data, str):
                    obj["artifact"] = _artifact_with_id(artifact_data)
                else:
                    obj["artifact"] = artifact_data
            else:
                obj["artifact"] = None
            obj["path"] = data.get("path")
            return obj

        @staticmethod
        def children(obj: Optional[Dict[str, Any]]) -> List[Object_]:
            """Get child objects from symlink object."""
            if obj is None:
                return []
            artifact = obj.get("artifact")
            if artifact is not None and isinstance(artifact, Object_):
                return [artifact]
            return []


def _is_artifact(value: Any) -> bool:
    """Check if value is an Artifact."""
    from tangram.directory import Directory
    from tangram.file import File

    return isinstance(value, (Directory, File, Symlink))


def _artifact_with_id(id: str) -> Object_:
    """Create an artifact from its ID."""
    from tangram.directory import Directory
    from tangram.file import File

    prefix = id[:3] if len(id) >= 3 else ""
    if prefix == "dir":
        return Directory.with_id(id)
    elif prefix == "fil":
        return File.with_id(id)
    elif prefix == "sym":
        return Symlink.with_id(id)
    else:
        raise ValueError(f"invalid artifact id: {id}")


# Type aliases.
Symlink.Id = str
Symlink.Arg = Union[str, Object_, "tg.Template", "Symlink", Dict[str, Any]]
Symlink.Data = Dict[str, Any]
