"""Directory class for Tangram.

This module mirrors the JavaScript packages/clients/js/src/directory.ts.
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from tangram.object_ import Object_

if TYPE_CHECKING:
    from tangram.file import File
    from tangram.symlink import Symlink

Artifact = Union["Directory", "File", "Symlink"]


async def directory(*args: Any) -> "Directory":
    """Create a new directory."""
    return await Directory.new(*args)


class Directory:
    """A directory is an artifact containing a map of names to artifacts."""

    _state: Object_.State

    def __init__(
        self,
        *,
        id: Optional[str] = None,
        object: Optional[Dict[str, Any]] = None,
        stored: bool = False,
    ) -> None:
        """Initialize a directory."""
        obj = {"kind": "directory", "value": object} if object is not None else None
        self._state = Object_.State(id=id, object=obj, stored=stored)

    @property
    def state(self) -> Object_.State:
        """Get the object state."""
        return self._state

    @classmethod
    def with_id(cls, id: str) -> "Directory":
        """Create a directory with the given ID."""
        return cls(id=id, stored=True)

    @classmethod
    def with_object(cls, object: Dict[str, Any]) -> "Directory":
        """Create a directory with the given object."""
        return cls(object=object, stored=False)

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "Directory":
        """Create a directory from serialized data."""
        return cls.with_object(Directory.Object.from_data(data))

    @classmethod
    async def new(cls, *args: Any) -> "Directory":
        """Create a new directory from arguments."""
        import tangram as tg

        resolved = [await tg.resolve(arg) for arg in args]
        entries: Dict[str, Artifact] = {}

        for arg in resolved:
            if arg is None:
                continue
            elif isinstance(arg, Directory):
                for name, entry in (await arg.entries()).items():
                    existing_entry = entries.get(name)
                    if isinstance(existing_entry, Directory) and isinstance(entry, Directory):
                        entry = await Directory.new(existing_entry, entry)
                    entries[name] = entry
            elif isinstance(arg, dict):
                for key, value in arg.items():
                    components = _path_components(key)
                    if not components:
                        raise RuntimeError("the path must have at least one component")
                    first_component = components[0]
                    trailing_components = components[1:]

                    if first_component in (".", "..") or first_component.startswith("/"):
                        raise RuntimeError("all path components must be normal")
                    name = first_component

                    existing_entry = entries.get(name)
                    if not isinstance(existing_entry, Directory):
                        existing_entry = None

                    if trailing_components:
                        trailing_path = "/".join(trailing_components)
                        new_entry = await Directory.new(existing_entry, {trailing_path: value})
                        entries[name] = new_entry
                    else:
                        if value is None:
                            entries.pop(name, None)
                        elif isinstance(value, (str, bytes)):
                            new_entry = await tg.file(value)
                            entries[name] = new_entry
                        elif isinstance(value, tg.Blob):
                            new_entry = await tg.file(value)
                            entries[name] = new_entry
                        elif isinstance(value, (tg.File, tg.Symlink)):
                            entries[name] = value
                        elif isinstance(value, Directory):
                            if existing_entry is not None:
                                value = await Directory.new(existing_entry, value)
                            entries[name] = value
                        elif isinstance(value, dict):
                            entries[name] = await Directory.new(existing_entry, value)
                        else:
                            raise RuntimeError(f"unsupported value type: {type(value)}")

        return cls.with_object({"entries": entries})

    @staticmethod
    def expect(value: Any) -> "Directory":
        """Assert that value is a Directory and return it."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Directory))
        return value

    @staticmethod
    def assert_(value: Any) -> None:
        """Assert that value is a Directory."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Directory))

    @property
    def id(self) -> str:
        """Get the directory ID."""
        from tangram.assert_ import assert_

        id = self._state.id
        assert_(Object_.Id.kind(id) == "directory")
        return id

    async def object(self) -> Dict[str, Any]:
        """Get the directory object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "directory")
        return obj.get("value", obj)

    async def load(self) -> Dict[str, Any]:
        """Load the directory object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "directory")
        return obj.get("value", obj)

    def unload(self) -> None:
        """Unload the directory data."""
        self._state.unload()

    async def store(self) -> str:
        """Store the directory and return its ID."""
        import tangram as tg

        await tg.Value.store(self)
        return self.id

    async def children(self) -> List[Object_]:
        """Get child objects."""
        return await self._state.children()

    async def get(self, path: str) -> Artifact:
        """Get an artifact at the given path."""
        from tangram.assert_ import assert_

        artifact = await self.try_get(path)
        assert_(artifact is not None, f'Failed to get the directory entry "{path}".')
        return artifact

    async def try_get(self, path: str) -> Optional[Artifact]:
        """Get an artifact at the given path, returning None if not found."""
        import tangram as tg

        components = _path_components(path)
        artifact: Artifact = self
        parents: List[Directory] = []

        while True:
            if not components:
                break
            component = components.pop(0)
            if component == "/":
                raise RuntimeError("invalid path")
            elif component == ".":
                continue
            elif component == "..":
                if not parents:
                    raise RuntimeError("path is external")
                artifact = parents.pop()
                continue

            if not isinstance(artifact, Directory):
                return None

            entries = await artifact.entries()
            entry = entries.get(component)
            if entry is None:
                return None

            parents.append(artifact)
            artifact = entry

            if isinstance(entry, tg.Symlink):
                artifact_ = await entry.artifact()
                path_ = await entry.path()
                if artifact_ is None and path_ is not None:
                    if not parents:
                        raise RuntimeError("path is external")
                    artifact = parents.pop()
                    components = _path_components(path_) + components
                elif artifact_ is not None and path_ is None:
                    return artifact_
                elif isinstance(artifact_, Directory) and path_ is not None:
                    return await artifact_.try_get(path_)
                else:
                    raise RuntimeError("invalid symlink")

        return artifact

    async def entries(self) -> Dict[str, Artifact]:
        """Get all directory entries."""
        import tangram as tg

        result: Dict[str, Artifact] = {}
        async for name, artifact in self:
            result[name] = artifact
        return result

    async def walk(self) -> AsyncIterator[Tuple[str, Artifact]]:
        """Recursively walk directory yielding (path, artifact) tuples."""
        async for name, artifact in self:
            yield name, artifact
            if isinstance(artifact, Directory):
                async for subpath, subartifact in artifact.walk():
                    yield f"{name}/{subpath}", subartifact

    async def __aiter__(self) -> AsyncIterator[Tuple[str, Artifact]]:
        """Iterate over directory entries asynchronously."""
        import tangram as tg
        from tangram.assert_ import assert_

        obj = await self.object()
        entries_data = obj.get("entries", {})
        for name, entry in entries_data.items():
            if isinstance(entry, Object_):
                yield name, entry
            elif isinstance(entry, str):
                artifact = _artifact_with_id(entry)
                yield name, artifact
            elif isinstance(entry, dict) and "index" in entry:
                graph = entry.get("graph")
                assert_(graph is not None, "missing graph")
                artifact = await graph.get(entry["index"])
                yield name, artifact
            else:
                yield name, entry


def _path_components(path: str) -> List[str]:
    """Split a path into components."""
    if not path:
        return []
    components = []
    if path.startswith("/"):
        components.append("/")
        path = path[1:]
    parts = path.split("/")
    for part in parts:
        if part and part != ".":
            components.append(part)
    return components


def _artifact_with_id(id: str) -> Artifact:
    """Create an artifact from its ID."""
    import tangram as tg

    prefix = id[:3] if len(id) >= 3 else ""
    if prefix == "dir":
        return Directory.with_id(id)
    elif prefix == "fil":
        return tg.File.with_id(id)
    elif prefix == "sym":
        return tg.Symlink.with_id(id)
    else:
        raise ValueError(f"invalid artifact id: {id}")


# Nested Object namespace.
class _DirectoryObject:
    """Namespace for Directory.Object operations."""

    @staticmethod
    def to_data(obj: Dict[str, Any]) -> Dict[str, Any]:
        """Convert object to data."""
        data: Dict[str, Any] = {}
        entries = obj.get("entries", {})
        if entries:
            data["entries"] = {}
            for name, artifact in entries.items():
                if hasattr(artifact, "id"):
                    data["entries"][name] = artifact.id
                else:
                    data["entries"][name] = artifact
        return data

    @staticmethod
    def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert data to object."""
        obj: Dict[str, Any] = {}
        entries_data = data.get("entries", {})
        entries = {}
        for name, entry_data in entries_data.items():
            if isinstance(entry_data, str):
                entries[name] = _artifact_with_id(entry_data)
            else:
                entries[name] = entry_data
        obj["entries"] = entries
        return obj

    @staticmethod
    def children(obj: Optional[Dict[str, Any]]) -> List[Object_]:
        """Get child objects from directory object."""
        if obj is None:
            return []
        result: List[Object_] = []
        entries = obj.get("entries", {})
        for artifact in entries.values():
            if isinstance(artifact, Object_):
                result.append(artifact)
        return result


Directory.Object = _DirectoryObject

# Type aliases.
Directory.Id = str
Directory.Arg = Union[None, "Directory", Dict[str, Any]]
Directory.Data = Dict[str, Any]
