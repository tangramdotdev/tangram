"""An artifact and the helpers from the JavaScript Artifact namespace."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypeGuard

if TYPE_CHECKING:
    from .graph import Pointer
    from .referent import Referent

from .directory import Directory
from .file import File
from .symlink import Symlink


class ArtifactId:
    @staticmethod
    def is_(value):
        if not isinstance(value, str):
            return False
        prefix = value[:3]
        return prefix == "dir" or prefix == "fil" or prefix == "sym"


class ArtifactData:
    @staticmethod
    def is_(value):
        # The JS graph data pointer predicate accepts strings or numeric indices.
        if isinstance(value, str) or (
            isinstance(value, dict) and type(value.get("index")) in (int, float)
        ):
            return True
        if not isinstance(value, dict):
            return False
        if "children" in value:
            return isinstance(value["children"], list)
        if "entries" in value:
            return isinstance(value["entries"], dict)
        if "contents" in value:
            return value["contents"] is None or isinstance(value["contents"], str)
        if "dependencies" in value:
            return value["dependencies"] is None or isinstance(
                value["dependencies"], dict
            )
        if "artifact" in value:
            artifact = value["artifact"]
            return (
                artifact is None
                or isinstance(artifact, str)
                or type(artifact) in (int, float)
                or isinstance(artifact, dict)
                and type(artifact.get("index")) in (int, float)
            )
        if "path" in value:
            return value["path"] is None or isinstance(value["path"], str)
        if "executable" in value:
            return isinstance(value["executable"], bool)
        if "module" in value:
            return value["module"] is None or isinstance(value["module"], str)
        return len(value) == 0


class _ArtifactType(type):
    def __instancecheck__(cls, value):
        return Artifact.is_(value)


class Artifact(metaclass=_ArtifactType):
    Id = ArtifactId
    Kind = Literal["directory", "file", "symlink"]
    Data = ArtifactData

    @staticmethod
    def with_referent(referent: Referent) -> Directory | File | Symlink:
        artifact = Artifact.with_id(referent.node)
        artifact.state.location = (referent.options or {}).get("location")
        artifact.state.tokens = (referent.options or {}).get("tokens") or {}
        return artifact

    @staticmethod
    def with_id(id: str) -> Directory | File | Symlink:
        if not isinstance(id, str):
            raise TypeError(f"expected a string: {id}")
        prefix = id[:3]
        if prefix == "dir":
            return Directory(id=id)
        elif prefix == "fil":
            return File(id=id)
        elif prefix == "sym":
            return Symlink(id=id)
        else:
            raise ValueError(f"invalid artifact id: {id}")

    @staticmethod
    def with_pointer(pointer: Pointer) -> Directory | File | Symlink:
        if pointer.kind == "directory":
            return Directory.with_pointer(pointer)
        elif pointer.kind == "file":
            return File.with_pointer(pointer)
        elif pointer.kind == "symlink":
            return Symlink.with_pointer(pointer)
        else:
            raise ValueError("invalid artifact kind")

    @staticmethod
    def is_(value: object) -> TypeGuard[Directory | File | Symlink]:
        return isinstance(value, (Directory, File, Symlink))

    @staticmethod
    def expect(value: object) -> Directory | File | Symlink:
        if not Artifact.is_(value):
            raise TypeError("expected an artifact")
        return value

    @staticmethod
    def assert_(value: object) -> None:
        if not Artifact.is_(value):
            raise TypeError("expected an artifact")
