"""Artifact type and utilities.

This module mirrors the JavaScript packages/clients/js/src/artifact.ts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Union

if TYPE_CHECKING:
    from tangram.directory import Directory
    from tangram.file import File
    from tangram.symlink import Symlink

# Artifact is a union of Directory, File, and Symlink.
Artifact = Union["Directory", "File", "Symlink"]

# Type aliases.
Id = str
Kind = Literal["directory", "file", "symlink"]


def with_id(id: Id) -> Artifact:
    """Create an artifact from its ID."""
    from tangram.assert_ import assert_
    from tangram.directory import Directory
    from tangram.file import File
    from tangram.symlink import Symlink

    assert_(isinstance(id, str), f"expected a string: {id}")
    prefix = id[:3] if len(id) >= 3 else ""
    if prefix == "dir":
        return Directory.with_id(id)
    elif prefix == "fil":
        return File.with_id(id)
    elif prefix == "sym":
        return Symlink.with_id(id)
    else:
        raise ValueError(f"invalid artifact id: {id}")


def is_(value: Any) -> bool:
    """Check if a value is an Artifact."""
    from tangram.directory import Directory
    from tangram.file import File
    from tangram.symlink import Symlink

    return isinstance(value, (Directory, File, Symlink))


def expect(value: Any) -> Artifact:
    """Assert that a value is an Artifact and return it."""
    from tangram.assert_ import assert_

    assert_(is_(value))
    return value


def assert_(value: Any) -> None:
    """Assert that a value is an Artifact."""
    from tangram.assert_ import assert_ as assert_fn

    assert_fn(is_(value))
