"""Checksum type and utilities.

This module mirrors the JavaScript packages/clients/js/src/checksum.ts.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Literal, Union

if TYPE_CHECKING:
    from tangram.blob import Blob

from tangram.artifact import Artifact

# Checksum algorithm types.
Algorithm = Literal["blake3", "sha256", "sha512"]

# Checksum is a string in the format "algorithm:hash" or "algorithm-hash".
Checksum = str


async def checksum(
    input: Union[str, bytes, "Blob", Artifact],
    algorithm: Algorithm,
) -> Checksum:
    """Compute a checksum for the given input."""
    return await new(input, algorithm)


async def new(
    input: Union[str, bytes, "Blob", Artifact],
    algorithm: Algorithm,
) -> Checksum:
    """Create a new checksum."""
    from tangram.blob import Blob
    from tangram.handle import get_handle

    if isinstance(input, (str, bytes)):
        return get_handle().checksum(input, algorithm)
    elif isinstance(input, Blob):
        import tangram as tg

        value = await tg.build(
            {"args": [input, algorithm], "executable": "checksum", "host": "builtin"}
        )
        assert_(value)
        return value
    elif is_artifact(input):
        import tangram as tg

        value = await tg.build(
            {"args": [input, algorithm], "executable": "checksum", "host": "builtin"}
        )
        assert_(value)
        return value
    else:
        from tangram.assert_ import unreachable

        return unreachable()


def algorithm(checksum: Checksum) -> Algorithm:
    """Get the algorithm from a checksum."""
    if ":" in checksum:
        return checksum.split(":")[0]  # type: ignore
    elif "-" in checksum:
        return checksum.split("-")[0]  # type: ignore
    else:
        raise ValueError("invalid checksum")


def is_(value: Any) -> bool:
    """Check if a value is a valid checksum."""
    if not isinstance(value, str):
        return False
    pattern = r"^(blake3|sha256|sha512)([-:])[a-zA-Z0-9+/]+=*$"
    return bool(re.match(pattern, value))


def expect(value: Any) -> Checksum:
    """Assert that a value is a checksum and return it."""
    from tangram.assert_ import assert_ as assert_fn

    assert_fn(is_(value))
    return value


def assert_(value: Any) -> None:
    """Assert that a value is a checksum."""
    from tangram.assert_ import assert_ as assert_fn

    assert_fn(is_(value))


def is_artifact(value: Any) -> bool:
    """Check if value is an Artifact."""
    from tangram.artifact import is_ as artifact_is

    return artifact_is(value)
