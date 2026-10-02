"""The assertion helpers from assert.ts; ``assert`` is a Python keyword."""

from typing import Never


def assert_(condition: object, message: str | None = None) -> None:
    """Assert that a condition is truthy, with an optional error message."""
    if not condition:
        raise AssertionError(message if message is not None else "failed assertion")


def unimplemented(message: str | None = None) -> Never:
    """Raise an error indicating that unimplemented code has been reached."""
    raise RuntimeError(message if message is not None else "reached unimplemented code")


def unreachable(message: str | None = None) -> Never:
    """Raise an error indicating that unreachable code has been reached."""
    raise RuntimeError(message if message is not None else "reached unreachable code")


def todo() -> Never:
    raise RuntimeError("reached todo")
