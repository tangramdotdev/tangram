"""Assertion utilities matching JS assert.ts."""

from typing import Any, NoReturn, Optional


def assert_(condition: Any, message: Optional[str] = None) -> None:
    """Assert a condition is true.

    Args:
        condition: The condition to check.
        message: Optional message to include in the error.

    Raises:
        AssertionError: If the condition is false.
    """
    if not condition:
        raise AssertionError(message or "failed assertion")


def unimplemented(message: Optional[str] = None) -> NoReturn:
    """Mark code as unimplemented.

    Args:
        message: Optional message to include in the error.

    Raises:
        AssertionError: Always.
    """
    raise AssertionError(message or "reached unimplemented code")


def unreachable(message: Optional[str] = None) -> NoReturn:
    """Mark code path as unreachable.

    Args:
        message: Optional message to include in the error.

    Raises:
        AssertionError: Always.
    """
    raise AssertionError(message or "reached unreachable code")


def todo() -> NoReturn:
    """Mark code as todo.

    Raises:
        AssertionError: Always.
    """
    raise AssertionError("reached todo")
