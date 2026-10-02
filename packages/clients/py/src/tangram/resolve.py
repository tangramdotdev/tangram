"""Resolve deeply nested awaitables while preserving atomic Tangram handles."""

import asyncio
import inspect
from collections.abc import Awaitable, Generator, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import fields, is_dataclass
from typing import Any, Protocol, overload

# Python cannot express TypeScript's recursive mapped and conditional types.
# These aliases describe the outer promise and the resolved result; resolve
# performs the recursive transformation at runtime.
type Unresolved[T] = T | Awaitable[T]
type Resolved[T] = T


class Atomic(Protocol):
    @property
    def __tangram_atomic__(self) -> object: ...


type Scalar = None | bool | int | float | str | bytes | bytearray | memoryview


_pending: ContextVar[dict[int, tuple[Awaitable[Any], asyncio.Future[Any]]] | None] = (
    ContextVar("tangram_resolve_pending", default=None)
)


class Resolve:
    # User-defined value wrappers can opt into the same atomic contract.
    atomic = "__tangram_atomic__"
    Atomic = Atomic


class Deferred[T]:
    """Make a Python coroutine reusable like a JavaScript promise."""

    def __init__(self, awaitable: Awaitable[T]) -> None:
        self.awaitable = awaitable
        self.task: asyncio.Future[T] | None = None

    def __await__(self) -> Generator[Any, None, T]:
        async def wait() -> T:
            if self.task is None:
                self.task = asyncio.ensure_future(self.awaitable)
            return await asyncio.shield(self.task)

        return wait().__await__()


def capture(value: Any, memo: dict[int, tuple[Any, Any]] | None = None) -> Any:
    """Retain reusable coroutine inputs without starting work outside an event loop."""
    memo = {} if memo is None else memo
    identity = id(value)
    if identity in memo:
        return memo[identity][1]
    if isinstance(value, (str, bytes, bytearray, memoryview)) or hasattr(
        value, Resolve.atomic
    ):
        return value
    if inspect.iscoroutine(value):
        result = Deferred(value)
        memo[identity] = (value, result)
        return result
    if inspect.isawaitable(value):
        return value
    if is_dataclass(value) and not isinstance(value, type):
        result = type(value).__new__(type(value))
        memo[identity] = (value, result)
        for field in fields(value):
            object.__setattr__(
                result, field.name, capture(getattr(value, field.name), memo)
            )
        return result
    if isinstance(value, Mapping):
        result = {}
        memo[identity] = (value, result)
        result.update((key, capture(child, memo)) for key, child in value.items())
        return result
    if isinstance(value, Sequence):
        result = []
        memo[identity] = (value, result)
        result.extend(capture(child, memo) for child in value)
        return result
    if not callable(value) and hasattr(value, "__dict__"):
        result = {}
        memo[identity] = (value, result)
        result.update((key, capture(child, memo)) for key, child in vars(value).items())
        return result
    return value


@overload
async def resolve[T: Scalar | Atomic](value: Unresolved[T]) -> T: ...


@overload
async def resolve[T: Scalar | Atomic](
    value: Sequence[Unresolved[T]],
) -> list[T]: ...


@overload
async def resolve[T: Scalar | Atomic](
    value: Mapping[str, Unresolved[T]],
) -> dict[str, T]: ...


@overload
async def resolve(value: object) -> Any: ...


async def resolve(value: object) -> Any:
    """Resolve values deeply; arbitrary record transformations lack static typing.

    The overloads preserve scalar and atomic results and simple homogeneous
    containers. The dynamic fallback is intentional: today's Python cannot map
    an arbitrary record or heterogeneous tuple into its recursively resolved type.
    Resolved[T] is an identity placeholder, not such a transformation.
    """
    pending = _pending.get()
    token = None
    if pending is None:
        pending = {}
        token = _pending.set(pending)

    async def inner(value: Any, ancestors: frozenset[int], path: str) -> Any:
        location = "" if path == "" else f" at {path}"
        while inspect.isawaitable(value):
            identity = id(value)
            if identity in ancestors:
                raise ValueError(f"cycle detected{location}")
            ancestors = ancestors | {identity}
            if identity not in pending:
                pending[identity] = (value, asyncio.ensure_future(value))
            value = await asyncio.shield(pending[identity][1])
        if value is None or isinstance(
            value, (bool, int, float, str, bytes, bytearray, memoryview)
        ):
            return value
        if hasattr(value, Resolve.atomic):
            return value
        identity = id(value)
        if identity in ancestors:
            raise ValueError(f"cycle detected{location}")
        ancestors = ancestors | {identity}
        if is_dataclass(value) and not isinstance(value, type):
            children = await asyncio.gather(
                *(
                    inner(getattr(value, field.name), ancestors, f"{path}.{field.name}")
                    for field in fields(value)
                )
            )
            return type(value)(
                **dict(
                    zip((field.name for field in fields(value)), children, strict=True)
                )
            )
        if isinstance(value, Sequence):
            return await asyncio.gather(
                *(
                    inner(child, ancestors, f"{path}[{index}]")
                    for index, child in enumerate(value)
                )
            )
        if isinstance(value, Mapping):
            children = await asyncio.gather(
                *(
                    inner(child, ancestors, f"{path}.{key}")
                    for key, child in value.items()
                )
            )
            return dict(zip(value, children, strict=True))
        if callable(value):
            raise TypeError("Python function commands require the embedded runtime")
        if hasattr(value, "__dict__"):
            entries = vars(value)
            children = await asyncio.gather(
                *(
                    inner(child, ancestors, f"{path}.{key}")
                    for key, child in entries.items()
                )
            )
            return dict(zip(entries, children, strict=True))
        from .mutation import UNSET

        description = (
            "undefined is not a value, use null instead"
            if value is UNSET
            else type(value).__name__
        )
        raise TypeError(f"invalid value to resolve{location}: {description}")

    try:
        return await inner(value, frozenset(), "")
    finally:
        if token is not None:
            _pending.reset(token)
