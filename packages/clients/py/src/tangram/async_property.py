"""Awaitable getters that can also accept an explicit client."""

from collections.abc import Awaitable, Callable, Generator
from functools import partial
from typing import Any, Concatenate, Self, cast, overload


class BoundAsyncProperty[**P, T]:
    def __init__(self, method: Callable[P, Awaitable[T]]) -> None:
        self.method = method

    def __await__(self) -> Generator[Any, None, T]:
        # Async getters must be callable without explicit getter arguments.
        method = cast(Callable[[], Awaitable[T]], self.method)
        return method().__await__()

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[T]:
        return self.method(*args, **kwargs)


class async_property[S, **P, T]:
    def __init__(self, method: Callable[Concatenate[S, P], Awaitable[T]]) -> None:
        self.method = method
        self.__doc__ = method.__doc__

    @overload
    def __get__(self, instance: None, owner: type[S] | None = None) -> Self: ...

    @overload
    def __get__(
        self, instance: S, owner: type[S] | None = None
    ) -> BoundAsyncProperty[P, T]: ...

    def __get__(self, instance: S | None, owner: type[S] | None = None):
        if instance is None:
            return self
        method: Callable[P, Awaitable[T]] = partial(self.method, instance)
        return BoundAsyncProperty(method)
