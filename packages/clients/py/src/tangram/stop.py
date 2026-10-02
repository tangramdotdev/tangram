"""A reusable completion signal."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Generator
from typing import Any


class Stop:
    def __init__(self):
        self._resolve: Callable[[], None] | None = None
        self._promise = _Promise()
        self._resolve = self._promise.resolve

    @property
    def promise(self) -> _Promise:
        return self._promise

    def stop(self) -> None:
        if self._resolve is not None:
            self._resolve()
        self._resolve = None


class _Promise:
    def __init__(self):
        self._event = asyncio.Event()

    def resolve(self) -> None:
        self._event.set()

    def __await__(self) -> Generator[Any, None, None]:
        async def wait():
            await self._event.wait()

        return wait().__await__()
